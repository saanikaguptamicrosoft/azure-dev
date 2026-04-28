// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package service

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"azure.ai.customtraining/pkg/client"
)

// StreamService handles polling and streaming log output from a running job.
type StreamService struct {
	client *client.Client
}

// NewStreamService creates a new stream service.
func NewStreamService(apiClient *client.Client) *StreamService {
	return &StreamService{client: apiClient}
}

// Log file patterns matching the Azure ML SDK behavior.
// Primary: Common Runtime user logs.
// Fallback: Legacy azureml-logs for older compute targets.
var (
	commonRuntimeLogPattern = regexp.MustCompile(`user_logs/std_log[\D]*[0]*(?:_ps)?\.txt`)
	legacyLogPattern        = regexp.MustCompile(`azureml-logs/[\d]{2}.+\.txt`)
)

// terminalStates are job statuses that indicate the job has finished.
var terminalStates = map[string]bool{
	"Completed":     true,
	"Failed":        true,
	"Canceled":      true,
	"NotResponding": true,
	"Paused":        true,
}

// activeStates are job statuses where streaming is applicable.
var activeStates = map[string]bool{
	"NotStarted":   true,
	"Queued":       true,
	"Preparing":    true,
	"Provisioning": true,
	"Starting":     true,
	"Running":      true,
	"Finalizing":   true,
}

// StreamResult contains the final state of a streamed job.
type StreamResult struct {
	JobName   string
	Status    string
	StudioURL string
}

// StreamJobLogs polls the job and streams log output until the job reaches a terminal state.
func (s *StreamService) StreamJobLogs(ctx context.Context, jobName string) (*StreamResult, error) {
	fmt.Fprintf(os.Stderr, "Streaming logs for job: %s\n\n", jobName)

	// Line-count tracking per file, matching the Azure ML SDK approach.
	// Each poll downloads full content, skips already-printed lines, prints the rest.
	processedLines := make(map[string]int)

	const (
		initialInterval    = 2 * time.Second
		maxInterval        = 5 * time.Second
		jobCheckFrequency  = 10
		maxConsecutiveErrs = 3
	)

	pollInterval := initialInterval
	pollCount := 0
	consecutiveErrs := 0
	trackingEndpoint := ""

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var jobStatus string
		var studioURL string
		if pollCount%jobCheckFrequency == 0 {
			job, err := s.client.GetJob(ctx, jobName)
			if err != nil {
				consecutiveErrs++
				if consecutiveErrs >= maxConsecutiveErrs {
					return nil, fmt.Errorf("failed to get job status after %d retries: %w", maxConsecutiveErrs, err)
				}
				time.Sleep(pollInterval)
				pollCount++
				continue
			}
			consecutiveErrs = 0

			jobStatus = job.Properties.Status
			studioURL = extractServiceEndpoint(job.Properties.Services, "Studio")

			if trackingEndpoint == "" {
				trackingEndpoint = extractServiceEndpoint(job.Properties.Services, "Tracking")
			}

			if terminalStates[jobStatus] {
				if trackingEndpoint != "" && pollCount > 0 {
					s.flushLogs(ctx, trackingEndpoint, jobName, processedLines)
				}
				return &StreamResult{
					JobName:   jobName,
					Status:    jobStatus,
					StudioURL: studioURL,
				}, nil
			}

			if !activeStates[jobStatus] {
				fmt.Fprintf(os.Stderr, "Job status: %s, waiting...\n", jobStatus)
				time.Sleep(pollInterval)
				pollCount++
				continue
			}
		}

		if trackingEndpoint != "" {
			hasNewContent, err := s.pollAndPrintLogs(ctx, trackingEndpoint, jobName, processedLines)
			if err != nil {
				consecutiveErrs++
				if consecutiveErrs >= maxConsecutiveErrs {
					return nil, fmt.Errorf("failed to stream logs after %d retries: %w", maxConsecutiveErrs, err)
				}
			} else {
				consecutiveErrs = 0
				if hasNewContent {
					pollInterval = initialInterval
				} else {
					pollInterval = min(pollInterval+time.Second, maxInterval)
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "Waiting for job to initialize...\n")
			pollInterval = maxInterval
		}

		pollCount++
		time.Sleep(pollInterval)
	}
}

// filterLogFiles selects streamable log files from the history details.
// Matches Common Runtime user logs first; falls back to legacy azureml-logs.
// Returns matched file names sorted alphabetically.
func filterLogFiles(logFiles map[string]string) []string {
	var matched []string
	for name := range logFiles {
		if commonRuntimeLogPattern.MatchString(name) {
			matched = append(matched, name)
		}
	}
	if len(matched) == 0 {
		// Fallback to legacy log pattern for older compute targets
		for name := range logFiles {
			if legacyLogPattern.MatchString(name) {
				matched = append(matched, name)
			}
		}
	}
	sort.Strings(matched)
	return matched
}

// pollAndPrintLogs fetches run history details and prints only new log lines.
func (s *StreamService) pollAndPrintLogs(
	ctx context.Context,
	trackingEndpoint string,
	jobName string,
	processedLines map[string]int,
) (bool, error) {
	details, err := s.client.GetRunHistoryDetails(ctx, trackingEndpoint, jobName)
	if err != nil {
		return false, err
	}
	if details == nil || len(details.LogFiles) == 0 {
		return false, nil
	}

	fileNames := filterLogFiles(details.LogFiles)
	if len(fileNames) == 0 {
		return false, nil
	}

	hasNewContent := false
	for _, fileName := range fileNames {
		sasURI := details.LogFiles[fileName]

		content, _, err := s.client.GetLogContent(ctx, sasURI, 0)
		if err != nil || content == "" {
			continue
		}

		lines := strings.Split(content, "\n")
		// Remove trailing empty element from final newline
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		previousLines := processedLines[fileName]
		if len(lines) <= previousLines {
			continue
		}

		for _, line := range lines[previousLines:] {
			fmt.Println(line)
		}
		hasNewContent = true
		processedLines[fileName] = len(lines)
	}

	return hasNewContent, nil
}

// flushLogs does a final poll to capture any remaining log output.
func (s *StreamService) flushLogs(
	ctx context.Context,
	trackingEndpoint string,
	jobName string,
	processedLines map[string]int,
) {
	_, _ = s.pollAndPrintLogs(ctx, trackingEndpoint, jobName, processedLines)
}

// extractServiceEndpoint extracts the endpoint URL from the job services map.
func extractServiceEndpoint(services map[string]interface{}, serviceName string) string {
	if services == nil {
		return ""
	}
	svc, ok := services[serviceName]
	if !ok {
		return ""
	}
	svcMap, ok := svc.(map[string]interface{})
	if !ok {
		return ""
	}
	endpoint, ok := svcMap["endpoint"]
	if !ok {
		return ""
	}
	str, ok := endpoint.(string)
	if !ok {
		return ""
	}
	return str
}
