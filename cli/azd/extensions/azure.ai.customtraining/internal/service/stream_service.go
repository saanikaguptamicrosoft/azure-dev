// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package service

import (
	"context"
	"fmt"
	"os"
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
	fmt.Fprintf(os.Stderr, "Stream logs for job %s...\n\n", jobName)

	offsets := make(map[string]int64)

	const (
		initialInterval    = 2 * time.Second
		maxInterval        = 5 * time.Second
		jobCheckFrequency  = 10
		maxConsecutiveErrs = 3
		initialTailBytes   = int64(8192)
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
				if trackingEndpoint != "" {
					s.flushLogs(ctx, trackingEndpoint, jobName, offsets, initialTailBytes)
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
			hasNewContent, err := s.pollAndPrintLogs(ctx, trackingEndpoint, jobName, offsets, initialTailBytes)
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

// pollAndPrintLogs fetches run history details and streams new log content.
func (s *StreamService) pollAndPrintLogs(
	ctx context.Context,
	trackingEndpoint string,
	jobName string,
	offsets map[string]int64,
	initialTailBytes int64,
) (bool, error) {
	details, err := s.client.GetRunHistoryDetails(ctx, trackingEndpoint, jobName)
	if err != nil {
		return false, err
	}
	if details == nil || len(details.LogFiles) == 0 {
		return false, nil
	}

	fileNames := make([]string, 0, len(details.LogFiles))
	for name := range details.LogFiles {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)

	hasNewContent := false
	for _, fileName := range fileNames {
		sasURI := details.LogFiles[fileName]

		offset, exists := offsets[fileName]
		if !exists {
			offset = -initialTailBytes
		}

		content, bytesRead, err := s.fetchLogChunk(ctx, sasURI, offset)
		if err != nil {
			continue
		}

		if bytesRead > 0 && content != "" {
			fmt.Printf("\n--- %s ---\n", fileName)
			fmt.Print(content)
			if !strings.HasSuffix(content, "\n") {
				fmt.Println()
			}
			hasNewContent = true

			if offset < 0 {
				offsets[fileName] = bytesRead
			} else {
				offsets[fileName] = offset + bytesRead
			}
		} else if !exists {
			offsets[fileName] = 0
		}
	}

	return hasNewContent, nil
}

// fetchLogChunk retrieves log content, handling initial tail (negative offset).
func (s *StreamService) fetchLogChunk(ctx context.Context, sasURI string, offset int64) (string, int64, error) {
	if offset < 0 {
		content, bytesRead, err := s.client.GetLogContent(ctx, sasURI, 0)
		if err != nil {
			return "", 0, err
		}
		tailSize := -offset
		if int64(len(content)) > tailSize {
			trimmed := content[int64(len(content))-tailSize:]
			if idx := strings.Index(trimmed, "\n"); idx >= 0 {
				trimmed = trimmed[idx+1:]
			}
			return trimmed, bytesRead, nil
		}
		return content, bytesRead, nil
	}

	return s.client.GetLogContent(ctx, sasURI, offset)
}

// flushLogs does a final poll to capture any remaining log output.
func (s *StreamService) flushLogs(
	ctx context.Context,
	trackingEndpoint string,
	jobName string,
	offsets map[string]int64,
	initialTailBytes int64,
) {
	_, _ = s.pollAndPrintLogs(ctx, trackingEndpoint, jobName, offsets, initialTailBytes)
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
