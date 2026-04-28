// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// GetLogContent fetches log file content from a SAS URI.
// Downloads the full content on each call (no Range header). The caller tracks
// which lines have already been printed, matching the Azure ML SDK approach.
// No authentication is needed since the URL contains a SAS token.
func (c *Client) GetLogContent(ctx context.Context, sasURI string, offset int64) (string, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sasURI, nil)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create log request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("log request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("log request returned status %d", resp.StatusCode)
	}

	// Cap read to 1MB per poll to avoid memory issues with very large logs
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("failed to read log content: %w", err)
	}

	return string(body), int64(len(body)), nil
}
