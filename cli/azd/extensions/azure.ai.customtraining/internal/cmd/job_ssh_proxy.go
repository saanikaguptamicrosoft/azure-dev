// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"azure.ai.customtraining/pkg/client"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/azure/azure-dev/cli/azd/pkg/azdext"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

// newJobSSHProxyCommand returns a hidden subcommand used internally as the
// SSH ProxyCommand. It opens a WebSocket tunnel to the SSH proxy endpoint
// and pipes stdin <-> WebSocket <-> stdout, so OpenSSH can talk to the job
// container as if it were a normal TCP connection.
func newJobSSHProxyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "_ssh-proxy <proxy-endpoint>",
		Short:  "Internal: WebSocket tunnel for SSH ProxyCommand",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := azdext.WithAccessToken(cmd.Context())
			return runSSHProxy(ctx, args[0])
		},
	}
	return cmd
}

func runSSHProxy(ctx context.Context, proxyEndpoint string) error {
	if strings.TrimSpace(proxyEndpoint) == "" {
		return fmt.Errorf("proxy endpoint argument is empty")
	}

	debug := os.Getenv("AZD_DEBUG") != "" || os.Getenv("AZURE_AI_SSH_DEBUG") != ""
	dbg := func(format string, a ...any) {
		if debug {
			fmt.Fprintf(os.Stderr, "[ssh-proxy] "+format+"\n", a...)
		}
	}

	// AML returns ProxyEndpoint as a fully-qualified wss:// URL ready to dial.
	// Do not append any path; the backend rejects unknown paths.
	wsURL := proxyEndpoint
	if strings.HasPrefix(wsURL, "https://") {
		wsURL = "wss://" + strings.TrimPrefix(wsURL, "https://")
	} else if strings.HasPrefix(wsURL, "http://") {
		wsURL = "ws://" + strings.TrimPrefix(wsURL, "http://")
	}
	dbg("dialing %s", wsURL)

	// Acquire ARM-scoped token (the WebSocket tunnel requires a management token).
	token, err := acquireARMToken(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire management token: %w", err)
	}
	dbg("acquired management token (len=%d)", len(token))

	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)

	// Bump handshake timeout from gorilla/websocket's default (45s) to 60s to
	// accommodate cold-path Azure ingress on freshly-Running containers.
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 60 * time.Second

	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("failed to open websocket tunnel (HTTP %d): %w", resp.StatusCode, err)
		}
		return fmt.Errorf("failed to open websocket tunnel: %w", err)
	}
	defer conn.Close()
	dbg("websocket connected")

	// Bidirectional pipe between stdio and the WebSocket.
	// We exit when either direction returns (EOF, error, or context cancel).
	errCh := make(chan error, 2)

	// Upstream: stdin -> websocket
	go func() {
		buf := make([]byte, 4096)
		for {
			n, rerr := os.Stdin.Read(buf)
			if n > 0 {
				if werr := conn.WriteMessage(websocket.BinaryMessage, buf[:n]); werr != nil {
					errCh <- werr
					return
				}
			}
			if rerr != nil {
				errCh <- rerr
				return
			}
		}
	}()

	// Downstream: websocket -> stdout
	go func() {
		for {
			_, msg, rerr := conn.ReadMessage()
			if len(msg) > 0 {
				if _, werr := os.Stdout.Write(msg); werr != nil {
					errCh <- werr
					return
				}
			}
			if rerr != nil {
				errCh <- rerr
				return
			}
		}
	}()

	// Wait for either direction to finish, or for context cancellation.
	select {
	case err := <-errCh:
		// EOF and normal close are not errors from the user's perspective.
		if err == nil || err == io.EOF || websocket.IsCloseError(err,
			websocket.CloseNormalClosure,
			websocket.CloseGoingAway,
			websocket.CloseAbnormalClosure) {
			return nil
		}
		fmt.Fprintf(os.Stderr, "Connection closed: %v\n", err)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquireARMToken fetches a bearer token for the ARM management scope.
// Uses AzureDeveloperCLICredential to match the rest of the extension.
func acquireARMToken(ctx context.Context) (string, error) {
	azdClient, err := azdext.NewAzdClient()
	if err != nil {
		return "", fmt.Errorf("failed to create azd client: %w", err)
	}
	defer azdClient.Close()

	cred, err := azidentity.NewAzureDeveloperCLICredential(&azidentity.AzureDeveloperCLICredentialOptions{
		AdditionallyAllowedTenants: []string{"*"},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create azure credential: %w", err)
	}

	// We don't need a full project endpoint here — just a credential to mint a token.
	// Build a minimal client purely so we can reuse its token helper.
	tmpClient, err := client.NewClient("https://placeholder.services.ai.azure.com/api/projects/p", cred)
	if err != nil {
		return "", err
	}
	return tmpClient.GetARMToken(ctx)
}
