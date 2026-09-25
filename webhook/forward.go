package webhook

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

// NewCmdForward returns a forward command.
func NewCmdForward() *cobra.Command {
	var (
		localURL      string
		eventTypes    []string
		targetRepo    string
		targetOrg     string
		githubHost    string
		webhookSecret string
	)

	cmd := &cobra.Command{
		Use:          "forward --events=<types> [--url=<url>]",
		Short:        "Receive test events locally",
		SilenceUsage: true,
		Example: heredoc.Doc(`
			# create a dev webhook for the 'issue_open' event in the monalisa/smile repo in GitHub running locally, and
			# forward payloads for the triggered event to http://localhost:9999/webhooks

			$ gh webhook forward --events=issues --repo=monalisa/smile --url="http://localhost:9999/webhooks"
			$ gh webhook forward --events=issues --org=github --url="http://localhost:9999/webhooks"
		`),
		RunE: func(c *cobra.Command, _ []string) error {
			if targetRepo == "" && targetOrg == "" {
				return errors.New("`--repo` or `--org` flag required")
			}

			if envHost := os.Getenv("GH_HOST"); envHost != "" && !c.Flags().Changed("github-host") {
				githubHost = envHost
			}

			authToken, err := authTokenForHost(githubHost)
			if err != nil {
				return fmt.Errorf("fatal: error fetching gh token: %w", err)
			}

			newHook := func() (*devHook, error) {
				return createHook(&hookOptions{
					gitHubHost: githubHost,
					eventTypes: eventTypes,
					authToken:  authToken,
					repo:       targetRepo,
					org:        targetOrg,
					secret:     webhookSecret,
				})
			}
			hook, err := newHook()
			if err != nil {
				return err
			}
			return runFwd(os.Stdout, localURL, authToken, hook, newHook)
		},
	}

	cmd.Flags().StringSliceVarP(&eventTypes, "events", "E", nil, "Names of the event `types` to forward. Use `*` to forward all events.")
	_ = cmd.MarkFlagRequired("events")
	cmd.Flags().StringVarP(&targetRepo, "repo", "R", "", "Name of the repo where the webhook is installed")
	cmd.Flags().StringVarP(&githubHost, "github-host", "H", "github.com", "GitHub host name")
	cmd.Flags().StringVarP(&localURL, "url", "U", "", "Address of the local server to receive events. If omitted, events will be printed to stdout.")
	cmd.Flags().StringVarP(&targetOrg, "org", "O", "", "Name of the org where the webhook is installed")
	cmd.Flags().StringVarP(&webhookSecret, "secret", "S", "", "Webhook secret for incoming events")

	return cmd
}

type wsEventReceived struct {
	Header http.Header
	Body   []byte
}

func runFwd(out io.Writer, url, token string, hook *devHook, newHook func() (*devHook, error)) error {
	if url == "" {
		fmt.Fprintln(os.Stderr, "notice: no `--url` specified; printing webhook payloads to stdout")
	}
	// The relay closes a long-lived socket with 1006 on its own schedule (observed
	// every ~80s to ~25min) and then refuses a second handshake on that session's
	// ws_url (500 bad handshake). So a reconnect that means anything starts from a
	// FRESH dev hook: drop the old one, create a new one, dial its ws_url. Keep at
	// it for as long as the process lives — a socket that stayed up for a while
	// resets the backoff, a socket that died straight away doubles it (5s → 5m);
	// only a run of connections that never came up at all gives up.
	backoff := reconnectBackoffMin
	neverUp := 0
	for {
		started := time.Now()
		err := handleWebsocket(out, url, token, hook.WsURL, hook.activate)
		if err == nil || isWebsocketCloseError(err, websocket.CloseNormalClosure) {
			return nil
		}
		connectedFor := time.Since(started)
		if connectedFor < reconnectHealthy {
			neverUp++
		} else {
			neverUp = 0
		}
		if neverUp >= reconnectGiveUpAfter {
			return fmt.Errorf("giving up after %d reconnects that never came up: %w", neverUp, err)
		}
		backoff = nextReconnectBackoff(backoff, connectedFor)
		fmt.Fprintf(os.Stderr, "notice: %v; reconnecting on a fresh hook in %s\n", err, backoff)
		time.Sleep(backoff)
		fresh, nerr := newHook()
		if nerr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not create a fresh hook (%v); retrying the old socket\n", nerr)
			continue
		}
		if derr := hook.delete(); derr != nil {
			fmt.Fprintf(os.Stderr, "warning: could not delete the old hook (%v); it stays inactive\n", derr)
		}
		hook = fresh
	}
}

const (
	reconnectBackoffMin = 5 * time.Second
	reconnectBackoffMax = 5 * time.Minute
	// A connection that lived at least this long counts as healthy: the next
	// reconnect starts from the minimum backoff again.
	reconnectHealthy = 30 * time.Second
	// Consecutive connections shorter than reconnectHealthy before giving up: a
	// bad token or a dead relay must not spin forever behind a supervisor.
	reconnectGiveUpAfter = 10
)

// nextReconnectBackoff returns the wait before the next reconnect, given the
// current wait and how long the connection that just closed had been up.
func nextReconnectBackoff(current, connectedFor time.Duration) time.Duration {
	if connectedFor >= reconnectHealthy {
		return reconnectBackoffMin
	}
	next := current * 2
	if next > reconnectBackoffMax {
		next = reconnectBackoffMax
	}
	return next
}

func isWebsocketCloseError(err error, code int) bool {
	var closeError *websocket.CloseError
	return errors.As(err, &closeError) && closeError.Code == code
}

// handleWebsocket mediates between websocket server and local web server
func handleWebsocket(out io.Writer, url, token, wsURL string, activateHook func() error) error {
	c, err := dial(token, wsURL)
	if err != nil {
		return fmt.Errorf("error dialing to ws server: %w", err)
	}
	defer c.Close()

	fmt.Fprintln(os.Stderr, "Forwarding Webhook events from GitHub...")
	if err := activateHook(); err != nil {
		return fmt.Errorf("error activating hook: %w", err)
	}

	for {
		var ev wsEventReceived
		err := c.ReadJSON(&ev)
		if err != nil {
			return fmt.Errorf("error receiving json event: %w", err)
		}

		resp, err := forwardEvent(out, url, ev)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: error forwarding event: %v\n", err)
			continue
		}

		err = c.WriteJSON(resp)
		if err != nil {
			return fmt.Errorf("error writing json event: %w", err)
		}
	}
}

// dial connects to the websocket server
func dial(token, url string) (*websocket.Conn, error) {
	h := make(http.Header)
	h.Set("Authorization", token)
	c, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			err = fmt.Errorf("code: %v - body: %s - err: %w", resp.StatusCode, body, err)
		}
		return nil, err
	}
	return c, nil
}

type httpEventForward struct {
	Status int
	Header http.Header
	Body   []byte
}

// forwardEvent forwards events to the server running on the local port specified by the user
func forwardEvent(w io.Writer, url string, ev wsEventReceived) (*httpEventForward, error) {
	if url == "" {
		event := ev.Header.Get("X-GitHub-Event")
		event = strings.ReplaceAll(event, "\n", "")
		event = strings.ReplaceAll(event, "\r", "")
		fmt.Fprintf(os.Stderr, "[LOG] received event %q\n", event)
		if _, err := w.Write(ev.Body); err != nil {
			return nil, err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return nil, err
		}
		return &httpEventForward{Status: 200, Header: make(http.Header), Body: []byte("OK")}, nil
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(ev.Body))
	if err != nil {
		return nil, err
	}

	for k := range ev.Header {
		req.Header.Set(k, ev.Header.Get(k))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &httpEventForward{
		Status: resp.StatusCode,
		Header: resp.Header,
		Body:   body,
	}, nil
}
