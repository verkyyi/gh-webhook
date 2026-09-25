package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cli/go-gh/v2/pkg/api"
	"github.com/cli/go-gh/v2/pkg/auth"
)

type createHookRequest struct {
	Name   string     `json:"name"`
	Events []string   `json:"events"`
	Active bool       `json:"active"`
	Config hookConfig `json:"config"`
}

type hookConfig struct {
	ContentType string `json:"content_type"`
	InsecureSSL string `json:"insecure_ssl"`
	URL         string `json:"url"`
	Secret      string `json:"secret,omitempty"`
}

type createHookResponse struct {
	Active bool       `json:"active"`
	Config hookConfig `json:"config"`
	Events []string   `json:"events"`
	ID     int        `json:"id"`
	Name   string     `json:"name"`
	URL    string     `json:"url"`
	WsURL  string     `json:"ws_url"`
}

type hookOptions struct {
	gitHubHost string
	authToken  string
	eventTypes []string
	repo       string
	org        string
	secret     string
}

// devHook is a created dev webhook: the relay websocket to dial, plus the
// REST calls that activate and (on reconnect) drop it.
type devHook struct {
	WsURL  string
	URL    string
	client *api.RESTClient
}

func (h *devHook) activate() error {
	if err := h.client.Patch(h.URL, strings.NewReader(`{"active": true}`), nil); err != nil {
		return fmt.Errorf("error activating webhook: %w", err)
	}
	return nil
}

// delete removes the dev hook. Best effort: a hook that outlives its socket is
// only an inactive registration, so callers log and move on.
func (h *devHook) delete() error {
	if h == nil || h.URL == "" {
		return nil
	}
	return h.client.Delete(h.URL, nil)
}

// createHook issues a request against the GitHub API to create a dev webhook
func createHook(o *hookOptions) (*devHook, error) {
	apiClient, err := api.NewRESTClient(api.ClientOptions{
		Host:      o.gitHubHost,
		AuthToken: o.authToken,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating REST client: %w", err)
	}
	path := fmt.Sprintf("repos/%s/hooks", o.repo)
	if o.org != "" {
		path = fmt.Sprintf("orgs/%s/hooks", o.org)
	}

	req := createHookRequest{
		Name:   "cli",
		Events: o.eventTypes,
		Active: false,
		Config: hookConfig{
			ContentType: "json",
			InsecureSSL: "0",
			Secret:      o.secret,
		},
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var res createHookResponse
	err = apiClient.Post(path, bytes.NewReader(reqBytes), &res)
	if err != nil {
		var apierr *api.HTTPError
		if errors.As(err, &apierr) && apierr.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("you do not have access to this feature")
		}
		return nil, fmt.Errorf("error creating webhook: %w", err)
	}

	return &devHook{WsURL: res.WsURL, URL: res.URL, client: apiClient}, nil
}

func authTokenForHost(host string) (string, error) {
	token, _ := auth.TokenForHost(host)
	if token == "" {
		return "", fmt.Errorf("gh auth token not found for host %q", host)
	}
	return token, nil
}
