package webhook

import (
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewCmdForwardSilencesUsage(t *testing.T) {
	cmd := NewCmdForward()

	if !cmd.SilenceUsage {
		t.Fatal("expected runtime errors not to print command usage")
	}
}

func TestIsWebsocketCloseErrorUnwrapsError(t *testing.T) {
	err := fmt.Errorf(
		"error receiving json event: %w",
		&websocket.CloseError{Code: websocket.CloseAbnormalClosure},
	)

	if !isWebsocketCloseError(err, websocket.CloseAbnormalClosure) {
		t.Fatal("expected wrapped abnormal closure to be recognized")
	}
}

func TestIsWebsocketCloseErrorRejectsDifferentCode(t *testing.T) {
	err := fmt.Errorf(
		"error receiving json event: %w",
		&websocket.CloseError{Code: websocket.CloseNormalClosure},
	)

	if isWebsocketCloseError(err, websocket.CloseAbnormalClosure) {
		t.Fatal("expected close error with a different code not to match")
	}
}

func TestNextReconnectBackoff(t *testing.T) {
	// a connection that lived resets to the minimum
	if got := nextReconnectBackoff(2*time.Minute, time.Hour); got != reconnectBackoffMin {
		t.Fatalf("healthy connection: want %s, got %s", reconnectBackoffMin, got)
	}
	// one that died at once doubles, and is capped
	if got := nextReconnectBackoff(reconnectBackoffMin, time.Second); got != 2*reconnectBackoffMin {
		t.Fatalf("early death: want %s, got %s", 2*reconnectBackoffMin, got)
	}
	if got := nextReconnectBackoff(4*time.Minute, time.Second); got != reconnectBackoffMax {
		t.Fatalf("cap: want %s, got %s", reconnectBackoffMax, got)
	}
}
