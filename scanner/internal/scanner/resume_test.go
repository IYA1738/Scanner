package scanner

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

func TestRunResumesInitialHeadBeforeFirstCursorWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	first := newResumeTestClient(t, ctx, &resumeTestRPC{head: 42, failAfterFirstHead: true})
	s := &Scanner{
		ctx:    ctx,
		Client: first,
		cfg:    config.ScannerConfig{WorkersNum: 1, GroupSize: 1},
	}
	err := s.Run(true)
	if closeErr := first.Close(); closeErr != nil {
		t.Fatalf("close first RPC client: %v", closeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "head lookup interrupted") {
		t.Fatalf("first Run error = %v, want the simulated head lookup failure", err)
	}

	requested := make(chan hexutil.Uint64, 1)
	s.Client = newResumeTestClient(t, ctx, &resumeTestRPC{head: 45, requested: requested})
	defer s.Client.Close()
	if err := s.Run(true); err == nil || !strings.Contains(err.Error(), "stop before saving cursor") {
		t.Fatalf("second Run error = %v, want the deliberate block fetch failure", err)
	}
	select {
	case got := <-requested:
		if got != 42 {
			t.Fatalf("first block after reconnect = %d, want 42", uint64(got))
		}
	default:
		t.Fatal("scanner did not request a block after reconnect")
	}
}

type resumeTestRPC struct {
	head               hexutil.Uint64
	failAfterFirstHead bool
	headCalls          atomic.Int32
	requested          chan<- hexutil.Uint64
}

func (r *resumeTestRPC) BlockNumber() (hexutil.Uint64, error) {
	if r.failAfterFirstHead && r.headCalls.Add(1) > 1 {
		return 0, errors.New("head lookup interrupted")
	}
	return r.head, nil
}

func (r *resumeTestRPC) GetBlockByNumber(number hexutil.Uint64, _ bool) (*struct{}, error) {
	select {
	case r.requested <- number:
	default:
	}
	return nil, errors.New("stop before saving cursor")
}

func newResumeTestClient(t *testing.T, ctx context.Context, service *resumeTestRPC) *rpc.RpcClient {
	t.Helper()
	server := gethrpc.NewServer()
	t.Cleanup(server.Stop)
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatalf("register test RPC: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/", server)
	mux.Handle("/ws", server.WebsocketHandler(nil))
	httpServer := httptest.NewServer(mux)
	t.Cleanup(httpServer.Close)
	client, err := rpc.NewRPCClient(ctx, config.RPCConfig{
		URL:        []string{httpServer.URL},
		WS_URL:     []string{"ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws"},
		RPCTimeout: 2,
	})
	if err != nil {
		t.Fatalf("connect to test RPC: %v", err)
	}
	return client
}
