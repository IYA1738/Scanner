package rpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
)

const testBlockHash = "0x1111111111111111111111111111111111111111111111111111111111111111"
const testTxHash = "0x2222222222222222222222222222222222222222222222222222222222222222"

func TestBlockByNumberTransactionTolerance(t *testing.T) {
	const from = "0x3333333333333333333333333333333333333333"
	const to = "0x4444444444444444444444444444444444444444"
	const parentHash = "0x5555555555555555555555555555555555555555555555555555555555555555"
	validTx := fmt.Sprintf(`{"hash":%q,"type":"0x2","from":%q,"to":%q,"value":"0x2a","input":"0x1234"}`, testTxHash, from, to)
	unknownTypeTx := fmt.Sprintf(`{"hash":%q,"type":"0x7e","value":"0x0","input":"0x"}`, testTxHash)
	invalidTx := `{"type":"0x2","value":"invalid"}`
	result := fmt.Sprintf(`{"number":"0x2a","hash":%q,"parentHash":%q,"timestamp":"0x64","gasUsed":"0x5208","gasLimit":"0xf4240","baseFeePerGas":"0x2a","transactions":[%s,%s,%s,%s]}`, testBlockHash, parentHash, validTx, unknownTypeTx, invalidTx, validTx)

	client := newBlockRPCClient(t, 42, result)
	block, err := client.BlockByNumber(t.Context(), 42)
	if err != nil {
		t.Fatalf("get block: %v", err)
	}
	if block == nil || block.Number != 42 || block.Hash != common.HexToHash(testBlockHash) {
		t.Fatalf("unexpected block: %+v", block)
	}
	if block.ParentHash != common.HexToHash(parentHash) ||
		block.Timestamp == nil || uint64(*block.Timestamp) != 100 ||
		uint64(block.GasUsed) != 21000 || uint64(block.GasLimit) != 1000000 ||
		block.BaseFeePerGas == nil || block.BaseFeePerGas.ToInt().Uint64() != 42 {
		t.Errorf("block header fields were not decoded: %+v", block)
	}
	if len(block.Transactions) != 4 {
		t.Fatalf("transaction positions lost: got %d transactions", len(block.Transactions))
	}

	for _, index := range []int{0, 3} {
		tx := block.Transactions[index]
		if tx.DecodeErr != nil || string(tx.Raw) != validTx {
			t.Fatalf("transaction %d was not preserved: %+v", index, tx)
		}
		if tx.Hash != common.HexToHash(testTxHash) || tx.Type != 2 ||
			tx.From == nil || *tx.From != common.HexToAddress(from) ||
			tx.To == nil || *tx.To != common.HexToAddress(to) ||
			tx.Value == nil || tx.Value.ToInt().Uint64() != 42 ||
			string(tx.Input) != "\x12\x34" {
			t.Errorf("transaction %d fields were not decoded: %+v", index, tx)
		}
	}
	unknown := block.Transactions[1]
	if unknown.DecodeErr != nil || unknown.Type != 0x7e || string(unknown.Raw) != unknownTypeTx {
		t.Errorf("unknown transaction type was not decoded: %+v", unknown)
	}
	invalid := block.Transactions[2]
	if invalid.DecodeErr == nil || string(invalid.Raw) != invalidTx {
		t.Errorf("invalid transaction must preserve Raw and DecodeErr: %+v", invalid)
	}
}

func TestBlockByNumberTransactionValidation(t *testing.T) {
	cases := []struct {
		raw     string
		wantErr bool
	}{
		{`null`, true},
		{`{}`, true},
		{fmt.Sprintf(`{"hash":%q}`, common.Hash{}.Hex()), true},
		{fmt.Sprintf(`{"hash":%q,"type":"0x6a"}`, testTxHash), false},
		{fmt.Sprintf(`{"hash":%q,"type":"0x2","to":null}`, testTxHash), false},
	}
	rawTxs := make([]string, len(cases))
	for i, test := range cases {
		rawTxs[i] = test.raw
	}
	result := fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":[%s]}`, testBlockHash, strings.Join(rawTxs, ","))
	client := newBlockRPCClient(t, 42, result)
	block, err := client.BlockByNumber(t.Context(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Transactions) != len(cases) {
		t.Fatalf("transaction positions lost: got %d transactions", len(block.Transactions))
	}
	for i, test := range cases {
		tx := block.Transactions[i]
		if (tx.DecodeErr != nil) != test.wantErr || string(tx.Raw) != test.raw {
			t.Errorf("transaction %d: want DecodeErr=%v and Raw=%s, got %+v", i, test.wantErr, test.raw, tx)
		}
	}
}

func TestBlockByNumberEmptyAndInvalidBlocks(t *testing.T) {
	cases := []struct {
		name    string
		result  string
		number  uint64
		wantErr bool
	}{
		{"empty block", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash), 42, false},
		{"not found", `null`, 42, true},
		{"missing transactions", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64"}`, testBlockHash), 42, true},
		{"null transactions", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":null}`, testBlockHash), 42, true},
		{"wrong height", fmt.Sprintf(`{"number":"0x2b","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash), 42, true},
		{"malformed number", fmt.Sprintf(`{"number":"invalid","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash), 42, true},
		{"missing hash", `{"number":"0x2a","timestamp":"0x64","transactions":[]}`, 42, true},
		{"zero hash", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":[]}`, common.Hash{}.Hex()), 42, true},
		{"missing timestamp", fmt.Sprintf(`{"number":"0x2a","hash":%q,"transactions":[]}`, testBlockHash), 42, true},
		{"null timestamp", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":null,"transactions":[]}`, testBlockHash), 42, true},
		{"malformed timestamp", fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"invalid","transactions":[]}`, testBlockHash), 42, true},
		{"missing genesis number", fmt.Sprintf(`{"hash":%q,"timestamp":"0x0","transactions":[]}`, testBlockHash), 0, true},
		{"genesis", fmt.Sprintf(`{"number":"0x0","hash":%q,"timestamp":"0x0","transactions":[]}`, testBlockHash), 0, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client := newBlockRPCClient(t, test.number, test.result)
			block, err := client.BlockByNumber(t.Context(), test.number)
			if test.wantErr {
				if err == nil || block != nil {
					t.Fatalf("expected nil block and error, got block=%+v, err=%v", block, err)
				}
				if client.httpStates[0].Failures != 1 {
					t.Errorf("invalid block was not recorded as a node failure")
				}
				return
			}
			if err != nil || block == nil || block.Number != test.number || len(block.Transactions) != 0 {
				t.Fatalf("expected empty block, got block=%+v, err=%v", block, err)
			}
			if block.Timestamp == nil || (test.number == 0 && *block.Timestamp != 0) {
				t.Errorf("timestamp was not preserved: %+v", block.Timestamp)
			}
		})
	}
}

func TestBlockByNumberOptionalBaseFee(t *testing.T) {
	for _, fee := range []string{"", `,"baseFeePerGas":null`} {
		t.Run(fee, func(t *testing.T) {
			result := fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":[]%s}`, testBlockHash, fee)
			client := newBlockRPCClient(t, 42, result)
			block, err := client.BlockByNumber(t.Context(), 42)
			if err != nil || block == nil || block.BaseFeePerGas != nil {
				t.Fatalf("expected absent base fee, got block=%+v, err=%v", block, err)
			}
		})
	}
}

func TestBlockByNumberRetriesInvalidBlocks(t *testing.T) {
	validBlock := fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash)
	for _, invalid := range []string{
		`null`,
		`{"number":"invalid","timestamp":"0x64"}`,
		fmt.Sprintf(`{"number":"0x2b","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash),
		`{"number":"0x2a","timestamp":"0x64","transactions":[]}`,
		fmt.Sprintf(`{"number":"0x2a","hash":%q,"transactions":[]}`, testBlockHash),
	} {
		t.Run(invalid, func(t *testing.T) {
			client := newBlockRPCClient(t, 42, invalid, validBlock)
			client.Cfg.RetryCount = 1
			block, err := client.BlockByNumber(t.Context(), 42)
			if err != nil || block == nil || block.Hash != common.HexToHash(testBlockHash) {
				t.Fatalf("expected successful retry, got block=%+v, err=%v", block, err)
			}
		})
	}
	t.Run("retry does not reuse previous fields", func(t *testing.T) {
		client := newBlockRPCClient(t, 42,
			fmt.Sprintf(`{"number":"0x2a","hash":%q,"timestamp":"0x64","transactions":null}`, testBlockHash),
			`{"number":"0x2a","timestamp":"0x64","transactions":[]}`,
		)
		client.Cfg.RetryCount = 1
		if block, err := client.BlockByNumber(t.Context(), 42); err == nil || block != nil {
			t.Fatalf("expected invalid retry response to fail, got block=%+v, err=%v", block, err)
		}
	})
	t.Run("retry does not reuse timestamp", func(t *testing.T) {
		client := newBlockRPCClient(t, 42,
			fmt.Sprintf(`{"number":"0x2b","hash":%q,"timestamp":"0x64","transactions":[]}`, testBlockHash),
			fmt.Sprintf(`{"number":"0x2a","hash":%q,"transactions":[]}`, testBlockHash),
		)
		client.Cfg.RetryCount = 1
		if block, err := client.BlockByNumber(t.Context(), 42); err == nil || block != nil {
			t.Fatalf("expected missing timestamp on retry to fail, got block=%+v, err=%v", block, err)
		}
	})
	t.Run("retry does not reuse base fee", func(t *testing.T) {
		client := newBlockRPCClient(t, 42,
			fmt.Sprintf(`{"number":"0x2b","hash":%q,"timestamp":"0x64","baseFeePerGas":"0x2a","transactions":[]}`, testBlockHash),
			validBlock,
		)
		client.Cfg.RetryCount = 1
		block, err := client.BlockByNumber(t.Context(), 42)
		if err != nil || block == nil || block.BaseFeePerGas != nil {
			t.Fatalf("expected absent base fee on retry, got block=%+v, err=%v", block, err)
		}
	})
}

func newBlockRPCClient(t *testing.T, blockNumber uint64, results ...string) *RpcClient {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode RPC request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Method != "eth_getBlockByNumber" || len(request.Params) != 2 ||
			string(request.Params[0]) != fmt.Sprintf("%q", hexutil.EncodeUint64(blockNumber)) || string(request.Params[1]) != "true" {
			t.Errorf("unexpected RPC request: %+v", request)
		}
		index := int(calls.Add(1)) - 1
		if index >= len(results) {
			t.Errorf("unexpected RPC request %d", index+1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, request.ID, results[index])
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		if got := int(calls.Load()); got != len(results) {
			t.Errorf("RPC requests: got %d, want %d", got, len(results))
		}
	})

	client, err := ethclient.Dial(server.URL)
	if err != nil {
		t.Fatalf("dial test RPC: %v", err)
	}
	rpcClient := &RpcClient{
		Client:     []*ethclient.Client{client},
		httpStates: map[int]RpcState{0: {}},
		httpQueue:  make(chan int, 1),
		stop:       make(chan struct{}),
	}
	rpcClient.httpQueue <- 0
	t.Cleanup(func() { rpcClient.Close() })
	return rpcClient
}
