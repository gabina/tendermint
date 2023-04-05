package core

import (
	"bytes"

	"github.com/andybalholm/brotli"
	abci "github.com/tendermint/tendermint/abci/types"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	rpctypes "github.com/tendermint/tendermint/rpc/jsonrpc/types"
	"github.com/tendermint/tendermint/types"
)

// Collector defines the mempool interface.
type Collector interface {
	// CheckTx executes a new transaction against the application to determine
	// its validity and whether it should be added to the mempool.
	//BroadcastTx() error

	// Add the plain tx without checking it
	AddTx(tx types.Tx)

	// Encode the plain tx
	EncodeTx(tx types.Tx) ([]byte, error)
}

type BrotliCollector struct {
	collectedPlainTxs []byte
	nTxs              int
}

func NewBrotliCollector() *BrotliCollector {
	var emptySlice []byte
	return &BrotliCollector{
		collectedPlainTxs: emptySlice,
		nTxs:              0,
	}
}

// For now, checking the tx is client reponsability
func (bcoll *BrotliCollector) AddTx(tx types.Tx) {
	bcoll.nTxs++
	// Add the separator between txs at the begining if this is the first tx in the batch
	if bcoll.nTxs > 0 {
		tx = append([]byte("/"), tx...)
	}
	bcoll.collectedPlainTxs = append((*bcoll).collectedPlainTxs, tx...)

	if bcoll.nTxs >= 3 {
		// Encode the collected txs
		encodedTxs, err := bcoll.EncodeTx(bcoll.collectedPlainTxs)
		if err != nil {
			panic(err)
		}
		BroadcastTxAsync(&rpctypes.Context{}, encodedTxs)

		// Reset collector state
		bcoll.collectedPlainTxs = nil
		bcoll.nTxs = 0

	}
}

func (bcoll *BrotliCollector) EncodeTx(tx types.Tx) ([]byte, error) {
	// Compress tx using brotli
	var buffer bytes.Buffer
	bw := brotli.NewWriter(nil)
	bw.Reset(&buffer)
	if _, err := bw.Write(tx); err != nil {
		return nil, err
	}
	if err := bw.Close(); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

//-----------------------------------------------------------------------------
// NOTE: tx should be signed, but this is only checked at the app level (not by Tendermint!)

// CollectThenBroadcastTxAsync returns right away, with no response. Does not wait for
// CheckTx nor DeliverTx results.
// The idea behind CollectThenBroadcastTxAsync is to minimize the number of txs by collecting several of them,
// and brotli them before the broadcast. That way one signle message would contain several txs.
func CollectThenBroadcastTxAsync(ctx *rpctypes.Context, tx types.Tx) (*ctypes.ResultBroadcastTx, error) {

	go collectThenBroadcastTx(tx)

	return &ctypes.ResultBroadcastTx{Hash: tx.Hash()}, nil
}

func collectThenBroadcastTx(tx types.Tx) {
	encodedTx, err := env.Collector.EncodeTx(tx)
	if err != nil {
		return
	}

	// Check the tx without adding it to the mempool yet
	res, err := env.ProxyAppMempool.CheckTxSync(abci.RequestCheckTx{Tx: encodedTx})
	if err != nil {
		return
	}

	// If the tx is valid, we add it
	if res.Code == 0 {
		env.Collector.AddTx(tx)
	}
}
