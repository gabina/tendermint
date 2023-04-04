package core

import (
	abci "github.com/tendermint/tendermint/abci/types"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	rpctypes "github.com/tendermint/tendermint/rpc/jsonrpc/types"
	"github.com/tendermint/tendermint/types"
)

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
