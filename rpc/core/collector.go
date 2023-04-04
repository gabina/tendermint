package core

import (
	"bytes"

	brotli "github.com/andybalholm/brotli"
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

	// Compress tx using brotli
	var buffer bytes.Buffer
	bw := brotli.NewWriter(nil)
	bw.Reset(&buffer)
	if _, err := bw.Write(tx); err != nil {
		panic(err)
	}
	if err := bw.Close(); err != nil {
		panic(err)
	}

	// This call should be async
	res, err := env.ProxyAppMempool.CheckTxSync(abci.RequestCheckTx{Tx: buffer.Bytes()})

	if err != nil {
		return nil, err
	}
	return &ctypes.ResultBroadcastTx{
		Code:      res.Code,
		Data:      res.Data,
		Log:       res.Log,
		Codespace: res.Codespace,
		Hash:      tx.Hash()}, nil
}
