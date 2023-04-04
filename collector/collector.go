package collector

import (
	"bytes"

	"github.com/andybalholm/brotli"
	"github.com/tendermint/tendermint/rpc/core"
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
	bcoll.collectedPlainTxs = append((*bcoll).collectedPlainTxs, tx...)

	if bcoll.nTxs >= 3 {
		// Encode the collected txs
		encodedTxs, err := bcoll.EncodeTx(bcoll.collectedPlainTxs)
		if err != nil {
			panic(err)
		}
		core.BroadcastTxAsync(&rpctypes.Context{}, encodedTxs)

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
