package main

const (
	NONSTANDARD           string = "nonstandard"
	PUBKEY                string = "pubkey"
	PUBKEYHASH            string = "pubkeyhash"
	SCRIPTHASH            string = "scripthash"
	MULTISIG              string = "multisig"
	WITNESS_V0_KEYHASH    string = "witness_v0_keyhash"
	WITNESS_V0_SCRIPTHASH string = "witness_v0_scripthash"
	WITNESS_V1_TAPROOT    string = "witness_v1_taproot"
	WITNESS_UNKNOWN       string = "witness_unknown"
	NULLDATA              string = "nulldata"
)

type UTXO struct {
	ID       string `json:"id,omitempty" bson:"_id,omitempty"`
	TxID     string `json:"tx_id" bson:"tx_id"`
	Vout     int64  `json:"vout" bson:"vout"`
	Height   int64  `json:"height" bson:"height"`
	Coinbase bool   `json:"coinbase" bson:"coinbase"`
	Amount   int64  `json:"amount" bson:"amount"`
	Script   string `json:"script" bson:"script"`
	Type     string `json:"type" bson:"type"`
	Address  string `json:"address" bson:"address"`
}
