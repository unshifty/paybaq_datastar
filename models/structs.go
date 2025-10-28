package models

import (
	"encoding/json"
	"time"
)

type EntityType string

const (
	EntityTypeLedger EntityType = "ledger"
)

type EventType string

const (
	LedgerCreated        EventType = "ledger.created"
	LedgerPersonAdded    EventType = "ledger.person_added"
	LedgerPaymentAdded   EventType = "ledger.payment_added"
	LedgerPersonDeleted  EventType = "ledger.person_deleted"
	LedgerPaymentDeleted EventType = "ledger.payment_deleted"
)

type Event struct {
	Id            int64           `db:"id"`
	EntityType    EntityType      `db:"entity_type"`
	EntityId      string          `db:"entity_id"`
	EntityVersion int64           `db:"entity_version"`
	EventType     EventType       `db:"event_type"`
	EventPayload  json.RawMessage `db:"event_payload"`
	Metadata      json.RawMessage `db:"metadata"`
	CreatedAt     time.Time       `db:"created_at"`
}

type Person struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type Payment struct {
	Id      string  `json:"id"`
	Amount  float64 `json:"amount"`
	PaidBy  int     `json:"paid_by"`
	PaidFor []int   `json:"paid_for"`
}

type Paybaq struct {
	From   int     `json:"from"`
	To     int     `json:"to"`
	Amount float64 `json:"amount"`
}

type Ledger struct {
	Id         string         `json:"id"`
	SchemaId   string         `json:"schema_id"`
	Revision   uint64         `json:"-"`
	Name       string         `json:"name"`
	People     map[int]Person `json:"people"`
	Payments   []Payment      `json:"payments"`
	Paybaqs    []Paybaq       `json:"paybaqs"`
	LedgerView LedgerView     `json:"-"`
}

type LedgerSummary struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

type LedgerView struct {
	Tab  string `json:"tab"`
	Mode string `json:"mode"`
}

type LedgerSignals struct {
	LedgerViews   map[string]LedgerView `json:"ledgerViews"`
	NewPersonName string                `json:"newpersonname"`
	ConnectionId  string                `json:"conn_id"`
}
