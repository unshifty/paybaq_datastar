package models

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

type SubjectType string

const (
	SubjectTypeLedger SubjectType = "ledger"
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
	Id              int             `db:"id"`
	SubjectType     SubjectType     `db:"subject_type"`
	SubjectId       string          `db:"subject_id"`
	SubjectRevision int             `db:"subject_revision"`
	EventType       EventType       `db:"event_type"`
	EventPayload    json.RawMessage `db:"event_payload"`
	Metadata        json.RawMessage `db:"metadata"`
	CreatedAt       time.Time       `db:"created_at"`
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
	CreatedAt  time.Time      `json:"-"`
	UpdatedAt  time.Time      `json:"-"`
	Revision   int            `json:"revision"`
	Name       string         `json:"name"`
	People     map[int]Person `json:"people"`
	Payments   []Payment      `json:"payments"`
	Paybaqs    []Paybaq       `json:"paybaqs"`
	LedgerView LedgerView     `json:"-"`
}

type LedgerSummary struct {
	Id        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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

type UserLedger struct {
	UserId   string `json:"user_id"`
	LedgerId string `json:"ledger_id"`
	Archived int    `json:"archived"`
}

func (ledger *Ledger) PeopleSortedByName() []Person {
	people := make([]Person, 0, len(ledger.People))
	for _, person := range ledger.People {
		people = append(people, person)
	}
	slices.SortFunc(people, func(a Person, b Person) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return people
}
