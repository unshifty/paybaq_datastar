package models

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
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
	LedgerPersonUpdated  EventType = "ledger.person_updated"
	LedgerPaymentUpdated EventType = "ledger.payment_updated"
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
	Id              int     `json:"id"`
	Name            string  `json:"name"`
	Balance         float64 `json:"balance"`
	DeleteRequested bool    `json:"-"`
}

type Payment struct {
	Id          int       `json:"id"`
	Description string    `json:"description"`
	Amount      float64   `json:"amount"`
	PaidBy      int       `json:"paid_by"`
	PaidFor     PersonIds `json:"paid_for"`
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
	View             string   `json:"view"`
	NewPersonName    string   `json:"new_person_name"`
	EditingPersonId  int      `json:"editing_person_id"`
	EditingPerson    *Person  `json:"editing_person"`
	EditingPaymentId int      `json:"editing_payment_id"`
	EditingPayment   *Payment `json:"editing_payment"`
	NewPayment       *Payment `json:"new_payment"`
}

// type LedgerView struct {
// 	View           string          `json:"view"`
// 	People         map[int]Person  `json:"people"`
// 	Payments       map[int]Payment `json:"payments"`
// 	AddingPerson   bool            `json:"adding_person"`
// 	AddingPayment  bool            `json:"adding_payment"`
// 	EditingPerson  bool            `json:"editing_person"`
// 	EditingPayment bool            `json:"editing_payment"`
// }

type LedgerSignals struct {
	LedgerViews  map[string]LedgerView `json:"ledgerViews"`
	ConnectionId string                `json:"conn_id"`
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

func (ledger *Ledger) PaymentsMostRecentFirst() []Payment {
	payments := make([]Payment, 0, len(ledger.Payments))
	for _, payment := range ledger.Payments {
		payments = append(payments, payment)
	}
	slices.SortFunc(payments, func(a Payment, b Payment) int {
		return cmp.Compare(b.Id, a.Id)
	})
	return payments
}

func (ledger *Ledger) GetPersonById(personId int) Person {
	for _, person := range ledger.People {
		if person.Id == personId {
			return person
		}
	}
	return Person{}
}

func (ledger *Ledger) GetPeopleByIds(ids []int) []Person {
	people := make([]Person, 0, len(ids))
	for _, id := range ids {
		if person, ok := ledger.People[id]; ok {
			people = append(people, person)
		}
	}
	return people
}

func NameList(people []Person, separator string) string {
	names := make([]string, 0, len(people))
	for _, person := range people {
		names = append(names, person.NameOrPlaceholder())
	}
	return strings.Join(names, separator)
}

func (person Person) NameOrPlaceholder() string {
	if person.Name != "" {
		return person.Name
	}
	return "Mystery Person"
}

type PersonIds []int

func (s *PersonIds) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}

	// Try as []int first (client might already send ints)
	var ints []int
	if err := json.Unmarshal(data, &ints); err == nil {
		*s = ints
		return nil
	}

	// Otherwise parse as []string
	var strs []string
	if err := json.Unmarshal(data, &strs); err != nil {
		return fmt.Errorf("expected array of strings or ints: %w", err)
	}

	out := make([]int, 0, len(strs))
	for _, str := range strs {
		str = strings.TrimSpace(str)
		if str == "" {
			continue // ignore blanks
		}
		i, err := strconv.Atoi(str)
		if err != nil {
			return fmt.Errorf("invalid int in paid_for: %q", str)
		}
		out = append(out, i)
	}
	*s = out
	return nil
}
