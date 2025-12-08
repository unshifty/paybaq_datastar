package models

import (
	"testing"
)

func TestUpdateLedgerBalancesAndPaybaqs(t *testing.T) {
	tests := []struct {
		name            string
		ledger          Ledger
		expectedPaybaqs []Paybaq
		expectedBalance map[int]float64
	}{
		{
			name: "Simple equal split",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
				},
				Payments: []Payment{
					{Id: 1, Amount: 100, PaidBy: 1, PaidFor: []int{1, 2}},
				},
			},
			expectedPaybaqs: []Paybaq{
				{From: 2, To: 1, Amount: 50},
			},
			expectedBalance: map[int]float64{
				1: 50,
				2: -50,
			},
		},
		{
			name: "Unequal split (one person pays for another)",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
				},
				Payments: []Payment{
					{Id: 1, Amount: 100, PaidBy: 1, PaidFor: []int{2}},
				},
			},
			expectedPaybaqs: []Paybaq{
				{From: 2, To: 1, Amount: 100},
			},
			expectedBalance: map[int]float64{
				1: 100,
				2: -100,
			},
		},
		{
			name: "Three people, one payer",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
					3: {Id: 3, Name: "Charlie"},
				},
				Payments: []Payment{
					{Id: 1, Amount: 90, PaidBy: 1, PaidFor: []int{1, 2, 3}},
				},
			},
			expectedPaybaqs: []Paybaq{
				{From: 2, To: 1, Amount: 30},
				{From: 3, To: 1, Amount: 30},
			},
			expectedBalance: map[int]float64{
				1: 60,
				2: -30,
				3: -30,
			},
		},
		{
			name: "Multiple payments",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
				},
				Payments: []Payment{
					{Id: 1, Amount: 100, PaidBy: 1, PaidFor: []int{1, 2}}, // Bob owes Alice 50
					{Id: 2, Amount: 40, PaidBy: 2, PaidFor: []int{1, 2}},  // Alice owes Bob 20
				},
			},
			expectedPaybaqs: []Paybaq{
				{From: 2, To: 1, Amount: 30},
			},
			expectedBalance: map[int]float64{
				1: 30,
				2: -30,
			},
		},
		{
			name: "Complex scenario",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
					3: {Id: 3, Name: "Charlie"},
				},
				Payments: []Payment{
					{Id: 1, Amount: 60, PaidBy: 1, PaidFor: []int{1, 2, 3}}, // A pays 60 for A,B,C (share 20). A:+40, B:-20, C:-20
					{Id: 2, Amount: 30, PaidBy: 2, PaidFor: []int{2, 3}},    // B pays 30 for B,C (share 15). B:+15, C:-15
				},
			},
			// Net:
			// A: +40
			// B: -20 + 15 = -5
			// C: -20 - 15 = -35
			expectedPaybaqs: []Paybaq{
				{From: 2, To: 1, Amount: 5},
				{From: 3, To: 1, Amount: 35},
			},
			expectedBalance: map[int]float64{
				1: 40,
				2: -5,
				3: -35,
			},
		},
		{
			name: "No payments",
			ledger: Ledger{
				People: map[int]Person{
					1: {Id: 1, Name: "Alice"},
					2: {Id: 2, Name: "Bob"},
				},
				Payments: []Payment{},
			},
			expectedPaybaqs: []Paybaq{},
			expectedBalance: map[int]float64{
				1: 0,
				2: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UpdateLedgerBalancesAndPaybaqs(&tt.ledger)

			// Check balances
			for id, expectedBal := range tt.expectedBalance {
				person, exists := tt.ledger.People[id]
				if !exists {
					t.Errorf("Person %d not found in ledger", id)
					continue
				}
				if person.Balance != expectedBal {
					t.Errorf("Person %d balance: got %f, want %f", id, person.Balance, expectedBal)
				}
			}

			// Check paybaqs
			// Note: The order of paybaqs might vary depending on map iteration order,
			// but for these simple cases it should be deterministic enough or we can relax the check.
			// For strict checking we'd need to sort or use a set comparison.
			// Given the implementation iterates over maps, order is not guaranteed.
			// However, for simple cases with 1 paybaq it's fine.
			// For multiple paybaqs, we should check if the set of paybaqs matches.

			if len(tt.ledger.Paybaqs) != len(tt.expectedPaybaqs) {
				t.Errorf("Paybaqs length: got %d, want %d", len(tt.ledger.Paybaqs), len(tt.expectedPaybaqs))
			} else {
				// Simple check for now: check if each expected paybaq exists in result
				for _, expected := range tt.expectedPaybaqs {
					found := false
					for _, actual := range tt.ledger.Paybaqs {
						if actual.From == expected.From && actual.To == expected.To && actual.Amount == expected.Amount {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("Expected paybaq not found: %+v. Got: %+v", expected, tt.ledger.Paybaqs)
					}
				}
			}
		})
	}
}
