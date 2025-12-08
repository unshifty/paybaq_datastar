package models

import (
	"maps"
	"math"
)

func UpdateLedgerBalancesAndPaybaqs(ledger *Ledger) {
	balances := computeBalances(ledger)
	workingBalances := maps.Clone(balances)
	paybaqs := getPaybaqs(workingBalances, []Paybaq{})
	for id, person := range ledger.People {
		person.Balance = balances[id]
		ledger.People[id] = person
	}
	ledger.Paybaqs = paybaqs
}

func GetBalancesAndPaybaqs(ledger *Ledger) (map[int]float64, []Paybaq) {
	balances := computeBalances(ledger)
	workingBalances := maps.Clone(balances)
	paybaqs := getPaybaqs(workingBalances, []Paybaq{})
	return balances, paybaqs
}

func getPaybaqs(balances map[int]float64, paybaqs []Paybaq) []Paybaq {
	payee := 0
	balanceDue := 0.0
	for personId, balance := range balances {
		if balance > 0.0 {
			payee = personId
			balanceDue = balance
			break
		}
	}
	if payee == 0 {
		return paybaqs
	}
	payer := 0
	balanceOwed := 0.0
	// find the first person with a matching balance owed, or any balance owed
	for personId, balance := range balances {
		if math.Abs(balance+balanceDue) < 0.01 {
			payer = personId
			balanceOwed = balance
			break
		} else if payer == 0 && balance < 0.0 {
			payer = personId
			balanceOwed = balance
		}
	}
	if balanceOwed == 0.0 {
		// if no balances owed, can't make a paybaq
		return paybaqs
	}

	diff := balanceOwed + balanceDue
	// if diff is less than zero, then positive balance is used up
	// if diff is greater than zero, then negative balance is used up
	// otherwise, diff is what is left
	newBalanceDue := math.Max(diff, 0.0)
	newBalanceOwed := math.Min(diff, 0.0)
	paybaqs = append(paybaqs, Paybaq{
		From:   payer,
		To:     payee,
		Amount: math.Round((balanceDue-newBalanceDue)*100) / 100,
	})
	balances[payer] = newBalanceOwed
	balances[payee] = newBalanceDue
	return getPaybaqs(balances, paybaqs)
}

func computeBalances(ledger *Ledger) map[int]float64 {
	balances := make(map[int]float64)
	for _, person := range ledger.People {
		balances[person.Id] = 0.0
	}
	for _, payment := range ledger.Payments {
		share := amountPerPerson(payment)
		if payment.PaidBy > 0 {
			balances[payment.PaidBy] += share * float64(len(payment.PaidFor))
			for _, personId := range payment.PaidFor {
				balances[personId] -= share
			}
		}
	}
	for personId, balance := range balances {
		if math.Abs(balance) < 0.01 {
			balance = 0.0
		}
		balances[personId] = balance
	}
	return balances
}

func amountPerPerson(payment Payment) float64 {
	if len(payment.PaidFor) == 0 {
		return 0.0
	}
	return payment.Amount / float64(len(payment.PaidFor))
}
