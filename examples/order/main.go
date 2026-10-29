// Command order runs a checkout saga against fake inventory, payment and shipping services:
// one order goes through, one card is declined, and one carrier times out twice first.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	saga "github.com/Urthella/go-saga"
)

type Order struct {
	ID       string
	Card     string
	ChargeID string
}

// timeouts counts down the carrier timeouts before a shipment goes through.
var timeouts = 2

func checkout() *saga.Saga[Order] {
	return &saga.Saga[Order]{
		Retry: saga.Retry{Attempts: 4, Backoff: 50 * time.Millisecond},
		Steps: []saga.Step[Order]{
			{
				Name: "reserve stock",
				Do: func(_ context.Context, o *Order, key string) error {
					fmt.Printf("  reserve stock      key=%s\n", key)
					return nil
				},
				Compensate: func(_ context.Context, o *Order, key string) error {
					fmt.Printf("  ↩ release stock    key=%s\n", key)
					return nil
				},
			},
			{
				Name: "charge card",
				Do: func(_ context.Context, o *Order, key string) error {
					if o.Card == "4000-0000-0000-0002" {
						fmt.Println("  charge card        declined")
						return saga.Permanent(errors.New("card declined"))
					}
					o.ChargeID = "ch_" + o.ID
					fmt.Printf("  charge card        %s\n", o.ChargeID)
					return nil
				},
				Compensate: func(_ context.Context, o *Order, key string) error {
					if o.ChargeID == "" { // the charge never happened: nothing to refund
						fmt.Println("  ↩ refund           nothing to refund")
						return nil
					}
					fmt.Printf("  ↩ refund           %s\n", o.ChargeID)
					o.ChargeID = ""
					return nil
				},
			},
			{
				Name: "book shipment",
				Do: func(_ context.Context, o *Order, key string) error {
					if o.ID == "1003" && timeouts > 0 {
						timeouts--
						fmt.Println("  book shipment      carrier timeout, retrying")
						return errors.New("carrier timeout")
					}
					fmt.Println("  book shipment      booked")
					return nil
				},
			},
		},
	}
}

func main() {
	for _, o := range []Order{
		{ID: "1001", Card: "4242-4242-4242-4242"},
		{ID: "1002", Card: "4000-0000-0000-0002"},
		{ID: "1003", Card: "4242-4242-4242-4242"},
	} {
		fmt.Printf("order %s\n", o.ID)
		res, err := checkout().Run(context.Background(), "order-"+o.ID, &o)
		if err != nil {
			fmt.Println("  error:", err)
			continue
		}
		fmt.Printf("  → %s", res.Status)
		if res.FailedStep != "" {
			fmt.Printf(" (%s: %v)", res.FailedStep, res.Err)
		}
		fmt.Print("\n\n")
	}
}
