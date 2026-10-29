# go-saga

[![ci](https://github.com/Urthella/go-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/Urthella/go-saga/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Urthella/go-saga.svg)](https://pkg.go.dev/github.com/Urthella/go-saga)

Sagas for Go: business transactions that span several services, where every step has a compensating action and a failure undoes the completed steps in reverse. Progress is logged after every step, so a saga cut short by a crash or a deploy **resumes where it stopped**. Generic over your data type, standard library only.

```go
checkout := &saga.Saga[Order]{
    Retry: saga.Retry{Attempts: 4, Backoff: 50 * time.Millisecond},
    Steps: []saga.Step[Order]{
        {Name: "reserve stock", Do: reserve, Compensate: release},
        {Name: "charge card", Do: charge, Compensate: refund},
        {Name: "book shipment", Do: ship},
    },
}
res, err := checkout.Run(ctx, "order-"+o.ID, &o)
```

Output of [examples/order](examples/order/main.go) (`go run ./examples/order`):

```text
order 1001
  reserve stock      key=order-1001/reserve stock
  charge card        ch_1001
  book shipment      booked
  → completed

order 1002
  reserve stock      key=order-1002/reserve stock
  charge card        declined
  ↩ refund           nothing to refund
  ↩ release stock    key=order-1002/reserve stock
  → compensated (charge card: card declined)

order 1003
  reserve stock      key=order-1003/reserve stock
  charge card        ch_1003
  book shipment      carrier timeout, retrying
  book shipment      carrier timeout, retrying
  book shipment      booked
  → completed
```

## How it behaves

- **Retries** with exponential backoff for every `Do` and `Compensate`. Wrap an error in `saga.Permanent(err)` when retrying can't help (a declined card) to start compensating at once.
- **Idempotency keys**: each call gets `"<saga id>/<step name>"`, the same across retries and restarts, for the remote service to deduplicate on.
- **The failed step is compensated too.** A call that timed out may still have gone through on the other side, so compensations run for it as well and must be no-ops when there's nothing to undo (see the refund for order 1002).
- **Stuck, not silent**: if a compensation keeps failing, the others still run and the result is `Stuck` with the failing steps listed, for a human to resolve.
- **Resume**: every step's outcome and the data after it go to a `Log`. Running a saga again with the same ID restores the data and continues forward, or continues compensating, without repeating finished work. Running a finished saga returns its result.
- **Interruption isn't failure**: if the context ends mid-call, `Run` returns the context's error without recording a failure, so the next `Run` retries that step instead of rolling everything back.

| Result | Meaning |
|---|---|
| `Completed` | every step succeeded |
| `Compensated` | a step failed and every compensation succeeded; `FailedStep` and `Err` say why |
| `Stuck` | some compensations failed for good; `StuckSteps` lists them |

## Logs

`Log` is a two-method interface: `Append` an event, read a saga's `Events`.

| | |
|---|---|
| `MemoryLog` (default) | resumable within a process |
| `NewFileLog(dir)` | survives restarts: one JSON-lines file per saga, `fsync`ed on every append |
| your own | implement `Log` on your database; `Append` must be durable before it returns |

`FileLog` handles the crash cases: a last line torn by a crash mid-write was never acknowledged, so it is dropped, while a broken line anywhere else is reported as corruption instead of silently shortening the history. Any saga ID maps to a safe, distinct file name (`orders/2026:42` can't escape the directory). Run a given saga ID from one process at a time.

```go
log, err := saga.NewFileLog("/var/lib/myapp/sagas")
checkout.Log = log
```

## Tests

```bash
go test -race ./...
```

Compensation order (failed step included), retries with stable keys, permanent errors, stuck compensations, resuming after an interruption both mid-forward and mid-compensation, re-running a finished saga, and for `FileLog`: resuming from a fresh log on the same directory, torn last lines vs. corruption, and safe file names.

## License

[MIT](LICENSE)
