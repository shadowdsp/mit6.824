# mit6.824
mit6.824 Distributed System 2020

## Lab1 MapReduce

### Debug Tips

1. In golang1.13, you should change the import path from `"../mr"` to `"mit6.824/src/mr"`

### Run

run master:

`go run mrmaster.go pg-*.txt > master.log 2>&1`

run worker:

`go build -buildmode=plugin ../mrapps/wc.go && go run mrworker.go wc.so > worker.log 2>&1`

