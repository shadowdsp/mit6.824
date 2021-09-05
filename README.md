# mit6.824
mit6.824 Distributed System 2020

## Lab1 MapReduce

[Implementation Blog](https://shadowdsp.github.io/2021/03/12/MIT6-824-2020-Lab1-MapReduce-%E5%AE%9E%E7%8E%B0/)
### Debug Tips

1. In golang1.13, you should change the import path from `"../mr"` to `"mit6.824/src/mr"`

### Run

run master:

`go run mrmaster.go pg-*.txt > master.log 2>&1`

run worker:

`go build -buildmode=plugin ../mrapps/wc.go && go run mrworker.go wc.so > worker.log 2>&1`

## Lab2A Raft Leader Election

[Implementation Blog](https://shadowdsp.github.io/2021/09/05/MIT6-824-2020-Lab2-A-Raft-Leader-Election/)
