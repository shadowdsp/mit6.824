# mit6.824
mit6.824 Distributed System 2020

# Debug Tips

1. In golang1.13, you should change the import path from `"../mr"` to `"mit6.824/src/mr"`

# Run

run master:

`go run mrmaster.go pg-*.txt > master.log 2>&1`

run worker:

`go build -buildmode=plugin ../mrapps/wc.go && go run mrworker.go wc.so > worker.log 2>&1`

# TODO

1. map 结果不正确，每个输出文件只有一个单词，被 overwrite 了。
2. reduce 结果不正确，输出为空。
