# make sure current dir is src/
root=$(pwd)
log_path=${root}/test_log.txt

echo "Save test log in path $log_path!"

cd raft && go test -race -run 2A >> $log_path 2>&1 && \
    go test -race -run 2B >> $log_path 2>&1 && \
    go test -race -run 2C >> $log_path 2>&1 && \
    cd $root/kvraft && go test -race -run 3A >> $log_path 2>&1 && \
    go test -race -run 3B >> $log_path 2>&1
