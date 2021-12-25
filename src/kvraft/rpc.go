package kvraft

import (
	"fmt"
	"time"

	"mit6.824/src/labrpc"
)

type RpcCallFailedError struct {
	Method string
}

func (e *RpcCallFailedError) Error() string {
	return fmt.Sprintf("RPC call [%s] failed!", e.Method)
}

type RpcCallTimeoutError struct {
	Method string
}

func (e *RpcCallTimeoutError) Error() string {
	return fmt.Sprintf("RPC call [%s] timed out!", e.Method)
}

func RpcCall(clientEnd *labrpc.ClientEnd, method string, args interface{}, reply interface{}) error {
	var err error
	ok := clientEnd.Call(method, args, reply)
	if ok {
		err = nil
	} else {
		err = &RpcCallFailedError{Method: method}
	}
	return err
}

func RpcCallWithTimeout(clientEnd *labrpc.ClientEnd, method string, args interface{}, reply interface{}, timeoutLimit time.Duration) error {
	ch := make(chan error, 1)
	go func() {
		ok := clientEnd.Call(method, args, reply)
		if ok {
			ch <- nil
		} else {
			ch <- &RpcCallFailedError{Method: method}
		}
	}()

	var err error
	select {
	case err = <-ch:
	case <-time.After(timeoutLimit):
		err = &RpcCallTimeoutError{Method: method}
	}
	return err
}
