package utilities

import (
	"Twopc-cli/twopcserver"
	"context"
	"fmt"
	"strconv"
	"sync"
)

type operation int

const (
	IP operation = iota
	CREATE
	READ
	UPDATE
	DELETE
	SEND
	RESET
	TEST
)

func Str2op(str string) operation {
	switch str {
	case "ip":
		return IP
	case "create":
		return CREATE
	case "read":
		return READ
	case "update":
		return UPDATE
	case "delete":
		return DELETE
	case "send":
		return SEND
	case "reset":
		return RESET
	case "test":
		return TEST
	default:
		return -1
	}
}

func GetCreateAccountRequest(arg string) (*twopcserver.CreateAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 32)
	if err != nil {
		return nil, err
	}
	out := int32(id)
	fmt.Println("out:", out)
	request := twopcserver.CreateAccountRequest{AccountId: out}
	return &request, nil
}

func GetReadAccountRequest(arg string) (*twopcserver.ReadAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 32)
	out := int32(id)
	if err != nil {
		return nil, err
	}
	request := twopcserver.ReadAccountRequest{AccountId: out}
	return &request, nil
}

func GetDeleteAccountRequest(arg string) (*twopcserver.DeleteAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 32)
	if err != nil {
		return nil, err
	}
	request := twopcserver.DeleteAccountRequest{AccountId: int32(id)}
	return &request, nil
}

func GetUpdateAccountRequest(arg1 string, arg2 string) (*twopcserver.UpdateAccountRequest, error) {
	id, err := strconv.ParseInt(arg1, 10, 32)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseInt(arg2, 10, 32)
	if err != nil {
		return nil, err
	}
	request := twopcserver.UpdateAccountRequest{AccountId: int32(id), Amount: int32(amount)}
	return &request, nil
}

func GetBeginTransactionRequest(account_id string, amount string) (*twopcserver.BeginTransactionRequest, error) {
	id, err := strconv.ParseInt(account_id, 10, 32)
	if err != nil {
		return nil, err
	}
	a, err := strconv.ParseInt(amount, 10, 32)
	fmt.Println("amount:", a)
	if err != nil {
		return nil, err
	}
	fmt.Println("id:", id)
	request := twopcserver.BeginTransactionRequest{AccountId: int32(id), Amount: int32(a)}
	return &request, nil
}

func BeginTXN(wg *sync.WaitGroup, client *twopcserver.TwoPhaseCommitServiceClient, req *twopcserver.BeginTransactionRequest, ch *chan bool) {
	defer wg.Done()
	ctx := context.TODO()
	res, err := (*client).BeginTransaction(ctx, req)
	if err != nil {
		fmt.Println("begin transaction error:", err)
		*ch <- false
		return
	}
	fmt.Println(res)
	*ch <- true
}
func GetCommitRequest(account_id string, amount string) (*twopcserver.CommitRequest, error) {
	id, err := strconv.ParseInt(account_id, 10, 32)
	if err != nil {
		return nil, err
	}
	a, err := strconv.ParseInt(amount, 10, 32)
	if err != nil {
		return nil, err
	}
	request := twopcserver.CommitRequest{AccountId: int32(id), Amount: int32(a)}
	return &request, nil
}

func CommitTXN(wg *sync.WaitGroup, client *twopcserver.TwoPhaseCommitServiceClient, req *twopcserver.CommitRequest) {
	defer wg.Done()
	ctx := context.TODO()
	res, err := (*client).Commit(ctx, req)
	if err != nil {
		fmt.Println("commit transaction error:", err)
		return
	}
	fmt.Println(res)
}
func GetAbortRequest(account_id string) (*twopcserver.AbortRequest, error) {
	id, err := strconv.ParseInt(account_id, 10, 32)
	if err != nil {
		return nil, err
	}
	request := twopcserver.AbortRequest{AccountId: int32(id)}
	return &request, nil
}
func AbortTXN(wg *sync.WaitGroup, client *twopcserver.TwoPhaseCommitServiceClient, req *twopcserver.AbortRequest) {
	defer wg.Done()
	ctx := context.TODO()
	res, err := (*client).Abort(ctx, req)
	if err != nil {
		fmt.Println("abort transaction error:", err)
		return
	}
	fmt.Println(res)
}
