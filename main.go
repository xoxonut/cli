package main

import (
	"Twopc-cli/ipconfig"
	"Twopc-cli/twopcserver"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type operation int

const (
	IP operation = iota
	CREATE
	READ
	UPDATE
	DELETE
	TXNOK
	TXNBAD
	RESET
)

func str2op(str string) operation {
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
	case "txnok":
		return TXNOK
	case "txnbad":
		return TXNBAD
	case "reset":
		return RESET
	default:
		return -1
	}
}

func GetCreateAccountRequest(arg string) (*twopcserver.CreateAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.CreateAccountRequest{AccountId: id}
	return &request, nil
}

func GetReadAccountRequest(arg string) (*twopcserver.ReadAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.ReadAccountRequest{AccountId: id}
	return &request, nil
}

func GetDeleteAccountRequest(arg string) (*twopcserver.DeleteAccountRequest, error) {
	id, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.DeleteAccountRequest{AccountId: id}
	return &request, nil
}

func GetUpdateAccountRequest(arg1 string, arg2 string) (*twopcserver.UpdateAccountRequest, error) {
	id, err := strconv.ParseInt(arg1, 10, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseInt(arg2, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.UpdateAccountRequest{AccountId: id, Amount: amount}
	return &request, nil
}

func GetBeginTransactionRequest(account_id string, amount int64, txn_id int64) (*twopcserver.BeginTransactionRequest, error) {
	aid, err := strconv.ParseInt(account_id, 10, 64)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	request := twopcserver.BeginTransactionRequest{AccountId: aid, Amount: amount, TransactionId: txn_id}
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
func GetCommitRequest(account_id string, txn_id int64) (*twopcserver.CommitRequest, error) {
	aid, err := strconv.ParseInt(account_id, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.CommitRequest{AccountId: aid, TransactionId: txn_id}
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
func GetAbortRequest(account_id string, txn_id int64) (*twopcserver.AbortRequest, error) {
	aid, err := strconv.ParseInt(account_id, 10, 64)
	if err != nil {
		return nil, err
	}
	request := twopcserver.AbortRequest{AccountId: aid, TransactionId: txn_id}
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

func main() {
	ip, err := ipconfig.GetIPs()
	if err != nil {
		fmt.Println("get ip error:", err)
		return
	}
	kafka_conn, err := grpc.Dial(ip.Kafka_ip, grpc.WithInsecure())
	if err != nil {
		fmt.Println("dial kafka error:", err)
		//return
	}
	defer kafka_conn.Close()
	kafka_client := twopcserver.NewTwoPhaseCommitServiceClient(kafka_conn)
	couchDB_conn, err := grpc.Dial(ip.CouchDB_ip, grpc.WithInsecure())
	if err != nil {
		fmt.Println("dial couchDB error:", err)
		//return
	}
	defer couchDB_conn.Close()
	couchDB_client := twopcserver.NewTwoPhaseCommitServiceClient(couchDB_conn)
	ctx := context.TODO()

	arg_nums := len(os.Args)
	s := os.Args[1]
	op := str2op(s)

	switch op {
	case IP:
		if arg_nums != 4 {
			os.Exit(1)
		}
		new_ip := os.Args[3]
		if os.Args[2] == "kafka" {
			ipconfig.SetIP(ipconfig.KAFKA, new_ip)
		} else if os.Args[2] == "couchdb" {
			ipconfig.SetIP(ipconfig.COUCHDB, new_ip)
		}
	case CREATE:
		if arg_nums != 4 {
			os.Exit(1)
		}
		account_id := os.Args[3]
		req, err := GetCreateAccountRequest(account_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		if os.Args[2] == "kafka" {
			res, err := kafka_client.CreateAccount(ctx, req)
			if err != nil {
				fmt.Println("create account error:", err)
				return
			}
			fmt.Println(res)
		} else if os.Args[2] == "couchdb" {
			res, err := couchDB_client.CreateAccount(ctx, req)
			if err != nil {
				fmt.Println("create account error:", err)
				return
			}
			fmt.Println(res)
		}
	case READ:
		if arg_nums != 4 {
			os.Exit(1)
		}
		account_id := os.Args[3]
		req, err := GetReadAccountRequest(account_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		if os.Args[2] == "kafka" {
			res, err := kafka_client.ReadAccount(ctx, req)
			if err != nil {
				fmt.Println("read account error:", err)
				return
			}
			fmt.Println(res)
		} else if os.Args[2] == "couchdb" {
			res, err := couchDB_client.ReadAccount(ctx, req)
			if err != nil {
				fmt.Println("read account error:", err)
				return
			}
			fmt.Println(res)
		}
	case UPDATE:
		if arg_nums != 5 {
			os.Exit(1)
		}
		account_id := os.Args[3]
		amount := os.Args[4]
		req, err := GetUpdateAccountRequest(account_id, amount)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		if os.Args[2] == "kafka" {
			res, err := kafka_client.UpdateAccount(ctx, req)
			if err != nil {
				fmt.Println("update account error:", err)
				return
			}
			fmt.Println(res)
		} else if os.Args[2] == "couchdb" {
			res, err := couchDB_client.UpdateAccount(ctx, req)
			if err != nil {
				fmt.Println("update account error:", err)
				return
			}
			fmt.Println(res)
		}
	case DELETE:
		if arg_nums != 4 {
			os.Exit(1)
		}
		account_id := os.Args[3]
		req, err := GetDeleteAccountRequest(account_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		if os.Args[2] == "kafka" {
			res, err := kafka_client.DeleteAccount(ctx, req)
			if err != nil {
				fmt.Println("delete account error:", err)
				return
			}
			fmt.Println(res)
		} else if os.Args[2] == "couchdb" {
			res, err := couchDB_client.DeleteAccount(ctx, req)
			if err != nil {
				fmt.Println("delete account error:", err)
				return
			}
			fmt.Println(res)
		}
	case TXNOK:
		if arg_nums != 7 {
			os.Exit(1)
		}
		giver_db := os.Args[2]
		giver_id := os.Args[3]
		amount, err := strconv.ParseInt(os.Args[6], 10, 64)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		rand.Seed(time.Now().UnixNano())
		txn_id := rand.Int63()
		if txn_id < 0 {
			txn_id = -txn_id
		}
		giver_begin_req, err := GetBeginTransactionRequest(giver_id, -amount, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		reciever_db := os.Args[4]
		receiver_id := os.Args[5]
		receiver_begin_req, err := GetBeginTransactionRequest(receiver_id, amount, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}

		wg := &sync.WaitGroup{}
		wg.Add(2)
		var giver_conn *twopcserver.TwoPhaseCommitServiceClient
		var receiver_conn *twopcserver.TwoPhaseCommitServiceClient
		if giver_db == "kafka" {
			giver_conn = &kafka_client
		} else if giver_db == "couchdb" {
			giver_conn = &couchDB_client
		}

		if reciever_db == "kafka" {
			receiver_conn = &kafka_client
		} else if reciever_db == "couchdb" {
			receiver_conn = &couchDB_client
		}
		ch := make(chan bool, 2)
		go BeginTXN(wg, giver_conn, giver_begin_req, &ch)
		go BeginTXN(wg, receiver_conn, receiver_begin_req, &ch)
		wg.Wait()
		for i := 0; i < 2; i++ {
			flag := <-ch
			if !flag {
				return
			}
		}
		close(ch)
		giver_commit_req, err := GetCommitRequest(giver_id, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		receiver_commit_req, err := GetCommitRequest(receiver_id, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		wg.Add(2)
		go CommitTXN(wg, giver_conn, giver_commit_req)
		go CommitTXN(wg, receiver_conn, receiver_commit_req)
		wg.Wait()
	case TXNBAD:
		if arg_nums != 7 {
			os.Exit(1)
		}
		giver_db := os.Args[2]
		giver_id := os.Args[3]
		amount, err := strconv.ParseInt(os.Args[6], 10, 64)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		rand.Seed(time.Now().UnixNano())
		txn_id := rand.Int63()
		if txn_id < 0 {
			txn_id = -txn_id
		}
		giver_begin_req, err := GetBeginTransactionRequest(giver_id, -amount, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		reciever_db := os.Args[4]
		receiver_id := os.Args[5]
		receiver_begin_req, err := GetBeginTransactionRequest(receiver_id, amount, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}

		wg := &sync.WaitGroup{}
		wg.Add(2)
		var giver_conn *twopcserver.TwoPhaseCommitServiceClient
		var receiver_conn *twopcserver.TwoPhaseCommitServiceClient
		if giver_db == "kafka" {
			giver_conn = &kafka_client
		} else if giver_db == "couchdb" {
			giver_conn = &couchDB_client
		}

		if reciever_db == "kafka" {
			receiver_conn = &kafka_client
		} else if reciever_db == "couchdb" {
			receiver_conn = &couchDB_client
		}
		ch := make(chan bool, 2)
		go BeginTXN(wg, giver_conn, giver_begin_req, &ch)
		go BeginTXN(wg, receiver_conn, receiver_begin_req, &ch)
		wg.Wait()

		giver_abort_req, err := GetAbortRequest(giver_id, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		receiver_abort_req, err := GetAbortRequest(receiver_id, txn_id)
		if err != nil {
			fmt.Println("get request error:", err)
			return
		}
		wg.Add(2)
		go AbortTXN(wg, giver_conn, giver_abort_req)
		go AbortTXN(wg, receiver_conn, receiver_abort_req)
		wg.Wait()
	case RESET:
		if arg_nums != 3 {
			os.Exit(1)
		}
		if os.Args[2] == "kafka" {
			res, err := kafka_client.Reset(ctx, &emptypb.Empty{})
			if err != nil {
				fmt.Println("reset error:", err)
				return
			}
			fmt.Println(res)
		} else if os.Args[2] == "couchdb" {
			res, err := couchDB_client.Reset(ctx, &emptypb.Empty{})
			if err != nil {
				fmt.Println("reset error:", err)
				return
			}
			fmt.Println(res)
		}
	default:
		fmt.Print("invalid operation")
		os.Exit(1)
	}
}
