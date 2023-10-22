package main

import (
	"Twopc-cli/ipconfig"
	ui "Twopc-cli/prompt-ui"
	"Twopc-cli/twopcserver"
	util "Twopc-cli/utilities"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/c-bata/go-prompt"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

var kafka_client twopcserver.TwoPhaseCommitServiceClient
var couchDB_client twopcserver.TwoPhaseCommitServiceClient

func checkAllTrue(ch chan bool) bool {
	for i := 0; i < 2; i++ {
		flag := <-ch
		if !flag {
			return false
		}
	}
	return true
}
func executor(in string) {
	in = strings.TrimSpace(in)
	if in == "exit" {
		fmt.Println("Bye!")
		os.Exit(0)
	}
	blocks := strings.Split(in, " ")
	op := util.Str2op(blocks[0])
	var cli *twopcserver.TwoPhaseCommitServiceClient
	switch op {
	case util.CREATE:
		if len(blocks) != 3 {
			fmt.Println("Invalid create command")
			return
		}
		account_id := blocks[2]
		req, err := util.GetCreateAccountRequest(account_id)
		if err != nil {
			fmt.Println("Invalid account id")
			return
		}
		if blocks[1] == "kafka" {
			cli = &kafka_client
		} else {
			cli = &couchDB_client
		}
		res, err := (*cli).CreateAccount(context.Background(), req)
		if err != nil {
			fmt.Println("Create account error:", err)
		} else {
			fmt.Println("Create account success:", res)
		}
		return
	case util.READ:
		if len(blocks) != 3 {
			fmt.Println("Invalid read command")
			return
		}
		account_id := blocks[2]
		req, err := util.GetReadAccountRequest(account_id)
		if err != nil {
			fmt.Println("Invalid account id")
			return
		}
		if blocks[1] == "kafka" {
			cli = &kafka_client
		} else {
			cli = &couchDB_client
		}
		res, err := (*cli).ReadAccount(context.Background(), req)
		if err != nil {
			fmt.Println("Read account error:", err)
		} else {
			fmt.Println("Read account success:", res)
		}
		return
	case util.UPDATE:
		if len(blocks) != 4 {
			fmt.Println("Invalid update command")
			return
		}
		account_id := blocks[2]
		amount := blocks[3]
		req, err := util.GetUpdateAccountRequest(account_id, amount)
		if err != nil {
			fmt.Println("Invalid account id")
			return
		}
		if blocks[1] == "kafka" {
			cli = &kafka_client
		} else {
			cli = &couchDB_client
		}
		res, err := (*cli).UpdateAccount(context.Background(), req)
		if err != nil {
			fmt.Println("Update account error:", err)
		} else {
			fmt.Println("Update account success:", res)
		}
		return
	case util.DELETE:
		if len(blocks) != 3 {
			fmt.Println("Invalid delete command")
			return
		}
		account_id := blocks[2]
		req, err := util.GetDeleteAccountRequest(account_id)
		if err != nil {
			fmt.Println("Invalid account id")
			return
		}
		if blocks[1] == "kafka" {
			cli = &kafka_client
		} else {
			cli = &couchDB_client
		}
		res, err := (*cli).DeleteAccount(context.Background(), req)
		if err != nil {
			fmt.Println("Delete account error:", err)
		} else {
			fmt.Println("Delete account success:", res)
		}
		return
	case util.SEND:
		if len(blocks) != 6 {
			fmt.Println("Invalid send command")
			return
		}
		var giver_cli *twopcserver.TwoPhaseCommitServiceClient
		var receiver_cli *twopcserver.TwoPhaseCommitServiceClient
		giver_id := blocks[2]
		receiver_id := blocks[4]
		amount := blocks[5]
		if blocks[1] == "kafka" {
			giver_cli = &kafka_client
		} else {
			giver_cli = &couchDB_client
		}
		if blocks[3] == "kafka" {
			receiver_cli = &kafka_client
		} else {
			receiver_cli = &couchDB_client
		}

		giver_txn_req, err := util.GetBeginTransactionRequest(giver_id, "-"+amount)
		if err != nil {
			fmt.Println("Invalid giver data")
		}
		receiver_txn_req, err := util.GetBeginTransactionRequest(receiver_id, amount)
		if err != nil {
			fmt.Println("Invalid receiver data")
		}
		wg := &sync.WaitGroup{}
		wg.Add(2)
		ch := make(chan bool, 2)
		go util.BeginTXN(wg, giver_cli, giver_txn_req, &ch)
		go util.BeginTXN(wg, receiver_cli, receiver_txn_req, &ch)
		wg.Wait()
		allTrue := checkAllTrue(ch)
		close(ch)
		if allTrue {
			giver_commit_req, err := util.GetCommitRequest(giver_id, "-"+amount)
			if err != nil {
				fmt.Println("Invalid giver data")
			}
			receiver_commit_req, err := util.GetCommitRequest(receiver_id, amount)
			if err != nil {
				fmt.Println("Invalid receiver data")
			}
			wg.Add(2)
			go util.CommitTXN(wg, giver_cli, giver_commit_req)
			go util.CommitTXN(wg, receiver_cli, receiver_commit_req)
			wg.Wait()
		} else {
			giver_abort_req, err := util.GetAbortRequest(giver_id)
			if err != nil {
				fmt.Println("Invalid giver data")
			}
			receiver_abort_req, err := util.GetAbortRequest(receiver_id)
			if err != nil {
				fmt.Println("Invalid receiver data")
			}
			wg.Add(2)
			go util.AbortTXN(wg, giver_cli, giver_abort_req)
			go util.AbortTXN(wg, receiver_cli, receiver_abort_req)
			wg.Wait()
		}
		return
	case util.RESET:
		if len(blocks) != 2 {
			fmt.Println("Invalid reset command")
			return
		}
		if blocks[1] == "kafka" {
			cli = &kafka_client
		} else {
			cli = &couchDB_client
		}
		res, err := (*cli).Reset(context.Background(), &emptypb.Empty{})
		if err != nil {
			fmt.Println("Reset error:", err)
		} else {
			fmt.Println("Reset success:", res)
		}
		return
	default:
		fmt.Println("Invalid operation")
		return
	}
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
	}
	defer kafka_conn.Close()
	kafka_client = twopcserver.NewTwoPhaseCommitServiceClient(kafka_conn)
	couchDB_conn, err := grpc.Dial(ip.Kafka_ip, grpc.WithInsecure())
	couchDB_client = twopcserver.NewTwoPhaseCommitServiceClient(couchDB_conn)
	if err != nil {
		fmt.Println("dial couchDB error:", err)
	}
	defer couchDB_conn.Close()
	couchDB_client = twopcserver.NewTwoPhaseCommitServiceClient(couchDB_conn)
	p := prompt.New(
		executor,
		ui.Completer,
		prompt.OptionPrefix(">>> "),
		prompt.OptionTitle("2PC CLI"),
	)
	p.Run()

}
