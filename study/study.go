package study

import (
	"Twopc-cli/ipconfig"
	"Twopc-cli/twopcserver"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var couchDB_conn *grpc.ClientConn
var kafka_conn *grpc.ClientConn
var couchDB_client twopcserver.TwoPhaseCommitServiceClient
var kafka_client twopcserver.TwoPhaseCommitServiceClient

func initClients() {
	ip, err := ipconfig.GetIPs()
	if err != nil {
		fmt.Println("get ip error:", err)
		return
	}
	kafka_conn, err = grpc.Dial(ip.Kafka_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("dial kafka error:", err)
	}
	kafka_client = twopcserver.NewTwoPhaseCommitServiceClient(kafka_conn)
	couchDB_conn, err = grpc.Dial(ip.CouchDB_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	couchDB_client = twopcserver.NewTwoPhaseCommitServiceClient(couchDB_conn)
	if err != nil {
		fmt.Println("dial couchDB error:", err)
	}
}

func preCommit(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string,
	wg *sync.WaitGroup, ch *chan bool) {
	defer wg.Done()
	req := twopcserver.BeginTransactionRequest{AccountId: int32(id), Amount: int32(amount), Uuid: uuid}
	_, err := (*cli).BeginTransaction(context.Background(), &req)
	if err != nil {
		fmt.Println("preCommit error:", err)
		*ch <- false
		return
	}
	*ch <- true
}
func commit(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string, wg *sync.WaitGroup) {
	defer wg.Done()
	req := twopcserver.CommitRequest{AccountId: int32(id), Amount: int32(amount), Uuid: uuid}
	_, err := (*cli).Commit(context.Background(), &req)
	if err != nil {
		fmt.Println("commit error:", err)
		return
	}
}
func abort(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string, wg *sync.WaitGroup) {
	defer wg.Done()
	req := twopcserver.AbortRequest{AccountId: int32(id), Uuid: uuid}
	_, err := (*cli).Abort(context.Background(), &req)
	if err != nil {
		fmt.Println("abort error:", err)
		return
	}
}
func txn(giver_cli *twopcserver.TwoPhaseCommitServiceClient, receiver_cli *twopcserver.TwoPhaseCommitServiceClient,
	gid int, rid int, amount int, wg *sync.WaitGroup) {
	defer wg.Done()
	uuid := uuid.New().String()
	ch := make(chan bool, 2)
	defer close(ch)
	inner_wg := &sync.WaitGroup{}
	inner_wg.Add(2)
	go preCommit(giver_cli, gid, amount, uuid, inner_wg, &ch)
	go preCommit(receiver_cli, rid, amount, uuid, inner_wg, &ch)
	inner_wg.Wait()
	inner_wg.Add(2)
	if (<-ch) && (<-ch) {
		//commit
		go commit(giver_cli, gid, -amount, uuid, inner_wg)
		go commit(receiver_cli, rid, amount, uuid, inner_wg)
	} else {
		//abort
		go abort(giver_cli, gid, -amount, uuid, inner_wg)
		go abort(receiver_cli, rid, amount, uuid, inner_wg)
	}
	inner_wg.Wait()
}
func Test(lim int) {
	initClients()
	limiter := rate.NewLimiter(rate.Limit(lim), lim)
	rand.Seed(time.Now().UnixNano())
	cli := [2]*twopcserver.TwoPhaseCommitServiceClient{&kafka_client, &couchDB_client}
	lim *= 60
	wg := &sync.WaitGroup{}
	s := time.Now()
	for i := 0; i < lim; i++ {
		limiter.Wait(context.Background())
		give := cli[rand.Intn(2)]
		receive := cli[rand.Intn(2)]
		gid := rand.Intn(10000) + 1
		rid := rand.Intn(10000) + 1
		amount := rand.Intn(5) + 1
		wg.Add(1)
		go txn(give, receive, gid, rid, amount, wg)
	}
	wg.Wait()
	fmt.Println("time: ", time.Since(s))
}