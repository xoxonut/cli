package study

import (
	"Twopc-cli/ipconfig"
	"Twopc-cli/twopcserver"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
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
var s = time.Now()
var e = s.Add(time.Minute)

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

func preCommit(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string) bool {

	req := twopcserver.BeginTransactionRequest{AccountId: int32(id), Amount: int32(amount), Uuid: uuid}
	ctx, cancel := context.WithDeadline(context.Background(), e)
	defer cancel()
	if _, err := (*cli).BeginTransaction(ctx, &req); err != nil {
		return false
	} else {
		return true
	}

}
func commit(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string, wg *sync.WaitGroup) {
	defer wg.Done()
	req := twopcserver.CommitRequest{AccountId: int32(id), Amount: int32(amount), Uuid: uuid}
	ctx, cancel := context.WithDeadline(context.Background(), e)
	defer cancel()
	_, err := (*cli).Commit(ctx, &req)
	if err != nil {
		fmt.Println("commit error:", err)
		return
	}
}
func abort(cli *twopcserver.TwoPhaseCommitServiceClient, id int, amount int, uuid string, wg *sync.WaitGroup) {
	defer wg.Done()
	req := twopcserver.AbortRequest{AccountId: int32(id), Uuid: uuid}
	ctx, cancel := context.WithDeadline(context.Background(), e)
	defer cancel()
	_, err := (*cli).Abort(ctx, &req)
	if err != nil {
		fmt.Println("abort error:", err)
		return
	}
}
func txn(giver_cli *twopcserver.TwoPhaseCommitServiceClient, receiver_cli *twopcserver.TwoPhaseCommitServiceClient,
	gid int, rid int, amount int, wg *sync.WaitGroup) {
	defer wg.Done()
	n := time.Now()
	uuid := uuid.New().String()
	inner_wg := &sync.WaitGroup{}
	g := preCommit(giver_cli, gid, amount, uuid)
	r := preCommit(receiver_cli, rid, amount, uuid)
	if g && r {
		//commit
		inner_wg.Add(2)
		go commit(giver_cli, gid, amount, uuid, inner_wg)
		go commit(receiver_cli, rid, amount, uuid, inner_wg)
	} else if !g && !r {
		return
	} else if g {
		fmt.Println("abort g", g)
		inner_wg.Add(1)
		go abort(giver_cli, rid, amount, uuid, inner_wg)
	} else if r {
		fmt.Println("abort r", r)
		inner_wg.Add(1)
		go abort(receiver_cli, rid, amount, uuid, inner_wg)
	}
	inner_wg.Wait()
	if time.Now().Before(e) {
		AVG = append(AVG, time.Since(n).Seconds())
	}
}
func Test(lim int) {
	initClients()
	limiter := rate.NewLimiter(rate.Limit(lim), lim)
	rand.Seed(time.Now().UnixNano())
	wg := &sync.WaitGroup{}
	AVG = make([]float64, 0, lim)
	s = time.Now()
	e = s.Add(time.Minute)
	cnt := 0
	time.Sleep(time.Second)
	for time.Now().Before(e) {
		limiter.Wait(context.Background())
		gid := rand.Intn(10000) + 1
		rid := rand.Intn(10000) + 1
		wg.Add(1)
		cnt++
		go txn(&kafka_client, &couchDB_client, gid, rid, 1, wg)
	}
	// fin := time.Now()
	wg.Wait()
	fmt.Println(len(AVG))
	logger.Println(lim, "rps: ", float64(cnt)/time.Minute.Seconds())
	logger.Println(lim, "tps: ", float64(len(AVG))/time.Minute.Seconds())
	sum := 0.0
	max := 0.0
	for _, v := range AVG {
		sum += v
		if v > max {
			max = v
		}
	}
	// logger.Println(lim, "max: ", max)
	logger.Println(lim, "avg: ", sum/float64(len(AVG)), "\n")
}

var AVG []float64
var f, _ = os.OpenFile("record.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
var logger = log.New(io.Writer(f), "", 0)
