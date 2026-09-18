package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "grpc-perf/grpc/internal/codec"
	pb "grpc-perf/grpc/proto"
)

type clientState struct {
	id        atomic.Uint64
	sent      atomic.Uint64
	responses atomic.Uint64
}

func main() {
	streams := flag.Int("streams", 1, "number of parallel streams on one gRPC connection")
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatalf("usage: %s [-streams N] <server-address> <port>", os.Args[0])
	}
	if *streams < 1 {
		log.Fatal("streams must be at least 1")
	}
	port, err := strconv.Atoi(flag.Arg(1))
	if err != nil || port < 1 || port > 65535 {
		log.Fatal("port must be a number from 1 to 65535")
	}
	address := net.JoinHostPort(flag.Arg(0), strconv.Itoa(port))
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithReadBufferSize(64<<10),
		grpc.WithWriteBufferSize(64<<10),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	client := pb.NewCounterClient(connection)

	var state clientState
	go state.reportRPS()
	var running sync.WaitGroup
	for i := 0; i < *streams; i++ {
		running.Add(1)
		go func() {
			defer running.Done()
			for {
				if err := state.run(client); err != nil {
					log.Printf("%s: %v; reconnecting stream in 1s", address, err)
					time.Sleep(time.Second)
				}
			}
		}()
	}
	running.Wait()
}

func (c *clientState) run(client pb.CounterClient) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Exchange(ctx)
	if err != nil {
		return err
	}

	sendErrors := make(chan error, 1)
	go func() {
		sendErrors <- c.sendRequests(stream)
		cancel()
	}()

	receiveErr := c.receiveResponses(stream)
	cancel()
	sendErr := <-sendErrors
	if sendErr != nil && (errors.Is(receiveErr, context.Canceled) || errors.Is(receiveErr, io.EOF)) {
		return sendErr
	}
	return receiveErr
}

func (c *clientState) sendRequests(stream pb.Counter_ExchangeClient) error {
	for {
		id := c.id.Add(1)
		if err := stream.SendMsg(&pb.Request{Id: id}); err != nil {
			return err
		}
		c.sent.Add(1)
	}
}

func (c *clientState) receiveResponses(stream pb.Counter_ExchangeClient) error {
	response := pb.ResponseFromVTPool()
	defer response.ReturnToVTPool()
	var count uint64
	defer func() {
		if count > 0 {
			c.responses.Add(count)
		}
	}()
	for {
		response.ResetVT()
		if err := stream.RecvMsg(response); err != nil {
			return err
		}
		if response.Id == 0 || response.Id > c.id.Load() || response.Data == 0 {
			return fmt.Errorf("invalid response: id=%d data=%d", response.Id, response.Data)
		}
		count++
		if count == 256 {
			c.responses.Add(count)
			count = 0
		}
	}
}

func (c *clientState) reportRPS() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastTime := time.Now()
	var lastSent, lastResponses uint64
	for range ticker.C {
		now := time.Now()
		sent := c.sent.Load()
		responses := c.responses.Load()
		seconds := now.Sub(lastTime).Seconds()
		fmt.Printf("sent/s=%.0f responses/s=%.0f total_responses=%d\n",
			float64(sent-lastSent)/seconds,
			float64(responses-lastResponses)/seconds,
			responses,
		)
		lastTime, lastSent, lastResponses = now, sent, responses
	}
}
