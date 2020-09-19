package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dgraph-io/dgo/x"
	"google.golang.org/grpc"
)

func alterSchema(dg *dgo.Dgraph, schema string) {
	op := api.Operation{Schema: schema}
	x.Check(dg.Alter(context.Background(), &op))
}

func check(err error) {
	if err != nil {
		panic("error")
	}
}

func main() {
	for i := 0; i < 100; i++ {
		test1()
		test2()
		fmt.Println(i)
	}
}

func test1() {
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	err0 := dg.Alter(context.Background(), &api.Operation{DropAll: true})
	check(err0)

	alterSchema(dg, "name: string .")
	txn := dg.NewTxn()
	defer txn.Discard(context.Background())

	// Do one query, so a new timestamp is assigned to the txn.
	q := `{me(func: uid(0x01)) { name }}`
	_, err2 := txn.Query(context.Background(), q)
	check(err2)

	var wg sync.WaitGroup
	wg.Add(2)
	start := time.Now()
	go func() {
		defer wg.Done()
		for time.Since(start) < 5*time.Second {
			mu := &api.Mutation{}
			mu.SetJson = []byte(`{"uid": "0x01", "name": "manish"}`)
			_, err3 := txn.Mutate(context.Background(), mu)
			check(err3)
		}
	}()

	go func() {
		defer wg.Done()
		for time.Since(start) < 5*time.Second {
			_, err4 := txn.Query(context.Background(), q)
			check(err4)
		}
	}()
	wg.Wait()
	fmt.Println("Done")
}

func test2() {
	conn, err := grpc.Dial("127.0.0.1:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	err0 := dg.Alter(context.Background(), &api.Operation{DropAll: true})
	check(err0)

	alterSchema(dg, "name: string .")

	txn := dg.NewTxn()
	mu := &api.Mutation{
		SetNquads: []byte(`_:1 <name> "abc" .`),
	}
	_, err = txn.Mutate(context.Background(), mu)
	check(err)
	err = txn.Discard(context.Background())
	// Since client is discarding this transaction server should not throw ErrAborted err.
	check(err)
}
