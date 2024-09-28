package main

import (
	"context"
	"log"

	kmicro "github.com/kultify-com/kmicro-go/pkg"
)

func main() {
	ctx := context.Background()

	// service 1
	node := kmicro.NewKMicro("service1", "1.0.0")
	err := node.Start(ctx, "nats://localhost:4222")
	defer node.Stop()
	if err != nil {
		log.Fatal(err)
	}

	node.AddEndpoint("hello", func(ctx context.Context, data []byte) ([]byte, error) {
		return []byte{1, 2, 3}, nil
	})

	// service 2
	node2 := kmicro.NewKMicro("service2", "1.0.0")
	err = node2.Start(ctx, "nats://localhost:4222")
	defer node2.Stop()
	if err != nil {
		log.Fatal(err)
	}

	node2.AddEndpoint("get_data", func(ctx context.Context, data []byte) ([]byte, error) {
		// return some json data
		s1Result, err := kmicro.DoCall(ctx, "service1.hello", nil)
		if err != nil {
			return []byte{}, err
		}
		var result = []byte{1, 2}
		result = append(result, s1Result...)
		return result, nil
	})

	// caller
	caller := kmicro.NewKMicro("caller", "1.0.0")
	err = caller.Start(ctx, "nats://localhost:4222")
	defer caller.Stop()
	if err != nil {
		log.Fatal(err)
	}
	ctx = caller.GetContext(ctx)
	res, err := kmicro.DoCall(ctx, "service2.get_data", []byte{})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("got result %v", res)
}
