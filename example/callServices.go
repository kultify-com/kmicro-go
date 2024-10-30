package main

import (
	"context"
	"log"

	"github.com/kultify-com/kmicro-go"
)

func main() {
	ctx := context.Background()
	knownHeaders := []string{"X-AUTH"}
	// service 1
	node := kmicro.NewKMicro("service1", "1.0.0", knownHeaders)
	err := node.Start(ctx, "nats://localhost:4222")
	defer node.Stop()
	if err != nil {
		log.Fatal(err)
	}

	node.AddEndpoint(ctx, "hello", func(ctx context.Context, data []byte) ([]byte, error) {
		node.Logger("hello endpoint").Info("handle hello")
		return []byte{1, 2, 3}, nil
	})

	// service 2
	node2 := kmicro.NewKMicro("service2", "1.0.0", knownHeaders)
	err = node2.Start(ctx, "nats://localhost:4222")
	defer node2.Stop()
	if err != nil {
		log.Fatal(err)
	}

	node2.AddEndpoint(ctx, "get_data", func(ctx context.Context, data []byte) ([]byte, error) {
		// return some json data
		node2.Logger("get data endpoint").Info("handle get data")
		s1Result, err := node2.Call(ctx, "service1.hello", nil)
		if err != nil {
			return []byte{}, err
		}
		var result = []byte{1, 2}
		result = append(result, s1Result...)
		return result, nil
	})

	// caller
	caller := kmicro.NewKMicro("caller", "1.0.0", knownHeaders)
	err = caller.Start(ctx, "nats://localhost:4222")
	defer caller.Stop()
	if err != nil {
		log.Fatal(err)
	}
	_, err = caller.Call(ctx, "service2.get_data", []byte{})
	if err != nil {
		log.Fatal(err)
	}
}
