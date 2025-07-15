package transfer_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/kultify-com/kmicro-go/transfer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	testContainerNats "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	setup(ctx)
	code := m.Run()
	teardown()
	os.Exit(code)
}

var natsContainer *testContainerNats.NATSContainer
var nc *nats.Conn
var js jetstream.JetStream

func setup(ctx context.Context) {
	natsContainer, err := testContainerNats.Run(ctx, "nats:2.11")
	if err != nil {
		log.Fatalf("failed to start NATS container: %s", err)
	}
	uri, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		log.Printf("failed to get connection string: %s", err)
		return
	}
	nc, err := nats.Connect(uri, nats.UserInfo(natsContainer.User, natsContainer.Password))
	if err != nil {
		log.Printf("failed to connect to NATS: %s", err)
		return
	}
	js, err = jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to get JetStream context: %s", err)
	}
}

func teardown() {
	nc.Close()
	if err := testcontainers.TerminateContainer(natsContainer); err != nil {
		log.Printf("failed to terminate container: %s", err)
	}
}

func TestStoreData(t *testing.T) {
	transferSvc := transfer.NewTransferService(js)
	err := transferSvc.CreateBucket(t.Context(), "test-bucket", transfer.DefaultTTL)
	if err != nil {
		t.Fatalf("failed to create bucket: %s", err)
	}
	defer transferSvc.DeleteBucket(t.Context(), "test-bucket")
	ref := transfer.TransferReference{
		Bucket: "test-bucket",
		Key:    "test-key",
	}
	data := []byte("test data")
	err = transferSvc.Write(t.Context(), ref, data)
	if err != nil {
		t.Fatalf("failed to write data: %s", err)
	}
	readData, err := transferSvc.Read(t.Context(), ref)
	if err != nil {
		t.Fatalf("failed to read data: %s", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("read data does not match written data: got %s, want %s", readData, data)
	}
	err = transferSvc.Delete(t.Context(), ref)
	if err != nil {
		t.Fatalf("failed to delete data: %s", err)
	}
}
