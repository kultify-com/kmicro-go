package transfer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testContainerNats "github.com/testcontainers/testcontainers-go/modules/nats"
)

var (
	ErrFileNotFound = fmt.Errorf("file not found")
	DefaultTTL      = time.Hour * 24 * 31 // 31 days
)

type TransferReference struct {
	Bucket string
	Key    string
}

type TrackLog struct {
	Operation string
	Ref       TransferReference
	Data      []byte
}

type TransferService struct {
	js             jetstream.JetStream
	enableTracking bool
	tracker        []TrackLog
	trackerMutex   sync.Mutex

	// nullable
	natsContainer *testContainerNats.NATSContainer
	natsConn      *nats.Conn
}

type TransferServiceInterface interface {
	CreateBucket(ctx context.Context, name string, ttl time.Duration) error
	DeleteBucket(ctx context.Context, name string) error
	Write(ctx context.Context, ref TransferReference, data []byte) error
	Read(ctx context.Context, ref TransferReference) ([]byte, error)
	Delete(ctx context.Context, ref TransferReference) error
	TearDown() error
}

type options struct {
	enableTracking bool

	// nullable settings

	natsContainer *testContainerNats.NATSContainer
	natsConn      *nats.Conn
}

// WithEnableTracking is an option to enable the tracking of all transfer operations.
func WithEnableTracking() func(*options) {
	return func(opts *options) {
		opts.enableTracking = true
	}
}

func withNATSContainer(nc *testContainerNats.NATSContainer) func(*options) {
	return func(opts *options) {
		opts.natsContainer = nc
	}
}

func withNatsConn(conn *nats.Conn) func(*options) {
	return func(opts *options) {
		opts.natsConn = conn
	}
}

// NewNullTransferService creates a TransferService with a NATS container for testing purposes.
func NewNullTransferService(opts ...func(*options)) *TransferService {
	ctx := context.Background()
	natsContainer, err := testContainerNats.Run(ctx, "nats:2.11")
	if err != nil {
		log.Fatalf("failed to start NATS container: %s", err)
	}
	uri, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("failed to get connection string: %s", err)

	}
	nc, err := nats.Connect(uri, nats.UserInfo(natsContainer.User, natsContainer.Password))
	if err != nil {
		log.Fatalf("failed to connect to NATS: %s", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("failed to get JetStream context: %s", err)
	}
	return NewTransferService(
		js,
		withNATSContainer(natsContainer),
		withNatsConn(nc),
	)
}

func NewTransferService(js jetstream.JetStream, opts ...func(*options)) *TransferService {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return &TransferService{
		js:             js,
		enableTracking: o.enableTracking,
		tracker:        []TrackLog{},
		trackerMutex:   sync.Mutex{},
	}
}

func (s *TransferService) SafeAppendTrackLog(operation string, ref TransferReference, data []byte) {
	if !s.enableTracking {
		return
	}
	s.trackerMutex.Lock()
	defer s.trackerMutex.Unlock()

	s.tracker = append(s.tracker, TrackLog{
		Operation: operation,
		Ref:       ref,
		Data:      data,
	})
}

func (s *TransferService) GetTrackLogs() []TrackLog {
	s.trackerMutex.Lock()
	defer s.trackerMutex.Unlock()
	logs := make([]TrackLog, len(s.tracker))
	copy(logs, s.tracker)
	return logs
}

func (s *TransferService) CreateBucket(ctx context.Context, name string, ttl time.Duration) error {
	_, err := s.js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: name,
		TTL:    ttl,
	})
	if err != nil {
		if err == jetstream.ErrBucketExists {
			return fmt.Errorf("bucket already exists: %w", err)
		}
		return fmt.Errorf("failed to create bucket: %w", err)
	}
	return nil
}

func (s *TransferService) DeleteBucket(ctx context.Context, name string) error {
	err := s.js.DeleteObjectStore(ctx, name)
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	return nil
}

func (s *TransferService) Write(ctx context.Context, ref TransferReference, data []byte) error {
	os, err := s.js.ObjectStore(ctx, ref.Bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		err := s.CreateBucket(ctx, ref.Bucket, DefaultTTL)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		os, err = s.js.ObjectStore(ctx, ref.Bucket)
		if err != nil {
			return fmt.Errorf("failed to get store after creating bucket: %w", err)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}
	_, err = os.PutBytes(ctx, ref.Key, data)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	s.SafeAppendTrackLog("write", ref, data)
	return nil
}

func (s *TransferService) Read(ctx context.Context, ref TransferReference) ([]byte, error) {
	os, err := s.js.ObjectStore(ctx, ref.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to get store: %w", err)
	}
	data, err := os.GetBytes(ctx, ref.Key)
	if err != nil {
		if err == jetstream.ErrObjectNotFound {
			return nil, ErrFileNotFound
		}
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	s.SafeAppendTrackLog("read", ref, data)
	return data, nil
}

func (s *TransferService) Delete(ctx context.Context, ref TransferReference) error {
	os, err := s.js.ObjectStore(ctx, ref.Bucket)
	if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}
	err = os.Delete(ctx, ref.Key)
	if err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}
	s.SafeAppendTrackLog("delete", ref, nil)
	return nil
}

func (s *TransferService) TearDown() error {
	if s.natsConn != nil {
		s.natsConn.Close()
	}
	if s.natsContainer != nil {
		if err := s.natsContainer.Stop(context.Background(), nil); err != nil {
			return fmt.Errorf("failed to terminate NATS container: %w", err)
		}
	}
	return nil
}
