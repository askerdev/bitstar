package main

import (
	"context"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/askerdev/bitstar"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	go func() {
		log.Println("Starting pprof server on :6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Fatalf("pprof server failed: %v", err)
		}
	}()

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)

	zapLogger, _ := config.Build()
	defer zapLogger.Sync()

	logger := zapLogger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	storage, err := bitstar.Open(ctx, db)
	if err != nil {
		panic(err)
	}
	defer storage.Close()

	const maxMsgSize = 134217728

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	storagepb.RegisterEventServiceServer(server, storage)
	reflection.Register(server)

	lis, err := net.Listen("tcp", ":11080")
	if err != nil {
		logger.Fatal(err)
	}

	logger.Info("listening on :11080")
	if err := server.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
