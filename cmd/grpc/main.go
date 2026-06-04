package main

import (
	"log"
	"net"

	"github.com/askerdev/bitstar"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)

	zapLogger, _ := config.Build()
	defer zapLogger.Sync()

	logger := zapLogger.Sugar()

	eventService := bitstar.NewEventService(logger, "tmp")

	const maxMsgSize = 134217728

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	storagepb.RegisterEventServiceServer(server, eventService)
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
