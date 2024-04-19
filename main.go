package main

import (
	"context"
	"emperror.dev/errors"
	"flag"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"gitlab.switch.ch/ub-unibas/dlza/dlza-manager/dlzamanagerproto"
	"io"
	"log"
	"os"
	"time"

	"gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-checker/configuration"
	handlerClient "gitlab.switch.ch/ub-unibas/dlza/microservices/dlza-manager-handler/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strconv"
)

var configParam = flag.String("config", "", "config file in toml format")

func main() {

	flag.Parse()

	configObj := configuration.GetConfig(*configParam)

	// create logger instance
	var out io.Writer = os.Stdout
	if string(configObj.Logging.LogFile) != "" {
		fp, err := os.OpenFile(string(configObj.Logging.LogFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", string(configObj.Logging.LogFile), err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(string(configObj.Logging.LogLevel)))
	var logger zLogger.ZLogger = &_logger
	daLogger := zLogger.NewZWrapper(logger)

	//////CheckerHandler gRPC connection

	CheckerHandlerServiceClient, connectionCheckerHandler, err := handlerClient.NewCheckerHandlerClient(configObj.Handler.Host+":"+strconv.Itoa(configObj.Handler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(errors.Wrap(err, "could not create UploaderStorageHandler gRPC connection: %v"))
	}
	defer connectionCheckerHandler.Close()

	ctx := context.Background()
	objectInstances, err := CheckerHandlerServiceClient.GetAllObjectInstances(ctx, &dlzamanagerproto.NoParam{})

	if err != nil {
		daLogger.Errorf("cannot get all object instances: %v", err)
	}
	_ = objectInstances

}
