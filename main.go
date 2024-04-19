package main

import (
	"emperror.dev/errors"
	"flag"
	lm "github.com/je4/utils/v2/pkg/logger"

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

	daLogger, lf := lm.CreateLogger("ocfl-reader",
		"",
		nil,
		"DEBUG",
		`%{time:2006-01-02T15:04:05.000} %{shortpkg}::%{longfunc} [%{shortfile}] > %{level:.5s} - %{message}`,
	)
	defer lf.Close()

	//////CheckerHandler gRPC connection

	CheckerHandlerServiceClient, connectionCheckerHandler, err := handlerClient.NewCheckerHandlerClient(configObj.Handler.Host+":"+strconv.Itoa(configObj.Handler.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(errors.Wrap(err, "could not create UploaderStorageHandler gRPC connection: %v"))
	}
	defer connectionCheckerHandler.Close()

}
