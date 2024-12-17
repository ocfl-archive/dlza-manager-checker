package main

import (
	"context"
	"crypto/tls"
	"emperror.dev/errors"
	"flag"
	"fmt"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	handlerClientProto "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClientProto "github.com/ocfl-archive/dlza-manager-storage-handler/storagehandlerproto"
	"github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"go.ub.unibas.ch/cloud/miniresolver/v2/pkg/resolver"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ocfl-archive/dlza-manager-checker/configuration"
)

var configParam = flag.String("config", "", "config file in toml format")

const (
	errorStatus = "error"
	okStatus    = "ok"
)

func main() {

	flag.Parse()

	var cfgFS fs.FS
	var cfgFile string
	if *configParam != "" {
		cfgFS = os.DirFS(filepath.Dir(*configParam))
		cfgFile = filepath.Base(*configParam)
	} else {
		cfgFS = configuration.ConfigFS
		cfgFile = "checker.toml"
	}

	conf := &configuration.DispatcherConfig{
		LocalAddr: "localhost:8443",
		//ResolverTimeout: config.Duration(10 * time.Minute),
		ExternalAddr:            "https://localhost:8443",
		LogLevel:                "DEBUG",
		ResolverTimeout:         configutil.Duration(10 * time.Minute),
		ResolverNotFoundTimeout: configutil.Duration(10 * time.Second),
		ActionTemplateTimeout:   configutil.Duration(120 * time.Second),
		CollectionCacheTimeout:  configutil.Duration(10 * time.Minute),
		CollectionCacheSize:     30,
		ItemCacheSize:           1000,
		ClientTLS: &loader.Config{
			Type: "DEV",
		},
	}

	if err := configuration.LoadDispatcherConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}

	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer loggerLoader.Close()
	}

	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		log.Fatalf("cannot create logger: %v", err)
	}
	if _logstash != nil {
		defer _logstash.Close()
	}
	if _logfile != nil {
		defer _logfile.Close()
	}
	l2 := _logger.With().Timestamp().Str("host", hostname).Str("addr", conf.LocalAddr).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	clientCert, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	defer clientLoader.Close()

	logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)
	resolverClient, err := resolver.NewMiniresolverClient(conf.ResolverAddr, conf.GRPCClient, clientCert, nil, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer resolverClient.Close()

	//////DispatcherHandler gRPC connection

	clientCheckerHandler, err := resolver.NewClient[handlerClientProto.CheckerHandlerServiceClient](
		resolverClient,
		handlerClientProto.NewCheckerHandlerServiceClient,
		handlerClientProto.CheckerHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientCheckerHandler grpc client: %v", err)
	}

	resolver.DoPing(clientCheckerHandler, logger)

	//////DispatcherStorageHandler gRPC connection

	clientCheckerStorageHandler, err := resolver.NewClient[storageHandlerClientProto.CheckerStorageHandlerServiceClient](
		resolverClient,
		storageHandlerClientProto.NewCheckerStorageHandlerServiceClient,
		storageHandlerClientProto.CheckerStorageHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientCheckerStorageHandler grpc client: %v", err)
	}

	resolver.DoPing(clientCheckerStorageHandler, logger)

	objectInstances, err := clientCheckerHandler.GetAllObjectInstances(context.Background(), &dlzamanagerproto.NoParam{})

	if err != nil {
		logger.Error().Msgf("cannot get all object instances: %v", err)
	}

	for _, objectInstance := range objectInstances.ObjectInstances {
		checksumRetr, err := clientCheckerStorageHandler.GetObjectInstanceChecksum(context.Background(), objectInstance)
		if err != nil {
			logger.Error().Msgf("cannot get checksum for object instance with id %s, %v", objectInstance.Id, err)
			objectInstance.Status = errorStatus
			err = updateInstanceAndCreateCheck(context.Background(), clientCheckerHandler, objectInstance, true, fmt.Sprintf("cannot get checksum for object instance: %s", err))
			if err != nil {
				logger.Error().Msgf("cannot update instance or create instance check object for file %s, %v", objectInstance.Path, err)
			}
			err := checkAmountOfErrorsAndReact(context.Background(), clientCheckerHandler, objectInstance, logger)
			if err != nil {
				logger.Error().Msgf("cannot checkAmountOfErrorsAndReact for object instance with path %v", objectInstance.Path, err)
			}
			continue
		}

		object, err := clientCheckerHandler.GetObjectById(context.Background(), &dlzamanagerproto.Id{Id: objectInstance.ObjectId})
		if err != nil {
			logger.Error().Msgf("cannot get object with id %s, %v", objectInstance.ObjectId, err)
			continue
		}
		var status string
		var message string
		var errorCheck bool
		if object.Checksum != checksumRetr.Id {
			logger.Error().Msgf("checksum check failed for object %s, checksums are not matching", objectInstance.Path)
			status = errorStatus
			message = "checksum check failed for object, checksums are not matching" + objectInstance.Path
			errorCheck = true
		} else {
			status = okStatus
			errorCheck = false
		}
		objectInstance.Status = status
		err = updateInstanceAndCreateCheck(context.Background(), clientCheckerHandler, objectInstance, errorCheck, message)
		if err != nil {
			logger.Error().Msgf("cannot update instance or create instance check object for file %v", objectInstance.Path, err)
		}
		if errorCheck {
			err := checkAmountOfErrorsAndReact(context.Background(), clientCheckerHandler, objectInstance, logger)
			if err != nil {
				logger.Error().Msgf("cannot checkAmountOfErrorsAndReact for object instance with path %v", objectInstance.Path, err)
			}
		}
	}

}

func updateInstanceAndCreateCheck(ctx context.Context, checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient, objectInstance *dlzamanagerproto.ObjectInstance, errorCheck bool, message string) error {
	_, err := checkerHandlerServiceClient.UpdateObjectInstance(ctx, objectInstance)
	if err != nil {
		return err
	}
	_, err = checkerHandlerServiceClient.CreateObjectInstanceCheck(ctx, &dlzamanagerproto.ObjectInstanceCheck{ObjectInstanceId: objectInstance.Id,
		Error: errorCheck, Message: message})
	if err != nil {
		return err
	}
	return nil

}

func checkAmountOfErrorsAndReact(ctx context.Context, checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient, objectInstance *dlzamanagerproto.ObjectInstance, logger zLogger.ZLogger) error {
	objectInstanceChecks, err := checkerHandlerServiceClient.GetObjectInstanceChecksByObjectInstanceId(ctx, &dlzamanagerproto.Id{Id: objectInstance.Id})
	if err != nil {
		logger.Error().Msgf("cannot GetObjectInstanceChecksByObjectInstanceId for object instance with path %v", objectInstance.Path, err)
		return errors.Wrapf(err, "cannot GetObjectInstanceChecksByObjectInstanceId for object instance with path %v", objectInstance.Path)
	}
	if len(objectInstanceChecks.ObjectInstanceChecks) >= 3 {

	}

	return nil
}
