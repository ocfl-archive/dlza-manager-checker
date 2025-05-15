package main

import (
	"context"
	"crypto/tls"
	"emperror.dev/errors"
	"flag"
	"fmt"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/ocfl-archive/dlza-manager-checker/internal"
	handlerClientProto "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	storageHandlerClientProto "github.com/ocfl-archive/dlza-manager-storage-handler/storagehandlerproto"
	"github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	archiveerror "github.com/ocfl-archive/error/pkg/error"
	"github.com/rs/zerolog/log"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
	"go.ub.unibas.ch/cloud/miniresolver/v2/pkg/resolver"
	"golang.org/x/exp/maps"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/ocfl-archive/dlza-manager-checker/configuration"
)

const (
	errorStatus  = "error"
	okStatus     = "ok"
	deleteStatus = "to delete"
	notAvailable = "not available"
	deprecated   = "deprecated"
	newStatus    = "new"
)

const (
	existsStatus = "exists"
	sha512Status = "sha512"
)

var objectCash map[string]*dlzamanagerproto.Object

const errorTopic string = "dlza-manager-checker"

var ErrorFactory = archiveerror.NewFactory(errorTopic)

var configParam = flag.String("config", "", "config file in toml format")

var conf *configuration.DispatcherConfig

var workerWaitingTime int

func worker(id int, in <-chan *dlzamanagerproto.Object, checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient,
	checkerStorageHandlerServiceClient storageHandlerClientProto.CheckerStorageHandlerServiceClient, wg *sync.WaitGroup, logger zLogger.ZLogger) {
	defer wg.Done()
	for {
		select {
		case obj, ok := <-in:
			if !ok {
				logger.Info().Msgf("Data channel is closed. Worker ID: %d", id)
				return
			}
			err := checkObjectsAndReact(checkerHandlerServiceClient, checkerStorageHandlerServiceClient, obj, logger)
			if err != nil {
				logger.Error().Msgf("cannot checkObjectInstancesDistributionAndReact for object with ID %s, err: %v", obj.Id, err)
				delete(objectCash, obj.Id)
				continue
			}
			logger.Info().Msgf("Worker ID: %d finished to process object with ID: %s", id, obj.Id)
			delete(objectCash, obj.Id)
			logger.Debug().Msgf("Worker ID: %d cleared cash. Cash length: %d", id, len(objectCash))
		case <-time.After(time.Duration(workerWaitingTime) * time.Second):
			//logger.Debug().Msgf("Timeout: no value received in %d second. Worker ID: %d", workerWaitingTime, id)
		}
	}
}

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

	conf = &configuration.DispatcherConfig{
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
		log.Fatal().Err(err).Msgf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	configErrorFactory()

	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal().Err(err).Msgf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatal().Err(err).Msgf("cannot create client loader: %v", err)
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
		log.Fatal().Err(err).Msgf("cannot create logger: %v", err)
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
		logger.Fatal().Msgf("cannot create resolver client: %s", err)
	}
	defer resolverClient.Close()

	//////CheckerStorageHandler gRPC connection

	clientCheckerHandler, err := resolver.NewClient[handlerClientProto.CheckerHandlerServiceClient](
		resolverClient,
		handlerClientProto.NewCheckerHandlerServiceClient,
		handlerClientProto.CheckerHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientCheckerHandler grpc client: %s", err)
	}

	//////CheckerStorageHandler gRPC connection

	clientCheckerStorageHandler, err := resolver.NewClient[storageHandlerClientProto.CheckerStorageHandlerServiceClient](
		resolverClient,
		storageHandlerClientProto.NewCheckerStorageHandlerServiceClient,
		storageHandlerClientProto.CheckerStorageHandlerService_ServiceDesc.ServiceName, conf.Domain)
	if err != nil {
		logger.Panic().Msgf("cannot create clientCheckerStorageHandler grpc client: %s", err)
	}
	workerWaitingTime = conf.WorkerWaitingTime
	objectCash = make(map[string]*dlzamanagerproto.Object)
	jobChan := make(chan *dlzamanagerproto.Object)
	wg := &sync.WaitGroup{}
	for i := 0; i < conf.AmountOfWorkers; i++ {
		wg.Add(1)
		go worker(i, jobChan, clientCheckerHandler, clientCheckerStorageHandler, wg, logger)
	}
	var end = make(chan struct{}, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for {
				object, err := clientCheckerHandler.GetObjectExceptListOlderThanWithChecks(context.Background(), &dlzamanagerproto.IdsWithSQLInterval{Ids: maps.Keys(objectCash), Interval: fmt.Sprintf("'%d' minute", conf.DaysWithoutCheck), AvailabilityInterval: fmt.Sprintf("'%d' minute", conf.DaysToWaitAvailability)})
				if err != nil {
					logger.Error().Msgf("cannot get GetObjectExceptListOlderThanWithChecks. err: %v", err)
				}
				if object.Id == "" {
					break
				}
				objectCash[object.Id] = object
				jobChan <- object
				if len(objectCash) == conf.AmountOfWorkers {
					for {
						time.Sleep(time.Duration(conf.TimeToWaitWorker) * time.Second)
						if len(objectCash) < conf.AmountOfWorkers {
							break
						}
					}
				}
			}
			select {
			case <-end:
				return
			case <-time.After(time.Duration(conf.CycleLength) * time.Second):
			}
		}
	}()
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)
	close(jobChan)
	close(end)
	wg.Wait()

}

func checkObjectsAndReact(checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient,
	checkerStorageHandlerServiceClient storageHandlerClientProto.CheckerStorageHandlerServiceClient, obj *dlzamanagerproto.Object, logger zLogger.ZLogger) error {
	objectInstances, err := checkerHandlerServiceClient.GetObjectsInstancesByObjectId(context.Background(), &dlzamanagerproto.Id{Id: obj.Id})
	if err != nil {
		logger.Error().Msgf("cannot get GetObjectsInstancesByObjectId for object with id: %s. err: %v", obj.Id, err)
		return errors.Wrapf(err, "cannot get GetObjectsInstancesByObjectId for object with id: %s", obj.Id)
	}
	for _, objectInstance := range objectInstances.ObjectInstances {
		if objectInstance.Status == newStatus || objectInstance.Status == okStatus {
			checksum, err := checkerStorageHandlerServiceClient.GetObjectInstanceChecksum(context.Background(), objectInstance)
			if err != nil {
				logger.Error().Msgf("cannot get GetObjectInstanceChecksum for object instance with id: %s. err: %v", objectInstance.Id, err)
				_, err = checkerHandlerServiceClient.CreateObjectInstanceCheck(context.Background(), &dlzamanagerproto.ObjectInstanceCheck{ObjectInstanceId: objectInstance.Id,
					Error: true, Message: fmt.Sprintf("cannot get checksum for object instance: %s", err), CheckType: existsStatus})
				if err != nil {
					logger.Error().Msgf("cannot create instance check object for file %s, %v", objectInstance.Path, err)
					continue
				}
				err := checkAmountOfErrorsAndReact(checkerHandlerServiceClient, objectInstance, logger)
				if err != nil {
					logger.Error().Msgf("cannot checkAmountOfErrorsAndReact for object instance with path %s, err: %v", objectInstance.Path, err)
					continue
				}
				continue
			}
			if obj.Checksum != checksum.Id {
				objectInstance.Status = errorStatus
				err = updateInstanceAndCreateCheck(checkerHandlerServiceClient, objectInstance, true, "", sha512Status)
				if err != nil {
					logger.Error().Msgf("cannot updateInstanceAndCreateCheck for object instance with ID %s for object with false checksum with ID %s, err: %v", objectInstance.Id, objectInstance.ObjectId, err)
					continue
				}
				continue
			}
			err = updateInstanceAndCreateCheck(checkerHandlerServiceClient, objectInstance, false, "", sha512Status)
			if err != nil {
				logger.Error().Msgf("cannot update instance or create instance check object for file %v, err: %v", objectInstance.Path, err)
			}

		}
	}

	return nil
}

func checkAmountOfErrorsAndReact(checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient,
	objectInstance *dlzamanagerproto.ObjectInstance, logger zLogger.ZLogger) error {
	objectInstanceChecks, err := checkerHandlerServiceClient.GetObjectInstanceChecksByObjectInstanceId(context.Background(), &dlzamanagerproto.Id{Id: objectInstance.Id})
	if err != nil {
		logger.Error().Msgf("cannot GetObjectInstanceChecksByObjectInstanceId for object instance with path %s, err: %v", objectInstance.Path, err)
		return errors.Wrapf(err, "cannot GetObjectInstanceChecksByObjectInstanceId for object instance with path %s", objectInstance.Path)
	}
	if len(objectInstanceChecks.ObjectInstanceChecks) == 3 {
		for _, objectInstanceCheck := range objectInstanceChecks.ObjectInstanceChecks {
			if !objectInstanceCheck.Error || objectInstanceCheck.CheckType != existsStatus {
				return nil
			}
		}
		objectInstance.Status = notAvailable
		_, err := checkerHandlerServiceClient.UpdateObjectInstance(context.Background(), objectInstance)
		if err != nil {
			logger.Error().Msgf("cannot UpdateObjectInstance with ID %s, err: %v", objectInstance.Id, err)
			return errors.Wrapf(err, "cannot UpdateObjectInstance with ID %s", objectInstance.Id)
		}
	}
	return nil
}

func updateInstanceAndCreateCheck(checkerHandlerServiceClient handlerClientProto.CheckerHandlerServiceClient, objectInstance *dlzamanagerproto.ObjectInstance, error bool, message string, checkType string) error {
	_, err := checkerHandlerServiceClient.UpdateObjectInstance(context.Background(), objectInstance)
	if err != nil {
		return err
	}
	_, err = checkerHandlerServiceClient.CreateObjectInstanceCheck(context.Background(), &dlzamanagerproto.ObjectInstanceCheck{ObjectInstanceId: objectInstance.Id,
		Error: error, Message: message, CheckType: checkType})
	if err != nil {
		return err
	}
	return nil

}

func configErrorFactory() {
	var archiveErrs []*archiveerror.Error
	if conf.ErrorConfig != "" {
		errorExt := filepath.Ext(conf.ErrorConfig)
		var err error
		switch errorExt {
		case ".toml":
			archiveErrs, err = archiveerror.LoadTOMLFile(conf.ErrorConfig)
		case ".yaml":
			archiveErrs, err = archiveerror.LoadYAMLFile(conf.ErrorConfig)
		default:
			err = errors.Errorf("unknown error config file extension %s", errorExt)
		}
		if err != nil {
			log.Fatal().Err(err).Msgf("cannot load error config file %s", conf.ErrorConfig)
		}
	} else {
		var err error
		const errorsEmbedToml string = "errors.toml"
		archiveErrs, err = archiveerror.LoadTOMLFileFS(internal.InternalFS, errorsEmbedToml)
		if err != nil {
			log.Fatal().Err(err).Msg("cannot load error config file")
		}
	}
	if err := ErrorFactory.RegisterErrors(archiveErrs); err != nil {
		log.Fatal().Err(err).Msg("cannot register errors")
	}
}
