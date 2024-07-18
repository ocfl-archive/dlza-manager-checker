package main

import (
	"context"
	"emperror.dev/errors"
	"flag"
	"github.com/je4/filesystem/v2/pkg/vfsrw"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	pb "github.com/ocfl-archive/dlza-manager-handler/handlerproto"
	"github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
	"github.com/rs/zerolog"
	"io"
	"log"
	"os"
	"time"

	"github.com/ocfl-archive/dlza-manager-checker/configuration"
	handlerClient "github.com/ocfl-archive/dlza-manager-handler/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strconv"
)

var configParam = flag.String("config", "", "config file in toml format")

const (
	errorStatus = "error"
	okStatus    = "ok"
)

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
	for _, objectInstance := range objectInstances.ObjectInstances {
		storageLocation, err := CheckerHandlerServiceClient.GetStorageLocationByObjectInstanceId(ctx, &dlzamanagerproto.Id{Id: objectInstance.Id})
		if err != nil {
			daLogger.Errorf("cannot get all storage location for object instance id %s, %v", objectInstance.Id, err)
		}
		vfsConfig, err := configuration.LoadVfsConfig(storageLocation.Connection)
		if err != nil {
			daLogger.Errorf("error mapping json for storage location connection field: %v", err)
		}
		vfs, err := vfsrw.NewFS(vfsConfig, daLogger)

		sourceFP, err := vfs.Open(objectInstance.Path)
		if err != nil {
			daLogger.Errorf("cannot read file '%s': %v", objectInstance.Path, err)
			objectInstance.Status = errorStatus
			message := "cannot read file " + objectInstance.Path

			err := updateInstanceAndCreateCheck(ctx, CheckerHandlerServiceClient, objectInstance, true, message)
			if err != nil {
				daLogger.Errorf("cannot update instance or create instance check object for file %s, %v", objectInstance.Path, err)
			}
			continue
		}

		targetFP := io.Discard
		csWriter, err := checksum.NewChecksumWriter(
			[]checksum.DigestAlgorithm{checksum.DigestSHA512},
			targetFP,
		)

		_size, err := io.Copy(csWriter, sourceFP)
		if err != nil {
			daLogger.Errorf("error writing file")
			if err := csWriter.Close(); err != nil {
				daLogger.Errorf("cannot close checksum writer: %v", err)
			}
			if err := sourceFP.Close(); err != nil {
				daLogger.Errorf("cannot close source: %v", err)
			}
		}
		if err := csWriter.Close(); err != nil {
			daLogger.Errorf("cannot close checksum writer: %v", err)
		}
		checksums, err := csWriter.GetChecksums()
		if err != nil {
			if err := sourceFP.Close(); err != nil {
				daLogger.Errorf("cannot close source: %v", err)
			}
			daLogger.Errorf("cannot get checksum: %v", err)
		}
		_ = _size
		_ = checksums

		object, err := CheckerHandlerServiceClient.GetObjectById(ctx, &dlzamanagerproto.Id{Id: objectInstance.ObjectId})
		if err != nil {
			daLogger.Errorf("cannot get object with id %s, %v", objectInstance.ObjectId, err)
			if err := sourceFP.Close(); err != nil {
				daLogger.Errorf("cannot close source: %v", err)
			}
			continue
		}
		var status string
		var message string
		var errorCheck bool
		if object.Checksum != checksums[checksum.DigestSHA512] {
			daLogger.Errorf("checksum check failed for object %s, checksums are not matching", objectInstance.Path)
			status = errorStatus
			message = "checksum check failed for object, checksums are not matching" + objectInstance.Path
			errorCheck = true
		} else {
			status = okStatus
			errorCheck = false
		}
		objectInstance.Status = status
		err = updateInstanceAndCreateCheck(ctx, CheckerHandlerServiceClient, objectInstance, errorCheck, message)
		if err != nil {
			daLogger.Errorf("cannot update instance or create instance check object for file %s, %v", objectInstance.Path, err)
		}

		if err := sourceFP.Close(); err != nil {
			daLogger.Errorf("cannot close source: %v", err)
		}
	}
}

func updateInstanceAndCreateCheck(ctx context.Context, checkerHandlerServiceClient pb.CheckerHandlerServiceClient, objectInstance *dlzamanagerproto.ObjectInstance, errorCheck bool, message string) error {
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
