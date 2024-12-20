package service

import (
	"github.com/ocfl-archive/dlza-manager/dlzamanagerproto"
)

func GetStorageLocationsToCopyTo(relevantStorageLocations *dlzamanagerproto.StorageLocations, storageLocationsInUse []*dlzamanagerproto.StorageLocation) []*dlzamanagerproto.StorageLocation {
	storageLocationsToCopyTo := make([]*dlzamanagerproto.StorageLocation, 0)

	for _, relevantStorageLocation := range relevantStorageLocations.StorageLocations {
		for index, currentStorageLocation := range storageLocationsInUse {
			if relevantStorageLocation.Id == currentStorageLocation.Id {
				break
			}
			if index == len(storageLocationsInUse)-1 {
				storageLocationsToCopyTo = append(storageLocationsToCopyTo, relevantStorageLocation)
			}
		}
	}
	return storageLocationsToCopyTo
}

func GetStorageLocationsToDeleteFrom(relevantStorageLocations *dlzamanagerproto.StorageLocations, storageLocationsInUse map[*dlzamanagerproto.ObjectInstance]*dlzamanagerproto.StorageLocation) map[*dlzamanagerproto.ObjectInstance]*dlzamanagerproto.StorageLocation {
	storageLocationsToDeleteFrom := make(map[*dlzamanagerproto.ObjectInstance]*dlzamanagerproto.StorageLocation)

	for oiInUse, storageLocationInUse := range storageLocationsInUse {
		for index, relevantStorageLocation := range relevantStorageLocations.StorageLocations {
			if storageLocationInUse.Id == relevantStorageLocation.Id {
				break
			}
			if index == len(relevantStorageLocations.StorageLocations)-1 {
				storageLocationsToDeleteFrom[oiInUse] = storageLocationInUse
			}
		}
	}
	return storageLocationsToDeleteFrom
}
