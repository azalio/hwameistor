package volume

import (
	"os"
	"path"

	"github.com/hwameistor/hwameistor/pkg/local-disk-manager/member/types"
	log "github.com/sirupsen/logrus"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
)

// Manager responsible for creating, updating, and deleting volumes on nodes
type Manager interface {
	// CreateVolume create volume from device exist in pool
	CreateVolume(name string, pool string, device string) error

	// DeleteVolume delete volume from pool and release bound disk
	DeleteVolume(name string, pool string) error

	// GetVolume return info about this volume
	GetVolume(name string) *types.Volume
}

type volume struct {
	hu hostutil.HostUtils
}

// CreateVolume create volume symlink for bound disk
func (v *volume) CreateVolume(volume string, pool string, device string) error {
	log.WithFields(log.Fields{"volume": volume, "pool": pool, "device": device}).Info("[LDM] Start CreateVolume")
	devicePath := path.Join("..", "disk", device)
	volumePath := types.ComposePoolVolumePath(pool, volume)
	log.WithFields(log.Fields{"devicePath": devicePath, "volumePath": volumePath}).Info("[LDM] Composed paths for volume symlink")
	exist, err := v.hu.PathExists(volumePath)
	if err != nil {
		log.WithFields(log.Fields{"volumePath": volumePath, "err": err}).Error("[LDM] Error checking existence of volumePath before symlink creation")
		return err
	}
	if exist {
		log.WithFields(log.Fields{"volumePath": volumePath}).Warn("[LDM] Volume symlink already exists, skipping creation")
		return nil
	}
	log.WithFields(log.Fields{"devicePath": devicePath, "volumePath": volumePath}).Info("[LDM] Attempting to create volume symlink")
	err = os.Symlink(devicePath, volumePath)
	if err != nil {
		log.WithFields(log.Fields{"devicePath": devicePath, "volumePath": volumePath, "err": err}).Error("[LDM] Failed to create volume symlink")
		return err
	}
	log.WithFields(log.Fields{"devicePath": devicePath, "volumePath": volumePath}).Info("[LDM] Successfully created volume symlink")
	return nil
}

func (v *volume) DeleteVolume(volume string, pool string) error {
	volumePath := types.ComposePoolVolumePath(pool, volume)
	exist, err := v.hu.PathExists(volumePath)
	if err != nil || !exist {
		return err
	}
	return os.Remove(volumePath)
}

func (v *volume) GetVolume(name string) *types.Volume {
	return nil
}

func New() Manager {
	return &volume{
		hu: hostutil.NewHostUtil(),
	}
}
