package udev

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pilebones/go-udev/netlink"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/harvester/node-disk-manager/pkg/block"
	"github.com/harvester/node-disk-manager/pkg/controller/blockdevice"
	"github.com/harvester/node-disk-manager/pkg/option"
	"github.com/harvester/node-disk-manager/pkg/utils"
)

type Udev struct {
	namespace   string
	nodeName    string
	startOnce   sync.Once
	scanner     *blockdevice.Scanner
	injectError bool
}

type udevMonitorInterface interface {
	Setup(ctx context.Context) error
	Monitor(ctx context.Context) error
	Teardown() error
}

func NewUdev(opt *option.Option, scanner *blockdevice.Scanner) *Udev {
	return &Udev{
		startOnce:   sync.Once{},
		namespace:   opt.Namespace,
		nodeName:    opt.NodeName,
		scanner:     scanner,
		injectError: opt.InjectUdevMonitorError,
	}
}

func (u *Udev) Monitor(ctx context.Context) {
	// we need to respawn the monitor with any error.
	// because any error will break the monitor loop.

	// Register all monitors
	monitors := u.getMonitors()
	for _, mon := range monitors {
		go u.startMonitor(ctx, mon.name, mon.monitor)
	}
}

// monitorConfig holds monitor name and instance
type monitorConfig struct {
	name    string
	monitor udevMonitorInterface
}

// getMonitors returns all configured monitors
func (u *Udev) getMonitors() []monitorConfig {
	return []monitorConfig{
		{name: "udev-monitor", monitor: newNetlinkMonitor(u)},
		{name: "mount-watcher", monitor: newMountMonitor(u)},
	}
}

// startMonitor starts a monitor service with automatic setup/teardown lifecycle
func (u *Udev) startMonitor(ctx context.Context, name string, mon udevMonitorInterface) {
	registerMonitorService(ctx, name, func(ctx context.Context) error {
		if err := mon.Setup(ctx); err != nil {
			return err
		}
		defer mon.Teardown()
		return mon.Monitor(ctx)
	})
}

// until ctx is cancelled. Only one instance of fn runs at a time,
// preventing goroutine accumulation on repeated failures.
func registerMonitorService(ctx context.Context, name string, fn func(context.Context) error) {
	logrus.Infof("%s starts", name)

	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := fn(ctx); err != nil {
			logrus.Errorf("%s failed: %v", name, err)
		}
	}, 0)

	logrus.Infof("%s stopped", name)
}

func (u *Udev) monitor(ctx context.Context, errors chan error) {
	logrus.Infoln("Start monitoring udev processed events")

	matcher, err := getOptionalMatcher(nil)
	if err != nil {
		logrus.Fatalf("Failed to get udev config, error: %s", err.Error())
	}

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		logrus.Fatalf("Unable to connect to Netlink Kobject UEvent socket, error: %s", err.Error())
	}
	defer conn.Close()

	uqueue := make(chan netlink.UEvent)
	errChan := make(chan error)
	quit := conn.Monitor(uqueue, errChan, matcher)
	defer close(quit)

	// simulator the error from udev monitor
	if u.injectError {
		logrus.Infof("Injecting error to udev monitor for testing")
		errors <- fmt.Errorf("testing error")
		u.injectError = false
		return
	}
	// Handling message from udev queue
	for {
		select {
		case uevent := <-uqueue:
			u.ActionHandler(uevent)
		case err := <-errChan:
			errors <- err
			return
		case <-ctx.Done():
			return
		}
	}
}

func (u *Udev) ActionHandler(uevent netlink.UEvent) {
	udevDevice := InitUdevDevice(uevent.Env)
	if !udevDevice.IsDisk() {
		return
	}
	logrus.WithFields(logrus.Fields{
		"udevAction": uevent.Action,
		"udevEnv":    fmt.Sprintf("%+v", uevent.Env),
	}).Debug("Prepare to handle udev action")

	devPath := udevDevice.GetDevName()
	var disk *block.Disk

	if strings.Contains(devPath, "dm-") {
		// wait for rebuilding the multipath device
		time.Sleep(1 * time.Second)
	}

	if uevent.Action == netlink.REMOVE {
		// Note: at this point, the device is gone, so we can't use GetDiskByDevPath()
		// to get any reliable information about the device, but we _can_ at least
		// dig out the vendor, model, serial number and WWN for logging purposes
		// if we create a minimal block.Disk then call UpdateDiskFromUdev().
		disk = &block.Disk{Name: strings.TrimPrefix(devPath, "/dev/")}
		udevDevice.UpdateDiskFromUdev(disk)
		logrus.WithFields(logrus.Fields{
			"device": devPath,
			"vendor": disk.Vendor,
			"model":  disk.Model,
			"serial": disk.SerialNumber,
			"wwn":    disk.WWN,
		}).Info("removing disk")
		// just wake up scanner to check if the disk is removed, do no-op internally
		u.wakeUpScanner(uevent, devPath, u.namespace)
		return
	}

	if uevent.Action != netlink.ADD {
		return
	}

	disk = u.scanner.BlockInfo.GetDiskByDevPath(devPath)

	if u.scanner.ApplyExcludeFiltersForDisk(disk) {
		return
	}

	// just wake up scanner to check if the disk is added, do no-op internally
	u.wakeUpScanner(uevent, devPath, u.namespace)
}

func (u *Udev) wakeUpScanner(uevent netlink.UEvent, devPath string, namespace string) {
	utils.CallerWithCondLock(u.scanner.Cond, func() any {
		logrus.WithFields(logrus.Fields{
			"namespace":  namespace,
			"kind":       "BlockDevice",
			"udevAction": uevent.Action,
			"device":     devPath,
		}).Info("udev action triggering scanner wake")
		u.scanner.Cond.Signal()
		return nil
	})
}

// getOptionalMatcher Parse and load config file which contains rules for matching
func getOptionalMatcher(filePath *string) (matcher netlink.Matcher, err error) {
	if filePath == nil || *filePath == "" {
		return nil, nil
	}

	stream, err := os.ReadFile(*filePath)
	if err != nil {
		return nil, err
	}

	if stream == nil {
		return nil, fmt.Errorf("empty, no rules provided in \"%s\", err: %w", *filePath, err)
	}

	var rules netlink.RuleDefinitions
	if err := json.Unmarshal(stream, &rules); err != nil {
		return nil, fmt.Errorf("wrong rule syntax, err: %v", err)
	}

	return &rules, nil
}
