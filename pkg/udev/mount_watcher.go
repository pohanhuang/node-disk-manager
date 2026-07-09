//go:build linux

package udev

import (
	"context"
	"fmt"
	"math"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"

	"github.com/harvester/node-disk-manager/pkg/utils"
)

// NDM mounts the host's /proc at /host/proc (see daemonset.yaml), so we watch
// /host/proc/1/mountinfo (host PID 1 = init) to observe the host mount namespace.
// Using /proc/self/mountinfo would only reflect the container's own namespace.
const procMountInfo = "/host/proc/1/mountinfo"

type mountMonitor struct {
	udevCtx *Udev
	fd      int
	fds     []unix.PollFd
}

func newMountMonitor(udevCtx *Udev) *mountMonitor {
	return &mountMonitor{
		udevCtx: udevCtx,
	}
}

func (m *mountMonitor) Setup(_ context.Context) error {
	logrus.Debugf("mount watcher: opening %s", procMountInfo)
	fd, err := unix.Open(procMountInfo, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		logrus.Errorf("mount watcher: failed to open %s: %v", procMountInfo, err)
		return err
	}
	logrus.Debugf("mount watcher: opened fd=%d", fd)

	if fd > math.MaxInt32 {
		unix.Close(fd)
		return fmt.Errorf("mount watcher: fd %d out of int32 range", fd)
	}

	m.fd = fd
	m.fds = []unix.PollFd{{Fd: int32(fd), Events: unix.POLLERR}} //nolint:gosec
	return nil
}

func (m *mountMonitor) Monitor(ctx context.Context) error {
	logrus.Infof("mount watcher: watching %s for mount table changes", procMountInfo)

	for {
		select {
		case <-ctx.Done():
			logrus.Debug("mount watcher: context cancelled, exiting")
			return nil
		default:
		}

		logrus.Debugf("mount watcher: polling fd=%d for POLLERR, timeout=5s", m.fd)
		n, err := unix.Poll(m.fds, 5*1000)
		if err != nil {
			if err == unix.EINTR {
				logrus.Debug("mount watcher: poll interrupted by signal, retrying")
				continue
			}
			logrus.Errorf("mount watcher: poll error: %v", err)
			return err
		}
		if n == 0 {
			logrus.Debug("mount watcher: poll timeout, no mount changes")
			continue
		}

		logrus.Debugf("mount watcher: POLLERR received (revents=0x%x), mount table changed", m.fds[0].Revents)
		utils.CallerWithCondLock(m.udevCtx.scanner.Cond, func() any {
			logrus.Info("mount watcher: mount change detected, waking scanner")
			m.udevCtx.scanner.Cond.Signal()
			return nil
		})
	}
}

func (m *mountMonitor) Teardown() error {
	if m.fd != 0 {
		logrus.Debugf("mount watcher: closing fd=%d", m.fd)
		unix.Close(m.fd)
		m.fd = 0
	}
	return nil
}
