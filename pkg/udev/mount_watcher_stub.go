//go:build !linux

package udev

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

// mountMonitor stub for non-Linux platforms
type mountMonitor struct {
	udevCtx *Udev
}

func newMountMonitor(udevCtx *Udev) *mountMonitor {
	return &mountMonitor{udevCtx: udevCtx}
}

func (m *mountMonitor) Setup(_ context.Context) error {
	logrus.Warn("mount watcher: not supported on non-Linux platforms")
	return fmt.Errorf("mount watcher not supported on this platform")
}

func (m *mountMonitor) Monitor(_ context.Context) error {
	return nil
}

func (m *mountMonitor) Teardown() error {
	return nil
}
