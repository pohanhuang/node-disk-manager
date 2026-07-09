//go:build linux

package udev

import (
	"context"
	"fmt"

	"github.com/pilebones/go-udev/netlink"
	"github.com/sirupsen/logrus"
)

const netlinkQueueSize = 256

type netlinkMonitor struct {
	udevCtx *Udev
	conn    *netlink.UEventConn
	matcher netlink.Matcher
	uqueue  chan netlink.UEvent
}

func newNetlinkMonitor(udevCtx *Udev) *netlinkMonitor {
	return &netlinkMonitor{
		udevCtx: udevCtx,
		uqueue:  make(chan netlink.UEvent, netlinkQueueSize),
	}
}

func (n *netlinkMonitor) Setup(_ context.Context) error {
	matcher, err := getOptionalMatcher(nil)
	if err != nil {
		return fmt.Errorf("failed to get udev config: %w", err)
	}

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		return fmt.Errorf("unable to connect to netlink socket: %w", err)
	}

	n.conn = conn
	n.matcher = matcher
	return nil
}

func (n *netlinkMonitor) Monitor(ctx context.Context) error {
	logrus.Infoln("Start monitoring udev processed events")
	errChan := make(chan error, 1)
	quit := n.conn.Monitor(n.uqueue, errChan, n.matcher)
	defer close(quit)

	// inject error for testing
	if n.udevCtx.injectError {
		logrus.Infof("Injecting error to udev monitor for testing")
		n.udevCtx.injectError = false
		return fmt.Errorf("testing error")
	}

	for {
		select {
		case uevent := <-n.uqueue:
			n.udevCtx.ActionHandler(uevent)
		case err := <-errChan:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}

func (n *netlinkMonitor) Teardown() error {
	if n.conn != nil {
		logrus.Debug("netlink monitor: closing connection")
		n.conn.Close()
		n.conn = nil
	}
	return nil
}
