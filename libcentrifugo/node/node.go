package node

import (
	"github.com/centrifugal/centrifugo/libcentrifugo/engine"
	"github.com/centrifugal/centrifugo/libcentrifugo/proto"
	"github.com/centrifugal/centrifugo/libcentrifugo/server"
)

type Node interface {
	// Run starts a node with provided Engine, Servers and Mediator.
	Run(e engine.Engine, servers map[string]server.Server) error

	// Shutdown shuts down a node.
	Shutdown() error

	// NotifyShutdown allows to get a channel which will be closed on node shutdown.
	NotifyShutdown() chan struct{}

	// Config allows to get node Config.
	Config() Config

	// SetConfig allows to set node config.
	SetConfig(*Config)

	// NewClient creates new client connection.
	NewClient(Session) (ClientConn, error)

	// NewAdminClient creates new admin connection.
	NewAdminClient(Session) (AdminConn, error)

	// ClientMsg handles client message received from channel -
	// broadcasts it to all connected interested clients.
	ClientMsg(proto.Channel, *proto.Message) error
	// JoinMsg handles join message in channel.
	JoinMsg(proto.Channel, *proto.JoinMessage) error
	// LeaveMsg handles leave message in channel.
	LeaveMsg(proto.Channel, *proto.LeaveMessage) error
	// AdminMsg handles admin message - broadcasts it to all connected admins.
	AdminMsg(*proto.AdminMessage) error
	// ControlMsg handles control message.
	ControlMsg(*proto.ControlMessage) error

	// NumSubscribers allows to get number of active channel subscribers.
	NumSubscribers(proto.Channel) int
	// Channels allows to get list of all active node channels.
	Channels() []proto.Channel

	// ApiCmd allows to handle API command.
	APICmd(proto.ApiCommand) (proto.Response, error)
}