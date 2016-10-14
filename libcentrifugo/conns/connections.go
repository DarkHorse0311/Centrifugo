package conns

import (
	"github.com/centrifugal/centrifugo/libcentrifugo/proto"
)

// ClientConn is an interface abstracting all methods used
// by application to interact with client connection.
type ClientConn interface {
	// UID returns unique connection id.
	UID() proto.ConnID
	// User return user ID associated with connection.
	User() proto.UserID
	// Channels returns a slice of channels connection subscribed to.
	Channels() []proto.Channel
	// Handle message coming from client.
	Handle(message []byte) error
	// Send allows to send message to connection client.
	Send(message []byte) error
	// Unsubscribe allows to unsubscribe connection from channel.
	Unsubscribe(ch proto.Channel) error
	// Close closes client's connection.
	Close(reason string) error
}

// AdminConn is an interface abstracting all methods used
// by application to interact with admin connection.
type AdminConn interface {
	// UID returns unique admin connection id.
	UID() proto.ConnID
	// Handle message coming from admin client.
	Handle(message []byte) error
	// Send allows to send message to admin connection.
	Send(message []byte) error
	// Close closes admin's connection.
	Close(reason string) error
}

// Session represents a connection transport between server and client.
type Session interface {
	// Send sends one message to session
	Send([]byte) error
	// Close closes the session with provided code and reason.
	Close(status uint32, reason string) error
}
