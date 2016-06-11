package libcentrifugo

import (
	"encoding/json"

	"github.com/FZambia/go-logger"

	"fmt"
	"github.com/buger/jsonparser"
)

var publishKeys = [][]string{
	[]string{"channel"},
	[]string{"data"},
	[]string{"client"},
}

var broadcastKeys = [][]string{
	[]string{"channels"},
	[]string{"data"},
	[]string{"client"},
}

// apiCmd builds API command and dispatches it into correct handler method.
func (app *Application) apiCmd(command apiCommand) (response, error) {

	var err error
	var resp response

	method := command.Method

	switch method {
	case "publish":
		var cmd publishAPICommand
		err = json.Unmarshal(command.Params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return nil, ErrInvalidMessage
		}
		resp, err = app.publishCmd(&cmd)
	case "broadcast":
		var cmd broadcastAPICommand
		var parseError error

		jsonparser.EachKey(command.Params, func(idx int, value []byte, vType jsonparser.ValueType, err error) {
			switch idx {
			case 0: // channels
				jsonparser.ArrayEach(value, func(v []byte, vt jsonparser.ValueType, offset int, err error) {
					if vt != jsonparser.String {
						parseError = ErrInvalidMessage
						return
					}
					cmd.Channels = append(cmd.Channels, Channel(v))
				})
			case 1: // data
				if vType == jsonparser.Null {
					cmd.Data = json.RawMessage([]byte("null"))
				} else if vType == jsonparser.String {
					cmd.Data = json.RawMessage([]byte(fmt.Sprintf("\"%s\"", string(value))))
				} else {
					cmd.Data = json.RawMessage(value)
				}
			case 2: // client
				if err != nil {
					if err == jsonparser.KeyPathNotFoundError {
						return
					}
					logger.ERROR.Println(err)
					parseError = ErrInvalidMessage
					return
				}
				if vType != jsonparser.String {
					logger.ERROR.Println("client must be string")
					parseError = ErrInvalidMessage
					return
				}
				cmd.Client = ConnID(value)
			}
		}, broadcastKeys...)
		if parseError != nil {
			return nil, parseError
		}
		resp, err = app.broadcastCmd(&cmd)
	case "unsubscribe":
		var cmd unsubscribeAPICommand
		err = json.Unmarshal(command.Params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return nil, ErrInvalidMessage
		}
		resp, err = app.unsubcribeCmd(&cmd)
	case "disconnect":
		var cmd disconnectAPICommand
		err = json.Unmarshal(command.Params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return nil, ErrInvalidMessage
		}
		resp, err = app.disconnectCmd(&cmd)
	case "presence":
		var cmd presenceAPICommand
		err = json.Unmarshal(command.Params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return nil, ErrInvalidMessage
		}
		resp, err = app.presenceCmd(&cmd)
	case "history":
		var cmd historyAPICommand
		err = json.Unmarshal(command.Params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return nil, ErrInvalidMessage
		}
		resp, err = app.historyCmd(&cmd)
	case "channels":
		resp, err = app.channelsCmd()
	case "stats":
		resp, err = app.statsCmd()
	case "node":
		resp, err = app.nodeCmd()
	default:
		return nil, ErrMethodNotFound
	}
	if err != nil {
		return nil, err
	}

	resp.SetUID(command.UID)

	return resp, nil
}

// publishCmd publishes data into channel.
func (app *Application) publishCmd(cmd *publishAPICommand) (response, error) {
	channel := cmd.Channel
	data := cmd.Data
	err := app.publish(channel, data, cmd.Client, nil, false)
	resp := newAPIPublishResponse()
	if err != nil {
		resp.SetErr(responseError{err, errorAdviceNone})
		return resp, nil
	}
	return resp, nil
}

// broadcastCmd publishes data into multiple channels.
func (app *Application) broadcastCmd(cmd *broadcastAPICommand) (response, error) {
	resp := newAPIBroadcastResponse()
	channels := cmd.Channels
	data := cmd.Data
	if len(channels) == 0 {
		logger.ERROR.Println("channels required for broadcast")
		resp.SetErr(responseError{ErrInvalidMessage, errorAdviceNone})
		return resp, nil
	}
	errs := make([]<-chan error, len(channels))
	for i, channel := range channels {
		errs[i] = app.publishAsync(channel, data, cmd.Client, nil, false)
	}
	var firstErr error
	for i := range errs {
		err := <-errs[i]
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			logger.ERROR.Printf("Error publishing into channel %s: %v", string(channels[i]), err.Error())
		}
	}
	if firstErr != nil {
		resp.SetErr(responseError{firstErr, errorAdviceNone})
	}
	return resp, nil
}

// unsubscribeCmd unsubscribes project's user from channel and sends
// unsubscribe control message to other nodes.
func (app *Application) unsubcribeCmd(cmd *unsubscribeAPICommand) (response, error) {
	resp := newAPIUnsubscribeResponse()
	channel := cmd.Channel
	user := cmd.User
	err := app.Unsubscribe(user, channel)
	if err != nil {
		resp.SetErr(responseError{err, errorAdviceNone})
		return resp, nil
	}
	return resp, nil
}

// disconnectCmd disconnects user by its ID and sends disconnect
// control message to other nodes so they could also disconnect this user.
func (app *Application) disconnectCmd(cmd *disconnectAPICommand) (response, error) {
	resp := newAPIDisconnectResponse()
	user := cmd.User
	err := app.Disconnect(user)
	if err != nil {
		resp.SetErr(responseError{err, errorAdviceNone})
		return resp, nil
	}
	return resp, nil
}

// presenceCmd returns response with presense information for channel.
func (app *Application) presenceCmd(cmd *presenceAPICommand) (response, error) {
	channel := cmd.Channel
	body := presenceBody{
		Channel: channel,
	}
	presence, err := app.Presence(channel)
	if err != nil {
		resp := newAPIPresenceResponse(body)
		resp.SetErr(responseError{err, errorAdviceNone})
		return resp, nil
	}
	body.Data = presence
	return newAPIPresenceResponse(body), nil
}

// historyCmd returns response with history information for channel.
func (app *Application) historyCmd(cmd *historyAPICommand) (response, error) {
	channel := cmd.Channel
	body := historyBody{
		Channel: channel,
	}
	history, err := app.History(channel)
	if err != nil {
		resp := newAPIHistoryResponse(body)
		resp.SetErr(responseError{err, errorAdviceNone})
		return resp, nil
	}
	body.Data = history
	return newAPIHistoryResponse(body), nil
}

// channelsCmd returns active channels.
func (app *Application) channelsCmd() (response, error) {
	body := channelsBody{}
	channels, err := app.channels()
	if err != nil {
		logger.ERROR.Println(err)
		resp := newAPIChannelsResponse(body)
		resp.SetErr(responseError{ErrInternalServerError, errorAdviceNone})
		return resp, nil
	}
	body.Data = channels
	return newAPIChannelsResponse(body), nil
}

// statsCmd returns active node stats.
func (app *Application) statsCmd() (response, error) {
	body := statsBody{}
	body.Data = app.stats()
	return newAPIStatsResponse(body), nil
}

// nodeCmd returns simple counter metrics which update in real time for the current node only.
func (app *Application) nodeCmd() (response, error) {
	body := nodeBody{}
	body.Data = app.node()
	return newAPINodeResponse(body), nil
}
