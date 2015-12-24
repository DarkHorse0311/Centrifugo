package libcentrifugo

import (
	//"encoding/json"
	"errors"
	//"strconv"
	//"strings"

	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/FZambia/go-logger"
	"github.com/tarantool/go-tarantool"
)

func (p *TarantoolPool) get() (conn *tarantool.Connection, err error) {
	if len(p.pool) == 0 {
		return nil, errors.New("Empty tarantool pool")
	}
	conn = p.pool[p.current]
	p.current++
	p.current = (p.current) % len(p.pool)
	return
}

type TarantoolEngine struct {
	app      *Application
	pool     *TarantoolPool
	endpoint string
}

type TarantoolEngineConfig struct {
	PoolConfig TarantoolPoolConfig
}

type TarantoolPool struct {
	pool    []*tarantool.Connection
	config  TarantoolPoolConfig
	current int
}

type TarantoolPoolConfig struct {
	Address  string
	PoolSize int
	Opts     tarantool.Opts
}

func NewTarantoolEngine(app *Application, conf TarantoolEngineConfig) *TarantoolEngine {
	logger.INFO.Printf("Initializing tarantool connection pool...")
	pool, err := newTarantoolPool(conf.PoolConfig)
	if err != nil {
		logger.FATAL.Fatalln(err)
	}

	e := &TarantoolEngine{
		app:  app,
		pool: pool,
	}

	return e
}

func newTarantoolPool(config TarantoolPoolConfig) (p *TarantoolPool, err error) {
	if config.PoolSize == 0 {
		return nil, errors.New("Size of tarantool pool is zero")
	}

	p = &TarantoolPool{
		pool:   make([]*tarantool.Connection, config.PoolSize),
		config: config,
	}

	for i := 0; i < config.PoolSize; i++ {
		logger.INFO.Printf("[%d] Connecting to tarantool on %s...", i, config.Address)
		p.pool[i], err = tarantool.Connect(config.Address, config.Opts)
		if err != nil {
			return
		}
		logger.INFO.Printf("[%d] Connected to tarantool on %s", i, config.Address)
	}

	return p, nil
}

func (e *TarantoolEngine) name() string {
	return "Tarantool"
}

func (e *TarantoolEngine) run() error {
	go e.runPubSub()
	return nil
}

func (e *TarantoolEngine) runPubSub() {
	conn, err := e.pool.get()
	if err != nil {
		logger.ERROR.Printf("pubsub tarantool pool error: %v\n", err.Error())
		return
	}

	for {
		response, err := conn.Call("bpop", []interface{}{100})
		if err != nil {
			logger.ERROR.Printf("publish tarantool pool error: %v\n", err.Error())
			return
		}
		for _, message := range response.Data {
			println(message)
		}
	}
}

func (e *TarantoolEngine) publish(chID ChannelID, message []byte) (bool, error) {

	conn, err := e.pool.get()
	if err != nil {
		logger.ERROR.Printf("publish tarantool pool error: %v\n", err.Error())
		return false, err
	}

	_, err = conn.Call("publish", []interface{}{chID, message})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (e *TarantoolEngine) subscribe(chID ChannelID) error {
	conn, err := e.pool.get()
	if err != nil {
		logger.ERROR.Printf("subscribe tarantool pool error: %v\n", err.Error())
		return err
	}

	_, err = conn.Call("subscribe", []interface{}{e.app.uid, chID})
	if err != nil {
		return err
	}

	return nil
}

func (e *TarantoolEngine) unsubscribe(chID ChannelID) error {
	conn, err := e.pool.get()
	if err != nil {
		logger.ERROR.Printf("unsubscribe tarantool pool error: %v\n", err.Error())
		return err
	}

	_, err = conn.Call("unsubscribe", []interface{}{e.app.uid, chID})
	return err
}

func (e *TarantoolEngine) addPresence(chID ChannelID, uid ConnID, info ClientInfo) error {
	// not implemented
	return nil
}

func (e *TarantoolEngine) removePresence(chID ChannelID, uid ConnID) error {
	// not implemented
	return nil
}

func (e *TarantoolEngine) presence(chID ChannelID) (map[ConnID]ClientInfo, error) {
	// not implemented
	return map[ConnID]ClientInfo{}, nil
}

func (e *TarantoolEngine) addHistory(chID ChannelID, message Message, opts addHistoryOpts) error {
	// not implemented
	return nil
}

func (e *TarantoolEngine) history(chID ChannelID, opts historyOpts) ([]Message, error) {
	// not implemented
	return []Message{}, nil
}

func (e *TarantoolEngine) channels() ([]ChannelID, error) {
	// not implemented
	return []ChannelID{}, nil
}
