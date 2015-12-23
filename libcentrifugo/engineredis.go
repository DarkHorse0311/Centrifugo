package libcentrifugo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/FZambia/go-logger"
	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/garyburd/redigo/redis"
)

// RedisEngine uses Redis datastructures and PUB/SUB to manage Centrifugo logic.
// This engine allows to scale Centrifugo - you can run several Centrifugo instances
// connected to the same Redis and load balance clients between instances.
type RedisEngine struct {
	sync.RWMutex
	app          *Application
	pool         *redis.Pool
	psc          redis.PubSubConn
	api          bool
	inPubSub     bool
	inAPI        bool
	numApiShards int
}

type RedisEngineConfig struct {
	Host         string
	Port         string
	Password     string
	DB           string
	URL          string
	PoolSize     int
	API          bool
	NumAPIShards int
}

func newPool(server, password, db string, psize int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   psize,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				logger.CRITICAL.Println(err)
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					logger.CRITICAL.Println(err)
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", db); err != nil {
				c.Close()
				logger.CRITICAL.Println(err)
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// NewRedisEngine initializes Redis Engine.
func NewRedisEngine(app *Application, conf *RedisEngineConfig) *RedisEngine {
	host := conf.Host
	port := conf.Port
	password := conf.Password

	db := "0"
	if conf.DB != "" {
		db = conf.DB
	}

	// If URL set then prefer it over other parameters.
	if conf.URL != "" {
		u, err := url.Parse(conf.URL)
		if err != nil {
			logger.FATAL.Fatalln(err)
		}
		if u.User != nil {
			var ok bool
			password, ok = u.User.Password()
			if !ok {
				password = ""
			}
		}
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			logger.FATAL.Fatalln(err)
		}
		path := u.Path
		if path != "" {
			db = path[1:]
		}
	}

	server := host + ":" + port

	pool := newPool(server, password, db, conf.PoolSize)

	e := &RedisEngine{
		app:          app,
		pool:         pool,
		api:          conf.API,
		numApiShards: conf.NumAPIShards,
	}
	usingPassword := "no"
	if password != "" {
		usingPassword = "yes"
	}
	logger.INFO.Printf("Redis engine: %s, database %s, pool size %d, using password: %s\n", server, db, conf.PoolSize, usingPassword)
	e.psc = redis.PubSubConn{Conn: e.pool.Get()}
	return e
}

func (e *RedisEngine) name() string {
	return "Redis"
}

func (e *RedisEngine) run() error {
	e.RLock()
	api := e.api
	e.RUnlock()
	go e.initializePubSub()
	if api {
		go e.initializeAPI()
	}
	go e.checkConnectionStatus()
	return nil
}

func (e *RedisEngine) checkConnectionStatus() {
	for {
		time.Sleep(time.Second)
		e.RLock()
		inPubSub := e.inPubSub
		inAPI := e.inAPI
		e.RUnlock()
		if !inPubSub {
			e.psc = redis.PubSubConn{Conn: e.pool.Get()}
			go e.initializePubSub()
		}
		if e.api && !inAPI {
			go e.initializeAPI()
		}
	}
}

type redisAPIRequest struct {
	Data []apiCommand
}

func (e *RedisEngine) initializeAPI() {
	e.Lock()
	e.inAPI = true
	e.Unlock()
	conn := e.pool.Get()
	defer conn.Close()
	defer func() {
		e.Lock()
		e.inAPI = false
		e.Unlock()
	}()
	e.app.RLock()
	apiKey := e.app.config.ChannelPrefix + "." + "api"
	e.app.RUnlock()

	done := make(chan struct{})
	defer close(done)

	popParams := []interface{}{apiKey}
	workQueues := make(map[string]chan []byte)
	workQueues[apiKey] = make(chan []byte, 256)

	for i := 0; i < e.numApiShards; i++ {
		queueKey := fmt.Sprintf("%s.%d", apiKey, i)
		popParams = append(popParams, queueKey)
		workQueues[queueKey] = make(chan []byte, 256)
	}

	// Add timeout param
	popParams = append(popParams, 0)

	// Start a worker for each queue
	for name, ch := range workQueues {
		go func(name string, in <-chan []byte) {
			logger.INFO.Printf("Starting worker for API queue %s", name)
			for {
				select {
				case body, ok := <-in:
					if !ok {
						return
					}
					var req redisAPIRequest
					err := json.Unmarshal(body, &req)
					if err != nil {
						logger.ERROR.Println(err)
						continue
					}
					for _, command := range req.Data {
						_, err := e.app.apiCmd(command)
						if err != nil {
							logger.ERROR.Println(err)
						}
					}
				case <-done:
					return
				}
			}
		}(name, ch)
	}

	for {
		reply, err := conn.Do("BLPOP", popParams...)
		if err != nil {
			logger.ERROR.Println(err)
			return
		}

		values, err := redis.Values(reply, nil)
		if err != nil {
			logger.ERROR.Println(err)
			return
		}
		if len(values) != 2 {
			logger.ERROR.Println("Wrong reply from Redis in BLPOP - expecting 2 values")
			continue
		}

		queue, okQ := values[0].([]byte)
		body, okVal := values[1].([]byte)
		if !okQ || !okVal {
			logger.ERROR.Println("Wrong reply from Redis in BLPOP - can not convert value")
			continue
		}

		// Pick worker based on queue
		q, ok := workQueues[string(queue)]
		if !ok {
			logger.ERROR.Println("Got message from a queue we didn't even know about!")
			continue
		}

		q <- body
	}
}

func (e *RedisEngine) initializePubSub() {
	defer e.psc.Close()
	e.Lock()
	e.inPubSub = true
	e.Unlock()
	defer func() {
		e.Lock()
		e.inPubSub = false
		e.Unlock()
	}()
	e.app.RLock()
	adminChannel := e.app.config.AdminChannel
	controlChannel := e.app.config.ControlChannel
	e.app.RUnlock()
	err := e.psc.Subscribe(adminChannel)
	if err != nil {
		e.psc.Close()
		return
	}
	err = e.psc.Subscribe(controlChannel)
	if err != nil {
		e.psc.Close()
		return
	}
	for _, chID := range e.app.clients.channels() {
		err = e.psc.Subscribe(chID)
		if err != nil {
			e.psc.Close()
			return
		}
	}
	for {
		switch n := e.psc.Receive().(type) {
		case redis.Message:
			e.app.handleMsg(ChannelID(n.Channel), n.Data)
		case redis.Subscription:
		case error:
			logger.ERROR.Printf("error: %v\n", n)
			e.psc.Close()
			return
		}
	}
}

func (e *RedisEngine) publish(chID ChannelID, message []byte) (bool, error) {
	conn := e.pool.Get()
	defer conn.Close()
	numSubscribers, err := redis.Int(conn.Do("PUBLISH", chID, message))
	return numSubscribers > 0, err
}

func (e *RedisEngine) subscribe(chID ChannelID) error {
	logger.DEBUG.Println("subscribe on Redis channel", chID)
	return e.psc.Subscribe(chID)
}

func (e *RedisEngine) unsubscribe(chID ChannelID) error {
	logger.DEBUG.Println("unsubscribe from Redis channel", chID)
	return e.psc.Unsubscribe(chID)
}

func (e *RedisEngine) getHashKey(chID ChannelID) string {
	e.app.RLock()
	defer e.app.RUnlock()
	return e.app.config.ChannelPrefix + ".presence.hash." + string(chID)
}

func (e *RedisEngine) getSetKey(chID ChannelID) string {
	e.app.RLock()
	defer e.app.RUnlock()
	return e.app.config.ChannelPrefix + ".presence.set." + string(chID)
}

func (e *RedisEngine) getHistoryKey(chID ChannelID) string {
	e.app.RLock()
	defer e.app.RUnlock()
	return e.app.config.ChannelPrefix + ".history.list." + string(chID)
}

func (e *RedisEngine) addPresence(chID ChannelID, uid ConnID, info ClientInfo) error {
	e.app.RLock()
	presenceExpireInterval := e.app.config.PresenceExpireInterval
	e.app.RUnlock()
	conn := e.pool.Get()
	defer conn.Close()
	infoJSON, err := json.Marshal(info)
	if err != nil {
		return err
	}
	expireAt := time.Now().Unix() + int64(presenceExpireInterval.Seconds())
	hashKey := e.getHashKey(chID)
	setKey := e.getSetKey(chID)
	conn.Send("MULTI")
	conn.Send("ZADD", setKey, expireAt, uid)
	conn.Send("HSET", hashKey, uid, infoJSON)
	conn.Send("EXPIRE", setKey, presenceExpireInterval)
	conn.Send("EXPIRE", hashKey, presenceExpireInterval)
	_, err = conn.Do("EXEC")
	return err
}

func (e *RedisEngine) removePresence(chID ChannelID, uid ConnID) error {
	conn := e.pool.Get()
	defer conn.Close()
	hashKey := e.getHashKey(chID)
	setKey := e.getSetKey(chID)
	conn.Send("MULTI")
	conn.Send("HDEL", hashKey, uid)
	conn.Send("ZREM", setKey, uid)
	_, err := conn.Do("EXEC")
	return err
}

func mapStringClientInfo(result interface{}, err error) (map[ConnID]ClientInfo, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("mapStringClientInfo expects even number of values result")
	}
	m := make(map[ConnID]ClientInfo, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, okKey := values[i].([]byte)
		value, okValue := values[i+1].([]byte)
		if !okKey || !okValue {
			return nil, errors.New("ScanMap key not a bulk string value")
		}
		var f ClientInfo
		err = json.Unmarshal(value, &f)
		if err != nil {
			return nil, errors.New("can not unmarshal value to ClientInfo")
		}
		m[ConnID(key)] = f
	}
	return m, nil
}

func (e *RedisEngine) presence(chID ChannelID) (map[ConnID]ClientInfo, error) {
	conn := e.pool.Get()
	defer conn.Close()
	now := time.Now().Unix()
	hashKey := e.getHashKey(chID)
	setKey := e.getSetKey(chID)
	reply, err := conn.Do("ZRANGEBYSCORE", setKey, 0, now)
	if err != nil {
		return nil, err
	}
	expiredKeys, err := redis.Strings(reply, nil)
	if err != nil {
		return nil, err
	}
	if len(expiredKeys) > 0 {
		conn.Send("MULTI")
		conn.Send("ZREMRANGEBYSCORE", setKey, 0, now)
		for _, key := range expiredKeys {
			conn.Send("HDEL", hashKey, key)
		}
		_, err = conn.Do("EXEC")
		if err != nil {
			return nil, err
		}
	}
	reply, err = conn.Do("HGETALL", hashKey)
	if err != nil {
		return nil, err
	}
	return mapStringClientInfo(reply, nil)
}

func (e *RedisEngine) addHistory(chID ChannelID, message Message, opts addHistoryOpts) error {
	conn := e.pool.Get()
	defer conn.Close()

	historyKey := e.getHistoryKey(chID)
	messageJSON, err := json.Marshal(message)
	if err != nil {
		return err
	}

	pushCommand := "LPUSH"

	if opts.DropInactive {
		pushCommand = "LPUSHX"
	}

	conn.Send("MULTI")
	conn.Send(pushCommand, historyKey, messageJSON)
	// All below commands are a simple no-op in redis if the key doesn't exist
	conn.Send("LTRIM", historyKey, 0, opts.Size-1)
	conn.Send("EXPIRE", historyKey, opts.Lifetime)
	_, err = conn.Do("EXEC")
	return err
}

func sliceOfMessages(result interface{}, err error) ([]Message, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	msgs := make([]Message, len(values))
	for i := 0; i < len(values); i++ {
		value, okValue := values[i].([]byte)
		if !okValue {
			return nil, errors.New("error getting Message value")
		}
		var m Message
		err = json.Unmarshal(value, &m)
		if err != nil {
			return nil, errors.New("can not unmarshal value to Message")
		}
		msgs[i] = m
	}
	return msgs, nil
}

func (e *RedisEngine) history(chID ChannelID, opts historyOpts) ([]Message, error) {
	conn := e.pool.Get()
	defer conn.Close()
	var rangeBound int = -1
	if opts.Limit > 0 {
		rangeBound = opts.Limit - 1 // Redis includes last index into result
	}
	historyKey := e.getHistoryKey(chID)
	reply, err := conn.Do("LRANGE", historyKey, 0, rangeBound)
	if err != nil {
		return nil, err
	}
	return sliceOfMessages(reply, nil)
}

func sliceOfChannelIDs(result interface{}, prefix string, err error) ([]ChannelID, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	channels := make([]ChannelID, len(values))
	for i := 0; i < len(values); i++ {
		value, okValue := values[i].([]byte)
		if !okValue {
			return nil, errors.New("error getting ChannelID value")
		}
		chID := ChannelID(value)
		channels[i] = chID
	}
	return channels, nil
}

// Requires Redis >= 2.8.0 (http://redis.io/commands/pubsub)
func (e *RedisEngine) channels() ([]ChannelID, error) {
	conn := e.pool.Get()
	defer conn.Close()
	prefix := e.app.channelIDPrefix()
	reply, err := conn.Do("PUBSUB", "CHANNELS", prefix+"*")
	if err != nil {
		return nil, err
	}
	return sliceOfChannelIDs(reply, prefix, nil)
}
