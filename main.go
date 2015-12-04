package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/FZambia/go-logger"
	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/spf13/cobra"
	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/github.com/spf13/viper"
	"github.com/centrifugal/centrifugo/Godeps/_workspace/src/gopkg.in/igm/sockjs-go.v2/sockjs"
	"github.com/centrifugal/centrifugo/libcentrifugo"
	"github.com/tarantool/go-tarantool"
)

const (
	VERSION = "1.1.0"
)

func setupLogging() {
	logLevel, ok := logger.LevelMatches[strings.ToUpper(viper.GetString("log_level"))]
	if !ok {
		logLevel = logger.LevelInfo
	}
	logger.SetLogThreshold(logLevel)
	logger.SetStdoutThreshold(logLevel)

	if viper.IsSet("log_file") && viper.GetString("log_file") != "" {
		logger.SetLogFile(viper.GetString("log_file"))
		// do not log into stdout when log file provided
		logger.SetStdoutThreshold(logger.LevelNone)
	}
}

func handleSignals(app *libcentrifugo.Application) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, os.Interrupt)
	for {
		sig := <-sigc
		logger.INFO.Println("Signal received:", sig)
		switch sig {
		case syscall.SIGHUP:
			// reload application configuration on SIGHUP
			logger.INFO.Println("Reloading configuration")
			err := viper.ReadInConfig()
			if err != nil {
				switch err.(type) {
				case viper.ConfigParseError:
					logger.CRITICAL.Printf("Error parsing configuration: %s\n", err)
					continue
				default:
					logger.CRITICAL.Println("No config file found")
					continue
				}
			}
			setupLogging()
			c := newConfig()
			app.SetConfig(c)
			logger.INFO.Println("Configuration successfully reloaded")
		case syscall.SIGINT, os.Interrupt:
			logger.INFO.Println("Shutting down")
			go time.AfterFunc(10*time.Second, func() {
				os.Exit(1)
			})
			app.Shutdown()
			os.Exit(130)
		}
	}
}

func Main() {

	var configFile string

	var port string
	var address string
	var debug bool
	var name string
	var web bool
	var webPath string
	var engn string
	var logLevel string
	var logFile string
	var insecure bool
	var insecureAPI bool
	var useSSL bool
	var sslCert string
	var sslKey string

	var redisHost string
	var redisPort string
	var redisPassword string
	var redisDB string
	var redisURL string
	var redisAPI bool
	var redisPool int

	// Tarantool
	var tntHost string
	var tntPort string
	var tntUser string
	var tntPassword string
	var tntPool int
	var tntTimeoutResponse int
	var tntTimeoutReconnect int
	var tntMaxReconnect int

	var rootCmd = &cobra.Command{
		Use:   "",
		Short: "Centrifugo",
		Long:  "Centrifugo. Real-time messaging (Websockets or SockJS) server in Go.",
		Run: func(cmd *cobra.Command, args []string) {

			viper.SetDefault("gomaxprocs", 0)
			viper.SetDefault("debug", false)
			viper.SetDefault("prefix", "")
			viper.SetDefault("web", false)
			viper.SetDefault("web_path", "")
			viper.SetDefault("web_password", "")
			viper.SetDefault("web_secret", "")
			viper.SetDefault("max_channel_length", 255)
			viper.SetDefault("channel_prefix", "centrifugo")
			viper.SetDefault("node_ping_interval", 5)
			viper.SetDefault("message_send_timeout", 0)
			viper.SetDefault("ping_interval", 25)
			viper.SetDefault("node_metrics_interval", 60)
			viper.SetDefault("stale_connection_close_delay", 25)
			viper.SetDefault("expired_connection_close_delay", 25)
			viper.SetDefault("max_client_queue_size", 10485760) // 10MB
			viper.SetDefault("presence_ping_interval", 25)
			viper.SetDefault("presence_expire_interval", 60)
			viper.SetDefault("private_channel_prefix", "$")
			viper.SetDefault("namespace_channel_boundary", ":")
			viper.SetDefault("user_channel_boundary", "#")
			viper.SetDefault("user_channel_separator", ",")
			viper.SetDefault("client_channel_boundary", "&")
			viper.SetDefault("sockjs_url", "https://cdn.jsdelivr.net/sockjs/1.0/sockjs.min.js")

			viper.SetDefault("secret", "")
			viper.SetDefault("connection_lifetime", 0)
			viper.SetDefault("watch", false)
			viper.SetDefault("publish", false)
			viper.SetDefault("anonymous", false)
			viper.SetDefault("presence", false)
			viper.SetDefault("history_size", 0)
			viper.SetDefault("history_lifetime", 0)
			viper.SetDefault("namespaces", "")

			viper.SetEnvPrefix("centrifugo")
			viper.BindEnv("debug")
			viper.BindEnv("engine")
			viper.BindEnv("insecure")
			viper.BindEnv("insecure_api")
			viper.BindEnv("web")
			viper.BindEnv("web_password")
			viper.BindEnv("web_secret")
			viper.BindEnv("secret")
			viper.BindEnv("connection_lifetime")
			viper.BindEnv("watch")
			viper.BindEnv("publish")
			viper.BindEnv("anonymous")
			viper.BindEnv("join_leave")
			viper.BindEnv("presence")
			viper.BindEnv("recover")
			viper.BindEnv("history_size")
			viper.BindEnv("history_lifetime")

			viper.BindPFlag("port", cmd.Flags().Lookup("port"))
			viper.BindPFlag("address", cmd.Flags().Lookup("address"))
			viper.BindPFlag("debug", cmd.Flags().Lookup("debug"))
			viper.BindPFlag("name", cmd.Flags().Lookup("name"))
			viper.BindPFlag("web", cmd.Flags().Lookup("web"))
			viper.BindPFlag("web_path", cmd.Flags().Lookup("web_path"))
			viper.BindPFlag("engine", cmd.Flags().Lookup("engine"))
			viper.BindPFlag("insecure", cmd.Flags().Lookup("insecure"))
			viper.BindPFlag("insecure_api", cmd.Flags().Lookup("insecure_api"))
			viper.BindPFlag("ssl", cmd.Flags().Lookup("ssl"))
			viper.BindPFlag("ssl_cert", cmd.Flags().Lookup("ssl_cert"))
			viper.BindPFlag("ssl_key", cmd.Flags().Lookup("ssl_key"))
			viper.BindPFlag("log_level", cmd.Flags().Lookup("log_level"))
			viper.BindPFlag("log_file", cmd.Flags().Lookup("log_file"))

			// Redis.
			viper.BindPFlag("redis_host", cmd.Flags().Lookup("redis_host"))
			viper.BindPFlag("redis_port", cmd.Flags().Lookup("redis_port"))
			viper.BindPFlag("redis_password", cmd.Flags().Lookup("redis_password"))
			viper.BindPFlag("redis_db", cmd.Flags().Lookup("redis_db"))
			viper.BindPFlag("redis_url", cmd.Flags().Lookup("redis_url"))
			viper.BindPFlag("redis_api", cmd.Flags().Lookup("redis_api"))
			viper.BindPFlag("redis_pool", cmd.Flags().Lookup("redis_pool"))

			// Tarantool.
			viper.BindPFlag("tnt_pool", cmd.Flags().Lookup("tnt_pool"))
			viper.BindPFlag("tnt_host", cmd.Flags().Lookup("tnt_host"))
			viper.BindPFlag("tnt_port", cmd.Flags().Lookup("tnt_port"))
			viper.BindPFlag("tnt_user", cmd.Flags().Lookup("tnt_user"))
			viper.BindPFlag("tnt_password", cmd.Flags().Lookup("tnt_password"))
			viper.BindPFlag("tnt_timeout_response", cmd.Flags().Lookup("tnt_timeout_request"))
			viper.BindPFlag("tnt_timeout_reconnect", cmd.Flags().Lookup("tnt_timeout_reconnect"))
			viper.BindPFlag("tnt_max_reconnect", cmd.Flags().Lookup("tnt_max_reconnect"))

			viper.SetConfigFile(configFile)

			logger.INFO.Printf("Centrifugo version: %s", VERSION)
			logger.INFO.Printf("Process PID: %d", os.Getpid())

			absConfPath, err := filepath.Abs(configFile)
			if err != nil {
				logger.FATAL.Fatalln(err)
			}
			logger.INFO.Println("Config file search path:", absConfPath)

			err = viper.ReadInConfig()
			if err != nil {
				switch err.(type) {
				case viper.ConfigParseError:
					logger.FATAL.Fatalf("Error parsing configuration: %s\n", err)
				default:
					logger.WARN.Println("No config file found")
				}
			}

			setupLogging()

			if os.Getenv("GOMAXPROCS") == "" {
				if viper.IsSet("gomaxprocs") && viper.GetInt("gomaxprocs") > 0 {
					runtime.GOMAXPROCS(viper.GetInt("gomaxprocs"))
				} else {
					runtime.GOMAXPROCS(runtime.NumCPU())
				}
			}

			logger.INFO.Println("GOMAXPROCS:", runtime.GOMAXPROCS(0))

			c := newConfig()
			err = c.Validate()
			if err != nil {
				logger.FATAL.Fatalln(err)
			}

			app, err := libcentrifugo.NewApplication(c)
			if err != nil {
				logger.FATAL.Fatalln(err)
			}

			if c.Insecure {
				logger.WARN.Println("application running in INSECURE client mode")
			}
			if c.InsecureAPI {
				logger.WARN.Println("application running in INSECURE API mode")
			}

			var e libcentrifugo.Engine
			switch viper.GetString("engine") {
			case "memory":
				e = libcentrifugo.NewMemoryEngine(app)
			case "redis":
				e = libcentrifugo.NewRedisEngine(
					app,
					viper.GetString("redis_host"),
					viper.GetString("redis_port"),
					viper.GetString("redis_password"),
					viper.GetString("redis_db"),
					viper.GetString("redis_url"),
					viper.GetBool("redis_api"),
					viper.GetInt("redis_pool"),
				)
			case "tarantool":
				config := libcentrifugo.TarantoolEngineConfig{
					PoolConfig: libcentrifugo.TarantoolPoolConfig{
						Address:  viper.GetString("tnt_host") + ":" + viper.GetString("tnt_port"),
						PoolSize: viper.GetInt("tnt_pool"),
						Opts: tarantool.Opts{
							Timeout:       time.Duration(viper.GetInt("tnt_timeout_response")) * time.Millisecond,
							Reconnect:     time.Duration(viper.GetInt("tnt_timeout_reconnect")) * time.Millisecond,
							MaxReconnects: uint(viper.GetInt("tnt_max_reconnect")),
							User:          viper.GetString("tnt_user"),
							Pass:          viper.GetString("tnt_password"),
						},
					},
				}
				e = libcentrifugo.NewTarantoolEngine(app, config)
			default:
				logger.FATAL.Fatalln("Unknown engine: " + viper.GetString("engine"))
			}

			logger.INFO.Println("Engine:", viper.GetString("engine"))
			logger.DEBUG.Printf("%v\n", viper.AllSettings())
			logger.INFO.Println("Use SSL:", viper.GetBool("ssl"))
			if viper.GetBool("ssl") {
				if viper.GetString("ssl_cert") == "" {
					logger.FATAL.Println("No SSL certificate provided")
					os.Exit(1)
				}
				if viper.GetString("ssl_key") == "" {
					logger.FATAL.Println("No SSL certificate key provided")
					os.Exit(1)
				}
			}
			app.SetEngine(e)
			err = app.Run()
			if err != nil {
				logger.FATAL.Fatalln(err)
			}

			go handleSignals(app)

			sockjsOpts := sockjs.DefaultOptions

			// Override sockjs url. It's important to use the same SockJS library version
			// on client and server sides, otherwise SockJS will report version mismatch
			// and won't work.
			sockjsUrl := viper.GetString("sockjs_url")
			if sockjsUrl != "" {
				logger.INFO.Println("SockJS url:", sockjsUrl)
				sockjsOpts.SockJSURL = sockjsUrl
			}
			if c.PingInterval < time.Second {
				logger.FATAL.Fatalln("Ping interval can not be less than one second.")
			}
			sockjsOpts.HeartbeatDelay = c.PingInterval

			var webFS http.FileSystem
			if viper.GetBool("web") {
				webFS = assetFS()
			}

			muxOpts := libcentrifugo.MuxOptions{
				Prefix:        viper.GetString("prefix"),
				Web:           viper.GetBool("web"),
				WebPath:       viper.GetString("web_path"),
				WebFS:         webFS,
				SockjsOptions: sockjsOpts,
			}

			mux := libcentrifugo.DefaultMux(app, muxOpts)

			addr := net.JoinHostPort(viper.GetString("address"), viper.GetString("port"))
			logger.INFO.Printf("Start serving on %s\n", addr)
			if useSSL {
				if err := http.ListenAndServeTLS(addr, sslCert, sslKey, mux); err != nil {
					logger.FATAL.Fatalln("ListenAndServe:", err)
				}
			} else {
				if err := http.ListenAndServe(addr, mux); err != nil {
					logger.FATAL.Fatalln("ListenAndServe:", err)
				}
			}
		},
	}
	rootCmd.Flags().StringVarP(&port, "port", "p", "8000", "port to bind to")
	rootCmd.Flags().StringVarP(&address, "address", "a", "", "address to listen on")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "debug mode - please, do not use it in production")
	rootCmd.Flags().StringVarP(&configFile, "config", "c", "config.json", "path to config file")
	rootCmd.Flags().StringVarP(&name, "name", "n", "", "unique node name")
	rootCmd.Flags().BoolVarP(&web, "web", "w", false, "serve admin web interface application")
	rootCmd.Flags().StringVarP(&webPath, "web_path", "", "", "optional path to web interface application")
	rootCmd.Flags().StringVarP(&engn, "engine", "e", "memory", "engine to use: memory or redis")
	rootCmd.Flags().BoolVarP(&insecure, "insecure", "", false, "start in insecure client mode")
	rootCmd.Flags().BoolVarP(&insecureAPI, "insecure_api", "", false, "use insecure API mode")
	rootCmd.Flags().BoolVarP(&useSSL, "ssl", "", false, "accept SSL connections. This requires an X509 certificate and a key file")
	rootCmd.Flags().StringVarP(&sslCert, "ssl_cert", "", "", "path to an X509 certificate file")
	rootCmd.Flags().StringVarP(&sslKey, "ssl_key", "", "", "path to an X509 certificate key")
	rootCmd.Flags().StringVarP(&logLevel, "log_level", "", "info", "set the log level: debug, info, error, critical, fatal or none")
	rootCmd.Flags().StringVarP(&logFile, "log_file", "", "", "optional log file - if not specified all logs go to STDOUT")

	// Redis engine
	rootCmd.Flags().StringVarP(&redisHost, "redis_host", "", "127.0.0.1", "redis host (Redis engine)")
	rootCmd.Flags().StringVarP(&redisPort, "redis_port", "", "6379", "redis port (Redis engine)")
	rootCmd.Flags().StringVarP(&redisPassword, "redis_password", "", "", "redis auth password (Redis engine)")
	rootCmd.Flags().StringVarP(&redisDB, "redis_db", "", "0", "redis database (Redis engine)")
	rootCmd.Flags().StringVarP(&redisURL, "redis_url", "", "", "redis connection URL (Redis engine)")
	rootCmd.Flags().BoolVarP(&redisAPI, "redis_api", "", false, "enable Redis API listener (Redis engine)")
	rootCmd.Flags().IntVarP(&redisPool, "redis_pool", "", 256, "Redis pool size (Redis engine)")

	// Tarantool engine
	rootCmd.Flags().StringVarP(&tntHost, "tnt_host", "", "127.0.0.1", "tarantool host (Tarantool engine)")
	rootCmd.Flags().StringVarP(&tntPort, "tnt_port", "", "3301", "tarantool port (Tarantool engine)")
	rootCmd.Flags().StringVarP(&tntUser, "tnt_user", "", "", "tarantool user (Tarantool engine)")
	rootCmd.Flags().StringVarP(&tntPassword, "tnt_password", "", "", "tarantool password (Tarantool engine)")
	rootCmd.Flags().IntVarP(&tntPool, "tnt_pool", "", 2, "tarantool connection pool size (Tarantool engine)")
	rootCmd.Flags().IntVarP(&tntTimeoutResponse, "tnt_timeout_response", "", 500, "timeout to wait response in milliseconds (Tarantool engine)")
	rootCmd.Flags().IntVarP(&tntTimeoutReconnect, "tnt_timeout_reconnect", "", 500, "timeout to wait until reconnection attempt in milliseconds (Tarantool engine)")
	rootCmd.Flags().IntVarP(&tntMaxReconnect, "tnt_max_reconnect", "", 0, "max number of reconnection attempts (Tarantool engine)")

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Centrifugo version number",
		Long:  `Print the version number of Centrifugo`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Centrifugo v%s\n", VERSION)
		},
	}

	var checkConfigFile string

	var checkConfigCmd = &cobra.Command{
		Use:   "checkconfig",
		Short: "Check configuration file",
		Long:  `Check Centrifugo configuration file`,
		Run: func(cmd *cobra.Command, args []string) {
			err := validateConfig(checkConfigFile)
			if err != nil {
				logger.FATAL.Fatalln(err)
			}
		},
	}
	checkConfigCmd.Flags().StringVarP(&checkConfigFile, "config", "c", "config.json", "path to config file to check")

	var outputConfigFile string

	var generateConfigCmd = &cobra.Command{
		Use:   "genconfig",
		Short: "Generate simple configuration file to start with",
		Long:  `Generate simple configuration file to start with`,
		Run: func(cmd *cobra.Command, args []string) {
			err := generateConfig(outputConfigFile)
			if err != nil {
				logger.FATAL.Fatalln(err)
			}
		},
	}
	generateConfigCmd.Flags().StringVarP(&outputConfigFile, "config", "c", "config.json", "path to output config file")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(checkConfigCmd)
	rootCmd.AddCommand(generateConfigCmd)
	rootCmd.Execute()
}

func main() {
	Main()
}
