package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"code.cloudfoundry.org/gorouter/logadapter"
	"code.cloudfoundry.org/gorouter/metrics_prometheus"
	"golang.org/x/sync/errgroup"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/tlsconfig"
	"github.com/cloudfoundry/dropsonde"
	"github.com/cloudfoundry/dropsonde/metric_sender"
	"github.com/cloudfoundry/dropsonde/metricbatcher"
	"github.com/nats-io/nats.go"

	"code.cloudfoundry.org/gorouter/accesslog"
	"code.cloudfoundry.org/gorouter/common/health"
	"code.cloudfoundry.org/gorouter/common/schema"
	"code.cloudfoundry.org/gorouter/common/secure"
	"code.cloudfoundry.org/gorouter/config"
	"code.cloudfoundry.org/gorouter/errorwriter"
	grlog "code.cloudfoundry.org/gorouter/logger"
	"code.cloudfoundry.org/gorouter/mbus"
	"code.cloudfoundry.org/gorouter/metrics"
	"code.cloudfoundry.org/gorouter/metrics/monitor"
	"code.cloudfoundry.org/gorouter/proxy"
	rregistry "code.cloudfoundry.org/gorouter/registry"
	"code.cloudfoundry.org/gorouter/route_fetcher"
	"code.cloudfoundry.org/gorouter/router"
	"code.cloudfoundry.org/gorouter/routeservice"
	rvarz "code.cloudfoundry.org/gorouter/varz"
	routing_api "code.cloudfoundry.org/routing-api"
	"code.cloudfoundry.org/routing-api/uaaclient"
)

var (
	configFile string
	h          *health.Health
)

func main() {
	flag.StringVar(&configFile, "c", "", "Configuration File")
	flag.Parse()

	prefix := "gorouter.stdout"
	coreLogger := grlog.CreateLogger()
	grlog.SetLoggingLevel("INFO")

	c, err := config.DefaultConfig()
	if err != nil {
		grlog.Fatal(coreLogger, "Error loading config", grlog.ErrAttr(err))
	}

	if configFile != "" {
		c, err = config.InitConfigFromFile(configFile)
		if err != nil {
			grlog.Fatal(coreLogger, "Error loading config:", grlog.ErrAttr(err))
		}
	}
	logCounter := schema.NewLogCounter()

	if c.Logging.Syslog != "" {
		prefix = c.Logging.Syslog
	}

	grlog.SetLoggingLevel(c.Logging.Level)
	grlog.SetTimeEncoder(c.Logging.Format.Timestamp)
	logger := grlog.CreateLoggerWithSource(prefix, "")
	logger.Info("starting")
	logger.Debug("local-az-set", slog.String("AvailabilityZone", c.Zone))

	var ew errorwriter.ErrorWriter
	if c.HTMLErrorTemplateFile != "" {
		ew, err = errorwriter.NewHTMLErrorWriterFromFile(c.HTMLErrorTemplateFile)
		if err != nil {
			grlog.Fatal(logger, "new-html-error-template-from-file", grlog.ErrAttr(err))
		}
	} else {
		ew = errorwriter.NewPlaintextErrorWriter()
	}

	logger.Info("retrieved-isolation-segments",
		slog.Any("isolation_segments", c.IsolationSegments),
		slog.String("routing_table_sharding_mode", c.RoutingTableShardingMode),
	)

	// setup number of procs
	if c.GoMaxProcs != 0 {
		runtime.GOMAXPROCS(c.GoMaxProcs)
	}

	if c.DebugAddr != "" {
		sink := logadapter.NewZapLevelSink(logger)
		_, err = debugserver.Run(c.DebugAddr, sink)
		if err != nil {
			logger.Error("failed-to-start-debug-server", grlog.ErrAttr(err))
		}
		logger.Info("debugserver-started",
			slog.String("address", c.DebugAddr),
			slog.String("log_level", c.Logging.Level),
			slog.String("log_format", c.Logging.Format.Timestamp),
		)
	}

	logger.Info("setting-up-nats-connection")
	natsReconnected := make(chan mbus.Signal)
	natsClient := mbus.Connect(c, natsReconnected, grlog.CreateLoggerWithSource(prefix, "nats"))

	var routingAPIClient routing_api.Client

	if c.RoutingApiEnabled() {
		logger.Info("setting-up-routing-api")

		routingAPIClient, err = setupRoutingAPIClient(logger, c)
		if err != nil {
			grlog.Fatal(logger, "routing-api-connection-failed", grlog.ErrAttr(err))
		}

	}

	dropReporter := initializeDropsondeReporter(prefix, logger, c)
	promReporter := initializePrometheusReporter(c)

	reporters := make([]metrics.MetricReporter, 0)
	if dropReporter != nil {
		reporters = append(reporters, dropReporter)
	}
	if promReporter != nil {
		reporters = append(reporters, promReporter)
	}
	metricReporter := metrics.NewMultiMetricReporter(reporters...)

	registry := rregistry.NewRouteRegistry(grlog.CreateLoggerWithSource(prefix, "registry"), c, metricReporter)
	varz := rvarz.NewVarz(registry)
	compositeReporter := &metrics.CompositeReporter{VarzReporter: varz, MetricReporter: metricReporter}

	if c.SuspendPruningIfNatsUnavailable {
		registry.SuspendPruning(func() bool { return !(natsClient.Status() == nats.CONNECTED) })
	}

	fdMonitor := initializeFDMonitor(metricReporter, grlog.CreateLoggerWithSource(prefix, "FileDescriptor"))

	accessLogger, err := accesslog.CreateRunningAccessLogger(
		grlog.CreateLoggerWithSource(prefix, "access-grlog"),
		accesslog.NewLogSender(c, dropsonde.AutowiredEmitter(), logger),
		c,
	)
	if err != nil {
		grlog.Fatal(logger, "error-creating-access-logger", grlog.ErrAttr(err))
	}

	var crypto secure.Crypto
	var cryptoPrev secure.Crypto
	if c.RouteServiceEnabled {
		crypto = createCrypto(logger, c.RouteServiceSecret)
		if c.RouteServiceSecretPrev != "" {
			cryptoPrev = createCrypto(logger, c.RouteServiceSecretPrev)
		}
	}

	routeServiceConfig := routeservice.NewRouteServiceConfig(
		grlog.CreateLoggerWithSource(prefix, "proxy"),
		c.RouteServiceEnabled,
		c.RouteServicesHairpinning,
		c.RouteServicesHairpinningAllowlist,
		c.RouteServiceTimeout,
		crypto,
		cryptoPrev,
		c.RouteServiceRecommendHttps,
		c.RouteServiceConfig.StrictSignatureValidation,
		c.RouteServiceConfig.EnableWebsockets,
	)

	// These TLS configs are just templates. If you add other keys you will
	// also need to edit proxy/utils/tls_config.go
	backendTLSConfig := &tls.Config{
		CipherSuites: c.CipherSuites,
		RootCAs:      c.CAPool,
		Certificates: []tls.Certificate{c.Backends.ClientAuthCertificate},
	}

	routeServiceTLSConfig := &tls.Config{
		CipherSuites:       c.CipherSuites,
		InsecureSkipVerify: c.SkipSSLValidation,
		RootCAs:            c.CAPool,
		Certificates:       []tls.Certificate{c.RouteServiceConfig.ClientAuthCertificate},
		MinVersion:         c.MinTLSVersion,
		MaxVersion:         c.MaxTLSVersion,
	}

	rss, err := router.NewRouteServicesServer(c)
	if err != nil {
		grlog.Fatal(logger, "new-route-services-server", grlog.ErrAttr(err))
	}

	h = &health.Health{}
	proxyHandler := proxy.NewProxy(
		logger,
		accessLogger,
		ew,
		c,
		registry,
		compositeReporter,
		routeServiceConfig,
		backendTLSConfig,
		routeServiceTLSConfig,
		h,
		rss.GetRoundTripper(),
	)

	var errorChannel chan error = nil

	goRouter, err := router.NewRouter(
		grlog.CreateLoggerWithSource(prefix, "router"),
		c,
		proxyHandler,
		natsClient,
		registry,
		varz,
		h,
		logCounter,
		errorChannel,
		rss,
	)

	h.OnDegrade = goRouter.DrainAndStop

	if err != nil {
		grlog.Fatal(logger, "initialize-router-error", grlog.ErrAttr(err))
	}

	var routeFetcher *route_fetcher.RouteFetcher
	if c.RoutingApiEnabled() {
		routeFetcher = setupRouteFetcher(grlog.CreateLoggerWithSource(prefix, "route-fetcher"), c, registry, routingAPIClient)
	}

	subscriber := mbus.NewSubscriber(natsClient, registry, c, natsReconnected, grlog.CreateLoggerWithSource(prefix, "subscriber"))
	natsMonitor := initializeNATSMonitor(subscriber, metricReporter, grlog.CreateLoggerWithSource(prefix, "NATSMonitor"))

	// gorouter wants to know if it should drain. This would be better done by
	// having a dedicated method on gorouter which we can call to drain it but
	// for now we can work around this.
	routerSignals := make(chan os.Signal, 1)
	goRouter.OnErrOrSignal(routerSignals)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	// Setup the singal handler, this will take care of what `sigmon.New` used
	// to do. With a bit more work this could be wired up to replicate the OG
	// behaviour of relaying the signals to the individual processes. But from
	// what I've seen we mostly use it as a signal to stop, not caring about the
	// exact signal.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)
	go func() {
		for sig := range signals {
			// Copy the signal to the goRouter as it has more logic based on the
			// signal received. We give the same gurantees as signal.Notify, if
			// signals are not being read we discard them.
			select {
			case routerSignals <- sig:
			default: // skip the signal if the channel is full.
			}

			cancel()
		}
	}()

	// See explanation below, this depends on routing-api being enabled.
	if routeFetcher != nil {
		ready := make(chan struct{})
		group.Go(func() error {
			return routeFetcher.Run(ctx.Done(), ready)
		})
		<-ready
	}

	// ready is the same as before, we use it to determine if we should move on
	// to the next goroutine or not. If you don't care about an oderly start
	// just fire away all the routines and mark yourself healthy once all of the
	// returned.
	ready := make(chan struct{})
	// This starts the goroutine in the background. Should it terminate and
	// return an error the associated context (see above) is cancelled which,
	// since we pass the done channel to each one, causes all goroutines to be
	// stopped.
	group.Go(func() error {
		// The Run pattern already somehwat aligns with our new pattern, it just
		// needs a type change on the cannel from `os.Signal` to `struct{}`.
		return fdMonitor.Run(ctx.Done(), ready)
	})
	// Wait until this goroutine marks itself as ready by closing the channel we
	// gave it.
	<-ready

	// And repeat. (you might want to use different names for the ready
	// channels ¯\_(ツ)_/¯)
	ready = make(chan struct{})
	group.Go(func() error {
		return subscriber.Run(ctx.Done(), ready)
	})
	<-ready

	ready = make(chan struct{})
	group.Go(func() error {
		return natsMonitor.Run(ctx.Done(), ready)
	})
	<-ready

	ready = make(chan struct{})
	group.Go(func() error {
		return goRouter.Run(ctx.Done(), ready)
	})
	<-ready

	go func() {
		time.Sleep(c.RouteLatencyMetricMuzzleDuration)    // this way we avoid reporting metrics for pre-existing routes
		metricReporter.UnmuzzleRouteRegistrationLatency() // Required for Envelope V1. Keep it while we have both Envelope V1 and Prometheus.
	}()

	h.SetHealth(health.Healthy)

	err = group.Wait()
	if err != nil {
		grlog.Fatal(logger, "gorouter.exited-with-failure", grlog.ErrAttr(err))
	}

	os.Exit(0)
}

// initializeDropsondeReporter initializes logs. It also initializes envelope V1 metrics if enabled
func initializeDropsondeReporter(prefix string, logger *slog.Logger, c *config.Config) *metrics.Metrics {
	err := dropsonde.Initialize(c.Logging.MetronAddress, c.Logging.JobName)
	if err != nil {
		grlog.Fatal(logger, "dropsonde-initialize-error", grlog.ErrAttr(err))
	}
	if !c.EnableEnvelopeV1Metrics {
		return nil
	}
	dropsondeMetricSender := metric_sender.NewMetricSender(dropsonde.AutowiredEmitter())
	return initializeMetrics(dropsondeMetricSender, c, grlog.CreateLoggerWithSource(prefix, "metricsreporter"))
}

// initializePrometheusReporter initializes metrics via Prometheus if enabled
func initializePrometheusReporter(c *config.Config) *metrics_prometheus.Metrics {
	if !c.Prometheus.Enabled {
		return nil
	}
	promRegistry := metrics_prometheus.NewMetricsRegistry(c.Prometheus)
	return metrics_prometheus.NewMetrics(promRegistry, c.PerRequestMetricsReporting, c.Prometheus.Meters)
}

func initializeFDMonitor(reporter metrics.MetricReporter, logger *slog.Logger) *monitor.FileDescriptor {
	pid := os.Getpid()
	path := fmt.Sprintf("/proc/%d/fd", pid)
	ticker := time.NewTicker(time.Second * 5)
	return monitor.NewFileDescriptor(path, ticker, reporter, logger)
}

func initializeNATSMonitor(subscriber *mbus.Subscriber, reporter metrics.MetricReporter, logger *slog.Logger) *monitor.NATSMonitor {
	ticker := time.NewTicker(time.Second * 5)
	return &monitor.NATSMonitor{
		Subscriber: subscriber,
		Reporter:   reporter,
		TickChan:   ticker.C,
		Logger:     logger,
	}
}

func initializeMetrics(sender *metric_sender.MetricSender, c *config.Config, logger *slog.Logger) *metrics.Metrics {
	// 5 sec is dropsonde default batching interval
	batcher := metricbatcher.New(sender, 5*time.Second)
	batcher.AddConsistentlyEmittedMetrics("bad_gateways",
		"backend_exhausted_conns",
		"backend_invalid_id",
		"backend_invalid_tls_cert",
		"backend_tls_handshake_failed",
		"rejected_requests",
		"total_requests",
		"responses",
		"responses.2xx",
		"responses.3xx",
		"responses.4xx",
		"responses.5xx",
		"responses.xxx",
		"routed_app_requests",
		"routes_pruned",
		"websocket_failures",
		"websocket_upgrades",
	)

	return &metrics.Metrics{Sender: sender, Batcher: batcher, PerRequestMetricsReporting: c.PerRequestMetricsReporting, Logger: logger}
}

func createCrypto(logger *slog.Logger, secret string) *secure.AesGCM {
	// generate secure encryption key using key derivation function (pbkdf2)
	secretPbkdf2 := secure.NewPbkdf2([]byte(secret), 16)
	crypto, err := secure.NewAesGCM(secretPbkdf2)
	if err != nil {
		grlog.Fatal(logger, "error-creating-route-service-crypto", grlog.ErrAttr(err))
	}
	return crypto
}

func setupRoutingAPIClient(logger *slog.Logger, c *config.Config) (routing_api.Client, error) {
	routingAPIURI := fmt.Sprintf("%s:%d", c.RoutingApi.Uri, c.RoutingApi.Port)

	tlsConfig, err := tlsconfig.Build(
		tlsconfig.WithInternalServiceDefaults(),
		tlsconfig.WithIdentity(c.RoutingApi.ClientAuthCertificate),
	).Client(
		tlsconfig.WithAuthority(c.RoutingApi.CAPool),
	)
	if err != nil {
		return nil, err
	}

	client := routing_api.NewClientWithTLSConfig(routingAPIURI, tlsConfig)

	logger.Debug("fetching-token")
	clockInstance := clock.NewClock()

	uaaConfig := uaaclient.Config{
		Port:              c.OAuth.Port,
		SkipSSLValidation: c.OAuth.SkipSSLValidation,
		ClientName:        c.OAuth.ClientName,
		ClientSecret:      c.OAuth.ClientSecret,
		CACerts:           c.OAuth.CACerts,
		TokenEndpoint:     c.OAuth.TokenEndpoint,
	}

	uaaTokenFetcher, err := uaaclient.NewTokenFetcher(c.RoutingApi.AuthDisabled, uaaConfig, clockInstance, uint(c.TokenFetcherMaxRetries), c.TokenFetcherRetryInterval, c.TokenFetcherExpirationBufferTimeInSeconds, grlog.NewLagerAdapter(c.Logging.Syslog))
	if err != nil {
		grlog.Fatal(logger, "initialize-uaa-client", grlog.ErrAttr(err))
	}

	if !c.RoutingApi.AuthDisabled {
		token, err := uaaTokenFetcher.FetchToken(context.Background(), true)
		if err != nil {
			return nil, fmt.Errorf("unable-to-fetch-token: %s", err.Error())
		}
		if token.AccessToken == "" {
			return nil, fmt.Errorf("empty token fetched")
		}
		client.SetToken(token.AccessToken)
	}
	// Test connectivity
	if _, err := client.Routes(); err != nil {
		return nil, err
	}

	return client, nil
}

func setupRouteFetcher(logger *slog.Logger, c *config.Config, registry rregistry.Registry, routingAPIClient routing_api.Client) *route_fetcher.RouteFetcher {
	cl := clock.NewClock()

	uaaConfig := uaaclient.Config{
		Port:              c.OAuth.Port,
		SkipSSLValidation: c.OAuth.SkipSSLValidation,
		ClientName:        c.OAuth.ClientName,
		ClientSecret:      c.OAuth.ClientSecret,
		CACerts:           c.OAuth.CACerts,
		TokenEndpoint:     c.OAuth.TokenEndpoint,
	}
	clock := clock.NewClock()
	uaaTokenFetcher, err := uaaclient.NewTokenFetcher(c.RoutingApi.AuthDisabled, uaaConfig, clock, uint(c.TokenFetcherMaxRetries), c.TokenFetcherRetryInterval, c.TokenFetcherExpirationBufferTimeInSeconds, grlog.NewLagerAdapter(c.Logging.Syslog))
	if err != nil {
		grlog.Fatal(logger, "initialize-uaa-client", grlog.ErrAttr(err))
	}

	_, err = uaaTokenFetcher.FetchToken(context.Background(), true)
	if err != nil {
		grlog.Fatal(logger, "unable-to-fetch-token", grlog.ErrAttr(err))
	}

	subscriptionRetryInterval := 1 * time.Second

	routeFetcher := route_fetcher.NewRouteFetcher(
		logger,
		uaaTokenFetcher,
		registry,
		c,
		routingAPIClient,
		subscriptionRetryInterval,
		cl,
	)
	return routeFetcher
}
