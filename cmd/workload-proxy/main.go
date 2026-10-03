// workload-proxy is a task Pod's unprivileged overlay sidecar. Subcommands:
//
//	enroll  writes the workload's identity from ZITI_ENROLL_TOKEN (init container)
//	serve   the loopback HTTP/CONNECT proxy and fixed forwards (restartable init)
//	wait    proves end-to-end readiness through serve (last init container)
//
// serve is also the default when the first argument is a flag. No subcommand
// creates an identity of its own, edits resolver/hosts files, or needs a Linux
// capability; every listener is a literal loopback address.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/agynio/k8s-runner/internal/workloadproxy"
	"github.com/openziti/sdk-golang/ziti"
)

type repeated []string

func (f *repeated) String() string         { return strings.Join(*f, ",") }
func (f *repeated) Set(value string) error { *f = append(*f, value); return nil }

func main() {
	args := os.Args[1:]
	command := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch command {
	case "serve":
		err = serve(ctx, args)
	case "enroll":
		err = enrollCommand(ctx, args)
	case "wait":
		err = waitCommand(ctx, args)
	default:
		err = fmt.Errorf("unknown subcommand %q (want serve, enroll or wait)", command)
	}
	if err != nil {
		log.Printf("workload-proxy %s: %v", command, err)
		os.Exit(1)
	}
}

func enrollCommand(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("enroll", flag.ContinueOnError)
	identity := flags.String("identity", "", "identity file to create")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *identity == "" {
		return errors.New("--identity is required")
	}
	// Read once and drop from the environment: nothing after this needs it.
	token := os.Getenv("ZITI_ENROLL_TOKEN")
	_ = os.Unsetenv("ZITI_ENROLL_TOKEN")
	if err := workloadproxy.EnrollIdentity(ctx, *identity, token, workloadproxy.ZitiEnroll); err != nil {
		return err
	}
	log.Print("workload identity ready")
	return nil
}

func waitCommand(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("wait", flag.ContinueOnError)
	proxy := flags.String("proxy", "127.0.0.1:18080", "the serve proxy listener")
	var httpProbes, connects, tripwires repeated
	flags.Var(&httpProbes, "http", "loopback forward that must answer HTTP (repeatable)")
	flags.Var(&connects, "connect", "overlay authority to CONNECT and request over plaintext HTTP (repeatable)")
	flags.Var(&tripwires, "tripwire", "unrouted authority the proxy must refuse (repeatable; defaults are built in)")
	timeout := flags.Duration("timeout", 180*time.Second, "readiness budget")
	interval := flags.Duration("interval", 250*time.Millisecond, "poll interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	options := workloadproxy.WaitOptions{Proxy: *proxy, HTTP: httpProbes, Connect: connects, Timeout: *timeout, Interval: *interval, Log: os.Stderr}
	if len(tripwires) > 0 {
		options.Tripwires = tripwires
	}
	return workloadproxy.Wait(ctx, options)
}

func serve(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	identity := flags.String("identity", "", "existing enrolled workload identity file")
	listen := flags.String("listen", "127.0.0.1:18080", "literal loopback HTTP proxy listener")
	refresh := flags.Duration("service-refresh", 5*time.Second, "how often the identity's service list is refreshed")
	var mappings, denies repeated
	flags.Var(&mappings, "forward", "loopback-listen=original-overlay-host:port (repeatable)")
	directEgress := flags.Bool("direct-egress", false, "dial destinations the overlay does not intercept directly, public addresses only")
	flags.Var(&denies, "direct-deny", "extra address or CIDR direct egress must never reach, such as the node's public address (repeatable)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *identity == "" {
		return errors.New("identity file is required")
	}
	var direct *workloadproxy.DirectEgress
	if *directEgress {
		deny, err := workloadproxy.ParseDeny(denies)
		if err != nil {
			return err
		}
		direct = workloadproxy.NewDirectEgress(deny)
	} else if len(denies) > 0 {
		return errors.New("--direct-deny requires --direct-egress")
	}
	forwards := map[string]string{}
	for _, mapping := range mappings {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			return errors.New("invalid forwarding mapping")
		}
		if _, duplicate := forwards[parts[0]]; duplicate {
			return fmt.Errorf("duplicate forwarding listener %q", parts[0])
		}
		forwards[parts[0]] = parts[1]
	}
	if err := workloadproxy.ValidateForwards(*listen, forwards); err != nil {
		return err
	}
	config, err := ziti.NewConfigFromFile(*identity)
	if err != nil {
		return errors.New("cannot load workload identity")
	}
	config.ConfigTypes = append(config.ConfigTypes, "intercept.v1")
	// Workload HTTP_PROXY settings must not route the controller back into this
	// proxy. A nil CtrlProxy would mean ProxyFromEnvironment, so it is an
	// explicit function that never proxies.
	config.CtrlProxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	// The default five-minute refresh would leave a newly attached egress rule
	// unusable for minutes; misses also trigger a bounded on-demand refresh.
	overlay, err := ziti.NewContextWithOpts(config, &ziti.Options{RefreshInterval: *refresh, RefreshJitter: 0.1})
	if err != nil {
		return errors.New("cannot initialize overlay")
	}
	defer overlay.Close()
	if err = overlay.Authenticate(); err != nil {
		return errors.New("overlay authentication failed")
	}
	contexts := ziti.NewSdkCollection()
	contexts.Add(overlay)
	// No overlay fallback dialer: the only path outside the overlay is the
	// explicit, public-only direct egress below.
	sdkDialer := contexts.NewDialer()
	if err := workloadproxy.RequireNoFallback(sdkDialer); err != nil {
		return err
	}
	dialer, ok := sdkDialer.(ziti.ContextDialer)
	if !ok {
		return errors.New("context-aware overlay dialer unavailable")
	}
	proxy := workloadproxy.NewWithOptions(dialer, workloadproxy.Options{
		Classifier: &workloadproxy.ContextClassifier{Context: overlay},
		Forwards:   forwards,
		Direct:     direct,
	})
	defer proxy.Close()
	ctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	errForwarder := errors.New("native forwarder stopped")
	for listen, target := range forwards {
		listener, err := workloadproxy.ListenLoopback(listen)
		if err != nil {
			return fmt.Errorf("invalid forwarding listener %q", listen)
		}
		defer listener.Close()
		go func() {
			if err := proxy.Forward(ctx, listener, target); err != nil && ctx.Err() == nil {
				stop(errForwarder)
			}
		}()
	}
	listener, err := workloadproxy.ListenLoopback(*listen)
	if err != nil {
		return errors.New("invalid proxy listener")
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 * 1024}
	go func() {
		<-ctx.Done()
		proxy.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	log.Printf("workload proxy listening on %s with %d fixed forwards, direct egress %t", *listen, len(forwards), direct != nil)
	if err = server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("proxy listener stopped")
	}
	// A failed forwarder fails the sidecar, which Kubernetes then restarts.
	if errors.Is(context.Cause(ctx), errForwarder) {
		return errForwarder
	}
	return nil
}
