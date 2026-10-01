// workload-proxy consumes the existing workload's enrolled Ziti identity. It
// creates no identity, edits no resolver/hosts files, and needs no capabilities.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/agynio/k8s-runner/internal/workloadproxy"
	"github.com/openziti/sdk-golang/ziti"
)

type forwards []string

func (f *forwards) String() string         { return strings.Join(*f, ",") }
func (f *forwards) Set(value string) error { *f = append(*f, value); return nil }

func main() {
	identity := flag.String("identity", "", "existing enrolled workload identity file")
	listen := flag.String("listen", "127.0.0.1:18080", "literal loopback HTTP proxy listener")
	var mappings forwards
	flag.Var(&mappings, "forward", "loopback-listen=original-overlay-host:port (repeatable)")
	flag.Parse()
	if *identity == "" {
		log.Fatal("identity file is required")
	}
	config, err := ziti.NewConfigFromFile(*identity)
	if err != nil {
		log.Fatal("cannot load workload identity")
	}
	config.ConfigTypes = append(config.ConfigTypes, "intercept.v1")
	// Workload HTTP_PROXY settings must not route the controller back into this
	// proxy. The controller still uses the identity's configured TLS trust.
	config.CtrlProxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	overlay, err := ziti.NewContext(config)
	if err != nil {
		log.Fatal("cannot initialize overlay")
	}
	defer overlay.Close()
	if err = overlay.Authenticate(); err != nil {
		log.Fatal("overlay authentication failed")
	}
	contexts := ziti.NewSdkCollection()
	contexts.Add(overlay)
	// No fallback dialer: unmatched or unauthorized destinations fail closed.
	dialer, ok := contexts.NewDialer().(ziti.ContextDialer)
	if !ok {
		log.Fatal("context-aware overlay dialer unavailable")
	}
	proxy := workloadproxy.New(dialer)
	defer proxy.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for _, mapping := range mappings {
		parts := strings.SplitN(mapping, "=", 2)
		if len(parts) != 2 {
			log.Fatal("invalid forwarding mapping")
		}
		if _, err = workloadproxy.Destination(parts[1]); err != nil {
			log.Fatal("invalid forwarding destination")
		}
		listener, err := workloadproxy.ListenLoopback(parts[0])
		if err != nil {
			log.Fatal("invalid forwarding listener")
		}
		defer listener.Close()
		go func() {
			if err := proxy.Forward(ctx, listener, parts[1]); err != nil && ctx.Err() == nil {
				log.Print("native forwarder stopped")
				stop()
			}
		}()
	}
	listener, err := workloadproxy.ListenLoopback(*listen)
	if err != nil {
		log.Fatal("invalid proxy listener")
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 64 * 1024}
	go func() {
		<-ctx.Done()
		proxy.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	if err = server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("proxy listener stopped")
	}
}
