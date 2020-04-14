// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdmain

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	"go.etcd.io/etcd/proxy/tcpproxy"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	hotConfigFile                string
	gatewayListenAddr            string
	gatewayEndpoints             []string
	gatewayDNSCluster            string
	gatewayDNSClusterServiceName string
	gatewayInsecureDiscovery     bool
	getewayRetryDelay            time.Duration
	gatewayCA                    string
)

var (
	rootCmd = &cobra.Command{
		Use:        "etcd",
		Short:      "etcd server",
		SuggestFor: []string{"etcd"},
	}
	tp *tcpproxy.TCPProxy = nil
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.AddCommand(newGatewayCommand())
}

// newGatewayCommand returns the cobra command for "gateway".
func newGatewayCommand() *cobra.Command {
	lpc := &cobra.Command{
		Use:   "gateway <subcommand>",
		Short: "gateway related command",
	}
	lpc.AddCommand(newGatewayStartCommand())

	return lpc
}

func newGatewayStartCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "start",
		Short: "start the gateway",
		Run:   startGateway,
	}

	cmd.Flags().StringVar(&hotConfigFile, "hot-config-file", "gateway.yaml", "config file")
	cmd.Flags().StringVar(&gatewayListenAddr, "listen-addr", "127.0.0.1:23790", "listen address")
	cmd.Flags().StringVar(&gatewayDNSCluster, "discovery-srv", "", "DNS domain used to bootstrap initial cluster")
	cmd.Flags().StringVar(&gatewayDNSClusterServiceName, "discovery-srv-name", "", "service name to query when using DNS discovery")
	cmd.Flags().BoolVar(&gatewayInsecureDiscovery, "insecure-discovery", false, "accept insecure SRV records")
	cmd.Flags().StringVar(&gatewayCA, "trusted-ca-file", "", "path to the client server TLS CA file.")

	cmd.Flags().DurationVar(&getewayRetryDelay, "retry-delay", time.Minute, "duration of delay before retrying failed endpoints")

	return &cmd
}

func loadHotConfig() {
	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	endpoints := viper.Get("endpoints")
	if endpoints == nil {
		fmt.Fprintln(os.Stderr, "load endpoints failed.")
		return
	}

	gatewayEndpoints = cast.ToStringSlice(endpoints)
	if 0 == len(gatewayEndpoints) {
		fmt.Fprintln(os.Stderr, "endpoints is empty.")
		os.Exit(1)
		return
	}

	fmt.Printf("load endpoints:%v\n", gatewayEndpoints)

	if tp != nil {
		tp.Update(gatewayEndpoints)
	}
}

func initConfig() {
	viper.SetConfigFile(hotConfigFile)

	loadHotConfig()

	viper.OnConfigChange(func(in fsnotify.Event) {
		loadHotConfig()
	})
	viper.WatchConfig()

	fmt.Println("Using hot config file:", viper.ConfigFileUsed())
}

func stripSchema(eps []string) []string {
	var endpoints []string
	for _, ep := range eps {
		if u, err := url.Parse(ep); err == nil && u.Host != "" {
			ep = u.Host
		}
		endpoints = append(endpoints, ep)
	}
	return endpoints
}

func startGateway(cmd *cobra.Command, args []string) {
	var lg *zap.Logger
	lg, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	srvs := discoverEndpoints(lg, gatewayDNSCluster, gatewayCA, gatewayInsecureDiscovery, gatewayDNSClusterServiceName)
	if len(srvs.Endpoints) == 0 {
		// no endpoints discovered, fall back to provided endpoints
		srvs.Endpoints = gatewayEndpoints
	}
	// Strip the schema from the endpoints because we start just a TCP proxy
	if 0 == len(srvs.SRVs) {
		srvs.SRVs = parseEndpoints(srvs.Endpoints)
	}

	var l net.Listener
	l, err = net.Listen("tcp", gatewayListenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	tp = &tcpproxy.TCPProxy{
		Logger:          lg,
		Listener:        l,
		Endpoints:       srvs.SRVs,
		MonitorInterval: getewayRetryDelay,
	}

	// At this point, etcd gateway listener is initialized
	notifySystemd(lg)

	tp.Run()
}

func parseEndpoints(endpoints []string) []*net.SRV {
	var srvs []*net.SRV
	endpoints = stripSchema(endpoints)
	for _, ep := range endpoints {
		h, p, serr := net.SplitHostPort(ep)
		if serr != nil {
			fmt.Printf("error parsing endpoint %q", ep)
			os.Exit(1)
		}
		var port uint16
		fmt.Sscanf(p, "%d", &port)
		srvs = append(srvs, &net.SRV{Target: h, Port: port})
	}

	if len(endpoints) == 0 {
		fmt.Println("no endpoints found")
		os.Exit(1)
	}
	return srvs
}
