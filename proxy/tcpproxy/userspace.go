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

package tcpproxy

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

var plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "proxy/tcpproxy")

type remote struct {
	mu       sync.Mutex
	srv      *net.SRV
	addr     string
	inactive bool
}

func (r *remote) inactivate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inactive = true
}

func (r *remote) tryReactivate() error {
	conn, err := net.Dial("tcp", r.addr)
	if err != nil {
		return err
	}
	conn.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inactive = false
	return nil
}

func (r *remote) isActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.inactive
}

type TCPProxy struct {
	Logger          *zap.Logger
	Listener        net.Listener
	Endpoints       []*net.SRV
	MonitorInterval time.Duration

	donec chan struct{}

	mu        sync.Mutex // guards the following fields
	remotes   []*remote
	pickCount int // for round robin

	cancelSig  [2]chan struct{}
	busyConn   map[net.Conn]struct{}
	busyConnMu sync.Mutex
}

func (tp *TCPProxy) Run() error {
	tp.donec = make(chan struct{})
	tp.busyConn = make(map[net.Conn]struct{})
	if tp.MonitorInterval == 0 {
		tp.MonitorInterval = 5 * time.Minute
	}
	for _, srv := range tp.Endpoints {
		addr := fmt.Sprintf("%s:%d", srv.Target, srv.Port)
		tp.remotes = append(tp.remotes, &remote{srv: srv, addr: addr})
	}

	eps := []string{}
	for _, ep := range tp.Endpoints {
		eps = append(eps, fmt.Sprintf("%s:%d", ep.Target, ep.Port))
	}
	if tp.Logger != nil {
		tp.Logger.Info("ready to proxy client requests", zap.Strings("endpoints", eps))
	} else {
		plog.Printf("ready to proxy client requests to %+v", eps)
	}

	go tp.runMonitor()
	for {
		in, err := tp.Listener.Accept()
		if err != nil {
			return err
		}

		go tp.serve(in)
	}
}

func (tp *TCPProxy) pick() *remote {
	var weighted []*remote
	var unweighted []*remote

	bestPr := uint16(65535)
	w := 0
	// find best priority class
	for _, r := range tp.remotes {
		switch {
		case !r.isActive():
		case r.srv.Priority < bestPr:
			bestPr = r.srv.Priority
			w = 0
			weighted = nil
			unweighted = []*remote{r}
			fallthrough
		case r.srv.Priority == bestPr:
			if r.srv.Weight > 0 {
				weighted = append(weighted, r)
				w += int(r.srv.Weight)
			} else {
				unweighted = append(unweighted, r)
			}
		}
	}
	if weighted != nil {
		if len(unweighted) > 0 && rand.Intn(100) == 1 {
			// In the presence of records containing weights greater
			// than 0, records with weight 0 should have a very small
			// chance of being selected.
			r := unweighted[tp.pickCount%len(unweighted)]
			tp.pickCount++
			return r
		}
		// choose a uniform random number between 0 and the sum computed
		// (inclusive), and select the RR whose running sum value is the
		// first in the selected order
		choose := rand.Intn(w)
		for i := 0; i < len(weighted); i++ {
			choose -= int(weighted[i].srv.Weight)
			if choose <= 0 {
				return weighted[i]
			}
		}
	}
	if unweighted != nil {
		for i := 0; i < len(tp.remotes); i++ {
			picked := tp.remotes[tp.pickCount%len(tp.remotes)]
			tp.pickCount++
			if picked.isActive() {
				return picked
			}
		}
	}
	return nil
}

func (tp *TCPProxy) serve(in net.Conn) {
	var (
		err error
		out net.Conn
	)

	var backendConn net.Conn
	for {
		tp.mu.Lock()
		remote := tp.pick()
		tp.mu.Unlock()
		if remote == nil {
			break
		}
		// TODO: add timeout
		out, err = net.Dial("tcp", remote.addr)
		if err == nil {
			backendConn = out
			break
		}
		remote.inactivate()
		if tp.Logger != nil {
			tp.Logger.Warn("deactivated endpoint", zap.String("address", remote.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
		} else {
			plog.Warningf("deactivated endpoint [%s] due to %v for %v", remote.addr, err, tp.MonitorInterval)
		}
	}

	tp.busyConnMu.Lock()
	tp.busyConn[backendConn] = struct{}{}
	tp.busyConnMu.Unlock()

	if out == nil {
		in.Close()
		tp.busyConnMu.Lock()
		delete(tp.busyConn, backendConn)
		tp.busyConnMu.Unlock()
		return
	}

	go func() {
		io.Copy(in, out)
		fmt.Println("stage 3")
		in.Close()
		out.Close()
		tp.busyConnMu.Lock()
		delete(tp.busyConn, backendConn)
		tp.busyConnMu.Unlock()
	}()

	fmt.Println("stage 1")
	io.Copy(out, in)
	fmt.Println("stage 2")
	out.Close()
	in.Close()
	tp.busyConnMu.Lock()
	delete(tp.busyConn, backendConn)
	tp.busyConnMu.Unlock()
}

func (tp *TCPProxy) runMonitor() {
	for {
		select {
		case <-time.After(tp.MonitorInterval):
			tp.mu.Lock()
			for _, rem := range tp.remotes {
				if rem.isActive() {
					continue
				}
				go func(r *remote) {
					if err := r.tryReactivate(); err != nil {
						if tp.Logger != nil {
							tp.Logger.Warn("failed to activate endpoint (stay inactive for another interval)", zap.String("address", r.addr), zap.Duration("interval", tp.MonitorInterval), zap.Error(err))
						} else {
							plog.Warningf("failed to activate endpoint [%s] due to %v (stay inactive for another %v)", r.addr, err, tp.MonitorInterval)
						}
					} else {
						if tp.Logger != nil {
							tp.Logger.Info("activated", zap.String("address", r.addr))
						} else {
							plog.Printf("activated %s", r.addr)
						}
					}
				}(rem)
			}
			tp.mu.Unlock()
		case <-tp.donec:
			return
		}
	}
}

func (tp *TCPProxy) Stop() {
	// graceful shutdown?
	// shutdown current connections?
	tp.Listener.Close()
	close(tp.donec)
}

func (tp *TCPProxy) runPendingHandler(pendingDelRemoteAddrs []string,
	cancelSig <-chan struct{},
	cancelOkSig chan<- struct{}) {
	checkTimer := time.NewTimer(time.Millisecond * 1)
	for {
		select {
		case <-checkTimer.C:
			tp.mu.Lock()
			for i, addr := range pendingDelRemoteAddrs {
				isBusy := false
				tp.busyConnMu.Lock()
				for conn, _ := range tp.busyConn {
					if conn != nil && (conn.RemoteAddr().String() == addr) {
						isBusy = true
					}
				}
				tp.busyConnMu.Unlock()
				if !isBusy {
					for idx, remote := range tp.remotes {
						if remote.addr == addr {
							fmt.Printf("del remote:%v\n", remote.addr)
							tp.remotes = append(tp.remotes[:idx], tp.remotes[idx+1:]...)
							pendingDelRemoteAddrs = append(pendingDelRemoteAddrs[:i], pendingDelRemoteAddrs[i+1:]...)
							break
						}
					}
				} else {
					fmt.Printf("endpoint %v is busy, go to retry\n", addr)
				}
			}
			tp.mu.Unlock()
			if 0 < len(pendingDelRemoteAddrs) {
				checkTimer.Reset(time.Second * 1)
			}
		case <-cancelSig:
			close(cancelOkSig)
			return
		}
	}
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

func parseEndpoint(endpoint string) *net.SRV {
	var srvs []*net.SRV
	endpoints := stripSchema([]string{endpoint})
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
	return srvs[0]
}

func (tp *TCPProxy) Update(endpoints []string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if len(tp.remotes) == 0 {
		return
	}

	if tp.cancelSig[0] != nil {
		close(tp.cancelSig[0])
		<-tp.cancelSig[1]
	}

	var addList []string
	var delList []string
	for _, newEndpoint := range endpoints {
		find := false
		for _, remote := range tp.remotes {
			if remote.addr == newEndpoint {
				find = true
				break
			}
		}
		if !find {
			addList = append(addList, newEndpoint)
		}
	}
	fmt.Printf("addList:%v\n", addList)
	for _, remote := range tp.remotes {
		find := false
		for _, newEndpoint := range endpoints {
			if remote.addr == newEndpoint {
				find = true
				break
			}
		}
		if !find {
			delList = append(delList, remote.addr)
		}
	}
	fmt.Printf("delList:%v\n", delList)

	for _, addr_ := range addList {
		endpoint := parseEndpoint(addr_)
		tp.remotes = append(tp.remotes, &remote{srv: endpoint, addr: addr_})
	}

	tp.cancelSig[0] = make(chan struct{})
	tp.cancelSig[1] = make(chan struct{})
	go tp.runPendingHandler(delList, tp.cancelSig[0], tp.cancelSig[1])
}
