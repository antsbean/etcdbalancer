package etcdbalancer

import (
	"context"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/resolver"
	"log"
	"strings"
	"sync"
	"time"
)

// Resolver grpc remote server address resolver
type Resolver struct {
	cc         resolver.ClientConn
	etcdClient *etcd.Client
	waitGroup  *sync.WaitGroup
	schema     string
	prefix     string
}

// NewResolver new resolver
func NewResolver(schema string, config etcd.Config) *Resolver {
	if cli, err := etcd.New(config); err != nil {
		panic(err)
	} else {
		return &Resolver{etcdClient: cli, waitGroup: &sync.WaitGroup{}, schema: schema}
	}
}

// Build implement grpc Builder interface
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	if r.etcdClient == nil {
		panic("etcd client is nil")
	}
	r.cc = cc
	var addresses []resolver.Address
	r.prefix = "/" + r.Scheme() + "/" + strings.TrimRight(target.Endpoint, "/") + "/"
	if addrs, err := r.initAddress(r.prefix); err == nil && len(addrs) > 0 {
		r.cc.UpdateState(resolver.State{Addresses: addrs})
	} else {
		log.Fatalf("prefix %s  get grpc server address nil", r.prefix)
	}
	r.watch(r.prefix, addresses)
	return r, nil
}

// Scheme implement grpc Builder interface
func (r *Resolver) Scheme() string {
	return r.schema
}

func (r *Resolver) ResolveNow(option resolver.ResolveNowOption) {
	if addrs, err := r.initAddress(r.prefix); err == nil && len(addrs) > 0 {
		r.cc.UpdateState(resolver.State{Addresses: addrs})
	} else {
		log.Fatalf("schema %s get grpc server address error %v", r.schema, err)
	}
}

// Close graceful close grpc resolver balancer
func (r *Resolver) Close() {
	if r.etcdClient != nil {
		if err := r.etcdClient.Close(); err != nil {
			log.Println(err)
		}
		r.etcdClient = nil
	}
	r.waitGroup.Wait()
	log.Println("call resolver close")
}

func (r *Resolver) initAddress(prefix string) (addList []resolver.Address, err error) {
	ctx, _ := context.WithTimeout(context.TODO(), time.Second*2)
	kv := etcd.NewKV(r.etcdClient)
	var getResp *etcd.GetResponse
	if getResp, err = kv.Get(ctx, prefix, etcd.WithPrefix()); err != nil {
		return
	} else {
		for i := range getResp.Kvs {
			addr := strings.TrimPrefix(string(getResp.Kvs[i].Key), prefix)
			addList = append(addList, resolver.Address{Addr: addr})
		}
	}
	return
}

func (r *Resolver) watch(prefix string, addresses []resolver.Address) {
	r.waitGroup.Add(1)
	go func() {
		defer r.waitGroup.Done()
		watcher := etcd.NewWatcher(r.etcdClient)
		for {
			select {
			case events, ok := <-watcher.Watch(context.TODO(), prefix, etcd.WithPrefix()):
				if !ok {
					return
				}
				if err := events.Err(); err != nil {
					return
				}
				for _, ev := range events.Events {
					addr := strings.TrimPrefix(string(ev.Kv.Key), prefix)
					switch ev.Type {
					case mvccpb.PUT:
						if !r.existAddr(addresses, addr) {
							addresses = append(addresses, resolver.Address{Addr: addr})
							r.cc.UpdateState(resolver.State{Addresses: addresses})
						}
					case mvccpb.DELETE:
						if newAddresses, ok := r.removeAddr(addresses, addr); ok {
							addresses = newAddresses
							r.cc.UpdateState(resolver.State{Addresses: addresses})
						}
					}
				}
			}
		}
	}()
}

func (r *Resolver) existAddr(addresses []resolver.Address, addr string) bool {
	for index := range addresses {
		if addresses[index].Addr == addr {
			return true
		}
	}
	return false
}

func (r *Resolver) removeAddr(addresses []resolver.Address, addr string) ([]resolver.Address, bool) {
	for index := range addresses {
		if addresses[index].Addr == addr {
			addresses[index] = addresses[len(addresses)-1]
			return addresses[:len(addresses)], true
		}
	}
	return nil, false
}
