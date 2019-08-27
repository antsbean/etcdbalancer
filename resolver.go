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

// https://morecrazy.github.io/2018/08/14/grpc-go%E5%9F%BA%E4%BA%8Eetcd%E5%AE%9E%E7%8E%B0%E6%9C%8D%E5%8A%A1%E5%8F%91%E7%8E%B0%E6%9C%BA%E5%88%B6/
// etcd3 用法 https://yuerblog.cc/2017/12/12/etcd-v3-sdk-usage/
type Resolver struct {
	cc         resolver.ClientConn
	etcdClient *etcd.Client
	waitGroup  *sync.WaitGroup
	schema     string
	prefix     string
}

func NewResolver(schema string, config etcd.Config) *Resolver {
	if cli, err := etcd.New(config); err != nil {
		panic(err)
	} else {
		return &Resolver{etcdClient: cli, waitGroup: &sync.WaitGroup{}, schema: schema}
	}
}

func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	if r.etcdClient == nil {
		panic("etcd client is nil")
	}
	log.Printf("target scheme %s, endpoint %s", target.Scheme, target.Endpoint)
	r.cc = cc

	var addresses []resolver.Address
	r.prefix = "/" + r.Scheme() + "/" + strings.TrimRight(target.Endpoint, "/") + "/"
	addresses = r.initAddress(r.prefix)
	if len(addresses) > 0 {
		r.cc.UpdateState(resolver.State{Addresses: addresses})
	} else {
		log.Fatalf("prefix %s  get grpc server address nil", r.prefix)
	}
	r.watch(r.prefix, addresses)
	return r, nil
}

func (r *Resolver) Scheme() string {
	return r.schema
}

func (r *Resolver) ResolveNow(option resolver.ResolveNowOption) {
	if addresses := r.initAddress(r.prefix); len(addresses) > 0 {
		r.cc.UpdateState(resolver.State{Addresses: addresses})
	} else {
		log.Fatalf("schema %s get grpc server address nil", r.schema, )
	}
}

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

func (r *Resolver) initAddress(prefix string) (addList []resolver.Address) {
	ctx, _ := context.WithTimeout(context.TODO(), time.Second*2)
	kv := etcd.NewKV(r.etcdClient)
	if getResp, err := kv.Get(ctx, prefix, etcd.WithPrefix()); err != nil {
		log.Println(err)
		return
	} else {
		for i := range getResp.Kvs {
			addr := strings.TrimPrefix(string(getResp.Kvs[i].Key), prefix)
			log.Printf("get repc server addr %s", addr)
			addList = append(addList, resolver.Address{Addr: addr})
		}
	}
	return
}

func (r *Resolver) watch(prefix string, addresses []resolver.Address) {
	r.waitGroup.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println(err)
			}
			r.waitGroup.Done()
		}()
		watcher := etcd.NewWatcher(r.etcdClient)
		for {
			select {
			case events, ok := <-watcher.Watch(context.TODO(), prefix, etcd.WithPrefix()):
				if !ok {
					return
				}
				if err := events.Err(); err != nil {
					log.Printf("watch canceled %v", err)
					return
				}
				for _, ev := range events.Events {
					addr := strings.TrimPrefix(string(ev.Kv.Key), prefix)
					switch ev.Type {
					case mvccpb.PUT:
						log.Printf("put addr %v", addr)
						if !r.exitAddr(addresses, addr) {
							addresses = append(addresses, resolver.Address{Addr: addr})
							r.cc.UpdateState(resolver.State{Addresses: addresses})
						}
					case mvccpb.DELETE:
						log.Printf("delete addr %v", addr)
						if newAddresses, ok := r.removeAddress(addresses, addr); ok {
							addresses = newAddresses
							r.cc.UpdateState(resolver.State{Addresses: addresses})
						}
					}
				}
			}
		}
	}()
}

func (r *Resolver) exitAddr(addresses []resolver.Address, addr string) bool {
	for index := range addresses {
		if addresses[index].Addr == addr {
			return true
		}
	}
	return false
}

func (r *Resolver) removeAddress(addresses []resolver.Address, addr string) ([]resolver.Address, bool) {
	for index := range addresses {
		if addresses[index].Addr == addr {
			addresses[index] = addresses[len(addresses)-1]
			return addresses[:len(addresses)], true
		}
	}
	return nil, false
}
