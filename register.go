package etcdbalancer

import (
	"context"
	"fmt"
	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"log"
	"sync"
	"time"
)

type EtcdRegister struct {
	etcdClient *etcd.Client
	key        string
	addr       string
	waitGroup  *sync.WaitGroup
	closedChan chan struct{}
}

func NewEtcdRegister(schema, name, addr string, config etcd.Config) *EtcdRegister {
	if cli, err := etcd.New(config); err != nil {
		panic(err)
	} else {
		return &EtcdRegister{etcdClient: cli,
			key:        fmt.Sprintf("/%s/%s/%s", schema, name, addr),
			addr:       addr,
			closedChan: make(chan struct{}),
			waitGroup:  &sync.WaitGroup{},
		}
	}
}

// Register register service
func (r *EtcdRegister) Register(ttl int64) error {
	if r.etcdClient == nil {
		panic("etcd client is nil")
	}
	lease := etcd.NewLease(r.etcdClient)
	kv := etcd.NewKV(r.etcdClient)
	if leaseId, err := r.withAlive(kv, lease, 0, r.key, ttl); err != nil {
		return err
	} else {
		r.waitGroup.Add(1)
		go func() {
			ticker := time.NewTicker(time.Second * time.Duration(ttl-10))
			defer func() {
				if err := recover(); err != nil {
					log.Printf("key %s leaseid %d error %s", r.key, leaseId, err)
				}
				ticker.Stop()
				r.waitGroup.Done()
				log.Println("exit server register to discovery")
			}()
			var err error
			for {
				select {
				case <-ticker.C:
					if leaseId, err = r.withAlive(kv, lease, leaseId, r.key, ttl); err != nil {
						log.Printf("key %s leaseid %d error %v", r.key, leaseId, err)
					}
				case <-r.closedChan:
					return
				}
			}
		}()
	}

	return nil
}

func (r *EtcdRegister) withAlive(kv etcd.KV, lease etcd.Lease, leaseId etcd.LeaseID, key string, ttl int64) (etcd.LeaseID, error) {
	if leaseId == 0 {
		leaseResp, err := lease.Grant(context.TODO(), ttl)
		if err != nil {
			return 0, err
		}
		if _, err := kv.Put(context.TODO(), r.key, r.addr, etcd.WithLease(leaseResp.ID)); err != nil {
			return 0, err
		}
		leaseId = leaseResp.ID
		return leaseId, nil
	} else if _, err := lease.KeepAlive(context.TODO(), leaseId); err == rpctypes.ErrLeaseNotFound {
		//TODO if keepalive failed, retry grant key ttl
		return r.withAlive(kv, lease, 0, key, ttl)
	} else if err != nil {
		return 0, err
	} else {
		log.Printf("key %s leaseid %d error %v", r.key, leaseId, err)
		return leaseId, nil
	}
}

// UnRegister remove service from etcd
func (r *EtcdRegister) UnRegister() {
	if r.etcdClient != nil {
		_, _ = r.etcdClient.Delete(context.Background(), r.key)
		_ = r.etcdClient.Close()
		r.etcdClient = nil
		close(r.closedChan)
		r.waitGroup.Wait()
	}
}
