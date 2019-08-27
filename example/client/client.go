package main

import (
	"context"
	"fmt"
	"github.com/antsbean/etcdbalancer"
	pb "github.com/antsbean/etcdbalancer/example/helloword"
	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var exitChan chan struct{}

func client(etcdAddr string) {
	schema := "helloword"
	etcdBuilder := etcdbalancer.NewResolver(schema, clientv3.Config{Endpoints: strings.Split(etcdAddr, ","), DialTimeout: 2 * time.Second})
	resolver.Register(etcdBuilder)
	conn, err := grpc.DialContext(context.TODO(), etcdBuilder.Scheme()+"://author/project/test", grpc.WithBalancerName("round_robin"), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("connect grpc server failed %v", err)
	}

	defer conn.Close()
	c := pb.NewGreeterClient(conn)
	var count int64 = 0
	for {
		select {
		case <-time.After(time.Second):
			count += 1
			if e, err := c.SayHello(context.TODO(), &pb.HelloRequest{Name: fmt.Sprintf("test_%d", count)}); err != nil {
				log.Fatalf("could not %v", err)
			} else {
				log.Printf("response %s", e.Message)
			}
		case <-exitChan:
			return
		}
	}
}

func main() {
	exitChan = make(chan struct{})
	var etcdAddr string
	pflag.StringVar(&etcdAddr, "etcd", "", "etcd server address")
	pflag.Parse()
	client(etcdAddr)
	chSig := make(chan os.Signal)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM, syscall.Signal(0xa))
	for sig := range chSig {
		log.Println(sig)
		close(exitChan)
		break
	}
}
