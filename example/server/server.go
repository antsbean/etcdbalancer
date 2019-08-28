package main

import (
	"context"
	"github.com/antsbean/etcdbalancer"
	pb "github.com/antsbean/etcdbalancer/example/helloword"
	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Server struct {
}

func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	log.Printf("received %v", in.GetName())
	return &pb.HelloResponse{Message: "response" + in.GetName()}, nil
}

var grpcSrv *grpc.Server
var etcdRegister *etcdbalancer.Register

func runRpcServer(etcdAddr string, addr string) {
	log.Printf("start grpc server %s", addr)
	if listener, err := net.Listen("tcp", addr); err != nil {
		log.Fatalf("failed to listen: %v", err)
	} else {
		defer listener.Close()
		grpcSrv = grpc.NewServer()
		pb.RegisterGreeterServer(grpcSrv, &Server{})
		etcdRegister = etcdbalancer.NewRegister("helloword", "project/test", addr,
			clientv3.Config{Endpoints: strings.Split(etcdAddr, ",")})

		if err := etcdRegister.RegisterServer(50); err != nil {
			log.Fatalf("etcd register server failed %v", err)
		}
		if err := grpcSrv.Serve(listener); err != nil {
			log.Fatalf("failed to server %v", err)
		}

	}
}

func stopServer() {
	etcdRegister.UnRegister()
	grpcSrv.GracefulStop()
}

func main() {
	var etcdAddr string
	var serverAddr string
	pflag.StringVar(&etcdAddr, "etcd", "", "etcd server address")
	pflag.StringVar(&serverAddr, "server_addr", serverAddr, "grpc server address")
	pflag.Parse()
	go runRpcServer(etcdAddr, serverAddr)
	chSig := make(chan os.Signal)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.Signal(0xa))
	for sig := range chSig {
		log.Println(sig)
		break
	}
	stopServer()
}
