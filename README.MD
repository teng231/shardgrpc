### SHARD_GRPC

### Structure

![https://res.cloudinary.com/telosma/image/upload/v1666462690/Screen_Shot_2022-10-23_at_01.14.48_rsqliz.png](https://res.cloudinary.com/telosma/image/upload/v1666462690/Screen_Shot_2022-10-23_at_01.14.48_rsqliz.png)

### Required

- Recommend server need using `statefulset` for easy routing.
- GRPC server need install in big machine(pod/service) for high perfomance.

### Install

```bash
    go get github.com/teng231/shardgrpc
```

- Using for client:
example
```go
    // for example
    conn, err := grpc.Dial("lhost-0:21240",
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithUnaryInterceptor(UnaryClientInterceptor(
            &DialConfig{ThrottlingDuration: 10 * time.Millisecond, MaxRetryConnect: 3},
            grpc.WithTransportCredentials(insecure.NewCredentials()),
        )))
    if err != nil {
        panic(err)
    }
    client := pb.NewVistorServiceClient(conn)
```

send request in process

```go
    // ...
    var header metadata.MD // variable to store header and trailer
    c, cancel := MakeContext(20, nil)
    defer cancel()
    c = metadata.AppendToOutgoingContext(c, "s_key", "teng")
    resp, err := client.ListVisitors(c, &pb.VisitorRequest{AccountId: "teng"}, grpc.Header(&header))
    // log.Print("header", header)
    log.Print(resp, err, header)
    if err != nil {
        log.Print(err)
    }
```

- Using for server:
example
```go
    func (me TestShardApiServer) ServeV1(hostname, port string, shardcount int) {
        lis, err := net.Listen("tcp", hostname+":"+port)
        if err != nil {
            panic(err)
        }
        opts := []grpc.ServerOption{
            grpc.KeepaliveParams(keepalive.ServerParameters{
                MaxConnectionAge: 30 * time.Second,
            }),
            grpc.UnaryInterceptor(
                UnaryServerInterceptorStatefullset(hostname, port, "", shardcount),
            ),
        }
        grpcServer := grpc.NewServer(opts...)
        pb.RegisterVistorServiceServer(grpcServer, &me)
        grpcServer.Serve(lis)
    }
```

### REFERENCES:

Intercepter Structure [https://github.com/ori-edge/grpc-interceptor-demo/blob/fd821c3556ff6eebd6eab1a617df0fb68f7c01a2/pkg/interceptor/interceptor.go](https://github.com/ori-edge/grpc-interceptor-demo/blob/fd821c3556ff6eebd6eab1a617df0fb68f7c01a2/pkg/interceptor/interceptor.go)

Structure of shard service [https://www.notion.so/Shard-service-c002bcb0b00c47669bce547be646cd9f](https://www.notion.so/Shard-service-c002bcb0b00c47669bce547be646cd9f)

structure example [https://medium0.com/m/global-identity?redirectUrl=https%3A%2F%2Fedgehog.blog%2Fa-guide-to-grpc-and-interceptors-265c306d3773](https://medium0.com/m/global-identity?redirectUrl=https%3A%2F%2Fedgehog.blog%2Fa-guide-to-grpc-and-interceptors-265c306d3773)