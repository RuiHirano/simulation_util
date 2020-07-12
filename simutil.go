package util

import (
	"fmt"
	"log"
	"sync"
	"time"
)

func connectServer(myProvider *api.Provider, synerexAddr string, nodeIdAddr string, demandCallback func(*api.SMServiceClient, *api.Demand), supplyCallback func(*api.SMServiceClient, *api.Supply)) {
	// Connect to Worker Node Server
	nodeapi := napi.NewNodeAPI()
	for {
		err := nodeapi.RegisterNodeName(nodeIdAddr, myProvider.Name, false)
		if err == nil {
			logger.Info("connected NodeID server!")
			go nodeapi.HandleSigInt()
			nodeapi.RegisterDeferFunction(nodeapi.UnRegisterNode)
			break
		} else {
			logger.Warn("NodeID Error... reconnecting...")
			time.Sleep(2 * time.Second)
		}
	}

	// Connect to Worker Synerex Server
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(synerexAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	nodeapi.RegisterDeferFunction(func() { conn.Close() })
	client := api.NewSynerexClient(conn)
	argJson := fmt.Sprintf("{Client:Gateway}")

	// Communicator
	workerapi := api.NewSimAPI()
	workerapi.RegistClients(client, myProvider.Id, argJson) // channelごとのClientを作成
	workerapi.SubscribeAll(demandCallback, supplyCallback)  // ChannelにSubscribe

	time.Sleep(3 * time.Second)

	workerStatusMap[synerexAddr] = &WorkerStatus{
		Main:      main,
		WorkerAPI: workerapi,
		Status:    ConnectStatus_UNCONNECTED,
	}

	// workerへ登録
	senderId := myProvider.Id
	targets := make([]uint64, 0)
	workerapi.RegistProviderRequest(senderId, targets, myProvider)

	go func() {
		for {
			if workerStatusMap[synerexAddr].Status == ConnectStatus_CONNECTED {
				logger.Debug("Regist Success to Worker! %v\n", synerexAddr)
				return
			} else {
				logger.Debug("Couldn't Regist Worker  %v ...Retry...\n", synerexAddr)
				time.Sleep(2 * time.Second)
				// workerへ登録
				workerapi.RegistProviderRequest(senderId, targets, myProvider)
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
	nodeapi.CallDeferFunctions() // cleanup!

}
