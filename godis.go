package main

import (
	"github.com/godis/obj"
	"github.com/godis/tools"
	"log"
	"os"
	"time"
)

var server obj.GodisServer

func AcceptHandler(loop *tools.AeLoop, fd int, extra interface{}) {
	cfd, err := tools.Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := obj.CreateClient(cfd, &server)
	//TODO: check max clients limit
	server.AddClient(cfd, client)
	server.AddFileEvent(cfd, tools.AE_READABLE, obj.ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

// ServerCron background job, runs every 100ms to remove a serial of expired keys until random missing or hit max count
func ServerCron(loop *tools.AeLoop, id int, extra interface{}) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.Db.Expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Val.IntVal() < time.Now().Unix() {
			server.Db.Data.Delete(entry.Key)
			server.Db.Expire.Delete(entry.Key)
		}
	}
}

func initServer(config *Config) (err error) {
	aeLoop, err := tools.AeLoopCreate()
	if err != nil {
		return err
	}
	serverFd, err := tools.TcpServer(config.Port)
	if err != nil {
		return err
	}
	db := &obj.GodisDB{
		Data:   obj.DictCreate(obj.DictType{HashFunc: obj.GStrHash, EqualFunc: obj.GStrEqual}),
		Expire: obj.DictCreate(obj.DictType{HashFunc: obj.GStrHash, EqualFunc: obj.GStrEqual}),
	}
	server = obj.CreateGodisServer(serverFd, config.Port, db, make(map[int]*obj.GodisClient), aeLoop, config.Pass)
	return err
}

func main() {
	path := os.Args[1]
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	err = initServer(config)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.AddFileEvent(server.FD, tools.AE_READABLE, AcceptHandler, nil)
	server.AddTimeEvent(tools.AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.AeMain()
}
