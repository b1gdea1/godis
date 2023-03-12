package main

import (
	"github.com/godis/obj"
	"github.com/godis/tools"
	"log"
	"os"
	"time"
)

//
//
//
//
//
//
//
//func getCommand(c *obj.GodisClient) {
//	key := c.args[1]
//	val := findKeyRead(key)
//	if val == nil {
//		//TODO: extract shared.strings
//		c.AddReplyStr("$-1\r\n")
//	} else if val.Type_ != obj.GSTR {
//		//TODO: extract shared.strings
//		c.AddReplyStr("-ERR: wrong type\r\n")
//	} else {
//		str := val.StrVal()
//		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
//	}
//}
//
//func setCommand(c *obj.GodisClient) {
//	key := c.args[1]
//	val := c.args[2]
//	if val.Type_ != obj.GSTR {
//		//TODO: extract shared.strings
//		c.AddReplyStr("-ERR: wrong type\r\n")
//	}
//	server.db.data.Set(key, val)
//	server.db.expire.Delete(key)
//	c.AddReplyStr("+OK\r\n")
//}
//
//func expireCommand(c *obj.GodisClient) {
//	key := c.args[1]
//	val := c.args[2]
//	if val.Type_ != obj.GSTR {
//		//TODO: extract shared.strings
//		c.AddReplyStr("-ERR: wrong type\r\n")
//	}
//	expire := tools.GetMsTime() + (val.IntVal() * 1000)
//	expObj := obj.CreateFromInt(expire)
//	server.db.expire.Set(key, expObj)
//	expObj.DecrRefCount()
//	c.AddReplyStr("+OK\r\n")
//}

//	func lookupCommand(cmdStr string) *GodisCommand {
//		for _, c := range cmdTable {
//			if c.name == cmdStr {
//				return &c
//			}
//		}
//		return nil
//	}
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
	server = obj.CreateGodisServer(serverFd, config.Port, db, make(map[int]*obj.GodisClient), aeLoop)
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
