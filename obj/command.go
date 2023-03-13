package obj

import (
	"fmt"
	"github.com/godis/tools"
)

type GodisCommand func(client *GodisClient)
type command struct {
	name  string
	arity int
	proc  GodisCommand
}

var commandTable = []command{
	{"get", 2, getCommand},
	{"set", 3, setCommand},
	{"expire", 3, expireCommand},
}

func (client *GodisClient) processCmd() {
	c := client.findCommand()
	if c == nil {
		client.AddReplyStr("-ERR: unknown command\r\n")
		client.resetClient()
		return
	}
	(*c)(client)
}

func (client *GodisClient) findCommand() *GodisCommand {
	for _, c := range commandTable {
		if c.name == client.args[0].StrVal() {
			if c.arity != len(client.args) {
				//client.AddReplyStr("-ERR: wrong number of args\r\n")
				//client.resetClient()
				return nil
			}
			return &c.proc
		}
	}
	return nil
}

func getCommand(client *GodisClient) {
	key := client.args[1]
	val := client.server.findKeyRead(key)
	if val == nil {
		//TODO: extract shared.strings
		client.AddReplyStr("$-1\r\n")
	} else if val.Type_ != GSTR {
		//TODO: extract shared.strings
		client.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		client.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(client *GodisClient) {
	key := client.args[1]
	val := client.args[2]
	if val.Type_ != GSTR {
		//TODO: extract shared.strings
		client.AddReplyStr("-ERR: wrong type\r\n")
	}
	client.server.Db.Data.Set(key, val)
	client.server.Db.Expire.Delete(key)
	client.AddReplyStr("+OK\r\n")
}

func expireCommand(client *GodisClient) {
	key := client.args[1]
	entry := client.server.Db.Data.Find(key)
	if entry == nil {
		client.AddReplyStr("-ERR: key does not exist\r\n")
	}
	val := client.args[2]
	if val.IntVal() == 0 {
		client.AddReplyStr("-ERR: wrong expire time\r\n")
	}
	client.server.Db.Expire.Set(key, CreateFromInt(tools.GetMsTime()+val.IntVal()*1000))
	client.AddReplyStr("+OK\r\n")
}
