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

var authedCommandTable = []command{
	{"get", 2, getCommand},
	{"set", 3, setCommand},
	{"expire", 3, expireCommand},
}

func (client *GodisClient) processAuthedCmd() {
	c := client.findCommand(&unauthedCommandTable)
	if c != nil {
		(*c)(client)
		return
	}
	c = client.findCommand(&authedCommandTable)
	if c == nil {
		if client.cmdTy != CommandUnknown {
			client.AddReplyStr("-ERR: unknown command\r\n")
			client.resetClient()
		}
		return
	}
	(*c)(client)
}

func (client *GodisClient) findCommand(table *[]command) *GodisCommand {
	for _, c := range *table {
		if c.name == client.args[0].StrVal() {
			if c.arity != len(client.args) {
				client.AddReplyStr("-ERR: wrong length of args\r\n")
				client.resetClient()
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

var unauthedCommandTable = []command{
	{"quit", 1, quitCommand},
	{"command", 1, commandCommand},
	{"auth", 2, authCommand},
}

func authCommand(client *GodisClient) {
	if client.args[1].StrVal() == client.server.Pass {
		client.authed = true
		client.AddReplyStr("+OK\r\n")
		client.resetClient()
	} else {
		client.authed = false
		client.AddReplyStr("-ERR: wrong password\r\n")
		client.resetClient()
	}
}

func commandCommand(client *GodisClient) {
	var ret []byte
	l := len(client.commandNames)
	sLen := fmt.Sprintf("*%d\r\n", l)
	ret = append(ret, []byte(sLen)...)
	for _, name := range client.commandNames {
		ret = append(ret, []byte(fmt.Sprintf("$%d%v\r\n", len(name), name))...)
	}
	client.AddReplyStr(string(ret))
	return
}

func quitCommand(client *GodisClient) {
	client.freeClient()
	return
}

func (client *GodisClient) processUnauthedCommand() {
	c := client.findCommand(&unauthedCommandTable)
	if c == nil {
		client.AddReplyStr("-ERR: not an unauthed command\r\n")
		client.resetClient()
		return
	}
	(*c)(client)
}
