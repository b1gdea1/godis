package obj

import "fmt"

func (client *GodisClient) processCmd() {
	args := client.args
	arity := client.commandsLens[args[0].StrVal()]
	fmt.Printf("length of %s is %d\r\n", args[0].StrVal(), arity)
	if arity <= 0 {
		client.AddReplyStr("-ERR: wrong length of args")
		client.resetClient()
		return
	}
	if arity != len(args) {
		client.AddReplyStr("-ERR: wrong number of args")
		client.resetClient()
		return
	}
	switch args[0].StrVal() {
	case "get":
		client.getCommand()
	case "set":
		client.setCommand()
	default:
		client.AddReplyStr("-ERR: unknown command")
	}
}
func (client *GodisClient) getCommand() {
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
func (client *GodisClient) setCommand() {
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
