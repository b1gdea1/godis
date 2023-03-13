package obj

import (
	"errors"
	"fmt"
	"github.com/godis/tools"
	"log"
	"strconv"
	"strings"
)

const (
	GodisIoBuf     int = 1024 * 16
	GodisMaxBulk   int = 1024 * 4
	GodisMaxInline int = 1024 * 4
)

type CmdType = byte
type GodisDB struct {
	Data   *Dict
	Expire *Dict
}

type command struct {
	name  string
	arity int
}

const (
	CommandUnknown CmdType = 0x00
	CommandInline  CmdType = 0x01
	CommandBulk    CmdType = 0x02
)

type GodisClient struct {
	fd           int
	db           *GodisDB
	args         []*Gobj
	reply        *List
	sentLen      int
	QueryBuf     []byte
	QueryLen     int
	cmdTy        CmdType
	bulkNum      int
	bulkLen      int
	server       *GodisServer
	commandsLens map[string]int
}

func (client *GodisClient) get() {
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

func (client *GodisClient) AddReplyStr(str string) {
	o := CreateObject(GSTR, str)
	client.AddReply(o)
	o.DecrRefCount()
}

func (client *GodisClient) ProcessCommand() {
	cmdStr := client.args[0].StrVal()
	if len(cmdStr) == 0 {
		return
	}
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		client.freeClient()
		return
	}
	client.processCmd()
	client.resetClient()
}

func (client *GodisClient) freeArgs() {
	for _, v := range client.args {
		v.DecrRefCount()
	}
}

func freeReplyList(client *GodisClient) {
	for client.reply.GetLen() != 0 {
		n := client.reply.GetHead()
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}

func (client *GodisClient) freeClient() {
	client.freeArgs()
	delete(client.server.clients, client.fd)
	client.server.aeLoop.RemoveFileEvent(client.fd, tools.AE_READABLE)
	client.server.aeLoop.RemoveFileEvent(client.fd, tools.AE_WRITABLE)
	freeReplyList(client)
	tools.Close(client.fd)
}

func (client *GodisClient) resetClient() {
	client.freeArgs()
	client.cmdTy = CommandUnknown
	client.bulkLen = 0
	client.bulkNum = 0
}
func (client *GodisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.QueryBuf[:client.QueryLen]), "\r\n")
	if index < 0 && client.QueryLen > GodisMaxInline {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.QueryBuf[s:e]))
	client.QueryBuf = client.QueryBuf[e+2:]
	client.QueryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index, err := client.findLineInQuery()
	if index < 0 {
		return false, err
	}
	subs := strings.Split(string(client.QueryBuf[:index]), " ")
	client.QueryBuf = client.QueryBuf[index+2:]
	client.QueryLen -= index + 2
	client.args = make([]*Gobj, len(subs))
	for i, v := range subs {
		client.args[i] = CreateObject(GSTR, v)
	}
	return true, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	// read bulk num
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}
		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*Gobj, bnum)
	}
	// read every bulk string
	for client.bulkNum > 0 {
		// read bulk length
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}
			if client.QueryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}
			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > GodisMaxBulk {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		// read bulk string
		if client.QueryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.QueryBuf[index] != '\r' || client.QueryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		client.args[len(client.args)-client.bulkNum] = CreateObject(GSTR, string(client.QueryBuf[:index]))
		client.QueryBuf = client.QueryBuf[index+2:]
		client.QueryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}
	// complete reading every bulk
	return true, nil
}

func (client *GodisClient) ProcessQueryBuf() error {
	for client.QueryLen > 0 {
		if client.cmdTy == CommandUnknown {
			if client.QueryBuf[0] == '*' {
				client.cmdTy = CommandBulk
			} else {
				client.cmdTy = CommandInline
			}
		}
		// trans query -> args
		var ok bool
		var err error
		if client.cmdTy == CommandInline {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == CommandBulk {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknown Godis Command Type")
		}
		if err != nil {
			return err
		}
		// after query -> args
		if ok {
			if len(client.args) == 0 {
				client.resetClient()
			} else {
				client.ProcessCommand()
			}
		} else {
			// cmd incomplete
			break
		}
	}
	return nil
}

func (client *GodisClient) AddReply(o *Gobj) {
	client.reply.Append(o)
	o.IncrRefCount()
	client.server.aeLoop.AddFileEvent(client.fd, tools.AE_WRITABLE, SendReplyToClient, client)
}

func ReadQueryFromClient(_ *tools.AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	if len(client.QueryBuf)-client.QueryLen < GodisMaxBulk {
		client.QueryBuf = append(client.QueryBuf, make([]byte, GodisMaxBulk)...)
	}
	n, err := tools.Read(fd, client.QueryBuf[client.QueryLen:])
	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		client.freeClient()
		return
	}
	client.QueryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.QueryBuf))
	err = client.ProcessQueryBuf()
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		client.freeClient()
		return
	}
}

func SendReplyToClient(loop *tools.AeLoop, fd int, extra interface{}) {
	client := extra.(*GodisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := tools.Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				client.freeClient()
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, tools.AE_WRITABLE)
	}
}

var commandTable = []command{
	{"get", 2},
	{"set", 3},
	{"expire", 3},
}

func CreateClient(fd int, server *GodisServer) *GodisClient {
	var client GodisClient
	client.fd = fd
	client.server = server
	client.db = server.Db
	client.QueryBuf = make([]byte, GodisIoBuf)
	client.reply = ListCreate(ListType{EqualFunc: GStrEqual})
	client.commandsLens = make(map[string]int)
	l := len(commandTable)
	for i := 0; i < l; i++ {
		client.commandsLens[commandTable[i].name] = commandTable[i].arity
	}
	return &client
}
