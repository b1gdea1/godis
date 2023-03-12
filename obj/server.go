package obj

import "github.com/godis/tools"

type GodisServer struct {
	FD      int
	port    int
	Db      *GodisDB
	clients map[int]*GodisClient
	aeLoop  *tools.AeLoop
}

func CreateGodisServer(fd int, port int, db *GodisDB, clients map[int]*GodisClient, aeLoop *tools.AeLoop) GodisServer {
	return GodisServer{
		FD:      fd,
		port:    port,
		Db:      db,
		clients: clients,
		aeLoop:  aeLoop,
	}
}

func (server *GodisServer) expireIfNeeded(key *Gobj) {
	entry := server.Db.Expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > tools.GetMsTime() {
		return
	}
	server.Db.Expire.Delete(key)
	server.Db.Data.Delete(key)
}
func (server *GodisServer) findKeyRead(key *Gobj) *Gobj {
	server.expireIfNeeded(key)
	return server.Db.Data.Get(key)
}

func (server *GodisServer) AddClient(clientFd int, client *GodisClient) {
	server.clients[clientFd] = client
}

func (server *GodisServer) AddFileEvent(fd int, mask tools.FeType, proc tools.FileProc, extra interface{}) {
	server.aeLoop.AddFileEvent(fd, mask, proc, extra)
}

func (server *GodisServer) AddTimeEvent(teType tools.TeType, i int64, cron tools.TimeProc, t interface{}) {
	server.aeLoop.AddTimeEvent(teType, i, cron, t)
}

func (server *GodisServer) AeMain() {
	server.aeLoop.AeMain()
}
