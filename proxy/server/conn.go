// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/hakwolf/smartProxy-mysql/backend"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/core/hack"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/pingcap/tidb/util/arena"
)

//client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg *mysql.PacketIO

	c net.Conn

	proxy *Server

	capability uint32

	connectionId uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	salt []byte

	nodes  map[string]*backend.Node
	schema *Schema

	txConns map[*backend.Node]*backend.BackendConn

	closed bool

	lastInsertId int64
	affectedRows int64

	stmtId uint32

	stmts map[uint32]*Stmt //prepare相关,client端到proxy的stmt

	configVer uint32 //check config version for reload online

	alloc        arena.Allocator   // an memory allocator for reducing memory allocation.
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func (cc *ClientConn) IsAllowConnect() bool {
	clientHost, _, err := net.SplitHostPort(cc.c.RemoteAddr().String())
	if err != nil {
		fmt.Println(err)
	}
	clientIP := net.ParseIP(clientHost)

	current, _, _ := cc.proxy.allowipsIndex.Get()
	ipVec := cc.proxy.allowips[current]
	if ipVecLen := len(ipVec); ipVecLen == 0 {
		return true
	}
	for _, ip := range ipVec {
		if ip.Match(clientIP) {
			return true
		}
	}

	golog.Error("server", "IsAllowConnect", "error", mysql.ER_ACCESS_DENIED_ERROR,
		"ip address", cc.c.RemoteAddr().String(), " access denied by kindshard.")
	return false
}

func (cc *ClientConn) Handshake() error {
	if err := cc.writeInitialHandshake(); err != nil {
		golog.Error("server", "Handshake", err.Error(),
			cc.connectionId, "msg", "send initial handshake error")
		return err
	}

	if err := cc.readHandshakeResponse(); err != nil {
		golog.Error("server", "readHandshakeResponse",
			err.Error(), cc.connectionId,
			"msg", "read Handshake Response error")
		return err
	}

	if err := cc.writeOK(nil); err != nil {
		golog.Error("server", "readHandshakeResponse",
			"write ok fail",
			cc.connectionId, "error", err.Error())
		return err
	}

	cc.pkg.Sequence = 0
	return nil
}

func (cc *ClientConn) Close() error {
	if cc.closed {
		return nil
	}

	cc.c.Close()

	cc.closed = true

	return nil
}

func (cc *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(cc.connectionId), byte(cc.connectionId>>8), byte(cc.connectionId>>16), byte(cc.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(cc.status), byte(cc.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return cc.writePacket(data)
}

func (cc *ClientConn) readPacket() ([]byte, error) {
	return cc.pkg.ReadPacket()
}

func (cc *ClientConn) writePacket(data []byte) error {
	return cc.pkg.WritePacket(data)
}

func (cc *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return cc.pkg.WritePacketBatch(total, data, direct)
}

func (cc *ClientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	cc.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	cc.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(cc.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	//check user
	if _, ok := cc.proxy.users[cc.user]; !ok {
		golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"client_user", cc.user,
			"config_set_user", cc.user,
			"passworld", cc.proxy.users[cc.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, cc.user, cc.c.RemoteAddr().String(), "Yes")
	}

	//check password
	checkAuth := mysql.CalcPassword(cc.salt, []byte(cc.proxy.users[cc.user]))
	if !bytes.Equal(auth, checkAuth) {
		golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", cc.user,
			"config_set_user", cc.user,
			"passworld", cc.proxy.users[cc.user])
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, cc.user, cc.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if cc.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(cc.db) + 1

	}
	cc.db = db

	return nil
}

func (cc *ClientConn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			golog.Error("ClientConn", "Run",
				err.Error(), 0,
				"stack", string(buf))
		}

		cc.Close()
	}()

	for {
		data, err := cc.readPacket()
		fmt.Print("data, err := cc.readPacket()")
		fmt.Println(data)
		if err != nil {
			return
		}

		if cc.configVer != cc.proxy.configVer {
			err := cc.reloadConfig()
			if nil != err {
				golog.Error("ClientConn", "Run",
					err.Error(), cc.connectionId,
				)
				cc.writeError(err)
				return
			}
			cc.configVer = cc.proxy.configVer
			golog.Debug("ClientConn", "Run",
				fmt.Sprintf("config reload ok, ver:%d", cc.configVer), cc.connectionId,
			)
		}

		if err := cc.dispatch(data); err != nil {
			cc.proxy.counter.IncrErrLogTotal()
			golog.Error("ClientConn", "Run",
				err.Error(), cc.connectionId,
			)
			cc.writeError(err)
			if err == mysql.ErrBadConn {
				cc.Close()
			}
		}

		if cc.closed {
			return
		}

		cc.pkg.Sequence = 0
	}
}

func (cc *ClientConn) dispatch(data []byte) error {
	cc.proxy.counter.IncrClientQPS()
	cmd := data[0]
	data = data[1:]
    datastr := hack.String(data)
	fmt.Println("////////////////////////////////")
	fmt.Println(data)
    fmt.Println(datastr)
    fmt.Println(fmt.Sprintf("当前命令 command %d ", cmd))
	switch cmd {
	case mysql.COM_QUIT:
		cc.handleRollback()
		cc.Close()
		return nil
	case mysql.COM_QUERY:
		return cc.handleQuery(datastr)
	case mysql.COM_PING:
		return cc.writeOK(nil)
	case mysql.COM_INIT_DB:
		return cc.handleUseDB(datastr)
	case mysql.COM_FIELD_LIST:
		return cc.handleFieldList(data)
	case mysql.COM_STMT_PREPARE:
		fmt.Println("COM_STMT_PREPARE")
		return cc.handleStmtPrepare(datastr)
	case mysql.COM_STMT_EXECUTE:
		fmt.Println("COM_STMT_EXECUTE")
		return cc.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		fmt.Println("COM_STMT_CLOSE")
		return cc.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		return cc.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		return cc.handleStmtReset(data)
	case mysql.COM_SET_OPTION:
		return cc.writeEOF(0)
	default:
		msg := fmt.Sprintf("command %d not supported now", cmd)
		fmt.Println(msg)
		golog.Error("ClientConn", "dispatch", msg, 0)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

func (cc *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: cc.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if cc.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return cc.writePacket(data)
}

func (cc *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if cc.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return cc.writePacket(data)
}

func (cc *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if cc.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return cc.writePacket(data)
}

func (cc *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if cc.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return cc.writePacketBatch(total, data, direct)
}

func (cc *ClientConn) reloadConfig() error {
	cc.proxy.configUpdateMutex.RLock()
	defer cc.proxy.configUpdateMutex.RUnlock()
	cc.schema = cc.proxy.GetSchema(cc.user)
	if nil == cc.schema {
		return fmt.Errorf("schema of user [%s] is null or user is deleted", cc.user)
	}
	cc.nodes = cc.proxy.nodes

	return nil
}
