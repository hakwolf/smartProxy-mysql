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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/hakwolf/smartProxy-mysql/core/errors"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/hakwolf/smartProxy-mysql/sqlparser"
)

var paramFieldData []byte
var columnFieldData []byte

func init() {
	var p = &mysql.Field{Name: []byte("?")}
	var c = &mysql.Field{}

	paramFieldData = p.Dump()
	columnFieldData = c.Dump()
}

type Stmt struct {
	id uint32

	params  int
	columns int

	args []interface{}

	s sqlparser.Statement

	sql string
}

func (s *Stmt) ResetParams() {
	s.args = make([]interface{}, s.params)
}

func (cc *ClientConn) handleStmtPrepare(sql string) error {
	if cc.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	s := new(Stmt)

	sql = strings.TrimRight(sql, ";")

	var err error
	s.s, err = sqlparser.Parse(sql)
	if err != nil {
		//return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	s.sql = sql

	defaultRule := cc.schema.rule.DefaultRule

	n := cc.proxy.GetNode(defaultRule.Nodes[0])

	co, err := cc.getBackendConn(n, false)
	defer cc.closeConn(co, false)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}

	err = co.UseDB(cc.db)
	if err != nil {
		//reset the database to null
		cc.db = ""
		return fmt.Errorf("prepare error %s", err)
	}

	t, err := co.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}
	s.params = t.ParamNum()
	s.columns = t.ColumnNum()

	s.id = cc.stmtId
	cc.stmtId++

	if err = cc.writePrepare(s); err != nil {
		return err
	}

	s.ResetParams()
	cc.stmts[s.id] = s

	err = co.ClosePrepare(t.GetId())
	if err != nil {
		return err
	}

	return nil
}

func (cc *ClientConn) writePrepare(s *Stmt) error {
	var err error
	data := make([]byte, 4, 128)
	total := make([]byte, 0, 1024)
	//status ok
	data = append(data, 0)
	//stmt id
	data = append(data, mysql.Uint32ToBytes(s.id)...)
	//number columns
	data = append(data, mysql.Uint16ToBytes(uint16(s.columns))...)
	//number params
	data = append(data, mysql.Uint16ToBytes(uint16(s.params))...)
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0)

	total, err = cc.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	if s.params > 0 {
		for i := 0; i < s.params; i++ {
			data = data[0:4]
			data = append(data, []byte(paramFieldData)...)

			total, err = cc.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = cc.writeEOFBatch(total, cc.status, false)
		if err != nil {
			return err
		}
	}

	if s.columns > 0 {
		for i := 0; i < s.columns; i++ {
			data = data[0:4]
			data = append(data, []byte(columnFieldData)...)

			total, err = cc.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = cc.writeEOFBatch(total, cc.status, false)
		if err != nil {
			return err
		}

	}
	total, err = cc.writePacketBatch(total, nil, true)
	total = nil
	if err != nil {
		return err
	}
	return nil
}

func (cc *ClientConn) handleStmtExecute(data []byte) error {
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}

	pos := 0
	id := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	s, ok := cc.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_execute")
	}

	flag := data[pos]
	pos++
	//now we only support CURSOR_TYPE_NO_CURSOR flag
	if flag != 0 {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
	}

	//skip iteration-count, always 1
	pos += 4

	var nullBitmaps []byte
	var paramTypes []byte
	var paramValues []byte

	paramNum := s.params

	if paramNum > 0 {
		nullBitmapLen := (s.params + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return mysql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		//new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (paramNum << 1)) {
				return mysql.ErrMalformPacket
			}

			paramTypes = data[pos : pos+(paramNum<<1)]
			pos += (paramNum << 1)

			paramValues = data[pos:]
		}

		if err := cc.bindStmtArgs(s, nullBitmaps, paramTypes, paramValues); err != nil {
			return err
		}
	}

	var err error

	switch stmt := s.s.(type) {
	case *sqlparser.Select:
		err = cc.handlePrepareSelect(stmt, s.sql, s.args)
	case *sqlparser.Insert:
		err = cc.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Update:
		err = cc.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Delete:
		err = cc.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Replace:
		err = cc.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Set:
		err = cc.handlePrepareExec(s.s, s.sql, s.args) //新增的  modfy by qjh
	default:
		err = cc.handlePrepareExec(s.s, s.sql, s.args)
		//err = fmt.Errorf("command %T not supported now", stmt)
	}

	s.ResetParams()

	return err
}

func (cc *ClientConn) handlePrepareSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {
	defaultRule := cc.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return errors.ErrNoDefaultNode
	}
	defaultNode := cc.proxy.GetNode(defaultRule.Nodes[0])

	//choose connection in slave DB first
	conn, err := cc.getBackendConn(defaultNode, true)
	defer cc.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		r := cc.newEmptyResultset(stmt)
		return cc.writeResultset(cc.status, r)
	}

	var rs []*mysql.Result
	rs, err = cc.executeInNode(conn, sql, args)
	if err != nil {
		golog.Error("ClientConn", "handlePrepareSelect", err.Error(), cc.connectionId)
		return err
	}

	status := cc.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = cc.writeResultset(status, rs[0].Resultset)
	} else {
		r := cc.newEmptyResultset(stmt)
		err = cc.writeResultset(status, r)
	}

	return err
}

func (cc *ClientConn) handlePrepareExec(stmt sqlparser.Statement, sql string, args []interface{}) error {
	defaultRule := cc.schema.rule.DefaultRule
	if len(defaultRule.Nodes) == 0 {
		return errors.ErrNoDefaultNode
	}
	defaultNode := cc.proxy.GetNode(defaultRule.Nodes[0])

	//execute in Master DB
	conn, err := cc.getBackendConn(defaultNode, false)
	defer cc.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		return cc.writeOK(nil)
	}

	var rs []*mysql.Result
	rs, err = cc.executeInNode(conn, sql, args)
	cc.closeConn(conn, false)

	if err != nil {
		golog.Error("ClientConn", "handlePrepareExec", err.Error(), cc.connectionId)
		return err
	}

	status := cc.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = cc.writeResultset(status, rs[0].Resultset)
	} else {
		err = cc.writeOK(rs[0])
	}

	return err
}

func (cc *ClientConn) bindStmtArgs(s *Stmt, nullBitmap, paramTypes, paramValues []byte) error {
	args := s.args

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool
	var err error

	for i := 0; i < s.params; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB,
			mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			v, isNull, n, err = mysql.LengthEnodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return err
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			return fmt.Errorf("Stmt Unknown FieldType %d", tp)
		}
	}
	return nil
}

func (cc *ClientConn) handleStmtSendLongData(data []byte) error {
	if len(data) < 6 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := cc.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_send_longdata")
	}

	paramId := binary.LittleEndian.Uint16(data[4:6])
	if paramId >= uint16(s.params) {
		return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}

	if s.args[paramId] == nil {
		s.args[paramId] = data[6:]
	} else {
		if b, ok := s.args[paramId].([]byte); ok {
			b = append(b, data[6:]...)
			s.args[paramId] = b
		} else {
			return fmt.Errorf("invalid param long data type %T", s.args[paramId])
		}
	}

	return nil
}

func (cc *ClientConn) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := cc.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_reset")
	}

	s.ResetParams()

	return cc.writeOK(nil)
}

func (cc *ClientConn) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(cc.stmts, id)

	return nil
}
