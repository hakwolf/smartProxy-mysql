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
	"fmt"
	"strings"
	"time"

	"github.com/hakwolf/smartProxy-mysql/backend"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/hakwolf/smartProxy-mysql/sqlparser"
)

var nstring = sqlparser.String

func (cc *ClientConn) handleSet(stmt *sqlparser.Set, sql string) (err error) {
	if len(stmt.Exprs) != 1 && len(stmt.Exprs) != 2 {
		return fmt.Errorf("must set one item once, not %s", nstring(stmt))
	}

	//log the SQL
	startTime := time.Now().UnixNano()
	defer func() {
		var state string
		if err != nil {
			state = "ERROR"
		} else {
			state = "OK"
		}
		execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
		if cc.proxy.logSql[cc.proxy.logSqlIndex] != golog.LogSqlOff &&
			execTime >= float64(cc.proxy.slowLogTime[cc.proxy.slowLogTimeIndex]) {
			cc.proxy.counter.IncrSlowLogTotal()
			golog.OutputSql(state, "%.1fms - %s->%s:%s",
				execTime,
				cc.c.RemoteAddr(),
				cc.proxy.addr,
				sql,
			)
		}

	}()

	k := string(stmt.Exprs[0].Name.Name)
	switch strings.ToUpper(k) {
	case `AUTOCOMMIT`, `@@AUTOCOMMIT`, `@@SESSION.AUTOCOMMIT`:
		return cc.handleSetAutoCommit(stmt.Exprs[0].Expr)
	case `NAMES`,
		`CHARACTER_SET_RESULTS`, `@@CHARACTER_SET_RESULTS`, `@@SESSION.CHARACTER_SET_RESULTS`,
		`CHARACTER_SET_CLIENT`, `@@CHARACTER_SET_CLIENT`, `@@SESSION.CHARACTER_SET_CLIENT`,
		`CHARACTER_SET_CONNECTION`, `@@CHARACTER_SET_CONNECTION`, `@@SESSION.CHARACTER_SET_CONNECTION`:
		if len(stmt.Exprs) == 2 {
			//SET NAMES 'charset_name' COLLATE 'collation_name'
			return cc.handleSetNames(stmt.Exprs[0].Expr, stmt.Exprs[1].Expr)
		}
		return cc.handleSetNames(stmt.Exprs[0].Expr, nil)
	default:
		golog.Error("ClientConn", "handleSet", "command not supported",
			cc.connectionId, "sql", sql)
		return cc.writeOK(nil)
	}
}

func (cc *ClientConn) handleSetAutoCommit(val sqlparser.ValExpr) error {
	flag := sqlparser.String(val)
	flag = strings.Trim(flag, "'`\"")
	// autocommit允许为 0, 1, ON, OFF, "ON", "OFF", 不允许"0", "1"
	if flag == `0` || flag == `1` {
		_, ok := val.(sqlparser.NumVal)
		if !ok {
			return fmt.Errorf("set autocommit error")
		}
	}
	switch strings.ToUpper(flag) {
	case `1`, `ON`:
		cc.status |= mysql.SERVER_STATUS_AUTOCOMMIT
		if cc.status&mysql.SERVER_STATUS_IN_TRANS > 0 {
			cc.status &= ^mysql.SERVER_STATUS_IN_TRANS
		}
		for _, co := range cc.txConns {
			if e := co.SetAutoCommit(1); e != nil {
				co.Close()
				cc.txConns = make(map[*backend.Node]*backend.BackendConn)
				return fmt.Errorf("set autocommit error, %v", e)
			}
			co.Close()
		}
		cc.txConns = make(map[*backend.Node]*backend.BackendConn)
	case `0`, `OFF`:
		cc.status &= ^mysql.SERVER_STATUS_AUTOCOMMIT
	default:
		return fmt.Errorf("invalid autocommit flag %s", flag)
	}

	return cc.writeOK(nil)
}

func (cc *ClientConn) handleSetNames(ch, ci sqlparser.ValExpr) error {
	var cid mysql.CollationId
	var ok bool

	value := sqlparser.String(ch)
	value = strings.Trim(value, "'`\"")

	charset := strings.ToLower(value)
	if charset == "null" {
		return cc.writeOK(nil)
	}
	if ci == nil {
		if charset == "default" {
			charset = mysql.DEFAULT_CHARSET
		}
		cid, ok = mysql.CharsetIds[charset]
		if !ok {
			return fmt.Errorf("invalid charset %s", charset)
		}
	} else {
		collate := sqlparser.String(ci)
		collate = strings.Trim(collate, "'`\"")
		collate = strings.ToLower(collate)
		cid, ok = mysql.CollationNames[collate]
		if !ok {
			return fmt.Errorf("invalid collation %s", collate)
		}
	}
	cc.charset = charset
	cc.collation = cid

	return cc.writeOK(nil)
}
