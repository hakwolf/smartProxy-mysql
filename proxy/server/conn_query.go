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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hakwolf/smartProxy-mysql/backend"
	"github.com/hakwolf/smartProxy-mysql/core/errors"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/core/hack"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/hakwolf/smartProxy-mysql/proxy/router"
	"github.com/hakwolf/smartProxy-mysql/sqlparser"
)

/*处理query语句*/
func (cc *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}

			err = errors.ErrInternalServer
			return
		}
	}()

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号
	hasHandled, err := cc.preHandleShard(sql)
	if err != nil {
		golog.Error("server", "preHandleShard", err.Error(), 0,
			"sql", sql,
			"hasHandled", hasHandled,
		)
		return err
	}
	if hasHandled {
		return nil
	}

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		golog.Error("server", "parse", err.Error(), 0, "hasHandled", hasHandled, "sql", sql)
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Select:
		return cc.handleSelect(v, nil)
	case *sqlparser.Insert:
		return cc.handleExec(stmt, nil)
	case *sqlparser.Update:
		return cc.handleExec(stmt, nil)
	case *sqlparser.Delete:
		return cc.handleExec(stmt, nil)
	case *sqlparser.Replace:
		return cc.handleExec(stmt, nil)
	case *sqlparser.Set:
		return cc.handleSet(v, sql)
	case *sqlparser.Begin:
		return cc.handleBegin()
	case *sqlparser.Commit:
		return cc.handleCommit()
	case *sqlparser.Rollback:
		return cc.handleRollback()
	case *sqlparser.Admin:
		if cc.user == "root" {
			return cc.handleAdmin(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.AdminHelp:
		if cc.user == "root" {
			return cc.handleAdminHelp(v)
		}
		return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.UseDB:
		return cc.handleUseDB(v.DB)
	case *sqlparser.SimpleSelect:
		return cc.handleSimpleSelect(v)
	case *sqlparser.Truncate:
		return cc.handleExec(stmt, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}

	return nil
}

func (cc *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	if !cc.isInTransaction() {
		if fromSlave {
			co, err = n.GetSlaveConn()
			if err != nil {
				co, err = n.GetMasterConn()
			}
		} else {
			co, err = n.GetMasterConn()
		}
		if err != nil {
			golog.Error("server", "getBackendConn", err.Error(), 0)
			return
		}
	} else {
		var ok bool
		co, ok = cc.txConns[n]

		if !ok {
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			if !cc.isAutoCommit() {
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else {
				if err = co.Begin(); err != nil {
					return
				}
			}

			cc.txConns[n] = co
		}
	}

	if err = co.UseDB(cc.db); err != nil {
		//reset the database to null
		cc.db = ""
		return
	}

	if err = co.SetCharset(cc.charset, cc.collation); err != nil {
		return
	}

	return
}

//获取shard的conn，第一个参数表示是不是select
func (cc *ClientConn) getShardConns(fromSlave bool, plan *router.Plan) (map[string]*backend.BackendConn, error) {
	var err error
	if plan == nil || len(plan.RouteNodeIndexs) == 0 {
		return nil, errors.ErrNoRouteNode
	}

	nodesCount := len(plan.RouteNodeIndexs)
	nodes := make([]*backend.Node, 0, nodesCount)
	for i := 0; i < nodesCount; i++ {
		nodeIndex := plan.RouteNodeIndexs[i]
		nodes = append(nodes, cc.proxy.GetNode(plan.Rule.Nodes[nodeIndex]))
	}
	if cc.isInTransaction() {
		if 1 < len(nodes) {
			return nil, errors.ErrTransInMulti
		}
		//exec in multi node
		if len(cc.txConns) == 1 && cc.txConns[nodes[0]] == nil {
			return nil, errors.ErrTransInMulti
		}
	}
	conns := make(map[string]*backend.BackendConn)
	var co *backend.BackendConn
	for _, n := range nodes {
		co, err = cc.getBackendConn(n, fromSlave)
		if err != nil {
			break
		}

		conns[n.Cfg.Name] = co
	}

	return conns, err
}

func (cc *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	if strings.ToLower(cc.proxy.logSql[cc.proxy.logSqlIndex]) != golog.LogSqlOff &&
		execTime >= float64(cc.proxy.slowLogTime[cc.proxy.slowLogTimeIndex]) {
		cc.proxy.counter.IncrSlowLogTotal()
		golog.OutputSql(state, "%.1fms - %s->%s:%s",
			execTime,
			cc.c.RemoteAddr(),
			conn.GetAddr(),
			sql,
		)
	}

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
}

func (cc *ClientConn) executeInMultiNodes(conns map[string]*backend.BackendConn, sqls map[string][]string, args []interface{}) ([]*mysql.Result, error) {
	if len(conns) != len(sqls) {
		golog.Error("ClientConn", "executeInMultiNodes", errors.ErrConnNotEqual.Error(), cc.connectionId,
			"conns", conns,
			"sqls", sqls,
		)
		return nil, errors.ErrConnNotEqual
	}

	var wg sync.WaitGroup

	if len(conns) == 0 {
		return nil, errors.ErrNoPlan
	}

	wg.Add(len(conns))

	resultCount := 0
	for _, sqlSlice := range sqls {
		resultCount += len(sqlSlice)
	}

	rs := make([]interface{}, resultCount)

	f := func(rs []interface{}, i int, execSqls []string, co *backend.BackendConn) {
		var state string
		for _, v := range execSqls {
			startTime := time.Now().UnixNano()
			r, err := co.Execute(v, args...)
			if err != nil {
				state = "ERROR"
				rs[i] = err
			} else {
				state = "OK"
				rs[i] = r
			}
			execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
			if cc.proxy.logSql[cc.proxy.logSqlIndex] != golog.LogSqlOff &&
				execTime >= float64(cc.proxy.slowLogTime[cc.proxy.slowLogTimeIndex]) {
				cc.proxy.counter.IncrSlowLogTotal()
				golog.OutputSql(state, "%.1fms - %s->%s:%s",
					execTime,
					cc.c.RemoteAddr(),
					co.GetAddr(),
					v,
				)
			}
			i++
		}
		wg.Done()
	}

	offset := 0
	for nodeName, co := range conns {
		s := sqls[nodeName] //[]string
		go f(rs, offset, s, co)
		offset += len(s)
	}

	wg.Wait()

	var err error
	r := make([]*mysql.Result, resultCount)
	for i, v := range rs {
		if e, ok := v.(error); ok {
			err = e
			break
		}
		if rs[i] != nil {
			r[i] = rs[i].(*mysql.Result)
		}
	}

	return r, err
}

func (cc *ClientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	if cc.isInTransaction() {
		return
	}

	if rollback {
		conn.Rollback()
	}

	conn.Close()
}

func (cc *ClientConn) closeShardConns(conns map[string]*backend.BackendConn, rollback bool) {
	if cc.isInTransaction() {
		return
	}

	for _, co := range conns {
		if rollback {
			co.Rollback()
		}
		co.Close()
	}
}

func (cc *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.NonStarExpr:
			if e.As != nil {
				r.Fields[i].Name = e.As
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

func (cc *ClientConn) handleExec(stmt sqlparser.Statement, args []interface{}) error {
	plan, err := cc.schema.rule.BuildPlan(cc.db, stmt)
	if err != nil {
		return err
	}
	conns, err := cc.getShardConns(false, plan)
	defer cc.closeShardConns(conns, err != nil)
	if err != nil {
		golog.Error("ClientConn", "handleExec", err.Error(), cc.connectionId)
		return err
	}
	if conns == nil {
		return cc.writeOK(nil)
	}

	var rs []*mysql.Result

	rs, err = cc.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	if err == nil {
		err = cc.mergeExecResult(rs)
	}

	return err
}

func (cc *ClientConn) mergeExecResult(rs []*mysql.Result) error {
	r := new(mysql.Result)
	for _, v := range rs {
		r.Status |= v.Status
		r.AffectedRows += v.AffectedRows
		if r.InsertId == 0 {
			r.InsertId = v.InsertId
		} else if r.InsertId > v.InsertId {
			//last insert id is first gen id for multi row inserted
			//see http://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
			r.InsertId = v.InsertId
		}
	}

	if r.InsertId > 0 {
		cc.lastInsertId = int64(r.InsertId)
	}
	cc.affectedRows = int64(r.AffectedRows)

	return cc.writeOK(r)
}
