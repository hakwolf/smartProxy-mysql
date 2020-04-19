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
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hakwolf/smartProxy-mysql/core/errors"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/core/hack"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/hakwolf/smartProxy-mysql/sqlparser"
)

const (
	Master = "master"
	Slave  = "slave"

	ServerRegion = "server"
	NodeRegion   = "node"

	//op
	ADMIN_OPT_ADD     = "add"
	ADMIN_OPT_DEL     = "del"
	ADMIN_OPT_UP      = "up"
	ADMIN_OPT_DOWN    = "down"
	ADMIN_OPT_SHOW    = "show"
	ADMIN_OPT_CHANGE  = "change"
	ADMIN_SAVE_CONFIG = "save"

	ADMIN_PROXY         = "proxy"
	ADMIN_NODE          = "node"
	ADMIN_SCHEMA        = "schema"
	ADMIN_LOG_SQL       = "log_sql"
	ADMIN_SLOW_LOG_TIME = "slow_log_time"
	ADMIN_ALLOW_IP      = "allow_ip"
	ADMIN_BLACK_SQL     = "black_sql"

	ADMIN_CONFIG = "config"
	ADMIN_STATUS = "status"
)

var cmdServerOrder = []string{"opt", "k", "v"}
var cmdNodeOrder = []string{"opt", "node", "k", "v"}

func (cc *ClientConn) handleNodeCmd(rows sqlparser.InsertRows) error {
	var err error
	var opt, nodeName, role, addr string

	vals := rows.(sqlparser.Values)
	if len(vals) == 0 {
		return errors.ErrCmdUnsupport
	}

	tuple := vals[0].(sqlparser.ValTuple)
	if len(tuple) != len(cmdNodeOrder) {
		return errors.ErrCmdUnsupport
	}

	opt = sqlparser.String(tuple[0])
	opt = strings.Trim(opt, "'")

	nodeName = sqlparser.String(tuple[1])
	nodeName = strings.Trim(nodeName, "'")

	role = sqlparser.String(tuple[2])
	role = strings.Trim(role, "'")

	addr = sqlparser.String(tuple[3])
	addr = strings.Trim(addr, "'")

	switch strings.ToLower(opt) {
	case ADMIN_OPT_ADD:
		err = cc.AddDatabase(
			nodeName,
			role,
			addr,
		)
	case ADMIN_OPT_DEL:
		err = cc.DeleteDatabase(
			nodeName,
			role,
			addr,
		)

	case ADMIN_OPT_UP:
		err = cc.UpDatabase(
			nodeName,
			role,
			addr,
		)
	case ADMIN_OPT_DOWN:
		err = cc.DownDatabase(
			nodeName,
			role,
			addr,
		)
	default:
		err = errors.ErrCmdUnsupport
		golog.Error("ClientConn", "handleNodeCmd", err.Error(),
			cc.connectionId, "opt", opt)
	}
	return err
}

func (cc *ClientConn) handleServerCmd(rows sqlparser.InsertRows) (*mysql.Resultset, error) {
	var err error
	var result *mysql.Resultset
	var opt, k, v string

	vals := rows.(sqlparser.Values)
	if len(vals) == 0 {
		return nil, errors.ErrCmdUnsupport
	}

	tuple := vals[0].(sqlparser.ValTuple)
	if len(tuple) != len(cmdServerOrder) {
		return nil, errors.ErrCmdUnsupport
	}

	opt = sqlparser.String(tuple[0])
	opt = strings.Trim(opt, "'")

	k = sqlparser.String(tuple[1])
	k = strings.Trim(k, "'")

	v = sqlparser.String(tuple[2])
	v = strings.Trim(v, "'")

	switch strings.ToLower(opt) {
	case ADMIN_OPT_SHOW:
		result, err = cc.handleAdminShow(k, v)
	case ADMIN_OPT_CHANGE:
		err = cc.handleAdminChange(k, v)
	case ADMIN_OPT_ADD:
		err = cc.handleAdminAdd(k, v)
	case ADMIN_OPT_DEL:
		err = cc.handleAdminDelete(k, v)
	case ADMIN_SAVE_CONFIG:
		err = cc.handleAdminSave(k, v)
	default:
		err = errors.ErrCmdUnsupport
		golog.Error("ClientConn", "handleNodeCmd", err.Error(),
			cc.connectionId, "opt", opt)
	}
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (cc *ClientConn) AddDatabase(nodeName string, role string, addr string) error {
	//can not add a new master database
	if role != Slave {
		return errors.ErrCmdUnsupport
	}

	return cc.proxy.AddSlave(nodeName, addr)
}

func (cc *ClientConn) DeleteDatabase(nodeName string, role string, addr string) error {
	//can not delete a master database
	if role != Slave {
		return errors.ErrCmdUnsupport
	}

	return cc.proxy.DeleteSlave(nodeName, addr)
}

func (cc *ClientConn) UpDatabase(nodeName string, role string, addr string) error {
	if role != Master && role != Slave {
		return errors.ErrCmdUnsupport
	}
	if role == Master {
		return cc.proxy.UpMaster(nodeName, addr)
	}

	return cc.proxy.UpSlave(nodeName, addr)
}

func (cc *ClientConn) DownDatabase(nodeName string, role string, addr string) error {
	if role != Master && role != Slave {
		return errors.ErrCmdUnsupport
	}
	if role == Master {
		return cc.proxy.DownMaster(nodeName, addr)
	}

	return cc.proxy.DownSlave(nodeName, addr)
}

func (cc *ClientConn) checkCmdOrder(region string, columns sqlparser.Columns) error {
	var cmdOrder []string
	node := sqlparser.SelectExprs(columns)

	switch region {
	case NodeRegion:
		cmdOrder = cmdNodeOrder
	case ServerRegion:
		cmdOrder = cmdServerOrder
	default:
		return errors.ErrCmdUnsupport
	}

	for i := 0; i < len(node); i++ {
		val := sqlparser.String(node[i])
		if val != cmdOrder[i] {
			return errors.ErrCmdUnsupport
		}
	}

	return nil
}

func (cc *ClientConn) handleAdminHelp(ah *sqlparser.AdminHelp) error {
	var Column = 2
	var rows [][]string
	var names []string = []string{"command", "description"}
	relativePath := "/doc/KingDoc/command_help"

	execPath, err := os.Getwd()
	if err != nil {
		return err
	}

	helpFilePath := execPath + relativePath
	file, err := os.Open(helpFilePath)
	if err != nil {
		return err
	}

	defer file.Close()
	rd := bufio.NewReader(file)
	for {
		line, err := rd.ReadString('\n')
		//end of file
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		//parse the command description with '|' separating
		line = strings.TrimSpace(line)
		if len(line) != 0 {
			cmdStr := strings.SplitN(line, "|", 2)
			if len(cmdStr) == 2 {
				rows = append(rows,
					[]string{
						strings.TrimSpace(cmdStr[0]),
						strings.TrimSpace(cmdStr[1]),
					},
				)
			}
		}
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	result, err := cc.buildResultset(nil, names, values)
	if err != nil {
		return err
	}
	return cc.writeResultset(cc.status, result)
}

func (cc *ClientConn) handleAdmin(admin *sqlparser.Admin) error {
	var err error
	var result *mysql.Resultset

	region := sqlparser.String(admin.Region)

	err = cc.checkCmdOrder(region, admin.Columns)
	if err != nil {
		return err
	}

	switch strings.ToLower(region) {
	case NodeRegion:
		err = cc.handleNodeCmd(admin.Rows)
	case ServerRegion:
		result, err = cc.handleServerCmd(admin.Rows)
	default:
		return fmt.Errorf("admin %s not supported now", region)
	}

	if err != nil {
		golog.Error("ClientConn", "handleAdmin", err.Error(),
			cc.connectionId, "sql", sqlparser.String(admin))
		return err
	}

	if result != nil {
		return cc.writeResultset(cc.status, result)
	}

	return cc.writeOK(nil)
}

func (cc *ClientConn) handleAdminShow(k, v string) (*mysql.Resultset, error) {
	if len(k) == 0 || len(v) == 0 {
		return nil, errors.ErrCmdUnsupport
	}
	if k == ADMIN_PROXY && v == ADMIN_CONFIG {
		return cc.handleShowProxyConfig()
	}

	if k == ADMIN_PROXY && v == ADMIN_STATUS {
		return cc.handleShowProxyStatus()
	}

	if k == ADMIN_NODE && v == ADMIN_CONFIG {
		return cc.handleShowNodeConfig()
	}

	if k == ADMIN_SCHEMA && v == ADMIN_CONFIG {
		return cc.handleShowSchemaConfig()
	}

	if k == ADMIN_ALLOW_IP && v == ADMIN_CONFIG {
		return cc.handleShowAllowIPConfig()
	}

	if k == ADMIN_BLACK_SQL && v == ADMIN_CONFIG {
		return cc.handleShowBlackSqlConfig()
	}

	return nil, errors.ErrCmdUnsupport
}

func (cc *ClientConn) handleAdminChange(k, v string) error {
	if len(k) == 0 || len(v) == 0 {
		return errors.ErrCmdUnsupport
	}
	if k == ADMIN_LOG_SQL {
		return cc.handleChangeLogSql(v)
	}

	if k == ADMIN_SLOW_LOG_TIME {
		return cc.handleChangeSlowLogTime(v)
	}

	if k == ADMIN_PROXY {
		return cc.handleChangeProxy(v)
	}

	return errors.ErrCmdUnsupport
}

func (cc *ClientConn) handleAdminAdd(k, v string) error {
	if len(k) == 0 || len(v) == 0 {
		return errors.ErrCmdUnsupport
	}
	if k == ADMIN_ALLOW_IP {
		return cc.handleAddAllowIP(v)
	}

	if k == ADMIN_BLACK_SQL {
		return cc.handleAddBlackSql(v)
	}

	return errors.ErrCmdUnsupport
}

func (cc *ClientConn) handleAdminDelete(k, v string) error {
	if len(k) == 0 || len(v) == 0 {
		return errors.ErrCmdUnsupport
	}
	if k == ADMIN_ALLOW_IP {
		return cc.handleDelAllowIP(v)
	}

	if k == ADMIN_BLACK_SQL {
		return cc.handleDelBlackSql(v)
	}

	return errors.ErrCmdUnsupport
}

func (cc *ClientConn) handleShowProxyConfig() (*mysql.Resultset, error) {
	var names []string = []string{"Key", "Value"}
	var rows [][]string
	var nodeNames []string
	var users []string

	const (
		Column = 2
	)
	for name := range cc.schema.nodes {
		nodeNames = append(nodeNames, name)
	}
	for user, _ := range cc.proxy.users {
		users = append(users, user)
	}

	rows = append(rows, []string{"Addr", cc.proxy.cfg.Addr})
	rows = append(rows, []string{"User_List", strings.Join(users, ",")})
	rows = append(rows, []string{"LogPath", cc.proxy.cfg.LogPath})
	rows = append(rows, []string{"LogLevel", cc.proxy.cfg.LogLevel})
	rows = append(rows, []string{"LogSql", cc.proxy.logSql[cc.proxy.logSqlIndex]})
	rows = append(rows, []string{"SlowLogTime", strconv.Itoa(cc.proxy.slowLogTime[cc.proxy.slowLogTimeIndex])})
	rows = append(rows, []string{"Nodes_Count", fmt.Sprintf("%d", len(cc.proxy.nodes))})
	rows = append(rows, []string{"Nodes_List", strings.Join(nodeNames, ",")})
	rows = append(rows, []string{"ClientConns", fmt.Sprintf("%d", cc.proxy.counter.ClientConns)})
	rows = append(rows, []string{"ClientQPS", fmt.Sprintf("%d", cc.proxy.counter.OldClientQPS)})
	rows = append(rows, []string{"ErrLogTotal", fmt.Sprintf("%d", cc.proxy.counter.OldErrLogTotal)})
	rows = append(rows, []string{"SlowLogTotal", fmt.Sprintf("%d", cc.proxy.counter.OldSlowLogTotal)})

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleShowNodeConfig() (*mysql.Resultset, error) {
	var names []string = []string{
		"Node",
		"Address",
		"Type",
		"State",
		"LastPing",
		"MaxConn",
		"IdleConn",
		"CacheConns",
		"PushConnCount",
		"PopConnCount",
	}
	var rows [][]string
	const (
		Column = 10
	)

	//var nodeRows [][]string
	for name, node := range cc.schema.nodes {
		//"master"
		idleConns,cacheConns,pushConnCount,popConnCount := node.Master.ConnCount()
		
		rows = append(
			rows,
			[]string{
				name,
				node.Master.Addr(),
				"master",
				node.Master.State(),
				fmt.Sprintf("%v", time.Unix(node.Master.GetLastPing(), 0)),
				strconv.Itoa(node.Cfg.MaxConnNum),
				strconv.Itoa(idleConns),
				strconv.Itoa(cacheConns),
				strconv.FormatInt(pushConnCount, 10),
				strconv.FormatInt(popConnCount, 10),
			})
		//"slave"
		for _, slave := range node.Slave {
			if slave != nil {
				idleConns,cacheConns,pushConnCount,popConnCount := slave.ConnCount()

				rows = append(
					rows,
					[]string{
						name,
						slave.Addr(),
						"slave",
						slave.State(),
						fmt.Sprintf("%v", time.Unix(slave.GetLastPing(), 0)),
						strconv.Itoa(node.Cfg.MaxConnNum),
						strconv.Itoa(idleConns),
						strconv.Itoa(cacheConns),
						strconv.FormatInt(pushConnCount, 10),
						strconv.FormatInt(popConnCount, 10),
					})
			}
		}
	}
	//rows = append(rows, nodeRows...)
	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleShowSchemaConfig() (*mysql.Resultset, error) {
	var rows [][]string
	var names []string = []string{
		"User",
		"DB",
		"Table",
		"Type",
		"Key",
		"Nodes_List",
		"Locations",
		"TableRowLimit",
	}
	var Column = len(names)

	for _, schemaConfig := range cc.proxy.cfg.SchemaList {
		//default Rule
		var defaultRule = cc.schema.rule.DefaultRule
		if defaultRule != nil {
			rows = append(
				rows,
				[]string{
					schemaConfig.User,
					defaultRule.DB,
					defaultRule.Table,
					defaultRule.Type,
					defaultRule.Key,
					strings.Join(defaultRule.Nodes, ", "),
					"",
					"0",
				},
			)
		}

		shardRule := schemaConfig.ShardRule
		for _, r := range shardRule {
			rows = append(
				rows,
				[]string{
					schemaConfig.User,
					r.DB,
					r.Table,
					r.Type,
					r.Key,
					strings.Join(r.Nodes, ", "),
					hack.ArrayToString(r.Locations),
					strconv.Itoa(r.TableRowLimit),
				},
			)
		}
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleShowAllowIPConfig() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"AllowIP",
	}

	//allow ips
	current, _, _ := cc.proxy.allowipsIndex.Get()
	var allowips = cc.proxy.allowips[current]
	if len(allowips) != 0 {
		for _, v := range allowips {
			if v.Info() != "" {
				rows = append(rows,
					[]string{
						v.Info(),
					})
			}
		}
	}

	if len(rows) == 0 {
		rows = append(rows, []string{""})
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleShowProxyStatus() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"status",
	}

	var status string
	status = cc.proxy.Status()
	rows = append(rows,
		[]string{
			status,
		})

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleShowBlackSqlConfig() (*mysql.Resultset, error) {
	var Column = 1
	var rows [][]string
	var names []string = []string{
		"BlackListSql",
	}

	//black sql
	var blackListSqls = cc.proxy.blacklistSqls[cc.proxy.blacklistSqlsIndex].sqls
	if len(blackListSqls) != 0 {
		for _, v := range blackListSqls {
			rows = append(rows,
				[]string{
					v,
				})
		}
	}

	if len(rows) == 0 {
		rows = append(rows, []string{""})
	}

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return cc.buildResultset(nil, names, values)
}

func (cc *ClientConn) handleChangeProxy(v string) error {
	return cc.proxy.ChangeProxy(v)
}

func (cc *ClientConn) handleChangeLogSql(v string) error {
	return cc.proxy.ChangeLogSql(v)
}

func (cc *ClientConn) handleChangeSlowLogTime(v string) error {
	return cc.proxy.ChangeSlowLogTime(v)
}

func (cc *ClientConn) handleAddAllowIP(v string) error {
	v = strings.TrimSpace(v)
	err := cc.proxy.AddAllowIP(v)
	return err
}

func (cc *ClientConn) handleDelAllowIP(v string) error {
	v = strings.TrimSpace(v)
	err := cc.proxy.DelAllowIP(v)
	return err
}

func (cc *ClientConn) handleAddBlackSql(v string) error {
	v = strings.TrimSpace(v)
	err := cc.proxy.AddBlackSql(v)
	return err
}

func (cc *ClientConn) handleDelBlackSql(v string) error {
	v = strings.TrimSpace(v)
	err := cc.proxy.DelBlackSql(v)
	return err
}

func (cc *ClientConn) handleAdminSave(k string, v string) error {
	if len(k) == 0 || len(v) == 0 {
		return errors.ErrCmdUnsupport
	}
	if k == ADMIN_PROXY && v == ADMIN_CONFIG {
		return cc.proxy.SaveProxyConfig()
	}

	return errors.ErrCmdUnsupport
}
