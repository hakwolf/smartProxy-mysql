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
	"fmt"
	"strconv"
	"strings"

	"github.com/hakwolf/smartProxy-mysql/core/errors"
	"github.com/hakwolf/smartProxy-mysql/core/golog"
	"github.com/hakwolf/smartProxy-mysql/core/hack"
	"github.com/hakwolf/smartProxy-mysql/mysql"
	"github.com/hakwolf/smartProxy-mysql/sqlparser"
)

const (
	MasterComment    = "/*master*/"
	SumFunc          = "sum"
	CountFunc        = "count"
	MaxFunc          = "max"
	MinFunc          = "min"
	LastInsertIdFunc = "last_insert_id"
	FUNC_EXIST       = 1
)

var funcNameMap = map[string]int{
	"sum":            FUNC_EXIST,
	"count":          FUNC_EXIST,
	"max":            FUNC_EXIST,
	"min":            FUNC_EXIST,
	"last_insert_id": FUNC_EXIST,
}

func (cc *ClientConn) handleFieldList(data []byte) error {
	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	if cc.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	nodeName := cc.schema.rule.GetRule(cc.db, table).Nodes[0]

	n := cc.proxy.GetNode(nodeName)
	co, err := cc.getBackendConn(n, false)
	defer cc.closeConn(co, false)
	if err != nil {
		return err
	}

	if err = co.UseDB(cc.db); err != nil {
		//reset the database to null
		cc.db = ""
		return err
	}

	if fs, err := co.FieldList(table, wildcard); err != nil {
		return err
	} else {
		return cc.writeFieldList(cc.status, fs)
	}
}

func (cc *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	cc.affectedRows = int64(-1)
	var err error
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = cc.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = cc.writeEOFBatch(total, status, true)
	return err
}

//处理select语句
func (cc *ClientConn) handleSelect(stmt *sqlparser.Select, args []interface{}) error {
	var fromSlave bool = true
	plan, err := cc.schema.rule.BuildPlan(cc.db, stmt)
	if err != nil {
		return err
	}
	if 0 < len(stmt.Comments) {
		comment := string(stmt.Comments[0])
		if 0 < len(comment) && strings.ToLower(comment) == MasterComment {
			fromSlave = false
		}
	}

	conns, err := cc.getShardConns(fromSlave, plan)
	if err != nil {
		golog.Error("ClientConn", "handleSelect", err.Error(), cc.connectionId)
		return err
	}
	if conns == nil {
		r := cc.newEmptyResultset(stmt)
		return cc.writeResultset(cc.status, r)
	}

	var rs []*mysql.Result
	rs, err = cc.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	cc.closeShardConns(conns, false)
	if err != nil {
		golog.Error("ClientConn", "handleSelect", err.Error(), cc.connectionId)
		return err
	}

	err = cc.mergeSelectResult(rs, stmt)
	if err != nil {
		golog.Error("ClientConn", "handleSelect", err.Error(), cc.connectionId)
	}

	return err
}

func (cc *ClientConn) mergeSelectResult(rs []*mysql.Result, stmt *sqlparser.Select) error {
	var r *mysql.Result
	var err error

	if len(stmt.GroupBy) == 0 {
		r, err = cc.buildSelectOnlyResult(rs, stmt)
	} else {
		//group by
		r, err = cc.buildSelectGroupByResult(rs, stmt)
	}
	if err != nil {
		return err
	}

	cc.sortSelectResult(r.Resultset, stmt)
	//to do, add log here, sort may error because order by key not exist in resultset fields

	if err := cc.limitSelectResult(r.Resultset, stmt); err != nil {
		return err
	}

	return cc.writeResultset(r.Status, r.Resultset)
}

//only process last_inser_id
func (cc *ClientConn) handleSimpleSelect(stmt *sqlparser.SimpleSelect) error {
	nonStarExpr, _ := stmt.SelectExprs[0].(*sqlparser.NonStarExpr)
	var name string = hack.String(nonStarExpr.As)
	if name == "" {
		name = "last_insert_id()"
	}
	var column = 1
	var rows [][]string
	var names []string = []string{
		name,
	}

	var t = fmt.Sprintf("%d", cc.lastInsertId)
	rows = append(rows, []string{t})

	r := new(mysql.Resultset)

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	r, _ = cc.buildResultset(nil, names, values)
	return cc.writeResultset(cc.status, r)
}

//build select result with group by opt
func (cc *ClientConn) buildSelectGroupByResult(rs []*mysql.Result,
	stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	var r *mysql.Result
	var groupByIndexs []int

	fieldLen := len(rs[0].Fields)
	startIndex := fieldLen - len(stmt.GroupBy)
	for startIndex < fieldLen {
		groupByIndexs = append(groupByIndexs, startIndex)
		startIndex++
	}

	funcExprs := cc.getFuncExprs(stmt)
	if len(funcExprs) == 0 {
		r, err = cc.mergeGroupByWithoutFunc(rs, groupByIndexs)
	} else {
		r, err = cc.mergeGroupByWithFunc(rs, groupByIndexs, funcExprs)
	}
	if err != nil {
		return nil, err
	}

	//build result
	names := make([]string, 0, 2)
	if 0 < len(r.Values) {
		r.Fields = r.Fields[:groupByIndexs[0]]
		for i := 0; i < len(r.Fields) && i < groupByIndexs[0]; i++ {
			names = append(names, string(r.Fields[i].Name))
		}
		//delete group by columns in Values
		for i := 0; i < len(r.Values); i++ {
			r.Values[i] = r.Values[i][:groupByIndexs[0]]
		}
		r.Resultset, err = cc.buildResultset(r.Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r.Resultset = cc.newEmptyResultset(stmt)
	}

	return r, nil
}

//only merge result with aggregate function in group by opt
func (cc *ClientConn) mergeGroupByWithFunc(rs []*mysql.Result, groupByIndexs []int,
	funcExprs map[int]string) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map, in order to make group
	resultMap, err := cc.loadResultWithFuncIntoMap(rs, groupByIndexs, funcExprs)
	if err != nil {
		return nil, err
	}

	//set status
	status := cc.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//change map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

//only merge result without aggregate function in group by opt
func (cc *ClientConn) mergeGroupByWithoutFunc(rs []*mysql.Result,
	groupByIndexs []int) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map
	resultMap, err := cc.loadResultIntoMap(rs, groupByIndexs)
	if err != nil {
		return nil, err
	}

	//set status
	status := cc.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//load map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

type ResultRow struct {
	Value   []interface{}
	RowData mysql.RowData
}

func (cc *ClientConn) generateMapKey(groupColumns []interface{}) (string, error) {
	bk := make([]byte, 0, 8)
	separatorBuf, err := formatValue("+")
	if err != nil {
		return "", err
	}

	for _, v := range groupColumns {
		b, err := formatValue(v)
		if err != nil {
			return "", err
		}
		bk = append(bk, b...)
		bk = append(bk, separatorBuf...)
	}

	return string(bk), nil
}

func (cc *ClientConn) loadResultIntoMap(rs []*mysql.Result,
	groupByIndexs []int) (map[string]*ResultRow, error) {
	//load Result into map
	resultMap := make(map[string]*ResultRow)
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := cc.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			resultMap[mk] = &ResultRow{
				Value:   r.Values[i],
				RowData: r.RowDatas[i],
			}
		}
	}

	return resultMap, nil
}

func (cc *ClientConn) loadResultWithFuncIntoMap(rs []*mysql.Result,
	groupByIndexs []int, funcExprs map[int]string) (map[string]*ResultRow, error) {

	resultMap := make(map[string]*ResultRow)
	rt := new(mysql.Result)
	rt.Resultset = new(mysql.Resultset)
	rt.Fields = rs[0].Fields

	//change Result into map
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := cc.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			if v, ok := resultMap[mk]; ok {
				//init rt
				rt.Values = nil
				rt.RowDatas = nil

				//append v and r into rt, and calculate the function value
				rt.Values = append(rt.Values, r.Values[i], v.Value)
				rt.RowDatas = append(rt.RowDatas, r.RowDatas[i], v.RowData)
				resultTmp := []*mysql.Result{rt}

				for funcIndex, funcName := range funcExprs {
					funcValue, err := cc.calFuncExprValue(funcName, resultTmp, funcIndex)
					if err != nil {
						return nil, err
					}
					//set the function value in group by
					resultMap[mk].Value[funcIndex] = funcValue
				}
			} else { //key is not exist
				resultMap[mk] = &ResultRow{
					Value:   r.Values[i],
					RowData: r.RowDatas[i],
				}
			}
		}
	}

	return resultMap, nil
}

//build select result without group by opt
func (cc *ClientConn) buildSelectOnlyResult(rs []*mysql.Result,
	stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	r := rs[0].Resultset
	status := cc.status | rs[0].Status

	funcExprs := cc.getFuncExprs(stmt)
	if len(funcExprs) == 0 {
		for i := 1; i < len(rs); i++ {
			status |= rs[i].Status
			for j := range rs[i].Values {
				r.Values = append(r.Values, rs[i].Values[j])
				r.RowDatas = append(r.RowDatas, rs[i].RowDatas[j])
			}
		}
	} else {
		//result only one row, status doesn't need set
		r, err = cc.buildFuncExprResult(stmt, rs, funcExprs)
		if err != nil {
			return nil, err
		}
	}
	return &mysql.Result{
		Status:    status,
		Resultset: r,
	}, nil
}

func (cc *ClientConn) sortSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.OrderBy == nil {
		return nil
	}

	sk := make([]mysql.SortKey, len(stmt.OrderBy))

	for i, o := range stmt.OrderBy {
		sk[i].Name = nstring(o.Expr)
		sk[i].Direction = o.Direction
	}

	return r.Sort(sk)
}

func (cc *ClientConn) limitSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.Limit == nil {
		return nil
	}

	var offset, count int64
	var err error
	if stmt.Limit.Offset == nil {
		offset = 0
	} else {
		if o, ok := stmt.Limit.Offset.(sqlparser.NumVal); !ok {
			return fmt.Errorf("invalid select limit %s", nstring(stmt.Limit))
		} else {
			if offset, err = strconv.ParseInt(hack.String([]byte(o)), 10, 64); err != nil {
				return err
			}
		}
	}

	if o, ok := stmt.Limit.Rowcount.(sqlparser.NumVal); !ok {
		return fmt.Errorf("invalid limit %s", nstring(stmt.Limit))
	} else {
		if count, err = strconv.ParseInt(hack.String([]byte(o)), 10, 64); err != nil {
			return err
		} else if count < 0 {
			return fmt.Errorf("invalid limit %s", nstring(stmt.Limit))
		}
	}
	if offset > int64(len(r.Values)) {
		r.Values = nil
		r.RowDatas = nil
		return nil
	}

	if offset+count > int64(len(r.Values)) {
		count = int64(len(r.Values)) - offset
	}

	r.Values = r.Values[offset : offset+count]
	r.RowDatas = r.RowDatas[offset : offset+count]

	return nil
}

func (cc *ClientConn) buildFuncExprResult(stmt *sqlparser.Select,
	rs []*mysql.Result, funcExprs map[int]string) (*mysql.Resultset, error) {

	var names []string
	var err error
	r := rs[0].Resultset
	funcExprValues := make(map[int]interface{})

	for index, funcName := range funcExprs {
		funcExprValue, err := cc.calFuncExprValue(
			funcName,
			rs,
			index,
		)
		if err != nil {
			return nil, err
		}
		funcExprValues[index] = funcExprValue
	}

	r.Values, err = cc.buildFuncExprValues(rs, funcExprValues)

	if 0 < len(r.Values) {
		for _, field := range rs[0].Fields {
			names = append(names, string(field.Name))
		}
		r, err = cc.buildResultset(rs[0].Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r = cc.newEmptyResultset(stmt)
	}

	return r, nil
}

//get the index of funcExpr, the value is function name
func (cc *ClientConn) getFuncExprs(stmt *sqlparser.Select) map[int]string {
	var f *sqlparser.FuncExpr
	funcExprs := make(map[int]string)

	for i, expr := range stmt.SelectExprs {
		nonStarExpr, ok := expr.(*sqlparser.NonStarExpr)
		if !ok {
			continue
		}

		f, ok = nonStarExpr.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		} else {
			f = nonStarExpr.Expr.(*sqlparser.FuncExpr)
			funcName := strings.ToLower(string(f.Name))
			switch funcNameMap[funcName] {
			case FUNC_EXIST:
				funcExprs[i] = funcName
			}
		}
	}
	return funcExprs
}

func (cc *ClientConn) getSumFuncExprValue(rs []*mysql.Result,
	index int) (interface{}, error) {
	var sumf float64
	var sumi int64
	var IsInt bool
	var err error
	var result interface{}

	for _, r := range rs {
		for k := range r.Values {
			result, err = r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}

			switch v := result.(type) {
			case int:
				sumi = sumi + int64(v)
				IsInt = true
			case int32:
				sumi = sumi + int64(v)
				IsInt = true
			case int64:
				sumi = sumi + v
				IsInt = true
			case float32:
				sumf = sumf + float64(v)
			case float64:
				sumf = sumf + v
			case []byte:
				tmp, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return nil, err
				}

				sumf = sumf + tmp
			default:
				return nil, errors.ErrSumColumnType
			}
		}
	}
	if IsInt {
		return sumi, nil
	} else {
		return sumf, nil
	}
}

func (cc *ClientConn) getMaxFuncExprValue(rs []*mysql.Result,
	index int) (interface{}, error) {
	var max interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					max = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if max.(int64) < result.(int64) {
					max = result
				}
			case uint64:
				if max.(uint64) < result.(uint64) {
					max = result
				}
			case float64:
				if max.(float64) < result.(float64) {
					max = result
				}
			case string:
				if max.(string) < result.(string) {
					max = result
				}
			}
		}
	}
	return max, nil
}

func (cc *ClientConn) getMinFuncExprValue(
	rs []*mysql.Result, index int) (interface{}, error) {
	var min interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					min = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if min.(int64) > result.(int64) {
					min = result
				}
			case uint64:
				if min.(uint64) > result.(uint64) {
					min = result
				}
			case float64:
				if min.(float64) > result.(float64) {
					min = result
				}
			case string:
				if min.(string) > result.(string) {
					min = result
				}
			}
		}
	}
	return min, nil
}

//calculate the the value funcExpr(sum or count)
func (cc *ClientConn) calFuncExprValue(funcName string,
	rs []*mysql.Result, index int) (interface{}, error) {

	var num int64
	switch strings.ToLower(funcName) {
	case CountFunc:
		if len(rs) == 0 {
			return nil, nil
		}
		for _, r := range rs {
			if r != nil {
				for k := range r.Values {
					result, err := r.GetInt(k, index)
					if err != nil {
						return nil, err
					}
					num += result
				}
			}
		}
		return num, nil
	case SumFunc:
		return cc.getSumFuncExprValue(rs, index)
	case MaxFunc:
		return cc.getMaxFuncExprValue(rs, index)
	case MinFunc:
		return cc.getMinFuncExprValue(rs, index)
	case LastInsertIdFunc:
		return cc.lastInsertId, nil
	default:
		if len(rs) == 0 {
			return nil, nil
		}
		//get a non-null value of funcExpr
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					return result, nil
				}
			}
		}
	}

	return nil, nil
}

//build values of resultset,only build one row
func (cc *ClientConn) buildFuncExprValues(rs []*mysql.Result,
	funcExprValues map[int]interface{}) ([][]interface{}, error) {
	values := make([][]interface{}, 0, 1)
	//build a row in one result
	for i := range rs {
		for j := range rs[i].Values {
			for k := range funcExprValues {
				rs[i].Values[j][k] = funcExprValues[k]
			}
			values = append(values, rs[i].Values[j])
			if len(values) == 1 {
				break
			}
		}
		break
	}

	//generate one row just for sum or count
	if len(values) == 0 {
		value := make([]interface{}, len(rs[0].Fields))
		for k := range funcExprValues {
			value[k] = funcExprValues[k]
		}
		values = append(values, value)
	}

	return values, nil
}
