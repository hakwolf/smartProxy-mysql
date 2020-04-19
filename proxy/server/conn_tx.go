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
	"github.com/hakwolf/smartProxy-mysql/backend"
	"github.com/hakwolf/smartProxy-mysql/mysql"
)

func (cc *ClientConn) isInTransaction() bool {
	return cc.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!cc.isAutoCommit()
}

func (cc *ClientConn) isAutoCommit() bool {
	return cc.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (cc *ClientConn) handleBegin() error {
	for _, co := range cc.txConns {
		if err := co.Begin(); err != nil {
			return err
		}
	}
	cc.status |= mysql.SERVER_STATUS_IN_TRANS
	return cc.writeOK(nil)
}

func (cc *ClientConn) handleCommit() (err error) {
	if err := cc.commit(); err != nil {
		return err
	} else {
		return cc.writeOK(nil)
	}
}

func (cc *ClientConn) handleRollback() (err error) {
	if err := cc.rollback(); err != nil {
		return err
	} else {
		return cc.writeOK(nil)
	}
}

func (cc *ClientConn) commit() (err error) {
	cc.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range cc.txConns {
		if e := co.Commit(); e != nil {
			err = e
		}
		co.Close()
	}

	cc.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}

func (cc *ClientConn) rollback() (err error) {
	cc.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range cc.txConns {
		if e := co.Rollback(); e != nil {
			err = e
		}
		co.Close()
	}

	cc.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}
