// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
)

type AliRDS struct {
	storage *OnlineDDLStorage
}

// NewAliRDS returns Aliyun RDS online plugin
func NewAliRDS(tctx *tcontext.Context, cfg *config.SubTaskConfig) (OnlinePlugin, error) {
	r := &AliRDS{
		storage: NewOnlineDDLStorage(tcontext.Background().WithLogger(tctx.L().WithFields(zap.String("online ddl", "alirds"))), cfg), // create a context for logger
	}

	return r, r.storage.Init(tctx)
}

func (r *AliRDS) Apply(tctx *tcontext.Context, tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, string, string, error) {
	if len(tables) < 1 {
		return nil, "", "", terror.ErrSyncerUnitAliRDSApplyEmptyTable.Generate()
	}

	schema, table := tables[0].Schema, tables[0].Name
	targetSchema, targetTable := r.RealName(schema, table)
	tp := r.TableType(table)

	switch tp {
	case realTable:
		switch stmt.(type) {
		case *ast.RenameTableStmt:
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameTableNotValid.Generate()
			}

			tp1 := r.TableType(tables[1].Name)
			if tp1 == trashTable {
				return nil, "", "", nil
			} else if tp1 == ghostTable {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameToGhostTable.Generate(statement)
			}
		}
		return []string{statement}, schema, table, nil

	case trashTable:
		switch stmt.(type) {
		case *ast.RenameTableStmt:
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameTableNotValid.Generate()
			}

			tp1 := r.TableType(tables[1].Name)
			if tp1 == ghostTable {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameGhostTblToOther.Generate(statement)
			}
		}

	case ghostTable:
		switch stmt.(type) {
		case *ast.CreateTableStmt:
			err := r.storage.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}
		case *ast.DropTableStmt:
			err := r.storage.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}
		case *ast.RenameTableStmt:
			if len(tables) != parserpkg.SingleRenameTableNameNum {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameTableNotValid.Generate()
			}

			tp1 := r.TableType(tables[1].Name)
			if tp1 == realTable {
				rdsInfo := r.storage.Get(schema, table)
				if rdsInfo != nil {
					return rdsInfo.DDLs, tables[1].Schema, tables[1].Name, nil
				}
				return nil, "", "", terror.ErrSyncerUnitAliRDSOnlineDDLOnGhostTbl.Generate(schema, table)
			} else if tp1 == ghostTable {
				return nil, "", "", terror.ErrSyncerUnitAliRDSRenameGhostTblToOther.Generate(statement)
			}

			err := r.storage.Delete(tctx, schema, table)
			if err != nil {
				return nil, "", "", err
			}
		}

	default:
		err := r.storage.Save(tctx, schema, table, targetSchema, targetTable, statement)
		if err != nil {
			return nil, "", "", err
		}
	}

	return nil, schema, table, nil
}

func (r *AliRDS) Finish(tctx *tcontext.Context, schema, table string) error {
	if r == nil {
		return nil
	}

	return r.storage.Delete(tctx, schema, table)
}

func (r *AliRDS) TableType(table string) TableType {
	if len(table) > 8 && strings.HasPrefix(table, "tp_") {
		if strings.Contains(table, "_ogt_") {
			return ghostTable
		}

		if strings.Contains(table, "_ogl_") || strings.Contains(table, "_del_") {
			return trashTable
		}
	}

	return realTable
}

func (r *AliRDS) RealName(schema, table string) (string, string) {
	tp := r.TableType(table)
	idx := -1

	if tp == ghostTable {
		idx = strings.Index(table, "_ogt_")
	} else if tp == trashTable {
		idx = strings.Index(table, "_ogl_")
		if idx == -1 {
			idx = strings.Index(table, "_del_")
		}
	}

	if idx > 0 {
		table = table[idx+5:]
	}
	return schema, table
}

func (r *AliRDS) ResetConn(tctx *tcontext.Context) error {
	return r.storage.ResetConn(tctx)
}

func (r *AliRDS) Clear(tctx *tcontext.Context) error {
	return r.storage.Clear(tctx)
}

func (r *AliRDS) Close() {
	r.storage.Close()
}
