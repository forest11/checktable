package main

import (
	"database/sql"

	"github.com/forest11/checktable/dbutil"
)

//TableInfo 表元数据
type TableInfo struct {
	dbName    string
	tableName string
	pkName    string
	filter    string
	where     string
	db        *sql.DB
}

//NewTableInfo 创建对象
func NewTableInfo(dbName, tableName, filter, where string) *TableInfo {
	return &TableInfo{
		dbName:    dbName,
		tableName: tableName,
		filter:    filter,
		where:     where,
	}
}

//DiffTableSchema 对比表字段是否一致
func DiffTableSchema(stbInfo, dtbInfo *TableInfo) (bool, error) {
	sCols, err := dbutil.GetTableSchema(stbInfo.db, stbInfo.dbName, stbInfo.tableName, stbInfo.filter)
	if err != nil {
		return false, err
	}

	dCols, err := dbutil.GetTableSchema(dtbInfo.db, dtbInfo.dbName, dtbInfo.tableName, dtbInfo.filter)
	if err != nil {
		return false, err
	}

	if sCols == dCols {
		return true, nil
	}
	return false, nil
}
