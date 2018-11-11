package main

import (
	"database/sql"
	"fmt"

	"github.com/astaxie/beego/logs"

	"github.com/forest11/checktable/dbutil"
)

//GetMinAndMaxPk 获取最大主键，最小主键
func (t *TableInfo) GetMinAndMaxPk() (int, int, error) {
	where := t.where
	if where == "" {
		where = "true"
	}
	query := fmt.Sprintf("select min(`%s`) as min, max(`%s`) as  max from `%s`.`%s` where %s", t.pkName, t.pkName, t.dbName, t.tableName, where)

	var min, max sql.NullInt64
	err := t.db.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return 0, 0, err
	}

	if !min.Valid {
		// min is NULL, means that no table data.
		return 0, 0, nil
	}
	return int(min.Int64), int(max.Int64), nil
}

//GetRowCount 获取行的总数
func (t *TableInfo) GetRowCount() (int, error) {
	/*
	   mysql> select count(*) as cnt from `test`.`t1`;
	   +------+
	   | cnt  |
	   +------+
	   |    2 |
	   +------+
	*/
	where := t.where
	if where == "" {
		where = "true"
	}
	query := fmt.Sprintf("select count(*) as cnt from `%s`.`%s` where %s", t.dbName, t.tableName, where)

	var cnt sql.NullInt64
	err := t.db.QueryRow(query).Scan(&cnt)
	if err != nil {
		return 0, err
	}

	if !cnt.Valid {
		return 0, fmt.Errorf("no fond table %s", t.tableName)
	}
	return int(cnt.Int64), nil
}

//GetRangeRowData 根据主键范围获取行数据
func (t *TableInfo) GetRangeRowData(pkStart, pkEnd int) (map[string]string, error) {
	fieldStr, err := dbutil.GetTableFieldStr(t.db, t.dbName, t.tableName, t.filter)
	if err != nil {
		return nil, err
	}

	where := fmt.Sprintf("%s >= %d and %s <= %d", t.pkName, pkStart, t.pkName, pkEnd)
	if t.where != "" && isAutoIncPk == false {
		where = fmt.Sprintf("%s and %s", t.where, where)
	}

	query := fmt.Sprintf("select %s,CONCAT_WS('#', %s) as cw from `%s`.`%s` where %s", t.pkName, fieldStr, t.dbName, t.tableName, where)
	rows, err := t.db.Query(query)
	if err != nil {
		return nil, err
	}

	queryList, err := dbutil.ScanRowToMapStr(rows)
	if err != nil {
		return nil, err
	}
	return queryList, nil
}

//DiffRowData 找出不同行数据
func DiffRowData(stbInfo, dtbInfo *TableInfo, chunk chunkInfo) error {
	s, err := stbInfo.GetRangeRowData(chunk.pkStart, chunk.pkEnd)
	if err != nil {
		return fmt.Errorf("sCheckSum GetRangeRowData err: %v", err)
	}
	logs.Debug("source row data: %v", s)

	d, err := dtbInfo.GetRangeRowData(chunk.pkStart, chunk.pkEnd)
	if err != nil {
		return fmt.Errorf("dCheckSum GetRangeRowData err: %v", err)
	}
	logs.Debug("dest row data: %v", d)

	sNoKey, dNoKey, diffValueKey := diffMap(s, d)
	if len(dNoKey) > 0 {
		insertList.rw.Lock()
		insertList.pk = append(insertList.pk, dNoKey...)
		insertList.rw.Unlock()
	}
	if len(sNoKey) > 0 {
		deleteList.rw.Lock()
		deleteList.pk = append(deleteList.pk, sNoKey...)
		deleteList.rw.Unlock()
	}
	if len(diffValueKey) > 0 {
		updateList.rw.Lock()
		updateList.pk = append(updateList.pk, diffValueKey...)
		updateList.rw.Unlock()
	}
	logs.Debug("insertList: %v\n, deleteList:%v, updateList:%v", insertList, deleteList, updateList)
	return nil
}
