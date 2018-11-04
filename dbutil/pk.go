package dbutil

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

//GetPKName 获取主键字段名
func GetPKName(db *sql.DB, dbName, tableName string) (string, error) {
	/*
		mysql> show index from `test`.`t2` where key_name='PRIMARY';
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t2    |          0 | PRIMARY  |            1 | id          | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	query := fmt.Sprintf("show index from `%s`.`%s` where key_name='PRIMARY'", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}

	fileds, _, err := ScanRowToMap(rows)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s", fileds["Column_name"]), nil
}

//GetTableField 根据配置获取表过滤字段
func GetTableField(db *sql.DB, dbName, tableName, filter string) ([]string, error) {
	/*
		filter_filed="t1,t2" ==> [t1 t2]
	*/
	var filedList []string

	if filter != "" {
		filedList = strings.Split(filter, ",")
		return filedList, nil
	}
	query := fmt.Sprintf("select COLUMN_NAME from `information_schema`.`COLUMNS` where table_schema = \"%s\" and table_name = \"%s\"", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	resultList, err := ScanRowToInterfaces(rows)
	if err != nil {
		return nil, err
	}

	for _, v := range resultList {
		t := v.([]byte)
		filedList = append(filedList, string(t))
	}

	return filedList, nil
}

//GetTableFieldStr 以“,”拼接返回数据
func GetTableFieldStr(db *sql.DB, dbName, tableName, filter string) (string, error) {
	filedList, err := GetTableField(db, dbName, tableName, filter)
	if err != nil {
		return "", err
	}
	filedStr := strings.Join(filedList, ",")
	return filedStr, nil
}

//GetTableSchema 只获取表所有字段名称
func GetTableSchema(db *sql.DB, dbName, tableName, filter string) (string, error) {
	var fileds string
	query := fmt.Sprintf("select COLUMN_NAME from `information_schema`.`COLUMNS` where table_schema = \"%s\" and table_name = \"%s\"", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	resultList, err := ScanRowToInterfaces(rows)
	if err != nil {
		return "", err
	}

	for _, v := range resultList {
		t := v.([]byte)
		fileds += fmt.Sprintf("%s,", string(t))
	}

	return fileds, nil
}

//GetOffsetPk 根据偏移量获取主键
func GetOffsetPk(db *sql.DB, dbName, tableName, pkName string, start, offset int) (int, error) {
	var pk int

	query := fmt.Sprintf("select %s from `%s`.`%s` where %s >= %d order by %s limit %d", pkName, dbName, tableName, pkName, start, pkName, offset)
	rows, err := db.Query(query)
	if err != nil {
		return 0, err
	}

	rowMap, _, err := ScanRowToMap(rows)
	if err != nil {
		return 0, err
	}
	t, err := strconv.ParseInt(fmt.Sprintf("%s", rowMap[pkName]), 0, 64)
	if err != nil {
		return 0, err
	}
	pk = int(t)
	return pk, nil
}
