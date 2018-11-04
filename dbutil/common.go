package dbutil

import (
	"database/sql"
	"fmt"
	"strings"
)

//InitDB 初始化数据库
func InitDB(addr, port, user, pwd, dbname string) (*sql.DB, error) {
	dbDsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=5s&parseTime=true&loc=Local", user, pwd, addr, port, dbname)
	dbConn, err := sql.Open("mysql", dbDsn)
	if err != nil {
		return nil, err
	}
	dbConn.SetMaxOpenConns(200)
	dbConn.SetMaxIdleConns(50)
	err = dbConn.Ping()
	return dbConn, err
}

//ScanRowToInterfaces 返回interface类型数据
func ScanRowToInterfaces(rows *sql.Rows) ([]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range vals {
		scans[i] = &vals[i]
	}

	rowSlice := make([]interface{}, 0)
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			rowSlice = append(rowSlice, v)
		}
	}
	return rowSlice, nil
}

//ScanRowToMap 返回map[string][]byte, map[string]bool
func ScanRowToMap(rows *sql.Rows) (map[string][]byte, map[string]bool, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	vals := make([][]byte, len(cols)) //val是每个列的值，获取到byte类型
	scans := make([]interface{}, len(cols))
	for i := range vals { //每一行数据都填充到[][]byte里面
		scans[i] = &vals[i]
	}

	result := make(map[string][]byte)
	null := make(map[string]bool)
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, nil, err
		}

		for i := range vals {
			result[cols[i]] = vals[i]
			null[cols[i]] = (vals[i] == nil)
		}
	}
	return result, null, nil
}

//ScanRowToMapStr 返回map[string]string
func ScanRowToMapStr(rows *sql.Rows) (map[string]string, error) {
	/*
		1 1#xxx  => map[1]=1#xx
		2 2#bbb  => map[2]=2#bbb
	*/
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vals := make([][]byte, len(cols)) //val是每个列的值，获取到byte类型
	scans := make([]interface{}, len(cols))
	for i := range vals { //每一行数据都填充到[][]byte里面
		scans[i] = &vals[i]
	}

	m := make(map[string]string)
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return nil, err
		}

		if vals[1] == nil {
			m[string(vals[0])] = "null"
		} else {
			m[string(vals[0])] = string(vals[1])
		}
	}
	return m, nil
}

//GetDBVersion 获db的版本
func GetDBVersion(db *sql.DB) (string, error) {
	query := "select version()"
	var version sql.NullString
	err := db.QueryRow(query).Scan(&version)
	if err != nil {
		return "", err
	}

	return version.String, nil
}

//IsTiDB 判断是否为tidb
func IsTiDB(db *sql.DB) (bool, error) {
	version, err := GetDBVersion(db)

	if err != nil {
		return false, err
	}

	return strings.Contains(strings.ToLower(version), "tidb"), nil
}

//GetCreateTableSQL 获取创建表语句
func GetCreateTableSQL(db *sql.DB, dbName, tableName string) (string, error) {
	/*
		mysql> show create table `test`.`t1`;
		+-------+------------------------------------------------------------------------------------------------------+
		| Table | Create Table                                                                                         |
		+-------+------------------------------------------------------------------------------------------------------+
		| t1    | CREATE TABLE `t1` (
		`id` int(2) DEFAULT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
		+-------+------------------------------------------------------------------------------------------------------+
	*/
	query := fmt.Sprintf("show create table `%s`.`%s`", dbName, tableName)
	var tName, createTable sql.NullString
	err := db.QueryRow(query).Scan(&tName, &createTable)
	if err != nil {
		return "", err
	}

	if !tName.Valid || !createTable.Valid {
		return "", fmt.Errorf("no fond table %s", tableName)
	}

	return createTable.String, nil
}
