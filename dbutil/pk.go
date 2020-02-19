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

//GetTableFieldStr 以“,”拼接返回数据表中的数据
func GetTableFieldStr(db *sql.DB, dbName, tableName, filter string) (string, error) {
	/* filter_filed="t1,t2" ==> return "t1","t2" */

	if filter != "" {
		return filter, nil
	}

	query := fmt.Sprintf("select COLUMN_NAME from `information_schema`.`COLUMNS` where table_schema = \"%s\" and table_name = \"%s\"", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	resultList, err := ScanRowToInterfaces(rows)
	if err != nil {
		return "", err
	}

	var filedList []string
	for _, v := range resultList {
		t := v.([]byte)
		filedList = append(filedList, string(t))
	}

	return strings.Join(filedList, ","), nil
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


// GetTableFieldAndType 返回数据和类型
func GetTableFieldAndType(db *sql.DB, dbName, tableName, filter string) ([]string, error) {
	/* 
	+--------------+-----------+
	| COLUMN_NAME  | DATA_TYPE |
	+--------------+-----------+
	| id           | int       |
	| title        | varchar   |
	| created_date | date      |
	+--------------+-----------+
	return [id#int title#varchar created_date#date]

	filter_filed="t1,t2" ==> return ["t1#int t2#varchar] 
	*/
	query := fmt.Sprintf("select COLUMN_NAME,DATA_TYPE from `information_schema`.`COLUMNS` where table_schema = \"%s\" and table_name = \"%s\"", dbName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	resultMap, err := ScanRowToMapStr(rows)
	if err != nil {
		return nil, err
	}

	var filedList []string
	if filter != "" {
		filterList := strings.Split(filter, ",")
		for _, f := range filterList {
			filedList = append(filedList, fmt.Sprintf("%s#%s", f, resultMap[f]))
		}
	} else {
		for k, v := range resultMap {
			filedList = append(filedList, fmt.Sprintf("%s#%s", k, v))
		}
	}

	return filedList, nil
}


// FormatCrc 格式化成crc字符串
func FormatCrc(filedList []string) string {
	var concatWs []string
	var concatIsnull []string

	var strType = []string{"char", "varchar"}
	var bigType = []string{"tinyblob", "tinytext", "blob", "text", "mediumblob", "mediumtext", "longblob", "longtext"}
	for _, filed := range filedList {
		cln := strings.Split(filed, "#")
		concatIsnull = append(concatIsnull, fmt.Sprintf("ISNULL(%s)", cln[0]))
		if stringInSlice(cln[1], strType) {  // 如果是char、varchar类型，转换为utf8mb4
			concatWs = append(concatWs, fmt.Sprintf("CONVERT(%s using utf8mb4)", cln[0]))
		} else if stringInSlice(cln[1], bigType) {  // 如果是blob、text类型，不对内容校验，使用crc32校验
			concatWs = append(concatWs, fmt.Sprintf("CRC32(%s)", cln[0]))
		} else {
			concatWs = append(concatWs, cln[0])
		}
	}
	// 生成crc的核心校验语句
	f := fmt.Sprintf("COALESCE(LOWER(CONCAT(LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc, 1, 16), 16, 10) AS UNSIGNED)), " +
			"10, 16), 16, '0'), LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc := md5(CONCAT_WS('#', %s, CONCAT(%s))), " + 
			"17, 16), 16, 10) AS UNSIGNED)), 10, 16), 16, '0'))),0) AS checksum", strings.Join(concatWs, ","), strings.Join(concatIsnull, ","))
	return f
}

// stringInSlice 遍历数组
func stringInSlice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}