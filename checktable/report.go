package main

import (
	"fmt"
	"strings"

	"github.com/forest11/checktable/config"
)

//目标库获取数据
func getData(list []string, db *TableInfo) {
	listChunk, rem := len(list)/100, len(list)%100
	if listChunk > 0 {
		for i := 0; i < listChunk; i++ {
			s := strings.Join(list[i*100:(i+1)*100], ",")
			sqlStr := fmt.Sprintf("%s -u%s -p%s -h%s -P%s --single-transaction --compact --set-gtid-purged=OFF -t -B %s --tables %s --where='%s in (%s)'", config.AppConf.MysqlDump, config.AppConf.SourceDB.User, config.AppConf.SourceDB.Pwd, config.AppConf.SourceDB.Addr, config.AppConf.SourceDB.Port, db.dbName, db.tableName, db.pkName, s)

			ret, err := execShell("sh", "-c", sqlStr)
			if err != nil {
				panic(fmt.Sprintf("mysqldump args is err: %v", err))
			}
			r := strings.Replace(ret, "INSERT", "REPLACE", -1)
			writeFile(config.AppConf.DumpFile, r)
		}
	}
	if rem != 0 {
		s := strings.Join(list[listChunk*100:listChunk*100+rem], ",")
		sqlStr := fmt.Sprintf("%s -u%s -p%s -h%s -P%s --single-transaction --compact --set-gtid-purged=OFF -t -B %s --tables %s --where='%s in (%s)'", config.AppConf.MysqlDump, config.AppConf.SourceDB.User, config.AppConf.SourceDB.Pwd, config.AppConf.SourceDB.Addr, config.AppConf.SourceDB.Port, db.dbName, db.tableName, db.pkName, s)

		ret, err := execShell("sh", "-c", sqlStr)
		if err != nil {
			panic(fmt.Sprintf("mysqldump args is err: %v", err))
		}
		r := strings.Replace(ret, "INSERT", "REPLACE", -1)
		writeFile(config.AppConf.DumpFile, r)
	}
}

//生成sql语句
func createSQL(sDb, dDb *TableInfo) error {
	if len(deleteList.pk) > 0 {
		if config.AppConf.Dump {
			for _, v := range deleteList.pk {
				delSQL := fmt.Sprintf("delete from %s where %s=%v;\n", dDb.tableName, dDb.pkName, v)
				writeFile(config.AppConf.DumpFile, delSQL)
			}
		} else {
			return fmt.Errorf("source table(%s) has no data: list: %v", sDb.tableName, deleteList.pk)
		}
	}

	if len(insertList.pk) > 0 {
		if config.AppConf.Dump && len(insertList.pk) < 10000 {
			getData(insertList.pk, sDb)
		} else {
			return fmt.Errorf("dest table(%s) has no data: list: %v", dDb.tableName, insertList.pk)
		}
	}

	if len(updateList.pk) > 0 {
		if config.AppConf.Dump && len(updateList.pk) < 10000 {
			getData(updateList.pk, sDb)
		} else {
			return fmt.Errorf("table(%s) field data is diff: list: %v", dDb.tableName, updateList.pk)
		}
	}

	return nil
}
