package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/forest11/checktable/dbutil"
	"github.com/astaxie/beego/logs"
)

//chunkInfo 对比chunk数据
type chunkInfo struct {
	pkStart int
	pkEnd   int
}

func newChunkInfo(start, end int) chunkInfo {
	return chunkInfo{
		pkStart: start,
		pkEnd:   end,
	}
}

//GetChunkCount 获取chunk数
func (t *TableInfo) GetChunkCount() (int, error) {
	rowCount, err := t.GetRowCount()
	if err != nil {
		return 0, err
	}
	num := (rowCount + chunkSize - 1) / chunkSize //取整
	return num, nil
}


// CheckDBIsTIDB 判断是否是tidb
func (t *TableInfo) CheckDBIsTidb() bool {
	isTidb, _ := dbutil.IsTiDB(t.db)
	return isTidb
}


//GetCrc32CheckSum 对数据使用CRC32计算
func (t *TableInfo) GetCrc32CheckSum(where string) (string, error) {
	/*
		select * from t2;
		+----+------+
		| id | name |
		+----+------+
		|  1 | xxx  |
		+----+------+

		tidb> SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', id,name)) AS UNSIGNED)), 10, 16)), 0) AS checksum FROM `test`.`t2` where id >= 1 and id <= 200000;
		+----------+
		| checksum |
		+----------+
		| f2df7701 |
		+----------+
		SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', id,name)) AS UNSIGNED)), 10, 16)), 0) AS checksum FROM `test`.`t2` where id >= 1 and id <= 200000;
		+----------+
		| checksum |
		+----------+
		| 39792cb  |
		+----------+
	*/

	colsStr, err := dbutil.GetTableFieldStr(t.db, t.dbName, t.tableName, t.filter)
	if err != nil {
		return "", err
	}

	if t.where != "" && isAutoIncPk == false {
		where = fmt.Sprintf("%s AND %s", t.where, where)
	}

	var query string
	query = fmt.Sprintf("SELECT COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#',%s)) AS UNSIGNED)), 10, 16)), 0) AS checksum FROM `%s`.`%s` WHERE %s",
						 colsStr, t.dbName, t.tableName, where)
	logs.Debug("CRC32 query: %v", query)

	var checksum sql.NullString
	err = t.db.QueryRow(query).Scan(&checksum)
	if err != nil {
		return "", err
	}

	if !checksum.Valid {
		return "", nil
	}
	return checksum.String, nil
}

//GetMd5CheckSum 对数据使用Md5计算
func (t *TableInfo) GetMd5CheckSum(where string) (string, error) {
	/* 
	SELECT COALESCE(LOWER(CONCAT(LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc, 1, 16), 16, 10) AS UNSIGNED))
	, 10, 16), 16, '0'), LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(@crc := md5(CONCAT_WS('#', id,CONVERT(title using utf8mb4),
	created_date, CONCAT(ISNULL(id),ISNULL(title),ISNULL(created_date)))), 17, 16), 16, 10) AS UNSIGNED)), 
	10, 16), 16, '0'))),0) AS checksum FROM `test`.`t2`;
	+----------------------------------+
	| checksum                         |
	+----------------------------------+
	| 55c5c6144eb1f07b47da59d6901f6c33 |
	+----------------------------------+
	*/
	colsStr, err := dbutil.GetTableFieldAndType(t.db, t.dbName, t.tableName, t.filter)
	if err != nil {
		return "", err
	}
	crc := dbutil.FormatCrc(colsStr)

	if t.where != "" && isAutoIncPk == false {
		where = fmt.Sprintf("%s AND %s", t.where, where)
	}

	var query string
	query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s", crc, t.dbName, t.tableName, where)
	logs.Debug("Md5 query: %v", query)

	var checksum sql.NullString
	err = t.db.QueryRow(query).Scan(&checksum)
	if err != nil {
		return "", err
	}

	if !checksum.Valid {
		return "", nil
	}
	return checksum.String, nil
}

//goDiffChunk 多线程执行任务
func goDiffChunk(stbInfo, dtbInfo *TableInfo, chunkChan chan chunkInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	for chunk := range chunkChan {
		DiffRowData(stbInfo, dtbInfo, chunk)
	}
}

// 非自增主键表分割chunk
func splitTableToChunkForRandomPk(stb, dtb *TableInfo, start, end int) (*[]chunkInfo, error) {
	var chunks []chunkInfo
	offset := start
	for offset < end {
		sNextPost, err := dbutil.GetOffsetPk(stb.db, stb.dbName, stb.tableName, stb.pkName, offset, chunkSize)
		if err != nil {
			return nil, err
		}
		dNextPost, err := dbutil.GetOffsetPk(dtb.db, dtb.dbName, dtb.tableName, dtb.pkName, offset, chunkSize)
		if err != nil {
			return nil, err
		}

		nextPost := getMax(sNextPost, dNextPost)
		n := newChunkInfo(offset, nextPost)
		chunks = append(chunks, n)
		offset = nextPost
	}
	if offset > end {
		chunks = append(chunks, newChunkInfo(end, offset))
	}
	logs.Debug("chunks: %#v", chunks)
	return &chunks, nil
}

// 自增主键分割表
func splitTableToChunkForAutoPk(stb, dtb *TableInfo, start, end int) (*[]chunkInfo, error) {
	var chunks []chunkInfo
	for offset := start; offset < end; offset += chunkSize {
		n := newChunkInfo(offset, offset+chunkSize)
		chunks = append(chunks, n)
	}
	logs.Debug("chunks: %#v", chunks)
	return &chunks, nil
}

func splitTableToChunk(stb, dtb *TableInfo, start, end int) (chunks *[]chunkInfo, err error) {
	if isAutoIncPk {
		chunks, err = splitTableToChunkForAutoPk(stb, dtb, start, end)
	} else {
		chunks, err = splitTableToChunkForRandomPk(stb, dtb, start, end)
	}
	return
}

//DiffChunk 对比chunk
func diffChunk(ctx context.Context, stbInfo, dtbInfo *TableInfo, min, max int) {
	chunkList, err := splitTableToChunk(stbInfo, dtbInfo, min, max)
	if err != nil {
		logs.Error("chunkList err: %v", err)
	}

	chunkChan := make(chan chunkInfo, threads)

	wg := new(sync.WaitGroup)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go goDiffChunk(stbInfo, dtbInfo, chunkChan, wg)
	}

	for _, chunk := range *chunkList {
		select {
			case <-ctx.Done():
				logs.Info("exit...")
				return
			default:
		}

		where := fmt.Sprintf("%s >= %d and %s <= %d", stbInfo.pkName, chunk.pkStart, stbInfo.pkName, chunk.pkEnd)

		var sCheckSum, dCheckSum string
		if stbInfo.CheckDBIsTidb() || dtbInfo.CheckDBIsTidb() {  // tidb使用crc 32
			sCheckSum, err = stbInfo.GetCrc32CheckSum(where)
			if err != nil {
				logs.Error("sCheckSum err: %v", err)
			}
			dCheckSum, err = dtbInfo.GetCrc32CheckSum(where)
			if err != nil {
				logs.Error("dCheckSum err: %v", err)
			}
		} else {
			sCheckSum, err = stbInfo.GetMd5CheckSum(where)
			if err != nil {
				logs.Error("sCheckSum err: %v", err)
			}
			dCheckSum, err = dtbInfo.GetMd5CheckSum(where)
			if err != nil {
				logs.Error("dCheckSum err: %v", err)
			}
		}

		if sCheckSum != dCheckSum {
			chunkChan <- chunk
			logs.Error("sCheckSum: %s dCheckSum: %s", sCheckSum, dCheckSum)
		}
	}
	close(chunkChan)
	wg.Wait()
}
