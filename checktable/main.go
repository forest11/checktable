package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/go-sql-driver/mysql"

	"github.com/astaxie/beego/logs"
	"github.com/forest11/checktable/config"
	"github.com/forest11/checktable/dbutil"
	"github.com/forest11/checktable/log"
)

type pkList struct {
	pk []string
	rw sync.RWMutex
}

var (
	chunkSize   int
	threads     int
	isAutoIncPk bool
	insertList  pkList
	updateList  pkList
	deleteList  pkList
)

// NewpKList 初始化对象
func NewpKList() pkList {
	return pkList{
		pk: make([]string, 0, 1000),
	}
}

func main() {
	var confFile = flag.String("f", "checksum.conf", "checktable conf")
	flag.Parse()
	err := config.InitConfig(*confFile)
	if err != nil {
		panic(fmt.Sprintf("init config failed, err:%v", err))
	}

	chunkSize = config.AppConf.ChunkSize
	threads = config.AppConf.ThreadsNum
	isAutoIncPk = config.AppConf.PkAutoInc

	err = log.InitLog(config.AppConf.LogPath, config.AppConf.Level)
	if err != nil {
		panic(fmt.Sprintf("init logger failed, err:%v", err))
	}
	logs.Info("app config:%#v", config.AppConf)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// 捕获退出信息
		exitc := make(chan os.Signal, 1)
		signal.Notify(exitc, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
		<-exitc
		cancel()
	}()

	run(ctx)
}

func run(ctx context.Context) {
	insertList = NewpKList()
	updateList = NewpKList()
	deleteList = NewpKList()
	sTB := NewTableInfo(config.AppConf.SourceDB.DBName, config.AppConf.SourceDB.TableName, config.AppConf.FilterFiled, config.AppConf.WhereFiled)
	dTB := NewTableInfo(config.AppConf.DestDB.DBName, config.AppConf.DestDB.TableName, config.AppConf.FilterFiled, config.AppConf.WhereFiled)

	sConn, err := dbutil.InitDB(config.AppConf.SourceDB.Addr, config.AppConf.SourceDB.Port, config.AppConf.SourceDB.User, config.AppConf.SourceDB.Pwd, config.AppConf.SourceDB.DBName)
	if err != nil {
		panic(err)
	}
	sTB.db = sConn
	defer sTB.db.Close()

	dConn, err := dbutil.InitDB(config.AppConf.DestDB.Addr, config.AppConf.DestDB.Port, config.AppConf.DestDB.User, config.AppConf.DestDB.Pwd, config.AppConf.DestDB.DBName)
	if err != nil {
		panic(err)
	}
	dTB.db = dConn
	defer dTB.db.Close()

	schemaIsOk, err := DiffTableSchema(sTB, dTB)
	if err != nil || !schemaIsOk {
		panic(fmt.Errorf("%s.%s filed is diff, err: %s", sTB.dbName, sTB.tableName, err))
	}

	pk, err := dbutil.GetPKName(sTB.db, sTB.dbName, sTB.tableName)
	if err != nil {
		panic(err)
	}
	sTB.pkName = pk
	dTB.pkName = pk

	sChunkCount, err := sTB.GetChunkCount()
	if err != nil {
		logs.Error("%s.%s count chunk err:%v", sTB.dbName, sTB.tableName, err)
	}

	dChunkCount, err := dTB.GetChunkCount()
	if err != nil {
		logs.Error("%s.%s count chunk err:%v", sTB.dbName, sTB.tableName, err)
	}

	chunkCount := getMax(sChunkCount, dChunkCount)
	logs.Info("chunkCount: %d", chunkCount)

	if threads > chunkCount {
		threads = chunkCount
	}
	logs.Debug("start threads: %d", threads)

	sMinPk, sMaxPk, err := sTB.GetMinAndMaxPk()
	if err != nil {
		logs.Error("diff chunk err:%v", err)
	}

	dMinPk, dMaxPk, err := dTB.GetMinAndMaxPk()
	if err != nil {
		logs.Error("diff chunk err:%v", err)
	}
	min, max := getMin(sMinPk, dMinPk), getMax(sMaxPk, dMaxPk)
	logs.Debug("min: %d, max %d", min, max)

	diffChunk(ctx, sTB, dTB, min, max)

	logs.Info("start create SQL")
	err = createSQL(sTB, dTB)
	if err != nil {
		logs.Error("create SQL err:%v", err)
	}
}
