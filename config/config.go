package config

import (
	"fmt"

	"github.com/astaxie/beego/config"
)

//DBInfo dbinfo
type DBInfo struct {
	Addr      string
	Port      string
	TableName string
	User      string
	Pwd       string
	DBName    string
}

//AppConfig 配置文件
type AppConfig struct {
	ChunkSize  int
	ThreadsNum int
	PkAutoInc  bool

	FilterFiled string
	WhereFiled  string
	MysqlDump   string
	DumpFile    string
	Dump        bool

	Level   string
	LogPath string

	SourceDB DBInfo
	DestDB   DBInfo
}

var AppConf AppConfig

//InitConfig 初始化配置文件
func InitConfig(confPath string) error {
	appConfig, err := config.NewConfig("ini", confPath)
	if err != nil {
		return err
	}

	AppConf.ChunkSize = appConfig.DefaultInt("default::chunk_size", 500)
	AppConf.ThreadsNum = appConfig.DefaultInt("default::threads_num", 20)
	AppConf.PkAutoInc = appConfig.DefaultBool("default::pk_auto_inc", true)

	AppConf.Level = appConfig.DefaultString("log::level", "debug")
	AppConf.LogPath = appConfig.DefaultString("log::log_path", "./checktable.log")

	AppConf.Dump = appConfig.DefaultBool("dump::dump_sql", false)
	AppConf.MysqlDump = appConfig.DefaultString("dump::mysqldump", "/usr/local/mysql/bin/mysqldump")
	AppConf.DumpFile = appConfig.DefaultString("dump::dump_file", "./dump.sql")

	AppConf.FilterFiled = appConfig.DefaultString("filter::filter_filed", "")
	AppConf.WhereFiled = appConfig.DefaultString("filter::where", "")

	AppConf.SourceDB.Addr = appConfig.DefaultString("source::addr", "127.0.0.1")
	AppConf.SourceDB.Port = appConfig.DefaultString("source::port", "3306")
	AppConf.SourceDB.User = appConfig.DefaultString("source::user", "mysql")
	AppConf.SourceDB.Pwd = appConfig.DefaultString("source::password", "123")

	soureDb := appConfig.DefaultString("source::database", "")
	if soureDb == "" {
		return fmt.Errorf("source database is null")
	}
	AppConf.SourceDB.DBName = soureDb

	sourceTB := appConfig.DefaultString("source::table_name", "")
	if sourceTB == "" {
		return fmt.Errorf("source table name is null")
	}
	AppConf.SourceDB.TableName = sourceTB

	AppConf.DestDB.Addr = appConfig.DefaultString("destination::addr", "127.0.0.1")
	AppConf.DestDB.Port = appConfig.DefaultString("destination::port", "3306")
	AppConf.DestDB.User = appConfig.DefaultString("destination::user", "mysql")
	AppConf.DestDB.Pwd = appConfig.DefaultString("destination::password", "123")

	destDB := appConfig.DefaultString("destination::database", "")
	if destDB == "" {
		return fmt.Errorf("destination database is null")
	}
	AppConf.DestDB.DBName = destDB
	destTb := appConfig.DefaultString("destination::table_name", "")
	if destTb == "" {
		return fmt.Errorf("destination table name is null")
	}
	AppConf.DestDB.TableName = destTb

	return nil
}
