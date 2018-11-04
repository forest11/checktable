package log

import (
	"encoding/json"

	"github.com/astaxie/beego/logs"
)

func getLevel(level string) int {
	switch level {
	case "debug":
		return logs.LevelDebug
	case "info":
		return logs.LevelInformational
	case "warn":
		return logs.LevelWarning
	case "error":
		return logs.LevelError
	default:
		return logs.LevelDebug
	}
}

// InitLog 初始化日志配置
func InitLog(logPath string, logLevel string) error {
	logConfig := make(map[string]interface{})

	logConfig["filename"] = logPath
	logConfig["level"] = getLevel(logLevel)

	logConfigStr, err := json.Marshal(logConfig)
	if err != nil {
		return err
	}
	logs.SetLogger(logs.AdapterFile, string(logConfigStr))
	return nil
}
