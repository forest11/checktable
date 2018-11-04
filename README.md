## 由于年初公司需要使用tidb从阿里云的rds，准实时同步数据，根据公司情况(所有表都有主键)写了一个数据校验程序，经过几次修改，目前用于rds和tidb的表级别数据校验
## 需要在执行程序的同级目录下创建一个logs, sql目录，输出日志和生成sql语句

### 编译
go build github.com/forest11/checktable/checktable

### 后台运行
```
./checktable -f conf/checksum.conf &
```
