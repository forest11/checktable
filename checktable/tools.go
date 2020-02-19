package main

import (
	"bufio"
	"os"
	"os/exec"
)

//调用操纵系统命令
func execShell(name string, arg ...string) (ret string, err error) {
	cmd := exec.Command(name, arg...)
	out, err := cmd.Output()
	ret = string(out)
	return
}

//写文件
func writeFile(fileName, inStr string) error {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWrite := bufio.NewWriter(file)

	if _, err = bufWrite.WriteString(inStr); err != nil {
		return err
	}
	if err = bufWrite.Flush(); err != nil {
		return err
	}
	return nil
}

//返回最大值
func getMax(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

//返回最小值
func getMin(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

// reflect.DeepEqual(c1, c2) 对比struct,map

//diffMap 获取2个map不同的数据
func diffMap(s, d map[string]string) (sNoKey, dNoKey, diffValueKey []string) {
	for sk, sv := range s {
		dv, ok := d[sk]
		if ok {
			if sv != dv {
				diffValueKey = append(diffValueKey, sk)
			}
		} else {
			dNoKey = append(dNoKey, sk)
		}
	}

	for dk := range d {
		_, ok := s[dk]
		if !ok {
			sNoKey = append(sNoKey, dk)
		}
	}
	return
}
