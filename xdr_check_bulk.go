package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"strings"
	//	"crypto/sha256"
	"fmt"
	//	"io"
	"os"
)

type ReadHdfs struct {
	File      string
	Offet     int
	Size      int
	Signature string
	FileSize  int
}

func Obj(line string) string {
	begin := strings.Index(line, "{")
	return line[begin:]
}

func ObjParse(obj string) {
	var o ReadHdfs
	json.Unmarshal([]byte(obj), &o)

	fmt.Println(o)
}

func Scanner(file string) *bufio.Scanner {
	f, err := os.Open(file)
	if nil != err {
		fmt.Println(err)
	}

	return bufio.NewScanner(f)
}

func XdrCheckBulk(scanner *bufio.Scanner) {
	for scanner.Scan() {
		obj := Obj(scanner.Text())
		ObjParse(obj)
	}
}

func LogParameter() *string {
	log := flag.String("log", "", "log")
	flag.Parse()

	return log
}

func main() {
	log := LogParameter()
	scanner := Scanner(*log)

	XdrCheckBulk(scanner)
}
