package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/colinmarc/hdfs"
	"github.com/widuu/goini"
	//	"io"
	"log"
	"os"
)

type ReadHdfs struct {
	File               string
	Offset             int64
	Size               int64
	Signature          string
	FileSize           int64
	FileSizeSufficient bool
	ReadCont           []byte
	Valid              bool
}

var (
	right  int
	wrong  int
	client *hdfs.Client
	err    error
	output *os.File
)

func Obj(line string) string {
	begin := strings.Index(line, "{")
	return line[begin:]
}

func ObjParse(obj string) (me ReadHdfs) {
	json.Unmarshal([]byte(obj), &me)

	return
}

func Scanner(file string) *bufio.Scanner {
	f, err := os.Open(file)
	if nil != err {
		log.Fatalln(err)
	}

	return bufio.NewScanner(f)
}

func XdrCheckBulk(scanner *bufio.Scanner) {
	for scanner.Scan() {
		obj := ObjParse(Obj(scanner.Text()))
		obj.RdHdfs()
		obj.IsValid()
		obj.IsFileSizeSufficient()
		obj.ClearReadCont()
		obj.Output()
	}
}

func LogParameter() *string {
	log := flag.String("log", "", "log")
	flag.Parse()

	return log
}

func HdfsClient() {
	namenode := goini.SetConfig("conf.ini").GetValue("hdfs", "namenode")
	client, err = hdfs.New(namenode + ":8020")
	if nil != err {
		log.Fatal("hdfs client err")
	}
}

func (me *ReadHdfs) RdHdfs() {
	f, err := client.Open(me.File)
	bytes := make([]byte, me.Size)
	_, err = f.ReadAt(bytes, me.Offset)

	if nil == err {
		me.ReadCont = bytes
	}
}

func (me *ReadHdfs) ClearReadCont() {
	me.ReadCont = nil
}

func OutputFile() {
	output, err = os.OpenFile("xdr_check_bulk_res", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if nil != err {
		log.Fatalln(err)
	}
}

func (me *ReadHdfs) Output() {
	fmt.Fprintf(output, "%+v\n", me)
}

func (me *ReadHdfs) IsValid() {
	if me.Signature == Sha256Code(me.ReadCont) {
		me.Valid = true
	}
}

func (me *ReadHdfs) IsFileSizeSufficient() {
	if me.Offset+me.Size < me.FileSize {
		me.FileSizeSufficient = true
	}
}

func Sha256Code(bytes []byte) string {
	h := sha256.New()
	h.Write(bytes)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	OutputFile()

	log := LogParameter()
	scanner := Scanner(*log)

	HdfsClient()

	XdrCheckBulk(scanner)
}
