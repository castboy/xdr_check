package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"

	"github.com/colinmarc/hdfs"
	"github.com/widuu/goini"
)

var client *hdfs.Client
var err error

func HdfsClient() {
	namenode := goini.SetConfig("conf.ini").GetValue("hdfs", "namenode")
	client, err = hdfs.New(namenode + ":8020")
	if nil != err {
		log.Fatal("hdfs client err")
	}
}

func RdHdfs(file string, offset int64, size int64) []byte {
	f, err := client.Open(file)
	bytes := make([]byte, size)
	_, err = f.ReadAt(bytes, offset)
	if nil != err {
		fmt.Printf("read hdfs %s from offset %d needSize %d, but only get %d", file, offset, size, f.Stat().Size())
	}

	return bytes
}

func isRightFile(hdfs []byte, xdrMark string) bool {
	right := true
	if xdrMark != sha256Code(hdfs) {
		right = false
	}

	return right
}

func sha256Code(bytes []byte) string {
	h := sha256.New()
	h.Write(bytes)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func main() {
	file := flag.String("file", "", "file")
	offset := flag.Int("offset", 0, "offset")
	size := flag.Int("size", 0, "size")
	signature := flag.String("signature", "", "signature")
	fmt.Println("stdin params below")
	fmt.Printf("file: %s", *file)
	fmt.Printf("offset: %d", *offset)
	fmt.Printf("size: %d", *size)
	fmt.Printf("signature: %s", *signature)
	flag.Parse()

	HdfsClient()
	bytes := RdHdfs(*file, int64(*offset), int64(*size))

	ok := isRightFile(bytes, *signature)
	if ok {
		fmt.Println("right xdr")
	} else {
		fmt.Println("wrong xdr")
	}

}
