package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/welldigital/cw/logfile"
)

var pathFlag = flag.String("path", "", "The path to gzipped CloudWatch data (default format of data exported to S3).")

func main() {
	flag.Parse()

	if *pathFlag == "" {
		fmt.Println("Missing -path flag")
		os.Exit(-1)
	}

	cwr := logfile.NewCloudWatchReader(*pathFlag)

	lec := make(chan logfile.Entry, 1024*1024)
	workCompleted := make(chan interface{})

	go func() {
		var read int
		for {
			le, ok := <-lec
			if !ok {
				workCompleted <- true
				return
			}
			read++

			if isJSON(le.Message) {
				fmt.Println(le.Message)
			}
		}
	}()

	_, err := cwr.Read(lec)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}
	close(lec)
	<-workCompleted
}

func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}
