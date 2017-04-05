package main

import (
	//	"encoding/json"

	"bytes"
	"compress/gzip"
	_ "encoding/base64"
	"fmt"
	"log"
	"os"
	_ "reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func main() {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	streamName := os.Args[1]

	consumeData(sess, "shardId-000000000001", streamName)

	//go run flmain/kinesisconsumer.go  vpc

}

func decompressData(b []byte) []byte {

	var buf bytes.Buffer
	zw, _ := gzip.NewReader(&buf)
	//defer zw.Close()

	_, err := zw.Read(b)

	if err != nil {
		log.Fatal(err)
	}

	if zw !=nil ; err := zw.Close(); err != nil {
		log.Fatal(err)
	}

	/*var p []byte
	zr, err := gzip.NewReader(&buf)
	defer zr.Close()
	_, err := zr.Read(p)*/

	return buf.Bytes()

}

func consumeData(sess *session.Session, shardId string, streamName string) {
	svc := kinesis.New(sess)

	params := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardId),    // Required
		ShardIteratorType: aws.String("LATEST"),   // Required
		StreamName:        aws.String(streamName), // Required
		//StartingSequenceNumber: aws.String("SequenceNumber"),
		//Timestamp:              aws.Time(time.Now()),
	}
	resp, err := svc.GetShardIterator(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return
	}

	kk := *resp.ShardIterator

	params1 := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(kk), // Required
		Limit:         aws.Int64(10),
	}

	for {
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("time is ", time.Now().UnixNano())

		resp1, err1 := svc.GetRecords(params1)

		if err1 != nil {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			return
		}

		//fmt.Println(len(resp1.Records))

		for x, _ := range resp1.Records {
			/*fmt.Println((reflect.TypeOf(resp1.Records[x].Data)))
			fmt.Println(z)
			fmt.Println((reflect.TypeOf(resp1.Records[x].Data.getData())))
			fmt.Println(resp1.Records[x].Data.getData())*/
			//fmt.Println(resp1.Records[x].Data)
			byteArray := decompressData(resp1.Records[x].Data)
			//byteArray := resp1.Records[x].Data
			pKeyb := resp1.Records[x].PartitionKey
			//seqNumb := resp1.Records[x].SequenceNumber

			s1 := string(byteArray[:])

			fmt.Println(s1)
			fmt.Println(*pKeyb)

			//fmt.Println(*seqNumb)

			//n := bytes.IndexByte(byteArray, 0)
			//n := bytes.Index(byteArray, []byte{0})

			//fmt.Println(resp1.Records[x].PartitionKey)
			//fmt.Println(resp1.Records[x].SequenceNumber)
			//z := resp1.Records[x].Data
			//fmt.Println(base64.StdEncoding.Decode(z, resp1.Records[x].Data))

			//fmt.Println(resp1.Records[x])

			//fmt.Println(base64.StdEncoding.DecodeString(x))
		}

		//resp1.Records[0].Data

		/*decoded, err := base64.StdEncoding.DecodeString()
		if err != nil {
			fmt.Println("decode error:", err)
			return
		}
		fmt.Println(string(decoded))*/

		// Pretty-print the response data.
		//fmt.Println(resp1)
	}

}
