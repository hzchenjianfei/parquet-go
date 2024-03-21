package tests

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
	"testing"

	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type student struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age    int32   `parquet:"name=age, type=INT32"`
	ID     int64   `parquet:"name=id, type=INT64"`
	Weight float32 `parquet:"name=weight, type=FLOAT"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
}

var (
	accessKey = "xxx"
	secretKey = "yyy"
	region    = "ap-northeast-1"
	endpoint  = "s3.ap-northeast-1.amazonaws.com"
)

// s3Example provides a sample write and read using the S3 Parquet File
func TestS3FlatTest(t *testing.T) {
	ctx := context.Background()

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
		//S3ForcePathStyle: aws.Bool(false),
	})
	if err != nil {
		log.Println("new s3 session error", err)
		panic(err)
	}
	s3.SetActiveSession(sess)

	bucket := "bbb"
	key := "test/foobar.parquet"
	num := 100

	// create new S3 file writer
	fw, err := s3.NewS3FileWriter(ctx, bucket, key, nil)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	// create new parquet file writer
	pw, err := writer.NewParquetWriter(fw, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	// write 100 student records to the parquet file
	for i := 0; i < num; i++ {
		stu := student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			ID:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	// write parquet file footer
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop err", err)
	}

	err = fw.Close()
	if err != nil {
		log.Println("Error closing S3 file writer", err)
		return
	}
	log.Println("Write Finished")

	// read the written parquet file
	// create new S3 file reader
	fr, err := s3.NewS3FileReader(ctx, bucket, key)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}

	// create new parquet file reader
	pr, err := reader.NewParquetReader(fr, new(student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	// read the student rows and print
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	// close the parquet file
	pr.ReadStop()
	err = fr.Close()
	if err != nil {
		log.Println("Error closing S3 file reader", err)
		return
	}
	log.Println("Read Finished")
}
