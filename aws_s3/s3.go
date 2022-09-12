package aws_s3

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"log"
	"os"
)

type Service struct {
	Client   *s3.S3
	EndPoint string
	Region   string
}

const DefaultClientName = "default-s3-client"

var clients map[string]*Service
var StdLogger stdLogger

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

func init() {
	StdLogger = log.New(os.Stdout, "[breaker] ", log.LstdFlags|log.Lshortfile)
}

func InitService(clientID, sk, token, region, point string) error {
	credential := credentials.NewStaticCredentials(clientID, sk, token)
	cfg := aws.NewConfig().WithCredentials(credential).WithRegion(region).
		WithEndpoint(point).WithS3ForcePathStyle(true).WithDisableSSL(true)
	sess, err := session.NewSession(cfg)
	if err != nil {
		return err
	}
	if len(clients) == 0 {
		clients = make(map[string]*Service, 0)
	}
	clients[clientID] = &Service{
		Client:   s3.New(sess),
		EndPoint: point,
		Region:   region,
	}
	return nil
}

func GetS3Client(clientName string) *Service {
	if client, ok := clients[clientName]; ok {
		return client
	}
	panic(fmt.Sprintf("client %s has not initial", clientName))
}

func (s *Service) GetObj(key, bucket string) ([]byte, error) {
	inputObject := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	out, err := s.Client.GetObject(inputObject)
	if err != nil && !IsNotFoundErr(err) {
		return nil, err
	}

	if out.Body != nil {
		res, err := ioutil.ReadAll(out.Body)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	err = awserr.New(s3.ErrCodeNoSuchKey, "empty body", nil)
	return nil, err
}

func (s *Service) PutObj(key, bucket string, data []byte) error {
	inputObject := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
		Body:        bytes.NewReader(data),
	}
	out, err := s.Client.PutObject(inputObject)
	if err != nil {
		outStr := ""
		if out != nil {
			outStr = out.String()
		}
		StdLogger.Print("PutS3Object ", err, outStr)
		return err
	}
	return nil
}

func IsNotFoundErr(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchKey:
			return true
		}
	}
	return false
}
