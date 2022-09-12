package aws_s3

import "testing"

const (
	Key    = "mail_content/test"
	Bucket = "test-bucket"
	SK     = "admin123"
	Region = "weed"
)

func TestPutAndGetObj(t *testing.T) {
	err := InitService(DefaultClientName, SK, "", Region, "127.0.0.1:8333")
	if err != nil {
		t.Error("InitService error", err)
	}
	err = GetS3Client(DefaultClientName).PutObj(Key, Bucket, []byte("this is for test"))
	if err != nil {
		t.Error("PutObj error", err)
	}
	res, err := GetS3Client(DefaultClientName).GetObj(Key, Bucket)
	if err != nil {
		t.Error("GetObj error", err, string(res))
	}
	t.Log(string(res))

}
