package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
)

func GzipEncode(input []byte) ([]byte, error) {
	// 创建一个新的 byte 输出流
	var buf bytes.Buffer
	// 创建一个新的 gzip 输出流

	//NoCompression      = flate.NoCompression      // 不压缩
	//BestSpeed          = flate.BestSpeed          // 最快速度
	//BestCompression    = flate.BestCompression    // 最佳压缩比
	//DefaultCompression = flate.DefaultCompression // 默认压缩比
	//gzip.NewWriterLevel()
	gzipWriter := gzip.NewWriter(&buf)
	// 将 input byte 数组写入到此输出流中
	_, err := gzipWriter.Write(input)
	if err != nil {
		_ = gzipWriter.Close()
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	// 返回压缩后的 bytes 数组
	return buf.Bytes(), nil
}

func GzipDecode(input []byte) ([]byte, error) {
	// 创建一个新的 gzip.Reader
	bytesReader := bytes.NewReader(input)
	gzipReader, err := gzip.NewReader(bytesReader)
	if err != nil {
		return nil, err
	}
	defer func() {
		// defer 中关闭 gzipReader
		_ = gzipReader.Close()
	}()
	buf := new(bytes.Buffer)
	// 从 Reader 中读取出数据
	if _, err := buf.ReadFrom(gzipReader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 压缩
func MarshalJsonAndGzip(data interface{}) ([]byte, error) {
	marshalData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	gzipData, err := GzipEncode(marshalData)
	if err != nil {
		return nil, err
	}
	return gzipData, err
}

// 解压
func UnmarshalDataFromJsonWithGzip(input []byte, output interface{}) error {
	decodeData, err := GzipDecode(input)
	if err != nil {
		return err
	}

	err = json.Unmarshal(decodeData, output)
	if err != nil {
		return err
	}
	return nil
}
