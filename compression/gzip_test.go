package compression

import "testing"

type UserTest struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestUnmarshalDataFromJsonWithGzip(t *testing.T) {
	user := UserTest{
		ID:   1,
		Name: "imooc",
	}
	userByte, err := MarshalJsonAndGzip(user)
	if err != nil {
		t.Errorf("MarshalJsonAndGzip err %v", err)
	}
	outputUser := UserTest{}

	err = UnmarshalDataFromJsonWithGzip(userByte, &outputUser)
	if err != nil {
		t.Errorf("UnmarshalDataFromJsonWithGzip err %v", err)
	}
	t.Log("outputUser : ", outputUser)
}
