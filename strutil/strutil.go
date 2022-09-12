package strutil

import (
	"fmt"
	"reflect"
	"strconv"
	"unicode"
	"unsafe"
)

func IncludeLetter(str string) bool {
	runes := []rune(str)
	for _, r := range runes {
		if unicode.IsLetter(r) && !unicode.Is(unicode.Scripts["Han"], r) {
			fmt.Println("r", r)
			return true
		}
	}
	return false
}

func IsDigit(str string) bool {
	for _, x := range []rune(str) {
		if !unicode.IsDigit(x) {
			return false
		}
	}
	return true
}

func Int64ToString(num int64) string {
	return strconv.FormatInt(num, 10)
}

func BytesToString(data *[]byte) string {
	return *(*string)(unsafe.Pointer(data))
}

func StringToBytes(data string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = data
	(*reflect.SliceHeader)(unsafe.Pointer(&b)).Cap = len(data)
	return
}
