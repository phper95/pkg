package strutil

import (
	"strconv"
	"unicode"
)

func IncludeLetter(str string) bool {
	runes := []rune(str)
	for _, r := range runes {
		if unicode.IsLetter(r) {
			return true
		}
	}
	return false
}

func Int64ToString(num int64) string {
	return strconv.FormatInt(num, 10)
}
