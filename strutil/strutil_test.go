package strutil

import "testing"

func TestIncludeLetter(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{}
	tests = append(tests, struct {
		name string
		args args
		want bool
	}{name: "t1", args: args{str: "手机123"}, want: false})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IncludeLetter(tt.args.str); got != tt.want {
				t.Errorf("IncludeLetter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInt64ToString(t *testing.T) {
	type args struct {
		num int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{}

	tests = append(tests, struct {
		name string
		args args
		want string
	}{name: "t1", args: args{num: 123}, want: "123"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Int64ToString(tt.args.num); got != tt.want {
				t.Errorf("Int64ToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsDigit(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDigit(tt.args.str); got != tt.want {
				t.Errorf("IsDigit() = %v, want %v", got, tt.want)
			}
		})
	}
}
