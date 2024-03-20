package slice

import "errors"

var (
	ErrIllegalIndex = errors.New(`非法下标`)
)

func Delete[T any](s []T, i int) ([]T, error) {
	if i < 0 || i > len(s) {
		return nil, ErrIllegalIndex
	}
	l := len(s) - 1
	for i := i; i < l; i++ {
		s[i] = s[i+1]
	}
	return s[:l], nil
}
