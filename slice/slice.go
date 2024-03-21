package slice

import "errors"

//要求一：能够实现删除操作就可以。
//要求二：考虑使用比较高性能的实现。
//要求三：改造为泛型方法
//TODO 要求四：支持缩容，并旦设计缩容机制。

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
