package types

// go1.18引入了泛型，千呼万唤始出来～

type Signed interface {
	~int | ~int32 | ~int64 | ~int8 | ~int16
}

type UnSigned interface {
	~uint | ~uint32 | ~uint64 | ~uint8 | ~uint16
}

type Float interface {
	~float32 | ~float64
}

type Integer interface {
	Signed | UnSigned
}

type Order interface {
	Integer | Float | ~string
}

func Min[T Order](a, b T) T {
	if a < b {
		return a
	}
	return b
}
