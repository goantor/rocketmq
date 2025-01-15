package consumer

import (
	"fmt"
	"testing"
)

func handlerTest(msg *Message) error {
	return nil
}

func TestRouter(t *testing.T) {
	r := newRouter()

	r.addRoute("two/:tag", handlerTest)
	r.addRoute("one/*dd1", handlerTest)

	n := r.getRoute("one/2")
	if n == nil {
		t.Error("getRoute failed")
		return
	}

	fmt.Println(n.pattern)
	n2 := r.getRoute("two/ddd")

	if n2 == nil {
		t.Error("getRoute failed")
		return
	}

	fmt.Println(n2.pattern)
}
