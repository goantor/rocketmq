package consumer

import (
	"fmt"
	mq "github.com/apache/rocketmq-clients/golang/v5"
	"strings"
)

type router struct {
	roots    map[string]*node
	handlers map[string]Handler
}

func newRouter() *router {
	return &router{
		roots:    make(map[string]*node),
		handlers: make(map[string]Handler),
	}
}

func parsePattern(pattern string) []string {
	vs := strings.Split(pattern, "/")
	parts := make([]string, 0)

	for _, item := range vs {
		if item != "" {
			parts = append(parts, item)
			if item[0] == '*' {
				break
			}
		}
	}
	return parts
}

func (r *router) addRoute(pattern string, handler Handler) {
	parts := parsePattern(pattern)
	topic := parts[0]
	_, ok := r.roots[topic]
	if !ok {
		r.roots[topic] = &node{}
	}
	r.roots[topic].insert(pattern, parts, 0)
	r.handlers[pattern] = handler
}

func (r *router) getRoute(path string) *node {
	searchParts := parsePattern(path)
	topic := searchParts[0]
	root, ok := r.roots[topic]

	if !ok {
		return nil
	}

	return root.search(searchParts, 0)
}

func (r *router) takeHandler(msg *mq.MessageView) (Handler, bool) {
	topic := msg.GetTopic()
	tags := msg.GetTag()
	path := fmt.Sprintf("%s/*", topic)
	if tags != nil && *tags != "" {
		path = fmt.Sprintf("%s/%s", topic, *tags)
	}

	n := r.getRoute(path)
	fmt.Println("n", n)
	if n == nil {
		return nil, false
	}
	pattern := n.pattern
	handler, ok := r.handlers[pattern]
	return handler, ok
}
