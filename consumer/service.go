package consumer

import (
	"context"
	"github.com/goantor/x"
)

type TopicGroup struct {
	prefix  string
	parent  *TopicGroup
	service *Service
}

func (t *TopicGroup) Topic(prefix string) *TopicGroup {
	service := t.service
	newGroup := &TopicGroup{
		prefix:  t.prefix + "/" + prefix,
		parent:  t,
		service: service,
	}
	service.groups = append(service.groups, newGroup)
	return newGroup
}

func (t *TopicGroup) addRoute(comp string, handler Handler) {
	pattern := t.prefix + "/" + comp
	t.service.router.addRoute(pattern, handler)
}

func (t *TopicGroup) Register(pattern string, handler Handler) {
	t.addRoute(pattern, handler)
}

type Service struct {
	*TopicGroup

	ctx    x.Context
	option IConsumerOption
	router *router
	groups []*TopicGroup
	client *SimpleConsumer // 目前 sdk 只有 一个 client，后续可抽象
}

func (s *Service) TakeName() string {
	return s.option.TakeUniqKey()
}

func (s *Service) Boot() error {
	if err := s.beforeBoot(); err != nil {
		panic(err)

		return err
	}

	s.client.Listen(s.ctx, s.router)
	return nil
}

func (s *Service) beforeBoot() error {
	var (
		err error
	)

	s.client, err = NewSimpleConsumer(s.option)
	if err != nil {
		return err
	}

	return s.client.Start()
}

func (s *Service) Shutdown(ctx context.Context) error {
	return s.client.Stop()
}

func (s *Service) Register(pattern string, handler Handler) {
	s.router.addRoute(pattern, handler)
}

func NewService(ctx x.Context, opt IConsumerOption) *Service {
	service := &Service{
		option: opt,
		router: newRouter(),
		ctx:    ctx,
	}

	service.TopicGroup = &TopicGroup{
		service: service,
	}

	service.groups = []*TopicGroup{service.TopicGroup}
	return service
}
