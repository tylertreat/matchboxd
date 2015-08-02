package matchboxd

import (
	"fmt"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const zMatchbox = "/matchbox"

var acl = zk.WorldACL(zk.PermAll)

type metadataClient interface {
	Dial() error
	Close() error
	Subscribe(topic string, id string) error
	Unsubscribe(topic string, id string) error
	Events() <-chan *Subscriber
	Errors() <-chan error
}

type zkMetadataClient struct {
	conn   *zk.Conn
	hosts  []string
	events chan *Subscriber
	errors chan error
	closed chan bool
}

func newZookeeperMetadataClient(hosts []string) metadataClient {
	return &zkMetadataClient{
		hosts:  hosts,
		events: make(chan *Subscriber, 1024),
		errors: make(chan error, 1),
		closed: make(chan bool, 1),
	}
}

func (z *zkMetadataClient) Dial() error {
	conn, _, err := zk.Connect(z.hosts, 2*time.Second)
	if err != nil {
		return err
	}
	z.conn = conn
	if _, err := z.createIfNotExists(zMatchbox, nil, 0, acl); err != nil {
		z.Close()
		return err
	}

	go z.sync()
	return nil
}

func (z *zkMetadataClient) Close() error {
	z.closed <- true
	z.conn.Close()
	return nil
}

func (z *zkMetadataClient) Events() <-chan *Subscriber {
	return z.events
}

func (z *zkMetadataClient) Errors() <-chan error {
	return z.errors
}

func (z *zkMetadataClient) Subscribe(topic string, id string) error {
	path, err := z.createIfNotExists(
		fmt.Sprintf("%s/%s_%s", zMatchbox, topic, id), nil, zk.FlagEphemeral, acl)
	if err != nil {
		return err
	}
	go z.watchSubscription(topic, id, path)
	return nil
}

func (z *zkMetadataClient) Unsubscribe(topic string, id string) error {
	err := z.conn.Delete(fmt.Sprintf("%s/%s_%s", zMatchbox, topic, id), -1)
	if err == nil || err == zk.ErrNoNode {
		return nil
	}
	return err
}

func (z *zkMetadataClient) watchSubscription(topic, id, path string) {
	_, _, events, err := z.conn.ExistsW(path)
	if err != nil {
		z.errors <- err
		return
	}

	event := <-events
	if event.Err != nil {
		z.errors <- event.Err
		return
	}

	if event.Type == zk.EventNodeDeleted {
		z.events <- &Subscriber{
			removed:      true,
			topic:        topic,
			SubscriberID: id,
		}
	}
}

func (z *zkMetadataClient) sync() {
	for {
		subs, _, watch, err := z.conn.ChildrenW(zMatchbox)
		if err != nil {
			z.errors <- err
			return
		}
		for _, sub := range subs {
			topicAndSubscriber := strings.Split(sub, separator)
			go z.watchSubscription(topicAndSubscriber[0], topicAndSubscriber[1],
				fmt.Sprintf("%s/%s", zMatchbox, sub))
			z.events <- &Subscriber{
				topic:        topicAndSubscriber[0],
				SubscriberID: topicAndSubscriber[1],
			}
		}

		select {
		case <-z.closed:
			close(z.events)
			return
		case event := <-watch:
			if event.Err != nil {
				z.errors <- event.Err
				return
			}
		}
	}
}

func (z *zkMetadataClient) createIfNotExists(path string, data []byte, flags int32,
	acl []zk.ACL) (string, error) {

	exists, _, err := z.conn.Exists(path)
	if err != nil {
		return "", err
	}
	if exists {
		return path, nil
	}
	return z.conn.Create(path, data, flags, acl)
}
