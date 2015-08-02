package matchboxd

import (
	"log"

	"github.com/Workiva/matchbox"
)

const separator = "_"

// Subscriber uniquely identifies a matchbox subscription.
type Subscriber struct {
	SubscriberID string
	removed      bool
	topic        string
}

// ID returns the unique identifier for the Subscriber.
func (s *Subscriber) ID() string {
	return s.SubscriberID
}

// Daemon is responsible for managing and replicating matchbox subscriptions.
type Daemon interface {
	matchbox.Matchbox

	// Start will start the daemon for replication.
	Start() error

	// Close stops the daemon.
	Close() error

	// Errors returns a channel which produces any errors which occur during
	// replication.
	Errors() <-chan error
}

type zookeeperDaemon struct {
	mb       matchbox.Matchbox
	metadata metadataClient
	zkHosts  []string
}

// NewDaemon creates a new Daemon backed by Zookeeper.
func NewDaemon(config *matchbox.Config, zkHosts []string) Daemon {
	return &zookeeperDaemon{
		mb:       matchbox.New(config),
		metadata: newZookeeperMetadataClient(zkHosts),
	}
}

func (z *zookeeperDaemon) Start() error {
	if err := z.metadata.Dial(); err != nil {
		return err
	}
	go z.sync()
	return nil
}

func (z *zookeeperDaemon) Close() error {
	return z.metadata.Close()
}

func (z *zookeeperDaemon) Errors() <-chan error {
	return z.metadata.Errors()
}

func (z *zookeeperDaemon) Subscribe(topic string, subscriber matchbox.Subscriber) {
	if err := z.metadata.Subscribe(topic, subscriber.ID()); err != nil {
		log.Println("matchboxd: %s", err.Error())
	}
	z.mb.Subscribe(topic, subscriber)
}

func (z *zookeeperDaemon) Unsubscribe(topic string, subscriber matchbox.Subscriber) {
	if err := z.metadata.Unsubscribe(topic, subscriber.ID()); err != nil {
		log.Println("matchboxd: %s", err.Error())
	}
	z.mb.Unsubscribe(topic, subscriber)
}

func (z *zookeeperDaemon) Subscribers(topic string) []matchbox.Subscriber {
	return z.mb.Subscribers(topic)
}

func (z *zookeeperDaemon) Subscriptions() map[string][]matchbox.Subscriber {
	return z.mb.Subscriptions()
}

func (z *zookeeperDaemon) Topics() []string {
	return z.mb.Topics()
}

func (z *zookeeperDaemon) sync() {
	for {
		event, ok := <-z.metadata.Events()
		if !ok {
			return
		}
		if event.removed {
			z.mb.Unsubscribe(event.topic, event)
		} else {
			z.mb.Subscribe(event.topic, event)
		}
	}
}
