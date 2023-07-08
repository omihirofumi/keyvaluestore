package kvs

type EventType byte

const (
	_                     = iota // iota == 0; ignore this value
	EventDelete EventType = iota // iota == 1
	EventPut                     // iota == 2; implicitly repeat last
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type TransactionLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()

	Close() error
}
