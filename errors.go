package HKafkaQueue

type ReadZeroError struct{}

func (e *ReadZeroError) Error() string {
	return "the message in queue is 0"
}
