package HKafkaQueue

type ReadZeroError struct{}

func (e *ReadZeroError) Error() string {
	return "the message in queue is 0"
}

type ReadDirtyError struct{}

func (e *ReadDirtyError) Error() string {
	return "read dirty msg ! when writing is not done"
}

type PutLengthError struct{}

func (e *PutLengthError) Error() string {
	return "put length error"
}
