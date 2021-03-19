package cdc

type ChannelStream struct {
	channel  chan *Wal2JsonMessage
	isActive bool
}

func NewChannelStream() *ChannelStream {
	return &ChannelStream{
		channel:  make(chan *Wal2JsonMessage),
		isActive: true,
	}
}

func (stream *ChannelStream) Send(walMessage *Wal2JsonMessage) {
	stream.channel <- walMessage
}

func (stream *ChannelStream) Receive() (*Wal2JsonMessage, error) {
	return <-stream.channel, nil
}

func (stream *ChannelStream) Close() error {
	close(stream.channel)
	return nil
}
