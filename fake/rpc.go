package fake

import (
	"fmt"
	"github.com/juliocnsouzadev/go-concurrency/model"
	"time"
)

type RpcMessage struct {
	Msg    model.Message
	Worker int
}

func (r *RpcMessage) ToString() string {
	return fmt.Sprintf("workerId %d, messageId %d", r.Worker, r.Msg.Id)
}

func DoRPC(msg *RpcMessage) {
	msgData := msg.ToString()
	fmt.Println("=> sending message: ", msgData)
	time.Sleep(100 * time.Millisecond)
	fmt.Println("=> message (", msgData, ") was sent")
}
