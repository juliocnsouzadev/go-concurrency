package fake

import (
	"fmt"
	"github.com/juliocnsouzadev/go-concurrency/model"
	"log"
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
	log.Println("sending message: ", msgData)
	time.Sleep(100 * time.Millisecond)
	log.Println("message (", msgData, ") was sent")
}
