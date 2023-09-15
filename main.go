package main

import (
	"fmt"
	"github.com/juliocnsouzadev/go-concurrency/fan_out"
	"github.com/juliocnsouzadev/go-concurrency/map_reduce"
	"github.com/juliocnsouzadev/go-concurrency/model"
	"slices"
	"time"
)

func main() {
	fanOutDemo()
	mapReduceDemo()
}

func fanOutDemo() {
	fmt.Println("\n\n** Fan Out **")
	numberOfMessages := 1000
	numberOfWorkers := 5

	messages := make([]model.Message, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		messages[i] = model.Message{
			Id:   i + 1,
			Body: fmt.Sprintf("Message #%d", i+1),
		}
	}
	fanOut := fan_out.NewFanOut(numberOfWorkers, 1*time.Second)
	fanOut.Process(messages)
}

func mapReduceDemo() {
	fmt.Println("\n\n** MapReduce **")
	lines := []string{
		"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
		"Lorem Ipsum has been the industry's standard dummy text ever since the",
		"when an unknown printer took a galley of type and scrambled it to make a type specimen book.",
		"It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
	}

	mapReduce := map_reduce.NewMapReduce(3, 6)
	result := mapReduce.Process(lines)

	var keys []string
	for key, _ := range result {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	fmt.Println("WordCount")
	for _, key := range keys {
		value := result[key]
		fmt.Printf("- %s : %d\n", key, value)
	}
}
