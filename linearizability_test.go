package art

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	printHistory = flag.Bool("history", false, "if true visualized history will be saved to file. by default history is saved only if linerizability test failed.")
)

const (
	writeOp = 1
	readOp  = 2
)

type modelInput struct {
	op    int
	key   []byte
	value interface{}
}

type modelOutput struct {
	key   []byte
	value interface{}
}

type timestampedEvent struct {
	event     porcupine.Event
	timestamp int64
}

var singleKeyModel = porcupine.Model{
	Init: func() interface{} {
		return nil
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		inp := input.(modelInput)
		out := output.(modelOutput)
		switch inp.op {
		case readOp:
			return out.value == state, state
		case writeOp:
			return true, inp.value
		default:
			panic(fmt.Sprintf("unknown op: %v", inp.op))
		}
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(modelInput)
		out := output.(modelOutput)
		switch inp.op {
		case readOp:
			return fmt.Sprintf("get('%x') -> '%v'", inp.key, out.value)
		case writeOp:
			return fmt.Sprintf("insert('%x', '%v')", inp.key, inp.value)
		default:
			panic(fmt.Sprintf("unknown op: %v", inp.op))
		}
	},
}

func TestLinearGetInsert(t *testing.T) {
	var tree Tree
	for i := 0; i < 1_000_000; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		tree.Insert(key, 0)
	}
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		opCount = 10
	)
	histories := []timestampedEvent{}
	testKey := make([]byte, 10)
	rand.Read(testKey)
	wg.Add(2)

	go func() {
		id := 1000
		history := make([]timestampedEvent, 0, 2*opCount)
		for i := 0; i < opCount; i++ {
			history = append(history, timestampedEvent{
				event: porcupine.Event{
					ClientId: 1,
					Id:       id + i,
					Kind:     porcupine.CallEvent,
					Value: modelInput{
						op:    writeOp,
						key:   testKey,
						value: i,
					},
				},
				timestamp: time.Now().UnixNano(),
			})
			tree.Insert(testKey, i)
			history = append(history, timestampedEvent{
				event: porcupine.Event{
					ClientId: 1,
					Id:       id + i,
					Kind:     porcupine.ReturnEvent,
					Value:    modelOutput{},
				},
				timestamp: time.Now().UnixNano(),
			})
			time.Sleep(time.Duration(rand.Int63n(100)) * time.Microsecond)
		}
		mu.Lock()
		histories = append(histories, history...)
		mu.Unlock()

		wg.Done()
	}()

	go func() {
		id := 2000
		history := make([]timestampedEvent, 0, 2*opCount)
		for i := 0; i < opCount; i++ {
			history = append(history, timestampedEvent{
				event: porcupine.Event{
					ClientId: 2,
					Id:       id + i,
					Kind:     porcupine.CallEvent,
					Value: modelInput{
						op:  readOp,
						key: testKey,
					},
				},
				timestamp: time.Now().UnixNano(),
			})
			value, _ := tree.Get(testKey)
			history = append(history, timestampedEvent{
				event: porcupine.Event{
					ClientId: 2,
					Id:       id + i,
					Kind:     porcupine.ReturnEvent,
					Value: modelOutput{
						key:   testKey,
						value: value,
					},
				},
				timestamp: time.Now().UnixNano(),
			})
			time.Sleep(time.Duration(rand.Int63n(100)) * time.Microsecond)
		}
		mu.Lock()
		histories = append(histories, history...)
		mu.Unlock()

		wg.Done()
	}()

	wg.Wait()

	sort.Slice(histories, func(i, j int) bool {
		return histories[i].timestamp < histories[j].timestamp
	})
	events := make([]porcupine.Event, len(histories))
	for i := range histories {
		events[i] = histories[i].event
	}
	result, info := porcupine.CheckEventsVerbose(singleKeyModel, events, 0)
	if !assert.True(t, porcupine.Ok == result, "history is not linearizable") || *printHistory {
		tmpfile, err := ioutil.TempFile("", "lintest-single-key-*.html")
		defer tmpfile.Close()
		require.NoError(t, err)
		require.NoError(t, porcupine.Visualize(singleKeyModel, info, tmpfile))
		t.Logf("history is written to %v", tmpfile.Name())
	}
}
