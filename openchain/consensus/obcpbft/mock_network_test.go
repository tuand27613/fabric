/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package obcpbft

import (
	"fmt"
	"sync"
	"time"

	"github.com/openblockchain/obc-peer/openchain/consensus"
	pb "github.com/openblockchain/obc-peer/protos"
)

type endpoint interface {
	stop()
	idleChan() <-chan struct{}
	deliver([]byte, *pb.PeerID)
	consensus.Inquirer
}

type taggedMsg struct {
	src int
	dst int
	msg []byte
}

type testnet struct {
	debug     bool
	N         int
	closed    chan struct{}
	endpoints []endpoint
	msgs      chan taggedMsg
	filterFn  func(int, int, []byte) []byte
}

type testEndpoint struct {
	id  uint64
	net *testnet
}

func makeTestEndpoint(id uint64, net *testnet) *testEndpoint {
	te := &testEndpoint{}
	te.id = id
	te.net = net
	return te
}

func (te *testEndpoint) GetOwnID() (id uint64, err error) {
	return te.id, nil
}

func (te *testEndpoint) GetOwnHandle() (handle *pb.PeerID, err error) {
	return &pb.PeerID{Name: fmt.Sprintf("vp%d", te.id)}, nil
}

func (te *testEndpoint) GetValidatorID(handle *pb.PeerID) (id uint64, err error) {
	if nil == te.net {
		err = fmt.Errorf("Network not initialized")
		return
	}

	for _, v := range te.net.endpoints {
		epHandle, _ := v.GetOwnHandle()
		if *handle == *epHandle {
			id, _ = v.GetOwnID()
			return
		}
	}

	err = fmt.Errorf("Handle %v not found in network list", handle)
	return
}

func (te *testEndpoint) GetValidatorHandle(id uint64) (handle *pb.PeerID, err error) {
	if nil == te.net {
		err = fmt.Errorf("Network not initialized")
		return
	}

	if int(id) < len(te.net.endpoints) {
		handle, _ = te.net.endpoints[id].GetOwnHandle()
		return
	}

	err = fmt.Errorf("ID %d not found in network list (length of list: %d)", id, len(te.net.endpoints))
	return
}

func (te *testEndpoint) GetValidatorHandles(ids []uint64) (handles []*pb.PeerID, err error) {
	handles = make([]*pb.PeerID, len(ids))
	for i, id := range ids {
		handles[i], err = te.GetValidatorHandle(id)
		if err != nil {
			break
		}
	}
	return
}

func (te *testEndpoint) GetConnectedValidators() (handles []*pb.PeerID, err error) {
	if nil != te.net {
		err = fmt.Errorf("Network not initialized")
		return
	}

	handles = make([]*pb.PeerID, len(te.net.endpoints))
	for i, v := range te.net.endpoints {
		handles[i], _ = v.GetOwnHandle()
	}

	return
}

func (te *testEndpoint) CheckWhitelistExists() (size int, err error) {
	if nil == te.net {
		err = fmt.Errorf("Network not initialized")
		return
	}
	size = len(te.net.endpoints)
	return
}

func (te *testEndpoint) SetWhitelistCap(cap int) error {
	// no-op
	return nil
}

// Broadcast delivers to all endpoints.  In contrast to the stack
// Broadcast, this will also deliver back to the replica.  We keep
// this behavior, because it exposes subtle bugs in the
// implementation.
func (te *testEndpoint) Broadcast(msg *pb.OpenchainMessage, peerType pb.PeerEndpoint_Type) error {
	te.net.broadcastFilter(te, msg.Payload)
	return nil
}

func (te *testEndpoint) Unicast(msg *pb.OpenchainMessage, receiverHandle *pb.PeerID) error {
	receiverID, err := te.GetValidatorID(receiverHandle)
	if err != nil {
		return fmt.Errorf("Couldn't unicast message to %s: %v", receiverHandle.Name, err)
	}
	internalQueueMessage(te.net.msgs, taggedMsg{int(te.id), int(receiverID), msg.Payload})
	return nil
}

func internalQueueMessage(queue chan<- taggedMsg, tm taggedMsg) {
	select {
	case queue <- tm:
	default:
		fmt.Println("TEST NET: Message cannot be queued without blocking, consider increasing the queue size")
		queue <- tm
	}
}

func (net *testnet) debugMsg(msg string, args ...interface{}) {
	if net.debug {
		fmt.Printf(msg, args...)
	}
}

func (net *testnet) broadcastFilter(te *testEndpoint, payload []byte) {
	select {
	case <-net.closed:
		fmt.Println("WARNING! Attempted to send a request to a closed network, ignoring")
		return
	default:
	}
	if net.filterFn != nil {
		payload = net.filterFn(int(te.id), -1, payload)
		net.debugMsg("TEST: filtered message\n")
	}
	if payload != nil {
		net.debugMsg("TEST: attempting to queue message %p\n", payload)
		internalQueueMessage(net.msgs, taggedMsg{int(te.id), -1, payload})
		net.debugMsg("TEST: message queued successfully %p\n", payload)
	} else {
		net.debugMsg("TEST: suppressing message with payload %p\n", payload)
	}
}

func (net *testnet) deliverFilter(msg taggedMsg, senderID int) {
	net.debugMsg("TEST: deliver\n")
	senderHandle, _ := net.endpoints[senderID].GetOwnHandle()
	if msg.dst == -1 {
		net.debugMsg("TEST: Sending broadcast %v\n", net.endpoints)
		wg := &sync.WaitGroup{}
		wg.Add(len(net.endpoints))
		for i, ep := range net.endpoints {
			id, _ := ep.GetOwnID()
			net.debugMsg("TEST: Looping broadcast %d\n", id)
			lid := i
			lep := ep
			go func() {
				defer wg.Done()
				if msg.src == lid {
					if net.debug {
						net.debugMsg("TEST: Skipping local delivery %d %d\n", lid, senderID)
					}
					// do not deliver to local replica
					return
				}
				payload := msg.msg
				net.debugMsg("TEST: Filtering %d\n", lid)
				if net.filterFn != nil {
					payload = net.filterFn(msg.src, lid, payload)
				}
				net.debugMsg("TEST: Delivering %d\n", lid)
				if payload != nil {
					net.debugMsg("TEST: Sending message %d\n", lid)
					lep.deliver(msg.msg, senderHandle)
					net.debugMsg("TEST: Sent message %d\n", lid)
				}
			}()
		}
		wg.Wait()
	} else {
		net.debugMsg("TEST: Sending unicast\n")
		net.endpoints[msg.dst].deliver(msg.msg, senderHandle)
	}
}

func (net *testnet) idleFan() <-chan struct{} {
	res := make(chan struct{})

	go func() {
		for _, inst := range net.endpoints {
			<-inst.idleChan()
		}
		net.debugMsg("TEST: closing idleChan\n")
		// Only close to the channel after all the consenters have written to us
		close(res)
	}()

	return res
}

func (net *testnet) processMessageFromChannel(msg taggedMsg, ok bool) bool {
	if !ok {
		net.debugMsg("TEST: message channel closed, exiting\n")
		return false
	}
	net.debugMsg("TEST: new message, delivering\n")
	net.deliverFilter(msg, msg.src)
	return true
}

func (net *testnet) process() error {
	for {
		net.debugMsg("TEST: process looping\n")
		select {
		case msg, ok := <-net.msgs:
			net.debugMsg("TEST: processing message without testing for idle\n")
			if !net.processMessageFromChannel(msg, ok) {
				return nil
			}
		case <-net.closed:
			return nil
		default:
			net.debugMsg("TEST: processing message or testing for idle\n")
			select {
			case <-net.idleFan():
				net.debugMsg("TEST: exiting process loop because of idleness\n")
				return nil
			case msg, ok := <-net.msgs:
				if !net.processMessageFromChannel(msg, ok) {
					return nil
				}
			case <-time.After(10 * time.Second):
				// Things should never take this long
				panic("Test waiting for new messages took 10 seconds, this generally indicates a deadlock condition")
			case <-net.closed:
				return nil
			}
		}
	}
}

func (net *testnet) processContinually() {
	for {
		select {
		case msg, ok := <-net.msgs:
			if !net.processMessageFromChannel(msg, ok) {
				return
			}
		case <-net.closed:
			return
		}
	}
}

func makeTestnet(N int, initFn func(id uint64, network *testnet) endpoint) *testnet {
	net := &testnet{}
	net.msgs = make(chan taggedMsg, 100)
	net.closed = make(chan struct{})
	net.endpoints = make([]endpoint, N)

	for i := range net.endpoints {
		net.endpoints[i] = initFn(uint64(i), net)
	}

	return net
}

func (net *testnet) clearMessages() {
	for {
		select {
		case <-net.msgs:
		default:
			return
		}
	}
}

func (net *testnet) stop() {
	for _, ep := range net.endpoints {
		ep.stop()
	}
	close(net.closed)
}
