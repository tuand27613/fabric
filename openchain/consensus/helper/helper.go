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

package helper

import (
	"fmt"

	"github.com/spf13/viper"
	"golang.org/x/net/context"

	"github.com/openblockchain/obc-peer/openchain/chaincode"
	"github.com/openblockchain/obc-peer/openchain/consensus"
	crypto "github.com/openblockchain/obc-peer/openchain/crypto"
	"github.com/openblockchain/obc-peer/openchain/ledger"
	"github.com/openblockchain/obc-peer/openchain/ledger/statemgmt"
	"github.com/openblockchain/obc-peer/openchain/peer"
	pb "github.com/openblockchain/obc-peer/protos"
)

// Helper contains the reference to the peer's MessageHandlerCoordinator
type Helper struct {
	coordinator peer.MessageHandlerCoordinator
	secOn       bool
	secHelper   crypto.Peer
	curBatch    []*pb.Transaction // TODO remove after issue #579
}

// NewHelper constructs the consensus helper object
func NewHelper(mhc peer.MessageHandlerCoordinator) consensus.Stack {
	return &Helper{coordinator: mhc,
		secOn:     viper.GetBool("security.enabled"),
		secHelper: mhc.GetSecHelper()}
}

// GetOwnID retrieve this peer's PBFT ID
func (h *Helper) GetOwnID() (id uint64, err error) {
	handle, err := h.GetOwnHandle()
	if err != nil {
		return
	}
	return h.GetValidatorID(handle)
}

// GetOwnHandle retrieve this peer's PeerID
func (h *Helper) GetOwnHandle() (handle *pb.PeerID, err error) {
	ep, err := h.coordinator.GetPeerEndpoint()
	if err != nil {
		return nil, err
	}
	return ep.ID, nil
}

// GetValidatorID retrieves a validating peer's PBFT ID
func (h *Helper) GetValidatorID(handle *pb.PeerID) (id uint64, err error) {
	whitelistedMap, _, _ := h.coordinator.GetWhitelist()
	if value, ok := whitelistedMap[*handle]; !ok {
		err = fmt.Errorf("Validator's handle (%v) not found in the whitelist: %+v", *handle, whitelistedMap)
	} else {
		id = uint64(value)
	}

	return
}

// GetValidatorHandle retrieves a validating peer's PeerID
func (h *Helper) GetValidatorHandle(id uint64) (handle *pb.PeerID, err error) {
	_, sortedValues, _ := h.coordinator.GetWhitelist()
	if int(id) < len(sortedValues) {
		return sortedValues[int(id)], nil
	}
	err = fmt.Errorf(`Couldn't retrieve validator's handle.
					  Requested validator index was %v,
					  length of whitelist keys slice is %v`, id, len(sortedValues))
	return
}

// GetValidatorHandles returns the PeerIDs corresponding to a list of PBFT IDs
func (h *Helper) GetValidatorHandles(ids []uint64) (handles []*pb.PeerID, err error) {
	handles = make([]*pb.PeerID, len(ids))
	for i, id := range ids {
		handles[i], err = h.GetValidatorHandle(id)
		if err != nil {
			break
		}
	}
	return
}

// GetConnectedValidators retrieves the list of connected validators
func (h *Helper) GetConnectedValidators() (handles []*pb.PeerID, err error) {
	peersMsg, err := h.coordinator.GetPeers()
	if err == nil {
		peers := peersMsg.GetPeers()
		for _, endpoint := range peers {
			if endpoint.Type == pb.PeerEndpoint_VALIDATOR {
				handles = append(handles, endpoint.ID)
			}
		}
	}
	return
}

// CheckWhitelistExists returns the length (number of entries) of this peer's whitelist
func (h *Helper) CheckWhitelistExists() (size int, err error) {
	return h.coordinator.CheckWhitelistExists()
}

// SetWhitelistCap sets the expected number of maximum validators on the network
// When this many VPs are visible on the network, the validator will record
// their IDs to a whitelist and save it to disk.
func (h *Helper) SetWhitelistCap(cap int) error {
	return h.coordinator.SetWhitelistCap(cap)
}

// Broadcast sends a message to all validating peers
func (h *Helper) Broadcast(msg *pb.OpenchainMessage, peerType pb.PeerEndpoint_Type) error {
	errors := h.coordinator.Broadcast(msg, peerType)
	if len(errors) > 0 {
		return fmt.Errorf("Couldn't broadcast successfully")
	}
	return nil
}

// Unicast sends a message to a specified receiver
func (h *Helper) Unicast(msg *pb.OpenchainMessage, receiverHandle *pb.PeerID) error {
	return h.coordinator.Unicast(msg, receiverHandle)
}

// Sign a message with this validator's signing key
func (h *Helper) Sign(msg []byte) ([]byte, error) {
	if h.secOn {
		return h.secHelper.Sign(msg)
	}
	logger.Debug("Security is disabled")
	return msg, nil
}

// Verify that the given signature is valid under the given replicaID's verification key
// If replicaID is nil, use this validator's verification key
// If the signature is valid, the function should return nil
func (h *Helper) Verify(replicaID *pb.PeerID, signature []byte, message []byte) error {
	if !h.secOn {
		logger.Debug("Security is disabled")
		return nil
	}

	logger.Debug("Verify message from: %v", replicaID.Name)

	// look for the sender among the list of peers
	peersMsg, err := h.coordinator.GetPeers()
	if err != nil {
		return err
	}
	peers := peersMsg.GetPeers()
	for _, endpoint := range peers {
		logger.Debug("Endpoint name: %v", endpoint.ID.Name)
		if *endpoint.ID == *replicaID {
			// call crypto verify() with that endpoint's pkiID
			return h.secHelper.Verify(endpoint.PkiID, signature, message)
		}
	}

	return fmt.Errorf("Could not verify message from %s (unknown peer)", replicaID.Name)
}

// BeginTxBatch gets invoked when the next round
// of transaction-batch execution begins
func (h *Helper) BeginTxBatch(id interface{}) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger: %v", err)
	}
	if err := ledger.BeginTxBatch(id); err != nil {
		return fmt.Errorf("Failed to begin transaction with the ledger: %v", err)
	}
	h.curBatch = nil // TODO, remove after issue 579
	return nil
}

// ExecTxs executes all the transactions listed in the txs array
// one-by-one. If all the executions are successful, it returns
// the candidate global state hash, and nil error array.
func (h *Helper) ExecTxs(id interface{}, txs []*pb.Transaction) ([]byte, error) {
	// TODO id is currently ignored, fix once the underlying implementation accepts id

	// The secHelper is set during creat ChaincodeSupport, so we don't need this step
	// cxt := context.WithValue(context.Background(), "security", h.coordinator.GetSecHelper())
	// TODO return directly once underlying implementation no longer returns []error
	res, _ := chaincode.ExecuteTransactions(context.Background(), chaincode.DefaultChain, txs)
	h.curBatch = append(h.curBatch, txs...) // TODO, remove after issue 579
	return res, nil
}

// CommitTxBatch gets invoked when the current transaction-batch needs
// to be committed. This function returns successfully iff the
// transactions details and state changes (that may have happened
// during execution of this transaction-batch) have been committed to
// permanent storage.
func (h *Helper) CommitTxBatch(id interface{}, metadata []byte) (*pb.Block, error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return nil, fmt.Errorf("Failed to get the ledger: %v", err)
	}
	// TODO fix this one the ledger has been fixed to implement
	if err := ledger.CommitTxBatch(id, h.curBatch, nil, metadata); err != nil {
		return nil, fmt.Errorf("Failed to commit transaction to the ledger: %v", err)
	}

	size := ledger.GetBlockchainSize()
	h.curBatch = nil // TODO, remove after issue 579

	block, err := ledger.GetBlockByNumber(size - 1)
	if err != nil {
		return nil, fmt.Errorf("Failed to get the block at the head of the chain: %v", err)
	}

	return block, nil
}

// RollbackTxBatch discards all the state changes that may have taken
// place during the execution of current transaction-batch
func (h *Helper) RollbackTxBatch(id interface{}) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger: %v", err)
	}
	if err := ledger.RollbackTxBatch(id); err != nil {
		return fmt.Errorf("Failed to rollback transaction with the ledger: %v", err)
	}
	h.curBatch = nil // TODO, remove after issue 579
	return nil
}

// PreviewCommitTxBatch retrieves a preview copy of the block that would be inserted into the ledger if CommitTxBatch were invoked.
// As a preview copy, it only guarantees that the hashable portions of the block will match the committed block.  Consequently,
// this preview block should only be used for hash computations and never distributed, passed into PutBlock, etc..
// The guarantee of hashable equality will be violated if additional ExecTXs calls are invoked.
func (h *Helper) PreviewCommitTxBatch(id interface{}, metadata []byte) (*pb.Block, error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return nil, fmt.Errorf("Failed to get the ledger: %v", err)
	}
	// TODO fix this once the underlying API is fixed
	block, err := ledger.GetTXBatchPreviewBlock(id, h.curBatch, metadata)
	if err != nil {
		return nil, fmt.Errorf("Failed to commit transaction to the ledger: %v", err)
	}
	return block, err
}

// GetBlock returns a block from the chain
func (h *Helper) GetBlock(blockNumber uint64) (block *pb.Block, err error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return nil, fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.GetBlockByNumber(blockNumber)
}

// GetCurrentStateHash returns the current/temporary state hash
func (h *Helper) GetCurrentStateHash() (stateHash []byte, err error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return nil, fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.GetTempStateHash()
}

// GetBlockchainSize returns the current size of the blockchain
func (h *Helper) GetBlockchainSize() (uint64, error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return 0, fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.GetBlockchainSize(), nil
}

// HashBlock returns the hash of the included block, useful for mocking
func (h *Helper) HashBlock(block *pb.Block) ([]byte, error) {
	return block.GetHash()
}

// PutBlock inserts a raw block into the blockchain at the specified index, nearly no error checking is performed
func (h *Helper) PutBlock(blockNumber uint64, block *pb.Block) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.PutRawBlock(block, blockNumber)
}

// ApplyStateDelta applies a state delta to the current state
// The result of this function can be retrieved using GetCurrentStateDelta
// To commit the result, call CommitStateDelta, or to roll it back
// call RollbackStateDelta
func (h *Helper) ApplyStateDelta(id interface{}, delta *statemgmt.StateDelta) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.ApplyStateDelta(id, delta)
}

// CommitStateDelta makes the result of ApplyStateDelta permanent
// and releases the resources necessary to rollback the delta
func (h *Helper) CommitStateDelta(id interface{}) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.CommitStateDelta(id)
}

// RollbackStateDelta undoes the results of ApplyStateDelta to revert
// the current state back to the state before ApplyStateDelta was invoked
func (h *Helper) RollbackStateDelta(id interface{}) error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.RollbackStateDelta(id)
}

// EmptyState completely empties the state and prepares it to restore a snapshot
func (h *Helper) EmptyState() error {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.DeleteALLStateKeysAndValues()
}

// VerifyBlockchain checks the integrity of the blockchain between indices start and finish,
// returning the first block who's PreviousBlockHash field does not match the hash of the previous block
func (h *Helper) VerifyBlockchain(start, finish uint64) (uint64, error) {
	ledger, err := ledger.GetLedger()
	if err != nil {
		return finish, fmt.Errorf("Failed to get the ledger :%v", err)
	}
	return ledger.VerifyChain(start, finish)
}

func (h *Helper) getRemoteLedger(peerID *pb.PeerID) (peer.RemoteLedger, error) {
	remoteLedger, err := h.coordinator.GetRemoteLedger(peerID)
	if nil != err {
		return nil, fmt.Errorf("Error retrieving the remote ledger for the given handle '%s' : %s", peerID, err)
	}

	return remoteLedger, nil
}

// GetRemoteBlocks will return a channel to stream blocks from the desired replicaID
func (h *Helper) GetRemoteBlocks(replicaID *pb.PeerID, start, finish uint64) (<-chan *pb.SyncBlocks, error) {
	remoteLedger, err := h.getRemoteLedger(replicaID)
	if nil != err {
		return nil, err
	}
	return remoteLedger.RequestBlocks(&pb.SyncBlockRange{
		Start: start,
		End:   finish,
	})
}

// GetRemoteStateSnapshot will return a channel to stream a state snapshot from the desired replicaID
func (h *Helper) GetRemoteStateSnapshot(replicaID *pb.PeerID) (<-chan *pb.SyncStateSnapshot, error) {
	remoteLedger, err := h.getRemoteLedger(replicaID)
	if nil != err {
		return nil, err
	}
	return remoteLedger.RequestStateSnapshot()
}

// GetRemoteStateDeltas will return a channel to stream a state snapshot deltas from the desired replicaID
func (h *Helper) GetRemoteStateDeltas(replicaID *pb.PeerID, start, finish uint64) (<-chan *pb.SyncStateDeltas, error) {
	remoteLedger, err := h.getRemoteLedger(replicaID)
	if nil != err {
		return nil, err
	}
	return remoteLedger.RequestStateDeltas(&pb.SyncBlockRange{
		Start: start,
		End:   finish,
	})
}
