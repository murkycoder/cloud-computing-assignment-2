/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
QuorumTracker::QuorumTracker(MessageType type, int transID, string key, string value){
	this->type = type;
	this->transID = transID;
	this->key = string(key);
	this->value = string(value);
	this->totalReplies = 0;
	this->successfulReplies = 0;
}

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	for (auto it = quorumTrackers.begin(); it != quorumTrackers.end(); it++){
		QuorumTracker * qt = it->second;
		delete qt;
	}
	quorumTrackers.clear();
}

/**
 * FUNCTION NAME: addQuorumTracker
 *
 * DESCRIPTION: create a quorum tracker object and add it to this node's tracker list
 *
 */
QuorumTracker * MP2Node::addQuorumTracker(MessageType type, string key, string value) {
	int transID = g_transID++;
	QuorumTracker * qt = new QuorumTracker(type, transID, key, value);
	quorumTrackers[transID] = qt;
	return qt;
}

/**
 * FUNCTION NAME: sendMessage
 *
 * DESCRIPTION: send message to replicas
 * 
 */
void MP2Node::sendMessage(QuorumTracker* qt){
	vector<Node> replicas = findNodes(qt->key);
	if(replicas.size() >= 3){
		Message msg = Message(qt->transID,
							memberNode->addr,
							qt->type, 
							qt->key, 
							qt->value,
							PRIMARY);
		emulNet->ENsend(&memberNode->addr, 
						&(replicas[0].nodeAddress),
						msg.toString());

		msg.replica = SECONDARY;
		emulNet->ENsend(&memberNode->addr, 
						&(replicas[1].nodeAddress),
						msg.toString());

		msg.replica = TERTIARY;
		emulNet->ENsend(&memberNode->addr, 
						&(replicas[2].nodeAddress),
						msg.toString());
		return;
	}
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	QuorumTracker * qt = addQuorumTracker(CREATE, key, value);
	sendMessage(qt);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	QuorumTracker * qt = addQuorumTracker(READ, key, "");
	sendMessage(qt);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	QuorumTracker * qt = addQuorumTracker(UPDATE, key, "");
	sendMessage(qt);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	QuorumTracker * qt = addQuorumTracker(DELETE, key, "");
	sendMessage(qt);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(int transID, string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	if(ht->create(key, value)) {
		log->logCreateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
	} else {
		log->logCreateFail(&memberNode->addr, false, transID, key, value);
		return false;
	}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(int transID, string key) {
	// Read key from local hash table and return value
	string value = ht->read(key);
	if(value != "") {
		log->logReadSuccess(&memberNode->addr, false, transID, key, value);
	} else {
		log->logReadFail(&memberNode->addr, false, transID, key);
	}
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(int transID, string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	if(ht->update(key, value)) {
		log->logUpdateSuccess(&memberNode->addr, false, transID, key, value);
		return true;
	} else {
		log->logUpdateFail(&memberNode->addr, false, transID, key, value);
		return false;
	}
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(int transID, string key) {
	// Delete the key from the local hash table
	if(ht->deleteKey(key)) {
		log->logDeleteSuccess(&memberNode->addr, false, transID, key);
		return true;
	} else {
		log->logDeleteFail(&memberNode->addr, false, transID, key);
		return false;
	}
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		Message *rvcdMsg = new Message(string(data, data + size));
		Message *replyMsg;
		/*
		 * Handle the message types here
		 */
		switch(rvcdMsg->type) {
			case CREATE:
				replyMsg = new Message(rvcdMsg->transID,	
									   memberNode->addr,
									   REPLY,
									   false);
				replyMsg->success = createKeyValue(rvcdMsg->transID,
												   rvcdMsg->key,
												   rvcdMsg->value, 
												   rvcdMsg->replica);
				emulNet->ENsend(&memberNode->addr,
								&rvcdMsg->fromAddr,
								replyMsg->toString());
				delete(replyMsg);	
				break;

			case READ:
				replyMsg = new Message(rvcdMsg->transID,	
									   memberNode->addr,
									   READREPLY,
									   "");
				replyMsg->value = readKey(rvcdMsg->transID,
										  rvcdMsg->key);
				emulNet->ENsend(&memberNode->addr,
								&rvcdMsg->fromAddr,
								replyMsg->toString());
				delete(replyMsg);	
				break;

			case UPDATE:
			 	replyMsg = new Message(rvcdMsg->transID,	
									   memberNode->addr,
									   REPLY,
									   false);
				replyMsg->success = updateKeyValue(rvcdMsg->transID,
												   rvcdMsg->key,
												   rvcdMsg->value, 
												   rvcdMsg->replica);
				emulNet->ENsend(&memberNode->addr,
								&rvcdMsg->fromAddr,
								replyMsg->toString());
				delete(replyMsg);	
				break;
			case DELETE:
				replyMsg = new Message(rvcdMsg->transID,	
									   memberNode->addr,
									   REPLY,
									   false);
				replyMsg->success = deletekey(rvcdMsg->transID,
												   rvcdMsg->key);
				emulNet->ENsend(&memberNode->addr,
								&rvcdMsg->fromAddr,
								replyMsg->toString());
				delete(replyMsg);	
				break;
			case REPLY:
				if(quorumTrackers.find(rvcdMsg->transID) != quorumTrackers.end()){
					QuorumTracker * qt = quorumTrackers[rvcdMsg->transID];
					qt->totalReplies++;
					if(rvcdMsg->success)
						qt->successfulReplies++;
					checkForQuorum(rvcdMsg->transID);
				}
				break;
			case READREPLY:
				if(quorumTrackers.find(rvcdMsg->transID) != quorumTrackers.end()){
					QuorumTracker * qt = quorumTrackers[rvcdMsg->transID];
					qt->totalReplies++;
					if(rvcdMsg->value != ""){
						qt->successfulReplies++;
						qt->value = rvcdMsg->value;
					}
					checkForQuorum(rvcdMsg->transID);
				}
				break;
			default:
				break;
		}

	}
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	checkForFailuresAndQuorum();
}

/**
 * FUNCTION NAME: checkForQuorum
 *
 * DESCRIPTION: check for quorum for quorumTrackers
 * 
 */
void MP2Node::checkForQuorum(int transID) {

	if (quorumTrackers.find(transID) == quorumTrackers.end())
		return;

	QuorumTracker * qt = quorumTrackers[transID];

	if (qt->successfulReplies == 2) {
		switch(qt->type) {
			case CREATE:
				log->logCreateSuccess(&memberNode->addr, true, qt->transID, qt->key, qt->value);
				break;
			case READ:
				log->logReadSuccess(&memberNode->addr, true,  qt->transID, qt->key, qt->value);
				break;
			case UPDATE:
				log->logUpdateSuccess(&memberNode->addr, true,  qt->transID, qt->key, qt->value);
				break;
			case DELETE:
				log->logDeleteSuccess(&memberNode->addr, true,  qt->transID, qt->key);
				break;
			default:
				break;
		}
		quorumTrackers.erase(qt->transID);
		free(qt);
	} else if (qt->totalReplies == 3 && qt->successfulReplies < 2) {
		switch(qt->type) {
			case CREATE:
				log->logCreateFail(&memberNode->addr, true,  qt->transID, qt->key, qt->value);
				break;
			case READ:
				log->logReadFail(&memberNode->addr, true, qt->transID, qt->key);
				break;
			case UPDATE:
				log->logUpdateFail(&memberNode->addr, true, qt->transID, qt->key, qt->value);
				break;
			case DELETE:
				log->logDeleteFail(&memberNode->addr, true,  qt->transID, qt->key);
				break;
			default:
				break;
		}
		quorumTrackers.erase(qt->transID);
		free(qt);
	}
}

/**
 * FUNCTION NAME: checkForQuorum
 *
 * DESCRIPTION: check for quorum for quorumTrackers
 * 
 */
void MP2Node::checkForFailuresAndQuorum() {

}
/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	
}
