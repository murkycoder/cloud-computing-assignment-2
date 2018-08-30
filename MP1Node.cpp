/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TGOSSIP;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   memberNode->inGroup = false;
   memberNode->nnb = 0;
   memberNode->memberList.clear();
   memberNode->heartbeat = 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    bool processed = false;
    MessageHdr msgHdr;
    memcpy(&msgHdr, data, sizeof(MessageHdr));
    
    char * dataWithoutHdr = data + sizeof(MessageHdr);

    switch(msgHdr.msgType){
        case JOINREQ:
            processed = processJoinReq(dataWithoutHdr);
            break;
        case JOINREP:
            processed = processJoinRep(dataWithoutHdr);
            break;
        case GOSSIP:
            processed = processGossip(dataWithoutHdr);
            break;
        default:
            break;
    }
    return processed;
    
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // if ping counter expires
    // then gossip your list to others
    if(memberNode->pingCounter == 0 ){
        memberNode->heartbeat++;
        memberNode->memberList[0].heartbeat++;
        memberNode->memberList[0].timestamp = par->getcurrtime();
        gossip();
        memberNode->pingCounter = TGOSSIP;
    }
    // otherwise continue until counter expires
    else{
        memberNode->pingCounter--;
    }

    // check for failed nodes in membership list
    checkForFailures();
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    updateMembershipList(memberNode->addr, memberNode->heartbeat);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
/**
 * FUNCTION NAME: processJoinReq
 *
 * DESCRIPTION: 
 */
bool MP1Node::processJoinReq(char * data){
    Address addr;
    memcpy(&(addr.addr), data, sizeof(Address));
    long heartbeat;
    memcpy(&heartbeat, (data + sizeof(Address)), sizeof(long));
    updateMembershipList(addr, heartbeat);

    size_t msgSize = sizeof(MessageHdr) + sizeof(Address) + sizeof(heartbeat);
    char * msg = (char *)malloc(msgSize);
    MessageHdr * msgHdr = (MessageHdr*)msg;
    msgHdr->msgType = JOINREP;
    memcpy(msg + sizeof(MessageHdr), &(memberNode->addr), sizeof(Address));
    memcpy(msg + sizeof(Address) + sizeof(MessageHdr), &(memberNode->heartbeat), sizeof(long));

    emulNet->ENsend(
        &(memberNode->addr), 
        &addr,
        msg,
        msgSize
    );

    msgHdr = NULL;
    free(msg);

    return true;
}
/**
 * FUNCTION NAME: processJoinRep
 *
 * DESCRIPTION: 
 */
bool MP1Node::processJoinRep(char * data){

    memberNode->inGroup = true;
    Address addr;
    memcpy(&(addr.addr), data, sizeof(Address));
    long heartbeat;
    memcpy(&heartbeat, (data + sizeof(Address)), sizeof(long));
    updateMembershipList(addr, heartbeat);

     return true;
}
/**
 * FUNCTION NAME: processGossip
 *
 * DESCRIPTION: 
 */
bool MP1Node::processGossip(char * data){
    int listSize;
    memcpy(&listSize, data, sizeof(int));
    size_t offset = sizeof(int);
    for(int i = 0; i < listSize; i++){
        //Address addr;
        int id;
        short port;
        long heartbeat;
        memcpy(&id, data + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&port, data + offset, sizeof(short));
        offset += sizeof(short);
        memcpy(&heartbeat, data + offset, sizeof(long));
        offset += sizeof(long);
        Address addr;
        memcpy(&(addr.addr[0]), &id, sizeof(int));
        memcpy(&(addr.addr[4]), &port, sizeof(short));
            
        updateMembershipList(addr, heartbeat);
    }
    return true;
}
/**
 * FUNCTION NAME: gossip
 *
 * DESCRIPTION: 
 */
bool MP1Node::gossip(){
    int timestamp = par->getcurrtime();
    int listSize = countNonFaulty(timestamp);
    
    size_t msgSize = sizeof(MessageHdr) + sizeof(int) +
                     listSize*(sizeof(Address) + sizeof(long));
    char * msg = (char *)malloc(msgSize);
    MessageHdr * msgHdr = (MessageHdr*)msg;
    msgHdr->msgType = GOSSIP;
    size_t offset = sizeof(MessageHdr);
    memcpy(msg + offset, &listSize, sizeof(int));
    offset += sizeof(int);
    for( auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); it++){
            if( timestamp - it->timestamp < TFAIL){
                memcpy(msg + offset, &(it->id), sizeof(int));
                offset += sizeof(int);
                memcpy(msg + offset, &(it->port), sizeof(short));
                offset += sizeof(short);
                memcpy(msg + offset, &(it->heartbeat), sizeof(long));
                offset += sizeof(long);
        }
    }
    for( auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); it++){
            Address addr;
            memcpy(&(addr.addr[0]), &(it->id), sizeof(int));
            memcpy(&(addr.addr[4]), &(it->port), sizeof(short));
            emulNet->ENsend(&(memberNode->addr), &addr, msg, msgSize);
    }
    msgHdr = NULL;
    free(msg);
    return true;
        
}
/**
 * FUNCTION NAME: updateMembershipList
 *
 * DESCRIPTION: update / add entry in the membership list 
 *              
 *             
 * RETURN: return true if modification made
 *                false otherwise
 */
bool MP1Node::updateMembershipList(Address addr, long heartbeat){
    printMembershipList();
    for( auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); it++){
            int id;
            short port;
            memcpy(&id, &(addr.addr[0]), sizeof(int));
            memcpy(&port, &(addr.addr[4]), sizeof(short));
            if(id == it->id && port == it->port) {
               if(it->heartbeat < heartbeat){
                    // update in list
                    it->settimestamp(par->getcurrtime());
                    it->setheartbeat(heartbeat);
                    return true;
                }
                // else no change
                return false;
            }
    }
    int id; 
    short port;
    memcpy(&id, &(addr.addr[0]), sizeof(int));
    memcpy(&port, &(addr.addr[4]), sizeof(short));
    memberNode->memberList.emplace_back(MemberListEntry(id, port, heartbeat, par->getcurrtime()));
    #ifdef DEBUGLOG
        log->logNodeAdd(&(memberNode->addr), &addr);
    #endif

    return true;
}
/**
 * FUNCTION NAME: checkForFailures
 *
 * DESCRIPTION: 
 */
void MP1Node::checkForFailures(){
    for(auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end();
        ){
            if(par->getcurrtime() - it->timestamp > TREMOVE){
                #ifdef DEBUGLOG
                    Address addr;
                    memcpy(&(addr.addr[0]), &(it->id), sizeof(int));
                    memcpy(&(addr.addr[4]), &(it->port), sizeof(short));
                    log->logNodeRemove(&memberNode->addr, &addr);
                #endif
                it = memberNode->memberList.erase(it);
            } else {
                it++;
            }
    }
}
/**
 * FUNCTION NAME: printMembershipList
 *
 * DESCRIPTION: 
 */
void MP1Node::printMembershipList(){
    printf("[ %d ] Member list\n");
    for(auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end();
        it++){
            printf("\t%d:%d\t%ld\t%d\n", it->id, it->port, it->heartbeat, it->timestamp);
    }
    printf("\n");
}
int MP1Node::countNonFaulty(int timestamp){
    int count = 0;
    for(auto it = memberNode->memberList.begin();
        it != memberNode->memberList.end();
        it++){
            if(timestamp - it->timestamp < TFAIL)  
                count++;
    }
    return count;
}