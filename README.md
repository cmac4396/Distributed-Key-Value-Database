# Distributed Key-Value Database

Candice Mac and Mouhamadou Sissoko -- CS3700 -- 12/17/21
***

An implementation of the [Raft consensus algorithm](https://raft.github.io/) for a distributed key-value store written in Python. Raft provides a solution to log replication, a functionality that stores requests in each instance of the datastore and processes the requests when the instance is the leader of the distributed system.   

## Approach

### Leader Election

All requests and responses made to the datastore are processed through the leader node of the distributed system. Leader election describes the processes and conditions that must take place for a node to become a leader. 

An election timeout and heartbeat timeout is established for all nodes. The heartbeat timeout resets when a non-leader node receives a message from the leader node. If the leader node crashes and the heartbeat timeout is reached, then the node will send request vote messages to its neighbors. After receiving majority votes, the node will be elected as a leader. A non-leader node votes for another node if the election timing is appropriate and if the requester's log is up to date. 

### Log Replication

When a leader receives a request, it has to send the request to the other nodes in the system to ensure that all logs are up to date. This is log replication. 

After receiving a request, the leader will store the request in its own log and send the request to other nodes. When a node receives a log entry from the leader, the node decides whether to add the entry to its log, depending on the index of the entry provided. Log entries that are duplicates or inconsistent with the node's log will not be added. When a node receives an entry that conflicts with its log, all conflicting entries are removed from the log. The node sends a message to the leader, indicating whether the entry was successfully added. The leader continues to send requests only when a majority of the nodes have successfully added the entry. When a majority of the nodes have added an entry, the entry is committed to the leader's state machine. Now, this request can be processed by the leader, and the leader will send a response back to the client. If a leader receives a message indicating that the entry was not appended, it sends the previous entry to the node, assuming that the node is not up to date. Eventually, the node will contain a fully replicated log.

###         

## Challenges

We had a problem where the matchIndex was not increasing monotonically as nextIndex was being incremented, and this is
because our best approach to incrementing matchIndex at success was to set it to nextIndex - 1, which may not be true
when an AppendEntries RPC is accepted with multiple entries. In the end, we were able to successfully increment
matchIndex accurately by setting matchIndex to the length of the follower's log who accepted the AppendEntries RPC.

We were also struggling in one of the tests (advanced-2.json), where the drop rate is 3/10 of the messages. In order to
guarantee that at least one AppendEntries RPC was sent to each replica per term, we decided to make the heartbeat
timeout 1/3 of the election timeout because we send all AppendEntries RPCs at heartbeat intervals. Because we are
sending more AppendEntries RPCs per term, our number of messages increases in exchange for being able to handle this
higher drop rate.

## Testing

We used the sim.py to test how our program handles different scenarios, including leader and replica failures, drops,
and network partitioning. 
