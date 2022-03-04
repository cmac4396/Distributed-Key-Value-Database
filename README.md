# Distributed Key-Value Database

Candice Mac and Mouhamadou Sissoko -- CS3700 -- 12/17/21
***

## Approach
***

### Leader Election

Talk about what was successful from section 5.2: leader election:
For our leader election, we essentially started off with initializing our election timeouts and heartbeat timeouts. Once
we set that up, we made it so that if, time between messages is greater than the election time out, then we would start
an election. After we implemented the starting of elections, we sent out request votes and the candidate with at least
Number of Replicas / 2 then the that candidate wins the election and immediately sends out heartbeats. For our
implementation of leader election, some election restrictions that we added are being at least-up-to-date with the
majority of the cluster, we use our request vote rpc to enforce this. It will either decline the vote for the candidate,
if the candidate isn't at least up to date with its log or accept the vote, if it is.

### Log Replication

For our implementation of log replication, we start off by handling client requests that are sent to non-leaders, by
simply redirecting them to the leader. In the event that the leader, receives a client request, it'll start by appending
the message to its log and sending append entries to the other replicas. Once the replicas receive the append entries,
they append it to their own logs and sends a reply back that has a field, which indicates whether it was successful or
not. If it is successful, the leader will increment its quorum and in the case that it isn't successful, we decrement
the nextIndex and send it with the next batch of entries, after the heartbeat election is over. For our quorum, we don't
commit on our end, until we get a majority of replicas have succeeded in accepting the append entry and once we do, we
will passively commit on the other replicas with an append entry. Followers send the reply with the success field as
false, if the term of the leader is less than the follower's term or if the log doesn't contain an entry as the leader's
most recent log index at the time the append entry rpc was sent. We also handle conflicting entries, by removing the
conflicting entries in the log and everything after it

###         

## Challenges

***
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

***
We used the sim.py to test how our program handles different scenarios, including leader and replica failures, drops,
and network partitioning. 