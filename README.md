# Distributed Log Consensus Algorithm

This project simulates a log consensus algorithm for distributed systems. This includes leader election, log consensus agreement, and fault-tolerant key-value storage. The goal is to maintain a consistent, replicated log of operations. Servers accept client operations
through <tt>Start()</tt>, and insert them into the log. In
this consensus model, only the leader is allowed to append to the log, and
disseminates new entries to other servers by including
them in its outgoing <tt>AppendEntries</tt> RPCs. Each server communicates to others through RPCs. Furthermore, there is a fault-tolerant key-value storage structured as a replicated state machine 
with several key-value servers that coordinate their activities
through the Raft log. 

This project was completed by me based off of skeleton code for a class assignment. Primary files written were raft.go, common.go, client.go, and server.go
