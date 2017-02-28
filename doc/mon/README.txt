paxos-call-chain.dot describe some details about the call chain being involved in the
Paxos algorithm, paying special consideration to the messages involved.

"PAXOS" : Paxos is a distributed consensus protocol which is used in many theorems like CAP theorem in which it helps to maintain consistency (linearizable consistency) across replicas. There is one other protocol like this named Raft.

This information is not easily obtainable by Doxygen, as it does not follow
the call chain when messages are involved, since it becomes an async workflow.

To obtain the graph one should run:

  dot -T<format> paxos-call-chain.dot -o paxos-call-chain.<format>

e.g.,

  dot -Tps paxos-call-chain.dot -o paxos-call-chain.ps

or

  dot -Tpng paxos-call-chain.dot -o paxos-call-chain.png

It should do the trick.

Also, for future reference, we consider that:
  - boxed nodes refer to the Leader;
  - elliptical nodes refer to the Peon;
  - diamond shaped nodes refer to state changes;
  - dotted lines illustrate a message being sent from the Leader to the Peon,
  or vice-versa.

