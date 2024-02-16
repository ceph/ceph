.. _dev_mon_elections:

=================
Monitor Elections
=================

The Original Algorithm
======================
Historically, monitor leader elections have been very simple: the lowest-ranked
monitor wins!

This is accomplished using a low-state "Elector" module (though it has now
been split into an Elector that handles message-passing, and an ElectionLogic
that makes the voting choices). It tracks the election epoch and not much
else. Odd epochs are elections; even epochs have a leader and let the monitor
do its ongoing work. When a timeout occurs or the monitor asks for a
new election, we bump the epoch and send out Propose messages to all known
monitors.
In general, if we receive an old message we either drop it or trigger a new
election (if we think the sender is newly-booted and needs to join quorum). If
we receive a message from a newer epoch, we bump up our epoch to match and
either Defer to the Proposer or else bump the epoch again and Propose
ourselves if we expect to win over them. When we receive a Propose within
our current epoch, we either Defer to the sender or ignore them (we ignore them
if they are of a higher rank than us, or higher than the rank we have already
deferred to).
(Note that if we have the highest rank it is possible for us to defer to every
other monitor in sequence within the same election epoch!)

This resolves under normal circumstances because all monitors agree on the
priority voting order, and epochs are only bumped when a monitor isn't
participating or sees a possible conflict with the known proposers.

The Problems
==============
The original algorithm didn't work at all under a variety of netsplit
conditions. This didn't manifest often in practice but has become
important as the community and commercial vendors move Ceph into
spaces requiring the use of "stretch clusters".

The New Algorithms
==================
We still default to the original ("classic") election algorithm, but
support letting users change to new ones via the CLI. These
algorithms are implemented as different functions and switch statements
within the ElectionLogic class.

The first algorithm is very simple: "disallow" lets you add monitors
to a list of disallowed leaders.
The second, "connectivity", incorporates connection score ratings
and elects the monitor with the best score.

Algorithm: disallow
===================
If a monitor is in the disallowed list, it always defers to another
monitor, no matter the rank. Otherwise, it is the same as the classic
algorithm is.
Since changing the disallowed list requires a paxos update, monitors
in an election together should always have the same set. This means
the election order is constant and static across the full monitor set
and elections resolve trivially (assuming a connected network).

This algorithm really just exists as a demo and stepping-stone to
the more advanced connectivity mode, but it may have utility in asymmetric
networks and clusters.

Algorithm: connectivity
=======================
This algorithm takes as input scores for each connection
(both ways, discussed in the next section) and attempts to elect the monitor
with the highest total score. We keep the same basic message-passing flow as the
classic algorithm, in which elections are driven by reacting to Propose messages.
But this has several challenges since unlike ranks, scores are not static (and
might change during an election!). To guarantee an election epoch does not
produce multiple leaders, we must maintain two key invariants:
* Monitors must maintain static scores during an election epoch
* Any deferral must be transitive -- if A defers to B and then to C,
B had better defer to C as well!

We handle these very explicitly: by branching a copy stable_peer_tracker
of our peer_tracker scoring object whenever starting an election (or
bumping the epoch), and by refusing to defer to a monitor if it won't
be deferred to by our current leader choice. (All Propose messages include
a copy of the scores the leader is working from, so peers can evaluate them.)

Of course, those modifications can easily block. To guarantee forward progress,
we make several further adjustments:
* If we want to defer to a new peer, but have already deferred to a peer
whose scores don't allow that, we bump the election epoch and start()
the election over again.
* All election messages include the scores the sender is aware of.

This guarantees we will resolve the election as long as the network is
reasonably stable (even if disconnected): As long as all score "views"
result in the same deferral order, an election will complete normally. And by
broadly sharing scores across the full set of monitors, monitors rapidly
converge on the global newest state.

This algorithm has one further important feature compared to the classic and
disallowed handlers: it can ignore out-of-quorum peers. Normally, whenever
a monitor B receives a Propose from an out-of-quorum peer C, B will itself trigger
a new election to give C an opportunity to join. But because the
highest-scoring monitor A may be netsplit from C, this is not desirable. So in
the connectivity election algorithm, B only "forwards" Propose messages when B's
scores indicate the cluster would choose a leader other than A.

Connection Scoring
==================
We implement scoring within the ConnectionTracker class, which is
driven by the Elector and provided to ElectionLogic as a resource. Elector
is responsible for sending out MMonPing messages, and for reporting the
results in to the ConnectionTracker as calls to report_[live|dead]_connection
with the relevant peer and the time units the call counts for. (These time units
are seconds in the monitor, but the ConnectionTracker is agnostic and our unit
tests count simple time steps.)

We configure a "half life" and each report updates the peer's current status
(alive or dead) and its total score. The new score is current_score * (1 - units_alive / (2 * half_life)) + (units_alive / (2 * half_life)). (For a dead report, we of course
subtract the new delta, rather than adding it).

We can further encode and decode the ConnectionTracker for wire transmission,
and receive_peer_report()s of a full ConnectionTracker (containing all
known scores) or a ConnectionReport (representing a single peer's scores)
to slurp up the scores from peers. These scores are of course all versioned so
we are in no danger of accidentally going backwards in time.
We can query an individual connection score (if the connection is down, it's 0)
or the total score of a specific monitor, which is the connection score from all
other monitors going in to that one.

By default, we consider pings failed after 2 seconds (mon_elector_ping_timeout)
and ping live connections every second (mon_elector_ping_divisor). The halflife
is 12 hours (mon_con_tracker_score_halflife).
