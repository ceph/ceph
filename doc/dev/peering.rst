======================
Peering
======================

Concepts
--------

*Peering*
   the process of bringing all of the OSDs that store
   a Placement Group (PG) into agreement about the state 
   of all of the objects (and their metadata) in that PG.
   Note that agreeing on the state does not mean that 
   they all have the latest contents. 

*Active Set*
   the set of OSDs who are (or were as of some epoch)
   in the list of nodes to store a particular PG.

*primary*
   the (by convention first) member of the *acting set*,
   who is the only OSD that will accept client initiated
   writes to objects in a placement group.

*replica*
   a non-primary OSD in the *acting set* for a placement group
   (and who has been recognized as such and *activated* by the primary).

*stray*
   an OSD who is not a member of the current *acting set*, but
   has not yet been told that it can delete its copies of a
   particular placement group.

*recovery*
   ensuring that copies of all of the objects in a PG
   are on all of the OSD's in the *acting set*.  Once
   *peering* has been performed, the primary can start
   accepting write operations, and *recovery* can proceed
   in the background.

*PG log*
   a list of recent updates made to objects in a PG.
   Note that these logs can be truncated after all OSDs
   in the *acting set* have acknowledged up to a certain
   point.

*back-log*
   If the failure of an OSD makes it necessary to replicate
   operations that have been truncated from the most recent
   PG logs, it will be necessary to reconstruct the missing
   information by walking the object space and generating
   log entries for
   operations to create the existing objects in their existing
   states.  While a back-log may be different than the actual
   set of operations that brought the PG to its current state,
   it is equivalent ... and that is good enough.

*missing set*
   Each OSD notes update log entries and if they imply updates to
   the contents of an object, adds that object to a list of needed
   updates.  This list is called the *missing set* for that <OSD,PG>.

*Authoritative History*
   a complete, and fully ordered set of operations that, if 
   performed, would bring an OSD's copy of a Placement Group 
   up to date.  

*epoch*
   a (monotonically increasing) OSD map version number

*last epoch start*
   the last epoch at which all nodes in the *acting set*
   for a particular placement group agreed on an 
   *authoritative history*.  At this point, *peering* is 
   deemed to have been successful.
   
*up through*
   when a primary successfully completes the *peering* process,
   it informs a monitor that an *authoritative history* has
   been established (for that PG) **up through** the current
   epoch, and the primary is now going active.

*last epoch clean*
   the last epoch at which all nodes in the *acting set*
   for a particular placement group were completely
   up to date (both PG logs and object contents).
   At this point, *recovery* is deemed to have been
   completed.

Description of the Peering Process
----------------------------------

The *Golden Rule* is that no write operation to any PG
is acknowledged to a client until it has been persisted 
by all members of the *acting set* for that PG.  This means
that if we can communicate with at least one member of
each *acting set* since the last successful *peering*, someone
will have a record of every (acknowledged) operation 
since the last successful *peering*.  
This means that it should be possible for the current 
primary to construct and disseminate a new *authoritative history*.

It is also important to appreciate the role of the OSD map 
(list of all known OSDs and their states, as well as some
information about the placement groups) in the *peering* 
process:

   When OSDs go up or down (or get added or removed)
   this has the potential to affect the *active sets*
   of many placement groups.

   When a primary successfully completes the *peering*
   process, this too is noted in the OSD map (*last epoch start*).

   Changes can only be made after successful *peering*
   (recorded in the PAXOS stream as a an "up through").

Thus, if a new primary has a copy of the latest OSD map,
it can infer which *active sets* may have accepted updates,
and thus which OSDs must be consulted before we can successfully
*peer*.
   
The high level process is for the current PG primary to:

  1. get the latest OSD map (to identify the members of the
     all interesting *acting sets*, and confirm that we are still the primary).

  2. generate a list of all of the acting sets (that achieved
     *last epoch start*) since the *last epoch clean*.  We can
     ignore acting sets that did not achieve *last epoch start*
     because they could not have accepted any updates.

     Successfull *peering* will require that we be able to contact at
     least one OSD from each of these *acting sets*.

  3. ask every node in that list what its first and last PG log entries are
     (which gives us a complete list of all known operations, and enables
     us to make a list of what log entries each member of the current
     *acting set* does not have).

  4. if anyone else has (in his PG log) operations that I do not have, 
     instruct them to send me the missing log entries 
     (constructing a *back-log* if necessary).

  5. for each member of the current *acting set*:

     a) ask him for copies of all PG log entries since *last epoch start*
        so that I can verify that they agree with mine (or know what 
        objects I will be telling him to delete).

        If the cluster failed before an operation was persisted by all 
        members of the *acting set*, and the subsequent *peering* did not
        remember that operation, and a node that did remember that 
        operation later rejoined, his logs would record a different
        (divergent) history than the *authoritative history* that was
        reconstructed in the *peering* after the failure.

        Since the *divergent* events were not recorded in other logs
        from that *acting set*, they were not acknowledged to the client,
        and there is no harm in discarding them (so that all OSDs agree
        on the *authoritative history*).  But, we will have to instruct
        any OSD that stores data from a divergent update to delete the
        affected (and now deemed to be apocryphal) objects.

     b) ask him for his *missing set* (object updates recorded
        in his PG log, but for which he does not have the new data).
        This is the list of objects that must be fully replicated
        before we can accept writes.

  6. at this point, my PG log contains an *authoritative history* of
     the placement group (which may have involved generating a *back-log*), 
     and I now have sufficient
     information to bring any other OSD in the *acting set* up to date.  
     I can now inform a monitor
     that I am "up through" the end of my *aurhoritative history*.  

     The monitor will persist this through PAXOS, so that any future
     *peering* of this PG will note that the *acting set* for this 
     interval may have made updates to the PG and that a member of
     this *acting set* must be included the next *peering*.
     
     This makes me active as the *primary* and establishes a new *last epoch start*.

  7. for each member of the current *acting set*:

     a) send them log updates to bring their PG logs into agreement with
        my own (*authoritative history*) ... which may involve deciding
        to delete divergent objects.

     b) await acknowledgement that they have persisted the PG log entries.

  8. at this point all OSDs in the *acting set* agree on all of the meta-data,
     and would (in any future *peering*) return identical accounts of all
     updates.

     a) start accepting client write operations (because we have unanimous
        agreement on the state of the objects into which those updates are
        being accepted).  Note, however, that we will delay any attempts to
        write to objects that are not yet fully replicated throughout the
        current *acting set*.

     b) start pulling object data updates that other OSDs have, but I do not.

     c) start pushing object data updates to other OSDs that do not yet have them.

        We push these updates from the primary (rather than having the replicas
        pull them) because this allows the primary to ensure that a replica has
        the current contents before sending it an update write.  It also makes
        it possible for a single read (from the primary) to be used to write
        the data to multiple replicas.  If each replica did its own pulls,
        the data might have to be read multiple times.

  9. once all replicas store the all copies of all objects (that existed
     prior to the start of this epoch) we can dismiss all of the *stray*
     replicas, allowing them to delete their copies of objects for which
     they are no longer in the *acting set*.  

     We could not dismiss the *strays* prior to this because it was possible
     that one of those *strays* might hold the sole surviving copy of an
     old object (all of whose copies disappeared before they could be
     replicated on members of the current *acting set*).
     
State Model
-----------

.. graphviz:: peering_graph.generated.dot
