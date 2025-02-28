============================
CRUSH MSR (Multi-step Retry)
============================

Motivation
----------

Conventional CRUSH has an important limitation: rules with
multiple `choose` steps which hit an `out` osd cannot retry
prior steps.  As an example, with a rule like
::

    rule replicated_rule_1 {
        ...
        step take default class hdd
        step chooseleaf firstn 3 type host
        step emit
    }

one might expect that if all of the OSDs on a particular host
are marked out, mappings including those OSDs would end up
on another host (provided that there are enough hosts).  Indeed,
that's what will happen.  Moreover, if 1/8 OSDs on a host are
marked out, roughly 1/8 of the PGs mapped to that host will end
up remapped to some other host keeping overall per-OSD utilization
even.

Suppose, instead, the rule were written like this:
::

    rule replicated_rule_1 {
        ...
        step take default class hdd
        step choose firstn 3 type host
        step choose firstn 1 type osd
        step emit
    }

The behavior would be very similar as long as no OSDs are marked
out.  However, if an OSD is marked out, any PGs mapped to that
OSD will be remapped to other OSDs on the same host resulting in
those OSDs being over-utilized relative to OSDs on other hosts.
Moreover, if all of the OSDs on a host are marked out, mappings
that happen to hit that host will fail resulting in undersized PGs.

As long as the goal is to split N OSDs between N failure domains,
the solution is simply to use the `chooseleaf` variant above.  However,
consider a use case where we want to split an 8+6 EC encoding over 4
hosts in order to tolerate the loss of a host and an OSD on another
host with 1.75x storage overhead.  The rule would have to look
something like:
::

    rule ecpool-86 {
        ...
        step take default class hdd
        step choose indep 4 type host
        step choose indep 4 type osd
        step emit
    }

This does split up to 16 OSDs between 4 hosts (with an 8+6 code,
it would put 4 OSDs on each of the first 3 and 2 on the last) and
meets our failure requirements.  However, for the reasons outlined
above, it will behave poorly as OSDs are marked out if there are
other hosts to rebalance to.  `chooseleaf` is not a solution here
because it does not support mapping more than one leaf below the
specified type.

MSR
---

CRUSH MSR (Multi-step Retry) rules solve the above problem by using a
different descent algorithm which retries all of the steps upon
hitting an out OSD.  Where classic CRUSH is breadth first (for each
step, it fully populates the vector before proceeding to the next
step), MSR rules are depth first -- for each choice, we recursively
descend through all of the steps before continuing with the next
choice.  The above use case can be satisfied with the following rule:

::

    rule ecpool-86 {
        type msr_indep
        ...
        step take default class hdd
        step choosemsr 4 type host
        step choosemsr 4 type osd
        step emit
    }

As with the `chooseleaf` example at the top, as OSDs are marked out,
those OSDs are be remapped proportionately to other hosts so long as
there are extras available.  For details on how that works while
still preserving failure domain isolation, see the comments in
mapper.c:crush_msr_choose.

Rule Structure
--------------

CRUSH MSR rules are crush rules with type CRUSH_RULE_TYPE_MSR_FIRSTN
or CRUSH_RULE_TYPE_MSR_INDEP (see mapper.c: rule_type_is_msr).  Unlike
with classic crush rules, individual steps do not specify firstn or
indep.  The output order is instead defined by the rule type for the
whole rule.

MSR rules have some structural differences from conventional rules:

- The rule type determines whether the mapping is FIRSTN or INDEP.
  Because the descent can retry steps, it doesn't really make sense
  for steps to individually specify output order and I'm not really
  aware of any use cases that would benefit from it.
- MSR rules *must* be structured as a (possibly empty) prefix of
  config steps (CRUSH_RULE_SET_CHOOSE_MSR*) followed by a sequence of
  EMIT blocks each comprised of a TAKE step, a sequence of CHOOSE_MSR
  steps, and ended by an EMIT step.
- MSR steps must be `choosemsr`.  `choose` and `chooseleaf` are not
  permitted.

Working Space
-------------

MSR rules also have different requirements for working space.
Conventional CRUSH requires 3 vectors of size result_max to use for
working space -- two to alternate as it processes each rule and one,
additionally, for `chooseleaf`.  MSR rules need N vectors where N is the
number of `choosemsr` steps in the longest EMIT block since it needs to
retain all of the choices made as part of each descent.

See mapper.h/c:crush_work_size, crush_msr_scan_rule for details.

Implementation
--------------

mapper.h/c:crush_do_rule internally branches to
mapper.c:crush_msr_do_rule for rules of type CRUSH_RULE_TYPE_MSR_*
(see mapper.c:rule_type_is_msr).

MSR related functions in mapper.c are annotated with more details
about the algorithm.
