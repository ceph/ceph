Issue Tracker
=============

See `Redmine Issue Tracker`_ for a brief introduction to the Ceph Issue
Tracker.

Ceph developers use the issue tracker to

1. keep track of issues - bugs, fix requests, feature requests, backport
requests, etc.

2. communicate with other developers and keep them informed as work
on the issues progresses.

Issue tracker conventions
-------------------------

When you start working on an existing issue, it's nice to let the other
developers know this - to avoid duplication of labor. Typically, this is
done by changing the :code:`Assignee` field (to yourself) and changing the
:code:`Status` to *In progress*. Newcomers to the Ceph community typically do
not have sufficient privileges to update these fields, however: they can
simply update the issue with a brief note.

.. table:: Meanings of some commonly used statuses

   ================ ===========================================
   Status           Meaning
   ================ ===========================================
   New              Initial status
   In Progress      Somebody is working on it
   Need Review      Pull request is open with a fix
   Pending Backport Fix has been merged, backport(s) pending
   Resolved         Fix and backports (if any) have been merged
   ================ ===========================================

.. _Redmine issue tracker: https://tracker.ceph.com
