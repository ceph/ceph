===========================
Ceph Dashboard Design Goals
===========================

.. note:: This document is intended to provide a focal point for discussing the overall design
   principles for mgr/dashboard

Introduction
============

Most distributed storage architectures are inherently complex and can present a management challenge
to Operations teams who are typically stretched across multiple product and platform disciplines. In
general terms, the complexity of any solution can have a direct bearing on the operational costs
incurred to manage it. The answer is simple...make it simple :)

This document is intended to highlight Ceph Dashboard design goals which may help to  

* reduce complexity
* increase productivity
* improve time-to-value
* increase observability

 
Understanding the Persona of the Target User
============================================

Ceph has historically been administered from the CLI. The CLI has always and will always offer the
richest, most flexible way to install and manage a Ceph cluster. Administrators who require and
demand this level of control are unlikely to adopt a UI for anything more than a technical curiosity.

The relevance of the UI is therefore more critical for a new SysAdmin, where it can help technology
adoption and reduce the operational friction that is normally experienced when implementing a new
solution.

Understanding the target user persona is therefore a fundamental first step in design. Attempting to
design a UI that meets the requirements of a 'seasoned' Ceph Administrator or Developer, and a
relatively new SysAdmin is unlikely to satisfy either user group.

Design Principles
=================

Key Principles
______________


#. **Clarity and consistency**. The UI should ensure the data shown is unambiguous and consistent across
   different views
#. **Data timeliness**. Data displayed in the UI must be timely. State information **must** be reasonably
   recent for it to be relevant and acted upon with confidence. In addition, the age of the data should
   be shown as age (e.g. 20s ago) rather than UTC timestamps to make it more immediately consumable by
   the Administrator.
#. **Automate through workflows**. If the admin has to follow a 'recipe' to perform a task, the goal of
   the dashboard UI should be to implement the flow.
#. **Provide a natural next step**. The UI **is** the *expert system*, so instead of expecting the user
   to know where they go next, the UI should lead them. This means linking components together to
   establish a flow and deeper integration between the alertmanager implementation and the dashboard
   elements enabling an Admin to efficiently step from alert to affected component.
#. **Platform visibility**. The platform (OS and hardware configuration) is a fundamental component of the
   solution, so providing platform level insights can help deliver a more holistic view of the Ceph cluster.
#. **Jargon Busting**. Jargon is an unavoidable component of most systems. However, a good system will
   include inline help to support new and infrequent users of the UI.


Common Pitfalls
_______________

* Don't re-implement CLI commands in the UI. The sysadmin will likely use the CLI primitives in scripts
  to automate tasks, so by simply adding a CLI feature we miss the workflow and add complexity, which
  potentially 'bloats' the UI.
* Don't think like a developer...try and adopt the mindset of an Administrator, who only works with the
  Ceph cluster part-time - this is the reality for today's Operations teams.


Focus On User Experience
========================
Ultimately, the goal must be to move away from pushing complexity onto the GUI user through multi-step
workflows like iSCSI configuration or setting specific cluster flags in defined sequences. Simplicity
should be the goal for the UI...let's leave the complexity to the CLI.
