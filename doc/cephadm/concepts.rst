Cephadm Scheduler
=================

Cephadm uses a declarative state to define the layout of the cluster. This
state consists of a list of service specificatins containing placement 
specifications (See :ref:`orchestrator-cli-service-spec` ). 

Cephadm constantly compares list of actually running daemons in the cluster
with the desired service specifications and will either add or remove new 
daemons.

First, cephadm will select a list of candidate hosts. It first looks for 
explicit host names and will select those. In case there are no explicit hosts 
defined, cephadm looks for a label specification. If there is no label defined 
in the specification, cephadm will select hosts based on a host pattern. If 
there is no pattern defined, cepham will finally select all known hosts as
candidates.

Then, cephadm will consider existing daemons of this servics and will try to 
avoid moving any daemons.

Cephadm supports the deployment of a specific amount of services. Let's 
consider a service specification like so:

.. code-block:: yaml

    service_type: mds
    service_name: myfs
    placement:
      count: 3
      label: myfs

This instructs cephadm to deploy three daemons on hosts labeld with 
``myfs`` across the cluster.

Then, in case there are less than three daemons deployed on the candidate 
hosts, cephadm will then then randomly choose hosts for deploying new daemons.

In case there are more than three daemons deployed, cephadm will remove 
existing daemons.

Finally, cephadm will remove daemons on hosts that are outside of the list of 
candidate hosts.

However, there is a special cases that cephadm needs to consider.

In case the are fewer hosts selected by the placement specification than 
demanded by ``count``, cephadm will only deploy on selected hosts.
