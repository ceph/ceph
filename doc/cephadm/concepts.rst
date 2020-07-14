Cephadm Concepts
================

.. _cephadm-fqdn:

Fully qualified domain names vs bare host names
-----------------------------------------------

cephadm has very minimal requirements when it comes to resolving host
names etc. When cephadm initiates an ssh connection to a remote host,
the host name  can be resolved in four different ways:

-  a custom ssh config resolving the name to an IP
-  via an externally maintained ``/etc/hosts``
-  via explictly providing an IP address to cephadm: ``ceph orch host add <hostname> <IP>``
-  automatic name resolution via DNS.

Ceph itself uses the command ``hostname`` to determine the name of the
current host.

.. note::

  cephadm demands that the name of the host given via ``ceph orch host add`` 
  equals the output of ``hostname`` on remote hosts.

Otherwise cephadm can't be sure, the host names returned by
``ceph * metadata`` match the hosts known to cephadm. This might result
in a :ref:`cephadm-stray-host` warning.

When configuring new hosts, there are two **valid** ways to set the 
``hostname`` of a host:

1. Using the bare host name. In this case:

-  ``hostname`` returns the bare host name.
- ``hostname -f`` returns the FQDN.

2. Using the fully qualified domain name as the host name. In this case:

-  ``hostname`` returns the FQDN
-  ``hostname -s`` return the bare host name

Note that ``man hostname`` recommends ``hostname`` to return the bare
host name:

    The FQDN (Fully Qualified Domain Name) of the system is the
    name that the resolver(3) returns for the host name, such as,
    ursula.example.com. It is usually the hostname followed by the DNS
    domain name (the part after the first dot). You can check the FQDN
    using hostname --fqdn or the domain name using dnsdomainname.

    ::

          You cannot change the FQDN with hostname or dnsdomainname.

          The recommended method of setting the FQDN is to make the hostname
          be an alias for the fully qualified name using /etc/hosts, DNS, or
          NIS. For example, if the hostname was "ursula", one might have
          a line in /etc/hosts which reads

                 127.0.1.1    ursula.example.com ursula

Which means, ``man hostname`` recommends ``hostname`` to return the bare
host name. This in turn means that Ceph will return the bare host names
when executing ``ceph * metadata``. This in turn means cephadm also
requires the bare host name when adding a host to the cluster: 
``ceph orch host add <bare-name>``.

..
  TODO: This chapter needs to provide way for users to configure
  Grafana in the dashboard, as this is right no very hard to do.
  
Cephadm Scheduler
-----------------

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