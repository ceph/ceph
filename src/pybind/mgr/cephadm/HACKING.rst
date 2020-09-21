Development
===========


There are multiple ways to set up a development environment for the SSH orchestrator.
In the following I'll use the `vstart` method.

1) Make sure remoto is installed (0.35 or newer)

2) Use vstart to spin up a cluster


::

   # ../src/vstart.sh -n --cephadm

*Note that when you specify `--cephadm` you have to have passwordless ssh access to localhost*

It will add your ~/.ssh/id_rsa and ~/.ssh/id_rsa.pub to `mgr/ssh/ssh_identity_{key, pub}`
and add your $HOSTNAME to the list of known hosts.

This will also enable the cephadm mgr module and enable it as the orchestrator backend.

*Optional:*

While the above is sufficient for most operations, you may want to add a second host to the mix.
There is `Vagrantfile` for creating a minimal cluster in `src/pybind/mgr/cephadm/`.

If you wish to extend the one-node-localhost cluster to i.e. test more sophisticated OSD deployments you can follow the next steps:

From within the `src/pybind/mgr/cephadm` directory.


1) Spawn VMs

::

   # vagrant up

This will spawn three machines by default.
mon0, mgr0 and osd0 with 2 additional disks.

You can change that by passing `MONS` (default: 1), `MGRS` (default: 1), `OSDS` (default: 1) and
`DISKS` (default: 2) environment variables to overwrite the defaults. In order to not always have
to set the environment variables you can now create as JSON see `./vagrant.config.example.json`
for details.

If will also come with the necessary packages preinstalled as well as your ~/.ssh/id_rsa.pub key
injected. (to users root and vagrant; the cephadm-orchestrator currently connects as root)


2) Update the ssh-config

The cephadm orchestrator needs to understand how to connect to the new node. Most likely the VM
isn't reachable with the default settings used:

```
Host *
User root
StrictHostKeyChecking no
```

You want to adjust this by retrieving an adapted ssh_config from Vagrant.

::

   # vagrant ssh-config > ssh-config


Now set the newly created config for Ceph.

::

   # ceph cephadm set-ssh-config -i <path_to_ssh_conf>


3) Add the new host

Add the newly created host(s) to the inventory.

::


   # ceph orch host add <host>


4) Verify the inventory

You should see the hostname in the list.

::

   # ceph orch host ls


5) Verify the devices

To verify all disks are set and in good shape look if all devices have been spawned
and can be found

::

   # ceph orch device ls


6) Make a snapshot of all your VMs!

To not go the long way again the next time snapshot your VMs in order to revert them back
if they are dirty.

In `this repository <https://github.com/Devp00l/vagrant-helper-scripts>`_ you can find two
scripts that will help you with doing a snapshot and reverting it, without having to manual
snapshot and revert each VM individually.


Understanding ``AsyncCompletion``
=================================

How can I store temporary variables?
------------------------------------

Let's imagine you want to write code similar to

.. code:: python

    hosts = self.get_hosts()
    inventory = self.get_inventory(hosts)
    return self._create_osd(hosts, drive_group, inventory)

That won't work, as ``get_hosts`` and ``get_inventory`` return objects
of type ``AsyncCompletion``.

Now let's imaging a Python 3 world, where we can use ``async`` and
``await``. Then we actually can write this like so:

.. code:: python

    hosts = await self.get_hosts()
    inventory = await self.get_inventory(hosts)
    return self._create_osd(hosts, drive_group, inventory)

Let's use a simple example to make this clear:

.. code:: python

    val = await func_1()
    return func_2(val)

As we're not yet in Python 3, we need to do write ``await`` manually by
calling ``orchestrator.Completion.then()``:

.. code:: python

    func_1().then(lambda val: func_2(val))

    # or
    func_1().then(func_2)

Now let's desugar the original example:

.. code:: python

    hosts = await self.get_hosts()
    inventory = await self.get_inventory(hosts)
    return self._create_osd(hosts, drive_group, inventory)

Now let's replace one ``async`` at a time:

.. code:: python

    hosts = await self.get_hosts()
    return self.get_inventory(hosts).then(lambda inventory:
        self._create_osd(hosts, drive_group, inventory))

Then finally:

.. code:: python

    self.get_hosts().then(lambda hosts:
        self.get_inventory(hosts).then(lambda inventory:
         self._create_osd(hosts,
                          drive_group, inventory)))

This also works without lambdas:

.. code:: python

    def call_inventory(hosts):
        def call_create(inventory)
            return self._create_osd(hosts, drive_group, inventory)

        return self.get_inventory(hosts).then(call_create)

    self.get_hosts(call_inventory)

We should add support for ``await`` as soon as we're on Python 3.

I want to call my function for every host!
------------------------------------------

Imagine you have a function that looks like so:

.. code:: python

    @async_completion
    def deploy_stuff(name, node):
        ...

And you want to call ``deploy_stuff`` like so:

.. code:: python

    return [deploy_stuff(name, node) for node in nodes]

This won't work as expected. The number of ``AsyncCompletion`` objects
created should be ``O(1)``. But there is a solution:
``@async_map_completion``

.. code:: python

    @async_map_completion
    def deploy_stuff(name, node):
        ...

    return deploy_stuff([(name, node) for node in nodes])

This way, we're only creating one ``AsyncCompletion`` object. Note that
you should not create new ``AsyncCompletion`` within ``deploy_stuff``, as
we're then no longer have ``O(1)`` completions:

.. code:: python

    @async_completion
    def other_async_function():
        ...

    @async_map_completion
    def deploy_stuff(name, node):
        return other_async_function() # wrong!

Why do we need this?
--------------------

I've tried to look into making Completions composable by being able to
call one completion from another completion. I.e. making them re-usable
using Promises E.g.:

.. code:: python

    >>> return self.get_hosts().then(self._create_osd)

where ``get_hosts`` returns a Completion of list of hosts and
``_create_osd`` takes a list of hosts.

The concept behind this is to store the computation steps explicit and
then explicitly evaluate the chain:

.. code:: python

    p = Completion(on_complete=lambda x: x*2).then(on_complete=lambda x: str(x))
    p.finalize(2)
    assert p.result = "4"

or graphically:

::

    +---------------+      +-----------------+
    |               | then |                 |
    | lambda x: x*x | +--> | lambda x: str(x)|
    |               |      |                 |
    +---------------+      +-----------------+
