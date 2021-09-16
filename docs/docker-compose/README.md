# Teuthology Development Environment Instruction

The purpose of this guide is to help developers set
up a development environment for Teuthology. We will be using 
Dockers to set up all the containers for
Postgres, Paddles, Pulpito, Beanstalk, Teuthology.
For now, this guide will rely on the sepia lab cluster
for test nodes.

# Add/Edit Teuthology config file

First you need to add `.teuthology.yaml`
to the same directory level as this 
README file. An easy way to do this is to
ssh into teuthology.front.sepia.com and you will
find the file in `/etc/teuthology.yaml`. Next,
copy and paste the file to your local machine
and edit (lock_server, results_server, results_ui_server) to:

```bash
lock_server: http://paddles:8080
results_server: http://paddles:8080
results_ui_server: http://pulpito:8081/
```

Next, add these lines to your `.teuthology.yaml`:
```bash
teuthology_path: ./
archive_base: ../archive_dir
reserve_machines: 0
```

# Installing and Running Docker

For Docker installation see: 
https://docs.docker.com/get-docker/

Make sure you are connected to Sepia lab VPN
before starting Docker, so that the containers
will use the VPN network route when executing
SSH commands.

To start building images and running containers:
```bash
./start.sh
```

Once you are finished you should have all 5 containers running
and should be able to access them. The script will also 1 dummy
job in the queue waiting to be executed by the teutholgy-dispatcher.

# Adding id_rsa private key

Add your `id_rsa` key that you use to
ssh into teuthology.front.sepia.com to your running 
teuthology container. You can find the key in
`~/.ssh/id_rsa` on your local machine.

Enable read & write permissions:
```bash
chmod 600 ~/.ssh/id_rsa
```

We need to disable key checking and known_host file creation\
by adding the follow line to `~/.ssh/config`:

```bash
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
```

# Reserving a machine in Sepia

ssh into teuthology.front.sepia.com,
lock a random machine, mark it down and give it an 
appropriate description of why you are locking a machine.

For example, to lock 1 random smithi machine use:
```bash
./virtualenv/bin/teuthology-lock --lock many 1 --machine-type smithi
```

To update the status and description:
```bash
./virtualenv/bin/teuthology-lock --update --status down --desc teuthology-dev-testing smithi022
```

# Adding test-nodes to Paddles

After reserving your machine, you can now add the machine
into your paddles inventory by following these steps
in your teuthology container:

```bash
cd ~/teuthology
source ./virtualenv/bin/activate
```

In `docs/_static/create_nodes.py`
edit (paddles_url, machine_type, lab_domain, and machine_index_range).

Here is what the file should look like when you are trying to add smithi022:
```python
11 from teuthology.lock.ops import update_inventory
12 
13 paddles_url = 'http://paddles:8080'
14 
15 machine_type = 'smithi'
16 lab_domain = 'front.sepia.ceph.com'
17 # Don't change the user. It won't work at this time.
18 user = 'ubuntu'
19 # We are populating 'typica003' -> 'typica192'
20 machine_index_range = range(22, 23)
21 
22 log = logging.getLogger(sys.argv[0])
```

Run the command:
```bash
python docs/_static/create_nodes.py
```
Output should look like this when successful:

```bash
$ python docs/_static/create_nodes.py
INFO:docs/_static/create_nodes.py:Creating smithi022.front.sepia.ceph.com
INFO:teuthology.orchestra.remote:Trying to reconnect to host
INFO:teuthology.orchestra.run.smithi022.stdout:x86_64
INFO:teuthology.orchestra.run.smithi022.stdout:NAME="Ubuntu"
INFO:teuthology.orchestra.run.smithi022.stdout:VERSION="18.04.5 LTS (Bionic Beaver)"
INFO:teuthology.orchestra.run.smithi022.stdout:ID=ubuntu
NFO:teuthology.orchestra.run.smithi022.stdout:ID_LIKE=debian
INFO:teuthology.orchestra.run.smithi022.stdout:PRETTY_NAME="Ubuntu 18.04.5 LTS"
INFO:teuthology.orchestra.run.smithi022.stdout:VERSION_ID="18.04"
INFO:teuthology.orchestra.run.smithi022.stdout:HOME_URL="https://www.ubuntu.com/"
INFO:teuthology.orchestra.run.smithi022.stdout:SUPPORT_URL="https://help.ubuntu.com/"
INFO:teuthology.orchestra.run.smithi022.stdout:BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
INFO:teuthology.orchestra.run.smithi022.stdout:PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
INFO:teuthology.orchestra.run.smithi022.stdout:VERSION_CODENAME=bionic
INFO:teuthology.orchestra.run.smithi022.stdout:UBUNTU_CODENAME=bionic
INFO:teuthology.lock.ops:Updating smithi022.front.sepia.ceph.com on lock server
INFO:teuthology.lock.ops:Creating new node smithi022.front.sepia.ceph.com on lock server
```
If the test-node is locked after adding it to paddles you can run this command to unlock it:

```bash
./virtualenv/bin/teuthology-lock --unlock --owner initial@setup smithi022
```

# Run teuthology-dispatcher

You can now test out your set up by running the dispatcher:

```bash
./virtualenv/bin/teuthology-dispatcher -v --archive-dir ../archive_dir  --log-dir log --machine-type smithi
```