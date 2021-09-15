# Teuthology Development Environment Instruction

The purpose of this guide is to help developers set
up a development environment for Teuthology using 
Dockers to set up all the containers for
Postgres, Paddles, Pulpito, Beanstalk, Teuthology.
For now, this guide will rely on the sepia lab cluster
for test nodes.

# Installing Docker

For Docker installation see: 
https://docs.docker.com/get-docker/

# Add id_rsa private key

Add your id_rsa private key that you use to
ssh into teuthology.front.sepia.com your running 
teuthology container. You can find the key in
~/.ssh/id_rsa on your local machine.
File should be in the same location in your
running teuthology container.

Enable permission:
```bash
chmod 600 ~/.ssh/id_rsa
```


Add the follow line to ~/.ssh/config:

```bash
Host *
   StrictHostKeyChecking no
   UserKnownHostsFile=/dev/null
```

# Reserve a machine in Sepia

ssh into teuthology.front.sepia.com

lock a random machine, mark it down and give it an 
appropriate description so that other teuthology users 
are aware of why you are locking that machine

```bash
./virtualenv/bin/teuthology-lock --lock many 1 --machine-type smithi
```

# Adding testnodes to Paddles

```bash
cd ~/teuthology
source ./virtualenv/bin/activate

# Edit docs/_static/create_nodes.py
# (paddles_url, machine_type, lab_domain, and machine_index_range)
# These can all be found in teuthology.yaml on a teuthology host

python docs/_static/create_nodes.py
```









