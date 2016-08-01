# Mirroring Ceph
Ceph is primarily distributed from download.ceph.com which is based in the US.

However, globally there are multiple mirrors which offer the same content. Often
faster than downloading from the primary source.

Using the script found in this directory you can easily mirror Ceph to your local
datacenter and serve packages from there to your servers.

## Guidelines
If you want to mirror Ceph please follow these guidelines:
* Please use a mirror close to you
* Do not sync in a shorter interval than 3 hours
* Avoid syncing at minute 0 of the hour, use something between 0 and 59.

## Mirror script
The 'mirror-ceph.sh' script is written in Bash and will use rsync to mirror
all the contents to a local directory.

Usage is simple:

<pre>
./mirror-ceph.sh -q -s eu -t /srv/mirrors/ceph
</pre>

This example will mirror all contents from the source 'eu' which is *eu.ceph.com*.

### Running with CRON
The script can easily be run with CRON:

<pre>
13 1,5,9,13,17,21 * * * /home/ceph/mirror-ceph.sh -q -s eu -t /srv/mirrors/ceph
</pre>

This will sync from *eu.ceph.com* on 01:13, 05:13, 09:13, 13:13, 17:13 and 21:13.

## Becoming a mirror source
If you have spare hardware and resources available you can opt for becoming a mirror
source for others.

A few things which are required:
* 1Gbit connection or more
* Native IPv4 **and** IPv6
* HTTP access
* rsync access
* 2TB of storage or more
* Monitoring of the mirror/source

You can then run the *mirror-ceph.sh* script and mirror all the contents.

### Logs
The project wants to analyze the downloads of Ceph a few times a year. From mirrors
we expect that they store HTTP access logs for at least 6 months so they can be
used for analysis.

### DNS
Using a DNS CNAME record a XX.ceph.com entry can be forwarded to the server and
added to the mirror script.

You can request such a DNS entry on the ceph mailinglists.

### Apache configuration
A Apache 2.4 VirtualHost example configuration can be found the Git repository
with the name *apache2.vhost.conf*
