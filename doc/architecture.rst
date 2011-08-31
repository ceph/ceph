======================
 Architecture of Ceph
======================

- Introduction to Ceph Project

  - High-level overview of project benefits for users (few paragraphs, mention each subproject)
  - Introduction to sub-projects (few paragraphs to a page each)

    - RADOS
    - RGW
    - RBD
    - Ceph

  - Example scenarios Ceph projects are/not suitable for
  - (Very) High-Level overview of Ceph

    This would include an introduction to basic project terminology,
    the concept of OSDs, MDSes, and Monitors, and things like
    that. What they do, some of why they're awesome, but not how they
    work.

- Discussion of MDS terminology, daemon types (active, standby,
  standby-replay)

.. todo:: write me

=================================
 Library architecture
=================================
Ceph is structured into libraries which are built and then combined together to
make executables and other libraries.

- libcommon: a collection of utilities which are available to nearly every ceph
  library and executable. In general, libcommon should not contain global
  variables, because it is intended to be linked into libraries such as
  libceph.so.

- libglobal: a collection of utilities focused on the needs of Ceph daemon
  programs. In here you will find pidfile management functions, signal
  handlers, and so forth.

.. todo:: document other libraries

=================================
 Configuration Management System
=================================
The configuration management system exists to provide every daemon with the
proper configuration information. The configuration can be viewed as a set of
key-value pairs.

How can the configuration be set? Well, there are several sources:
 - the ceph configuration file, usually named ceph.conf
 - command line arguments::
    --debug-ms=1
    --debug-pg=10
    etc.
 - arguments injected at runtime by using injectargs

======================================================
 The Configuration File
======================================================
Most configuration settings originate in the Ceph configuration file.

How do we find the configuration file? Well, in order, we check:
 - the default locations
 - the environment variable CEPH_CONF
 - the command line argument -c

Each stanza of the configuration file describes the key-value pairs that will be in
effect for a particular subset of the daemons. The "global" stanza applies to
everything. The "mon", "osd", and "mds" stanzas specify settings to take effect
for all monitors, all osds, and all mds servers, respectively.  A stanza of the
form mon.$name, osd.$name, or mds.$name gives settings for the monitor, OSD, or
MDS of that name, respectively. Configuration values that appear later in the
file win over earlier ones.

A sample configuration file can be found in src/sample.ceph.conf.

======================================================
 Metavariables
======================================================
The configuration system supports certain "metavariables." If these occur
inside a configuration value, they are expanded into something else-- similar to
how bash shell expansion works.

There are a few different metavariables:
 - $host: expands to the current hostname
 - $type: expands to one of "mds", "osd", or "mon"
 - $id: expands to the daemon identifier. For osd.0, this would be "0"; for mds.a, it would be "a"
 - $num: same as $id
 - $name: expands to $type.$id

======================================================
 Interfacing with the Configuration Management System
======================================================
There are two ways for Ceph code to get configuration values. One way is to
read it directly from a variable named "g_conf," or equivalently,
"g_ceph_ctx->_conf." The other is to register an observer that will called
every time the relevant configuration values changes.  This observer will be
called soon after the initial configuration is read, and every time after that
when one of the relevant values changes. Each observer tracks a set of keys
and is invoked only when one of the relevant keys changes.

The interface to implement is found in common/config_obs.h.

The observer method should be preferred in new code because
 - It is more flexible, allowing the code to do whatever reinitialization needs
   to be done to implement the new configuration value.
 - It is the only way to create a std::string configuration variable that can
   be changed by injectargs.
 - Even for int-valued configuration options, changing the values in one thread
   while another thread is reading them can lead to subtle and
   impossible-to-diagnose bugs.

For these reasons, reading directly from g_conf should be considered deprecated
and not done in new code.  Do not ever alter g_conf.

=================================
 Debug Logs
=================================
The main debugging tool for Ceph is the dout and derr logging functions.
Collectively, these are referred to as "dout logging."

Dout has several log faculties, which can be set at various log
levels using the configuration management system. So it is possible to enable
debugging just for the messenger, by setting debug_ms to 10, for example.

Dout is implemented mainly in common/DoutStreambuf.cc

The dout macro avoids even generating log messages which are not going to be
used, by enclosing them in an "if" statement. What this means is that if you
have the debug level set at 0, and you run this code

``dout(20) << "myfoo() = " << myfoo() << dendl;``


myfoo() will not be called here.

Unfortunately, the performance of debug logging is relatively low. This is
because there is a single, process-wide mutex which every debug output
statement takes, and every debug output statement leads to a write() system
call or a call to syslog(). There is also a computational overhead to using C++
streams to consider. So you will need to be parsimonius in your logging to get
the best performance.

Sometimes, enabling logging can hide race conditions and other bugs by changing
the timing of events. Keep this in mind when debugging.

=================================
 CephContext
=================================
A CephContext represents a single view of the Ceph cluster. It comes complete
with a configuration, a set of performance counters (PerfCounters), and a
heartbeat map. You can find more information about CephContext in
src/common/ceph_context.h.

Generally, you will have only one CephContext in your application, called
g_ceph_context. However, in library code, it is possible that the library user
will initialize multiple CephContexts. For example, this would happen if he
called rados_create more than once.

A ceph context is required to issue log messages. Why is this? Well, without
the CephContext, we would not know which log messages were disabled and which
were enabled.  The dout() macro implicitly references g_ceph_context, so it
can't be used in library code.  It is fine to use dout and derr in daemons, but
in library code, you must use ldout and lderr, and pass in your own CephContext
object. The compiler will enforce this restriction.
