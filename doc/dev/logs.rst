============
 Debug Logs
============

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
