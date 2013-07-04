
This is an alternative Ceph plugin for Wireshark. It's not yet as functional as
the standard plugin. However it is written to conform to the Wireshark coding 
guidelines so that at some point in the future it may be possible to make 
this a built-in dissector in Wireshark. 

At present the plugin can dissect handshaking and a handful of the many message
types that Ceph servers use. It is port agnostic and attempts to identify who
the sender and receivers are by looking at the messages being passed. I have
tried to make the dissecting code less dependant on the underlying transport
just in case it needs to be ported.

There is no support for IPv6 addresses yet or CRC checking which I have 
removed temporarily to simplifying things. If you look at the code you might 
wonder why it does not use the Ceph headers to describe message structure, this 
is to avoid the many differences you can find with struct packing that might
break dissection on other platforms supported by Wireshark.

The plugin has been tested against Wireshark 1.10.0 on Ubuntu precise and 
Windows 7 64-bit builds.

Linux Build

1. Copy the contents of this directory into the plugins/ceph directory in the 
    Wireshark source, you will need to create this.
2. From the Wireshark source directory run:
    patch -p1 < plugins/ceph/ws-1.10.0.patch
3. Compile Wireshark as normal
    ./autogen.sh
    ./configure
    make
    sudo make install

Windows 7 Build

Building Wireshark under Windows is rather involved so ideally avoid this!

If you can't, either patch the source on a Linux machine and copy to your 
target machine then follow the standard build instructions or install cygwin 
and apply the patch before following normal build instructions.

Kevin Jones
k.j.jonez@gmail.com
Last Updated: 1st July 2013





