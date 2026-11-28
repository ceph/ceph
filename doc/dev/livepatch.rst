=====================
 Live patching (dev)
=====================

.. warning:: This is experimental. It is a developer tool for trying out
   function-level hot fixes on a running daemon, and with the current libpulp
   release applying a patch can freeze the daemon; see `Limits`_.

A build with ``-DWITH_LIVEPATCH=ON`` can have single functions replaced in a
running daemon with `libpulp <https://github.com/SUSE/libpulp>`_: the process
keeps its pid, its sessions and its in-memory state, and the replacement can
be reverted again. This is for a fix to the code a daemon is *already
running*. It is not an upgrade mechanism: the patch is bound to one exact
binary, it cannot change data structures, and it is gone when the daemon
restarts.

How it works
============

``WITH_LIVEPATCH`` compiles everything with
``-fpatchable-function-entry=16,14``, which puts a 16-byte pad of nops at
every function (14 bytes before the entry point and 2 at it). A daemon
started with ``libpulp.so`` preloaded can then be patched from outside with
libpulp's ``ulp`` tool, which stops the process with ptrace, loads a shared
object with the replacement function into it, and overwrites the pad of the
old function with a jump to the new one.

Linux on x86_64 only. The ``ulp`` tool needs ptrace rights on the daemon
(root, with the default ``kernel.yama.ptrace_scope``).

Building and starting
=====================

Build and install libpulp, then::

    ./do_cmake.sh -DWITH_LIVEPATCH=ON
    ninja -C build
    ulp post build/bin/ceph-mds          # once per binary
    LD_PRELOAD=/usr/local/lib/libpulp.so.0 build/bin/ceph-mds -i a ...

``ulp livepatchable build/bin/ceph-mds`` says whether a binary can be
patched, and ``ulp patches -p <pid>`` lists what is applied to a process.

Writing a patch
===============

The replacement is a free function with C linkage that takes the object as
its first argument, which has the same calling convention as the member
function it replaces::

    #include "mds/Server.h"

    extern "C" bool lp_Server_check_dir_max_entries(Server *self,
                                                    const MDRequestRef& mdr,
                                                    CDir *in)
    {
      ...
    }

Compile it with exactly the flags of the source file that holds the original
function (``ninja -t compdb`` prints them), plus ``-fPIC
-fno-access-control``, and link it with ``-shared``. Symbols the patch uses
are resolved against the running daemon. Then describe the patch in a
``.dsc`` file and embed that description::

    /path/to/lp_dir_max_entries.so
    @/path/to/build/bin/ceph-mds
    _ZN6Server21check_dir_max_entriesERK...:lp_Server_check_dir_max_entries

    ulp packer lp_dir_max_entries.dsc

``src/script/livepatch/build-patch.sh`` does all of this, and
``src/script/livepatch/example/`` has two patches to start from::

    src/script/livepatch/build-patch.sh build \
        src/script/livepatch/example/lp_dir_max_entries.cc ceph-mds \
        mds/Server.cc 'Server21check_dir_max_entries' \
        lp_Server_check_dir_max_entries

``qa/standalone/misc/livepatch.sh`` applies and reverts the other one on a
monitor. It is skipped where libpulp is not installed.

Applying and reverting
======================

::

    ulp trigger -p <pid> -r 100 lp_dir_max_entries.so
    ulp trigger -p <pid> -r 100 --revert lp_dir_max_entries.so
    ulp patches -p <pid>

``-r`` retries: libpulp refuses to patch (``Resource temporarily
unavailable``) while any thread is inside ``malloc``, ``free`` or the dynamic
loader.

Seeing what is applied
======================

The ``version`` command of a daemon started with libpulp reports the runtime
and the patches applied::

    $ ceph tell mds.a version
    {
        "version": "...",
        "release": "...",
        "release_type": "...",
        "livepatch_enabled": true,
        "livepatch_runtime": "libpulp 0.3.17",
        "livepatches": [
            {
                "id": "...",
                "target": "/path/to/build/bin/ceph-mds",
                "file": "/path/to/lp_dir_max_entries.so",
                "applied": 1795330000
            }
        ]
    }

The list is read from libpulp's internal state, and only for libpulp 0.3.x,
whose layout it was written against.

Limits
======

* **Same build only.** A patch is bound to the build-id of the binary it was
  made for.
* **Functions with a symbol only.** A function that the compiler inlined, a
  function defined in a header, and a file-static function cannot be
  replaced; patch their callers instead. A function split by the compiler
  also has a ``.cold`` part, which stays as it was.
* **No data layout changes.** The replacement runs against the objects the
  daemon already has.
* **Not persistent.** A restarted daemon runs the unpatched binary.
* **A thread that is inside the old function finishes in the old function.**
* **libpulp 0.3.17 can deadlock a daemon that uses tcmalloc.** It stops every
  thread and then calls ``dlopen`` and ``calloc`` on one of them. Its check
  for a thread being inside the allocator only sees ``malloc`` and ``free``,
  and with tcmalloc ``operator new`` and ``operator delete`` do not go through
  those. If a stopped thread holds an allocator lock, the daemon stays frozen
  until ``ulp trigger`` times out (200 seconds by default; ``--timeout``
  changes it), and it may not survive that. See
  https://github.com/SUSE/libpulp/issues/273. Do not use this on a daemon
  whose data you care about until that is fixed.
* **x86_64 Linux only**, and only tested with GCC.
