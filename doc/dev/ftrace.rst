Tracing Kernel Functions with trace-cmd
=======================================

This guide shows how to use ``trace-cmd`` to perform a `function_graph` trace
of kernel functions triggered by a specific command.

Prerequisites
-------------
Ensure that the ``trace-cmd`` is installed. You can install it using your
package manager:

.. code-block:: bash

   # For Debian/Ubuntu
   sudo apt update
   sudo apt install trace-cmd

   # For RHEL/CentOS/Fedora
   sudo dnf install trace-cmd


Running a Function Graph Trace
------------------------------
To trace kernel functions while executing a specific command, use the following command:

.. prompt:: bash $

   sudo trace-cmd record -p function_graph -e all -b 131072 <your_command>

**Parameters:**

- ``record``: Tells ``trace-cmd`` to start recording a trace.
- ``-p function_graph``: Specifies the `function_graph` tracer, which traces
  function calls and returns, showing the flow of function execution.
- ``-e all``: Enables all functions for tracing. You can specify particular 
  functions (for example, ``-e sched:sched_switch``) if needed.
- ``-b 131072``: The output can be quite large so we reserve 128MB for each core.
- ``<your_command>``: Replace ``<your_command>`` with the command you want to
  trace. For example, to trace ``ls``, run:

   .. prompt:: bash $

     sudo trace-cmd record -p function_graph -e all ls

Viewing the Trace Results
-------------------------
After the command completes, ``trace-cmd`` saves the trace data in a file named
``trace.dat`` by default. To view the trace output run the following command:

.. prompt:: bash $

   sudo trace-cmd report

The output shows each function call, the time taken, and all nested calls in a
graph format, providing insights into the kernel's function execution path
triggered by the command.

Example
-------

.. prompt:: bash $

   sudo trace-cmd record -p function_graph -e all mount -t ceph $USER@$FSID.a=/
   /mnt/mycephfs -o mon_addr=$IP:$PORT,ms_mode=crc,secret=$CKEY

This generates a ``trace.dat`` file, which can be viewed with the command
``trace-cmd report``:

.. code-block:: bash 

           mount-13161 [014] 172368.963735: funcgraph_entry:                   |          do_new_mount() {
           mount-13161 [014] 172368.963735: funcgraph_entry:                   |            get_fs_type() {
           mount-13161 [014] 172368.963735: funcgraph_entry:        0.090 us   |              _raw_read_lock();
           mount-13161 [014] 172368.963736: funcgraph_entry:        0.384 us   |              try_module_get();
           mount-13161 [014] 172368.963736: funcgraph_entry:        0.088 us   |              _raw_read_unlock();
           mount-13161 [014] 172368.963737: funcgraph_exit:         1.462 us   |            }
           mount-13161 [014] 172368.963737: funcgraph_entry:                   |            fs_context_for_mount() {
           mount-13161 [014] 172368.963737: funcgraph_entry:                   |              alloc_fs_context() {
           mount-13161 [014] 172368.963737: funcgraph_entry:                   |                __kmalloc_cache_noprof() {
           mount-13161 [014] 172368.963737: funcgraph_entry:        0.085 us   |                  __cond_resched();
           mount-13161 [014] 172368.963737: funcgraph_entry:                   |                  __memcg_slab_post_alloc_hook() {
           mount-13161 [014] 172368.963737: funcgraph_entry:        0.098 us   |                    obj_cgroup_charge();
           mount-13161 [014] 172368.963738: funcgraph_entry:        0.084 us   |                    __rcu_read_lock();
           mount-13161 [014] 172368.963738: funcgraph_entry:        0.087 us   |                    __rcu_read_unlock();
           mount-13161 [014] 172368.963738: funcgraph_entry:        0.100 us   |                    mod_objcg_state();
           mount-13161 [014] 172368.963738: funcgraph_exit:         0.853 us   |                  }
           mount-13161 [014] 172368.963738: funcgraph_exit:         1.270 us   |                }
           mount-13161 [014] 172368.963738: funcgraph_entry:                   |                get_filesystem() {
           mount-13161 [014] 172368.963738: funcgraph_entry:        0.085 us   |                  __module_get();
           mount-13161 [014] 172368.963739: funcgraph_exit:         0.275 us   |                }
           mount-13161 [014] 172368.963739: funcgraph_entry:        0.087 us   |                __mutex_init();
           mount-13161 [014] 172368.963739: funcgraph_entry:                   |                ceph_init_fs_context() {
           mount-13161 [014] 172368.963739: funcgraph_entry:                   |                  __kmalloc_cache_noprof() {
           mount-13161 [014] 172368.963739: funcgraph_entry:        0.088 us   |                    __cond_resched();
           mount-13161 [014] 172368.963740: funcgraph_exit:         0.626 us   |                  }
           mount-13161 [014] 172368.963740: funcgraph_entry:                   |                  ceph_alloc_options() {
           mount-13161 [014] 172368.963740: funcgraph_entry:                   |                    __kmalloc_cache_noprof() {
           mount-13161 [014] 172368.963740: funcgraph_entry:        0.090 us   |                      __cond_resched();
           mount-13161 [014] 172368.963740: funcgraph_exit:         0.367 us   |                    }
           mount-13161 [014] 172368.963741: funcgraph_entry:                   |                    __kmalloc_cache_noprof() {
           mount-13161 [014] 172368.963741: funcgraph_entry:        0.105 us   |                      __cond_resched();
           mount-13161 [014] 172368.963741: funcgraph_exit:         0.883 us   |                    }
           mount-13161 [014] 172368.963742: funcgraph_exit:         1.552 us   |                  }
           mount-13161 [014] 172368.963742: funcgraph_entry:                   |                  __kmalloc_cache_noprof() {
           mount-13161 [014] 172368.963742: funcgraph_entry:        0.088 us   |                    __cond_resched();
           mount-13161 [014] 172368.963742: funcgraph_exit:         0.382 us   |                  }
           mount-13161 [014] 172368.963742: funcgraph_entry:                   |                  kstrdup() {
           mount-13161 [014] 172368.963742: funcgraph_entry:                   |                    __kmalloc_node_track_caller_noprof() {
           mount-13161 [014] 172368.963742: funcgraph_entry:        0.084 us   |                      __cond_resched();
           mount-13161 [014] 172368.963743: funcgraph_exit:         0.401 us   |                    }
           mount-13161 [014] 172368.963743: funcgraph_exit:         0.594 us   |                  }
           mount-13161 [014] 172368.963743: funcgraph_entry:        0.119 us   |                  int_sqrt();
           mount-13161 [014] 172368.963743: funcgraph_exit:         3.934 us   |                }
           mount-13161 [014] 172368.963743: funcgraph_exit:         6.284 us   |              }
           mount-13161 [014] 172368.963743: funcgraph_exit:         6.472 us   |            }
           mount-13161 [014] 172368.963743: funcgraph_entry:                   |            put_filesystem() {
           mount-13161 [014] 172368.963743: funcgraph_entry:        0.096 us   |              module_put();
           mount-13161 [014] 172368.963744: funcgraph_exit:         0.285 us   |            }
           mount-13161 [014] 172368.963744: funcgraph_entry:                   |            vfs_parse_fs_string() {
           mount-13161 [014] 172368.963744: funcgraph_entry:                   |              kmemdup_nul() {
           mount-13161 [014] 172368.963744: funcgraph_entry:                   |                __kmalloc_node_track_caller_noprof() {
           mount-13161 [014] 172368.963744: funcgraph_entry:        0.090 us   |                  __cond_resched();
           mount-13161 [014] 172368.963744: funcgraph_exit:         0.484 us   |                }
           mount-13161 [014] 172368.963745: funcgraph_exit:         0.677 us   |              }
           mount-13161 [014] 172368.963745: funcgraph_entry:                   |              vfs_parse_fs_param() {
           mount-13161 [014] 172368.963745: funcgraph_entry:        0.421 us   |                lookup_constant();
           mount-13161 [014] 172368.963745: funcgraph_entry:        0.202 us   |                lookup_constant();
           mount-13161 [014] 172368.963746: funcgraph_entry:                   |                security_fs_context_parse_param() {
           mount-13161 [014] 172368.963746: funcgraph_entry:        0.088 us   |                  static_key_count();
           mount-13161 [014] 172368.963746: funcgraph_entry:        0.087 us   |                  static_key_count();
           mount-13161 [014] 172368.963746: funcgraph_entry:        0.088 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.088 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.089 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.087 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.089 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.090 us   |                  static_key_count();
           mount-13161 [014] 172368.963747: funcgraph_entry:        0.089 us   |                  static_key_count();
           mount-13161 [014] 172368.963748: funcgraph_entry:        0.087 us   |                  static_key_count();
           mount-13161 [014] 172368.963748: funcgraph_entry:        0.088 us   |                  static_key_count();
           mount-13161 [014] 172368.963748: funcgraph_entry:        0.088 us   |                  static_key_count();
           mount-13161 [014] 172368.963748: funcgraph_exit:         2.331 us   |                }
           mount-13161 [014] 172368.963748: funcgraph_entry:                   |                ceph_parse_mount_param() {
           mount-13161 [014] 172368.963749: funcgraph_entry:                   |                  ceph_parse_param() {
           mount-13161 [014] 172368.963749: funcgraph_entry:        0.550 us   |                    __fs_parse();
           mount-13161 [014] 172368.963749: funcgraph_exit:         0.814 us   |                  }

