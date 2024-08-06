This directory contains a number of pre-defined FIO workloads to use with RBD.

A number of FIO options can be parameterised when running FIO, for example, the following
creates an RBD image with the given name and prefills it with the mentioned .fio workload:

```bash
RBD_NAME=fio_test_0 RBD_SIZE="10G" fio /fio/examples/rbd_prefill.fio
```

Here is the description for the main examples included:

- rbd_randread.fio: 4k random read workload
- rbd_randwrite.fio: 4k random write workload
- rbd_seqread.fio: 64k sequential write
- rbd_seqwrite.fio: 64K sequential write

The scripts with _lr_ are similar as those above with the FIO option 'latency_target' enabled,
this is to get the iodepth from FIO for the specified time window -- analogous to a full response
latency execution. Consult the FIO manual for details.
