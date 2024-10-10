# aprg - Automatic Performance Report Generator - POC toolkit

This is a POC/prototype of a number of ideas to:

- run a set of workloads using FIO with RBD,
- get some measurements for CPU and RAM utilisation,
- collate these with the results from FIO in a .JSON file.

This started for Crimson performance investigations, esp for tracker
https://tracker.ceph.com/issues/66525

I was not aware of CBT at the time I started.
I plan to port these ideas into Python for CBT, started on PR 306 and follow ups.

The toolkit has only been used in Ceph Developer mode, with vstart.sh. I have
added the test scenarios for both Classic and Crimson OSD.

The description of the scripts follows:

- bashrc - some profile that I am used
- cephlog - config for log rotation
- toprc - profile for the Linux top tool that is required for the parse-top.pl script
- cephlogoff.sh - disable some Ceph logging
- cephmkrbd.sh - create a single RBD image (default, see the script on using a
  parameter to define the number of images to create)
- cephteardown.sh - remove pools and volumes
- parse-top.pl - parse the output from top to monitor CPU utilisation, focusing on
  pid given as arguments
- postproc.sh - an example of doing post processing
- run_fio.sh - main script to drive FIO, collect measurements, and produce response charts
- run_batch.sh - example of end-to-end performance run execution
- run_batch_double.sh - example showing a configuration involving two OSD
- run_batch_range_alien.sh - example showing a config for Crimson OSD
- run_batch_seastar_alien_ratios.sh - ditto
- run_batch_single.sh
- run_hockey_classic.sh
- run_hockey_crimson.sh
- run_hockey_crimson_1osd.sh
- run_multiple_fio.sh
- rbd_fio_examples - directory with examples of predefined FIO workload definitions
- rg_template - templates for the report generator
- ceph-aprg.docker - example for a container including all dependencies for the toolkit

## Usage:

In the simplest form, the following will create a cluster with Crimson OSD, single reactor, the
FIO client running on 8 CPU cores, and will produce charts with response latency curves:

```bash
# run_hockey_crimson.sh
```

By default, the results will be saved as archives in /tmp:

```bash
crimson_1osd_8fio_rc_1procs_randread.zip  crimson_1osd_8fio_rc_1procs_randwrite.zip  crimson_1osd_8fio_rc_1procs_seqread.zip crimson_1osd_8fio_rc_1procs_seqwrite.zip.
```
each archive contains the FIO .json output results, the top output CPU measurments, as well as .png for the IOPs versus latency charts. Since the example involves
a range of data values (for increasing iodepth), only the final chart coalescing all the data points is of interest:

-- Add example charts here, might need to be edited in the github webportal

The .pdf report is generated from a template, at the moment is fixed but can be changed according to a main test plan (as in CBT)

```bash
 # /tinytex/tools/texlive/bin/x86_64-linux/pdflatex -interaction=nonstopmode report_tmp.tex
 # /tinytex/tools/texlive/bin/x86_64-linux/pdflatex -interaction=nonstopmode report_tmp.tex
```

(yes, it must be run twice for the references to be sorted correctly).

An example .pdf is in the rg_template/ subdir.

Additionally to tinytex, I'll look at supporting https://github.com/rst2pdf/rst2pdf as well.
