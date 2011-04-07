# TODO help should not fail
  $ ceph --help
  usage: ceph [options] [commands]
  If no commands are specified, enter interactive mode.
  Commands:
     stop              -- cleanly shut down file system
     (osd|pg|mds) stat -- get monitor subsystem status
     ...
  Options:
     -i infile
     -o outfile
          specify input or output file (for certain commands)
     -s or --status
          print current system status
     -w or --watch
          watch system status changes in real time (push)
  --conf/-c        Read configuration from the given configuration file
  -D               Run in the foreground.
  -f               Run in foreground. Show all log messages on stderr.
  --id             set ID
  --name           set ID.TYPE
  --version        show version and quit
  
  [1]
