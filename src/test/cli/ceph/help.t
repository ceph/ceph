# TODO help should not fail
  $ ceph --help
  usage: ceph [options] [commands]
  If no commands are specified, enter interactive mode.
  Commands:
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
  -d               Run in foreground, log to stderr.
  -f               Run in foreground, log to usual location.
  --id/-i          set ID portion of my name
  --name/-n        set name (TYPE.ID)
  --version        show version and quit
  
  [1]
