import docopt

import teuthology.config
import teuthology.prune

doc = """
usage:
    teuthology-prune-logs -h
    teuthology-prune-logs [-v] [options]

Prune old logfiles from the archive

optional arguments:
  -h, --help            Show this help message and exit
  -v, --verbose         Be more verbose
  -a ARCHIVE, --archive ARCHIVE
                        The base archive directory
                        [default: {archive_base}]
  --dry-run             Don't actually delete anything; just log what would be
                        deleted
  -p DAYS, --pass DAYS  Remove all logs for jobs which passed and are older
                        than DAYS. Negative values will skip this operation.
                        [default: 14]
  -f DAYS, --fail DAYS  Like --pass, but for failed jobs. [default: -1]
  -r DAYS, --remotes DAYS
                        Remove the 'remote' subdir of jobs older than DAYS.
                        Negative values will skip this operation.
                        [default: 60]
  -z DAYS, --compress DAYS
                        Compress (using gzip) any teuthology.log files older
                        than DAYS. Negative values will skip this operation.
                        [default: 30]
""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.prune.main(args)
