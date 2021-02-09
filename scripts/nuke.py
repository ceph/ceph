import docopt

import teuthology.nuke

doc = """
usage:
  teuthology-nuke --help
  teuthology-nuke [-v] [--owner OWNER] [-n NAME] [-u] [-i] [-r|-R] [-s] [-k]
                       [-p PID] [--dry-run] (-t CONFIG... | -a DIR)
  teuthology-nuke [-v] [-u] [-i] [-r] [-s] [--dry-run] --owner OWNER --stale
  teuthology-nuke [-v] [--dry-run] --stale-openstack

Reset test machines

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         be more verbose
  -t CONFIG [CONFIG ...], --targets CONFIG [CONFIG ...]
                        yaml config containing machines to nuke
  -a DIR, --archive DIR
                        archive path for a job to kill and nuke
  --stale               attempt to find and nuke 'stale' machines
                        (e.g. locked by jobs that are no longer running)
  --stale-openstack     nuke 'stale' OpenStack instances and volumes
                        and unlock OpenStack targets with no instance
  --dry-run             Don't actually nuke anything; just print the list of
                        targets that would be nuked
  --owner OWNER         job owner
  -p PID, --pid PID     pid of the process to be killed
  -r, --reboot-all      reboot all machines (default)
  -R, --no-reboot       do not reboot the machines
  -s, --synch-clocks    synchronize clocks on all machines
  -u, --unlock          Unlock each successfully nuked machine, and output
                        targets thatcould not be nuked.
  -n NAME, --name NAME  Name of run to cleanup
  -i, --noipmi          Skip ipmi checking
  -k, --keep-logs       Preserve test directories and logs on the machines

Examples:
teuthology-nuke -t target.yaml --unlock --owner user@host
teuthology-nuke -t target.yaml --pid 1234 --unlock --owner user@host
"""


def main():
    args = docopt.docopt(doc)
    teuthology.nuke.main(args)
