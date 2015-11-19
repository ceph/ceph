import docopt

import teuthology.config
import teuthology.tree

doc = """
usage:
    teuthology-tree -h
    teuthology-tree [-p <prefixes>] [--] <suite_dir>

Describe the contents of a qa suite by extracting comments
starting with particular prefixes from files in the suite.

positional arguments:
  <suite_dir>            path under which to archive results

optional arguments:
  -h, --help                          Show this help message and exit
  -p <prefixes>, --prefix <prefixes>  Comma-separated list of prefixes
                                      [default: desc]
""".format(archive_base=teuthology.config.config.archive_base)


def main():
    args = docopt.docopt(doc)
    teuthology.tree.main(args)
