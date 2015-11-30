import docopt

import teuthology.config
import teuthology.describe_tests

doc = """
usage:
    teuthology-describe-tests -h
    teuthology-describe-tests [options] [--] <suite_dir>

Describe the contents of a qa suite by extracting comments
starting with particular prefixes from files in the suite.

By default, the remainder of a line starting with '# desc:' will
be included from each file in the specified suite directory.

positional arguments:
  <suite_dir>            qa suite path to traverse and describe

optional arguments:
  -h, --help                          Show this help message and exit
  -p <prefixes>, --prefix <prefixes>  Comma-separated list of prefixes
                                      [default: desc]
  --show-facet [yes|no]               List the facet of each file
                                      [default: yes]
"""


def main():
    args = docopt.docopt(doc)
    teuthology.describe_tests.main(args)
