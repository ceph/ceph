import docopt

import teuthology.config
import teuthology.describe_tests

doc = """
usage:
    teuthology-describe-tests -h
    teuthology-describe-tests [options] [--] <suite_dir>

Describe the contents of a qa suite by reading 'description' elements
from yaml files in the suite.

The 'description' element should contain a list with a dictionary
of fields, e.g.:

description:
- field1: value1
  field2: value2
  field3: value3
  desc: short human-friendly description

Fields are user-defined, and are not required to be in all yaml files.

positional arguments:
  <suite_dir>                        path of qa suite

optional arguments:
  -h, --help                         Show this help message and exit
  -f <fields>, --fields <fields>     Comma-separated list of fields to
                                     include [default: desc]
  --show-facet [yes|no]              List the facet of each file
                                     [default: yes]
"""


def main():
    args = docopt.docopt(doc)
    teuthology.describe_tests.main(args)
