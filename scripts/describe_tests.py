import docopt

import teuthology.config
import teuthology.describe_tests

doc = """
usage:
    teuthology-describe-tests -h
    teuthology-describe-tests [options] [--] <suite_dir>

Describe the contents of a qa suite by reading 'meta' elements from
yaml files in the suite.

The 'meta' element should contain a list with a dictionary
of key/value pairs for entries, i.e.:

meta:
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
  --format [plain|json|csv]          Output format (written to stdout)
                                     [default: plain]

options only for describing combinations represented by a suite:
  -c, --combinations                 Describe test combinations rather than
                                     individual yaml fragments
  --filter <keywords>                Only list jobs whose filenames contain at
                                     least one of the keywords in the comma
                                     separated keyword string specified.
  --filter-out <keywords>            Do not list jobs whose filenames contain
                                     any of the keywords in the comma separated
                                     keyword string specified.
  -l <jobs>, --limit <jobs>          List at most this many jobs
                                     [default: 0]
  --subset <index/outof>             Instead of listing the entire
                                     suite, break the set of jobs into
                                     <outof> pieces (each of which
                                     will contain each facet at least
                                     once) and list piece <index>.
                                     Listing 0/<outof>, 1/<outof>,
                                     2/<outof> ... <outof>-1/<outof>
                                     will list all jobs in the
                                     suite (many more than once).
"""


def main():
    args = docopt.docopt(doc)
    teuthology.describe_tests.main(args)
