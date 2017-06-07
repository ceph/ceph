#!/bin/bash

. "`dirname $0`/test-rgw-common.sh"
. "`dirname $0`/test-rgw-meta-sync.sh"

# Do not use eval here. We have eval in test-rgw-common.sh:x(), so adding
# one here creates a double-eval situation. Passing arguments with spaces
# becomes impossible when double-eval strips escaping and quotes.
$@
