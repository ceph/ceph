#! /usr/bin/env bash
ed /etc/ansible/hosts << EOF
$
a

[mons]
${1}

[osds]
${2}
${3}
${4}

.
w
q
EOF
