#! /bin/sh

sudo vi -e /etc/sudoers <<EOF
g/  requiretty/s// !requiretty/
g/ !visiblepw/s//  visiblepw/
w!
q
EOF
exit

