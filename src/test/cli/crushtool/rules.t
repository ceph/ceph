  $ crushtool -c $TESTDIR/rules.txt --create-replicated-rule foo default host -o one > /dev/null
  $ crushtool -d one
  # begin crush map
  
  # devices
  device 0 osd.0 class ssd
  device 1 osd.1 class ssd
  device 2 osd.2 class ssd
  device 3 osd.3 class hdd
  device 4 osd.4 class hdd
  device 5 osd.5 class hdd
  
  # types
  type 0 osd
  type 1 host
  type 2 root
  
  # buckets
  host foo {
  \tid -3\t\t# do not change unnecessarily (esc)
  \tid -4 class ssd\t\t# do not change unnecessarily (esc)
  \tid -7 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.0 weight 1.000 (esc)
  \titem osd.1 weight 1.000 (esc)
  \titem osd.2 weight 1.000 (esc)
  }
  host bar {
  \tid -2\t\t# do not change unnecessarily (esc)
  \tid -5 class ssd\t\t# do not change unnecessarily (esc)
  \tid -8 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.3 weight 1.000 (esc)
  \titem osd.4 weight 1.000 (esc)
  \titem osd.5 weight 1.000 (esc)
  }
  root default {
  \tid -1\t\t# do not change unnecessarily (esc)
  \tid -6 class ssd\t\t# do not change unnecessarily (esc)
  \tid -9 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 6.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem foo weight 3.000 (esc)
  \titem bar weight 3.000 (esc)
  }
  
  # rules
  rule data {
  \tid 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule foo {
  \tid 1 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map










  $ crushtool -c $TESTDIR/rules.txt --create-replicated-rule foo-ssd default host -o two --device-class ssd > /dev/null
  $ crushtool -d two
  # begin crush map
  
  # devices
  device 0 osd.0 class ssd
  device 1 osd.1 class ssd
  device 2 osd.2 class ssd
  device 3 osd.3 class hdd
  device 4 osd.4 class hdd
  device 5 osd.5 class hdd
  
  # types
  type 0 osd
  type 1 host
  type 2 root
  
  # buckets
  host foo {
  \tid -3\t\t# do not change unnecessarily (esc)
  \tid -4 class ssd\t\t# do not change unnecessarily (esc)
  \tid -7 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.0 weight 1.000 (esc)
  \titem osd.1 weight 1.000 (esc)
  \titem osd.2 weight 1.000 (esc)
  }
  host bar {
  \tid -2\t\t# do not change unnecessarily (esc)
  \tid -5 class ssd\t\t# do not change unnecessarily (esc)
  \tid -8 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 3.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.3 weight 1.000 (esc)
  \titem osd.4 weight 1.000 (esc)
  \titem osd.5 weight 1.000 (esc)
  }
  root default {
  \tid -1\t\t# do not change unnecessarily (esc)
  \tid -6 class ssd\t\t# do not change unnecessarily (esc)
  \tid -9 class hdd\t\t# do not change unnecessarily (esc)
  \t# weight 6.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem foo weight 3.000 (esc)
  \titem bar weight 3.000 (esc)
  }
  
  # rules
  rule data {
  \tid 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  rule foo-ssd {
  \tid 1 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default class ssd (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map





