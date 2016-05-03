#!/bin/bash -e

source $(dirname $0)/../detect-build-env-vars.sh

[ -z "$CEPH_ROOT" ] && CEPH_ROOT=..

dir=$CEPH_ROOT/ceph-object-corpus

set -e

failed=0
numtests=0
pids=""

if [ -x ./ceph-dencoder ]; then
  CEPH_DENCODER=./ceph-dencoder
else
  CEPH_DENCODER=ceph-dencoder
fi

myversion=`$CEPH_DENCODER version`
DEBUG=0
WAITALL_DELAY=.1
debug() { if [ "$DEBUG" -gt 0 ]; then echo "DEBUG: $*" >&2; fi }

test_object() {
    local type=$1
    local output_file=$2
    local failed=0
    local numtests=0

    tmp1=`mktemp /tmp/typ-XXXXXXXXX`
    tmp2=`mktemp /tmp/typ-XXXXXXXXX`

    rm -f $output_file
    if $CEPH_DENCODER type $type 2>/dev/null; then
      #echo "type $type";
      echo "        $vdir/objects/$type"

      # is there a fwd incompat change between $arversion and $version?
      incompat=""
      incompat_paths=""
      sawarversion=0
      for iv in `ls -v $dir/archive`; do
        if [ "$iv" = "$arversion" ]; then
          sawarversion=1
        fi

        if [ $sawarversion -eq 1 ] && [ -e "$dir/archive/$iv/forward_incompat/$type" ]; then
          incompat="$iv"

          # Check if we'll be ignoring only specified objects, not whole type. If so, remember
          # all paths for this type into variable. Assuming that this path won't contain any
          # whitechars (implication of above for loop).
          if [ -d "$dir/archive/$iv/forward_incompat/$type" ]; then
            if [ -n "`ls -v $dir/archive/$iv/forward_incompat/$type/`" ]; then
              incompat_paths="$incompat_paths $dir/archive/$iv/forward_incompat/$type"
            else
              echo "type $type directory empty, ignoring whole type instead of single objects"
            fi;
          fi
        fi

        if [ "$iv" = "$version" ]; then
          break
        fi
      done

      if [ -n "$incompat" ]; then
        if [ -z "$incompat_paths" ]; then
          echo "skipping incompat $type version $arversion, changed at $incompat < code $myversion"
          continue
        else
          # If we are ignoring not whole type, but objects that are in $incompat_path,
          # we don't skip here, just give info.
          echo "postponed skip one of incompact $type version $arversion, changed at $incompat < code $myversion"
        fi;
      fi

      for f in `ls $vdir/objects/$type`; do

        skip=0;
        # Check if processed object $f of $type should be skipped (postponed skip)
        if [ -n "$incompat_paths" ]; then
            for i_path in $incompat_paths; do
              # Check if $f is a symbolic link and if it's pointing to existing target
              if [ -L "$i_path/$f" ]; then
                echo "skipping object $f of type $type"
                skip=1
                break
              fi;
            done;
        fi;

        if [ $skip -ne 0 ]; then
          continue
        fi;

        $CEPH_DENCODER type $type import $vdir/objects/$type/$f decode dump_json > $tmp1 &
        pid1="$!"
        $CEPH_DENCODER type $type import $vdir/objects/$type/$f decode encode decode dump_json > $tmp2 &
        pid2="$!"
        #echo "\t$vdir/$type/$f"
        if ! wait $pid1; then
          echo "**** failed to decode $vdir/objects/$type/$f ****"
          failed=$(($failed + 1))
          rm -f $tmp1 $tmp2
          continue      
        fi
        if ! wait $pid2; then
          echo "**** failed to decode+encode+decode $vdir/objects/$type/$f ****"
          failed=$(($failed + 1))
          rm -f $tmp1 $tmp2
          continue
        fi

        # nondeterministic classes may dump
        # nondeterministically.  compare the sorted json
        # output.  this is a weaker test, but is better than
        # nothing.
        if ! $CEPH_DENCODER type $type is_deterministic; then
          echo "  sorting json output for nondeterministic object"
          for f in $tmp1 $tmp2; do
            sort $f | sed 's/,$//' > $f.new
            mv $f.new $f
          done
        fi

        if ! cmp $tmp1 $tmp2; then
          echo "**** reencode of $vdir/objects/$type/$f resulted in a different dump ****"
          diff $tmp1 $tmp2
          failed=$(($failed + 1))
        fi
        numtests=$(($numtests + 1))
        echo "failed=$failed" > $output_file
        echo "numtests=$numtests" >> $output_file
      done
    else
      echo "skipping unrecognized type $type"
    fi

    rm -f $tmp1 $tmp2
}

waitall() { # PID...
   ## Wait for children to exit and indicate whether all exited with 0 status.
   local errors=0
   while :; do
     debug "Processes remaining: $*"
     for pid in "$@"; do
       shift
       if kill -0 "$pid" 2>/dev/null; then
         debug "$pid is still alive."
         set -- "$@" "$pid"
       elif wait "$pid"; then
         debug "$pid exited with zero exit status."
       else
         debug "$pid exited with non-zero exit status."
         errors=$(($errors + 1))
       fi
     done
     [ $# -eq 0 ] && break
     sleep ${WAITALL_DELAY:-1}
    done
   [ $errors -eq 0 ]
}

######
# MAIN
######

# Using $MAX_PARALLEL_JOBS jobs if defined, unless the number of logical
# processors
max_parallel_jobs=${MAX_PARALLEL_JOBS:-$(nproc)}

for arversion in `ls -v $dir/archive`; do
  vdir="$dir/archive/$arversion"
  #echo $vdir

  if [ ! -d "$vdir/objects" ]; then
    continue;
  fi

  output_file=`mktemp /tmp/typ-XXXXXXXXX`
  running_jobs=0
  for type in `ls $vdir/objects`; do
    test_object $type $output_file.$running_jobs &
    pids="$pids $!"
    running_jobs=$(($running_jobs + 1))

    # Once we spawned enough jobs, let's wait them to complete
    # Every spawned job have almost the same execution time so
    # it's not a big deal having them not ending at the same time
    if [ "$running_jobs" -eq "$max_parallel_jobs" ]; then
        waitall $pids
        pids=""
        # Reading the output of jobs to compute failed & numtests
        # Tests are run in parallel but sum should be done sequentialy to avoid
        # races between threads
        while [ "$running_jobs" -ge 0 ]; do
            if [ -f $output_file.$running_jobs ]; then
                read_failed=$(grep "^failed=" $output_file.$running_jobs | cut -d "=" -f 2)
                read_numtests=$(grep "^numtests=" $output_file.$running_jobs | cut -d "=" -f 2)
                rm -f $output_file.$running_jobs
                failed=$(($failed + $read_failed))
                numtests=$(($numtests + $read_numtests))
            fi
            running_jobs=$(($running_jobs - 1))
        done
        running_jobs=0
    fi
  done
done

if [ $failed -gt 0 ]; then
  echo "FAILED $failed / $numtests tests."
  exit 1
fi
echo "passed $numtests tests."

