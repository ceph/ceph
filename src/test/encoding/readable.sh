#!/usr/bin/env bash
set -e


[ -z "$CEPH_ROOT" ] && CEPH_ROOT=..

# check if CORPUS_PATH is set then use it for dir, if not use $CEPH_ROOT/ceph-object-corpus
if [ -n "$CORPUS_PATH" ]; then
    dir=$CORPUS_PATH
else
    source $(dirname $0)/../detect-build-env-vars.sh
    dir=$CEPH_ROOT/ceph-object-corpus
fi

failed=0
numtests=0
pids=""

if [ -x ./ceph-dencoder ]; then
  CEPH_DENCODER=./ceph-dencoder
else
  CEPH_DENCODER=ceph-dencoder
fi

myversion=$($CEPH_DENCODER version)
echo "Using ceph-dencoder version $myversion"
if [ -z "$myversion" ]; then
  echo "Failed to get version from $CEPH_DENCODER"
  exit 1
fi
DEBUG=0
WAITALL_DELAY=.1
debug() { if [ "$DEBUG" -gt 0 ]; then echo "DEBUG: $*" >&2; fi }

version_le() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" = "$1" ]
}

version_lt() {
  version_le "$1" "$2" && [ "$1" != "$2" ]
}

version_ge() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]
}

versions_span() {
  local left=$1
  local right=$2
  local pivot=$3

  if version_le "$left" "$pivot" && version_ge "$right" "$pivot"; then
    return 0
  fi
  if version_le "$right" "$pivot" && version_ge "$left" "$pivot"; then
    return 0
  fi
  return 1
}

test_object() {
    local type=$1
    local output_file=$2
    local failed=0
    local numtests=0

    tmp1=$(mktemp /tmp/test_object_1-XXXXXXXXX)
    tmp2=$(mktemp /tmp/test_object_2-XXXXXXXXX)

    rm -f $output_file
    if $CEPH_DENCODER type $type 2>/dev/null; then
      #echo "type $type";
      echo "        $vdir/objects/$type"

      # Check for forward and backward incompat changes
      forward_versions=""
      forward_paths=""
      backward_incompat=""
      backward_incompat_paths=""
      sawarversion=0
      for iv in $(ls "$dir/archive" | sort -V); do
        if [ "$iv" = "$arversion" ]; then
          sawarversion=1
        fi

        # Record forward incompatibility markers
        marker_path="$dir/archive/$iv/forward_incompat/$type"
        if [ -e "$marker_path" ]; then
          if [ -d "$marker_path" ]; then
            if [ -n "$(ls "$marker_path"/ 2>/dev/null )" ]; then
              forward_paths="$forward_paths $iv:$marker_path"
            else
              echo "type $type directory empty, ignoring whole type instead of single objects"
              forward_versions="$forward_versions $iv"
            fi
          else
            forward_versions="$forward_versions $iv"
          fi
        fi

        # Check for backward incompatibility (affects this arversion)
        # backward_incompat at version X means decoders < X can't decode objects from X onwards
        # Check for backward_incompat in versions UP TO AND INCLUDING arversion
        if [ $sawarversion -eq 0 ] || [ "$iv" = "$arversion" ]; then
          if [ -e "$dir/archive/$iv/backward_incompat/$type" ]; then
            backward_incompat="$iv"

            # Check if we'll be ignoring only specified objects, not whole type
            if [ -d "$dir/archive/$iv/backward_incompat/$type" ]; then
              if [ -n "$(ls $dir/archive/$iv/backward_incompat/$type/ 2>/dev/null )" ]; then
                backward_incompat_paths="$backward_incompat_paths $dir/archive/$iv/backward_incompat/$type"
              else
                echo "type $type directory empty, ignoring whole type instead of single objects"
              fi;
            fi
          fi
        fi

      done

      # Skip if forward incompatibility places archive and decoder on opposite sides of a change
      if [ -n "$forward_versions" ]; then
        for forward_version in $forward_versions; do
          if versions_span "$myversion" "$arversion" "$forward_version"; then
            if version_lt "$myversion" "$forward_version"; then
              echo "skipping forward incompat $type version $arversion, requires decoder >= $forward_version, current decoder is $myversion"
            else
              echo "skipping forward incompat $type version $arversion, decoder >= $forward_version incompatible with objects < $forward_version (current decoder is $myversion)"
            fi
            rm -f $tmp1 $tmp2
            return
          fi
        done
      fi

      # Skip if our decoder is too old for backward compatibility requirement
      # Only skip whole type if NO per-object markers exist (like forward_incompat)
      if [ -n "$backward_incompat" ] && [ -z "$backward_incompat_paths" ]; then
        # Use sort -V for proper version comparison (handles 19.5 vs 19.10 correctly)
        if version_lt "$myversion" "$backward_incompat"; then
          echo "skipping backward incompat $type version $arversion, requires decoder >= $backward_incompat, current decoder is $myversion"
          rm -f $tmp1 $tmp2
          return
        fi
      fi

      for f in $(ls "$vdir/objects/$type"); do

        skip=0;
        # Check if processed object $f of $type should be skipped (postponed skip)
        # Check both forward_incompat and backward_incompat paths
        if [ -n "$forward_paths" ]; then
          for entry in $forward_paths; do
            marker_version=${entry%%:*}
            marker_path=${entry#*:}
            if versions_span "$myversion" "$arversion" "$marker_version"; then
              if [ -L "$marker_path/$f" ]; then
                if version_lt "$myversion" "$marker_version"; then
                  echo "skipping object $f of type $type (forward incompat requires decoder >= $marker_version, current is $myversion)"
                else
                  echo "skipping object $f of type $type (forward incompat for decoder >= $marker_version, current is $myversion)"
                fi
                skip=1
                break
              fi
            fi
          done
        fi;

        if [ -n "$backward_incompat_paths" ]; then
          # Only skip individual objects marked as backward_incompat if decoder is too old
          # Use sort -V for proper version comparison
          if version_lt "$myversion" "$backward_incompat"; then
            for b_path in $backward_incompat_paths; do
              # Check if $f is a symbolic link and if it's pointing to existing target
              if [ -L "$b_path/$f" ]; then
                echo "skipping object $f of type $type (backward incompat)"
                skip=1
                break
              fi;
            done;
          fi
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
          echo "**** failed to decode type $type from archive $arversion object $f (path $vdir/objects/$type/$f) ****"
          failed=$(($failed + 1))
          rm -f $tmp1 $tmp2
          continue
        fi
        if ! wait $pid2; then
          echo "**** failed to decode+encode+decode type $type from archive $arversion object $f (path $vdir/objects/$type/$f) ****"
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
          for tmpfile in $tmp1 $tmp2; do
            sort $tmpfile | sed 's/,$//' > $tmpfile.new
            mv $tmpfile.new $tmpfile
          done
        fi

        if ! cmp $tmp1 $tmp2; then
          echo "**** reencode of $vdir/objects/$type/$f resulted in a different dump ****"
          diff $tmp1 $tmp2
          failed=$(($failed + 1))
        fi
        numtests=$(($numtests + 1))
        rm -f $tmp1 $tmp2
      done
    else
      echo "skipping unrecognized type $type"
      rm -f $tmp1 $tmp2
    fi

    echo "failed=$failed" > $output_file
    echo "numtests=$numtests" >> $output_file
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

do_join() {
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
}

# Using $MAX_PARALLEL_JOBS jobs if defined, unless the number of logical
# processors
if [ $(uname) == FreeBSD -o $(uname) == Darwin ]; then
  NPROC=$(sysctl -n hw.ncpu)
  max_parallel_jobs=${MAX_PARALLEL_JOBS:-${NPROC}}
else
  max_parallel_jobs=${MAX_PARALLEL_JOBS:-$(nproc)}
fi

output_file=$(mktemp /tmp/output_file-XXXXXXXXX)
running_jobs=0

for arversion in $(ls $dir/archive | sort -V); do
  vdir="$dir/archive/$arversion"
  #echo $vdir

  if [ ! -d "$vdir/objects" ]; then
    continue;
  fi

  for type in $(ls $vdir/objects); do
    test_object $type $output_file.$running_jobs &
    pids="$pids $!"
    running_jobs=$(($running_jobs + 1))

    # Once we spawned enough jobs, let's wait them to complete
    # Every spawned job have almost the same execution time so
    # it's not a big deal having them not ending at the same time
    if [ "$running_jobs" -eq "$max_parallel_jobs" ]; then
	do_join
    fi
    rm -f ${output_file}*
  done
done

do_join

if [ $failed -gt 0 ]; then
  echo "FAILED $failed / $numtests tests."
  exit 1
fi

if [ $numtests -eq 0 ]; then
  echo "FAILED: no tests found to run!"
  exit 1
fi

echo "passed $numtests tests."
