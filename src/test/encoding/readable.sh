#!/bin/sh -e

dir=../ceph-object-corpus

set -e

tmp1=`mktemp /tmp/typ-XXXXXXXXX`
tmp2=`mktemp /tmp/typ-XXXXXXXXX`

failed=0
numtests=0

myversion=`./ceph-dencoder version`

for arversion in `ls -v $dir/archive`; do
  vdir="$dir/archive/$arversion"
  #echo $vdir

  if [ ! -d "$vdir/objects" ]; then
    continue;
  fi

  for type in `ls $vdir/objects`; do
    if ./ceph-dencoder type $type 2>/dev/null; then
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

        #echo "\t$vdir/$type/$f"
        if ! ./ceph-dencoder type $type import $vdir/objects/$type/$f decode dump_json > $tmp1; then
          echo "**** failed to decode $vdir/objects/$type/$f ****"
          failed=$(($failed + 1))
          continue      
        fi
        if ! ./ceph-dencoder type $type import $vdir/objects/$type/$f decode encode decode dump_json > $tmp2; then
          echo "**** failed to decode+encode+decode $vdir/objects/$type/$f ****"
          failed=$(($failed + 1))
          continue
        fi

        # nondeterministic classes may dump
        # nondeterministically.  compare the sorted json
        # output.  this is a weaker test, but is better than
        # nothing.
        if ! ./ceph-dencoder type $type is_deterministic; then
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
      done
    else
      echo "skipping unrecognized type $type"
    fi
  done
done

rm -f $tmp1 $tmp2

if [ $failed -gt 0 ]; then
  echo "FAILED $failed / $numtests tests."
  exit 1
fi
echo "passed $numtests tests."

