#!/usr/bin/env bash
#
# Copyright (C) 2022 Red Hat <contact@redhat.com>
#
# Author: Sridhar Seshasayee <sseshasa@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7124" # git grep '\<7124\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    CEPH_ARGS+="--debug-mclock 20 "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function TEST_profile_builtin_to_custom() {
    local dir=$1

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1

    # Verify the default mclock profile on the OSD
    local mclock_profile=$(ceph config get osd.0 osd_mclock_profile)
    test "$mclock_profile" = "balanced" || return 1

    # Verify the running mClock profile
    mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile |\
      jq .osd_mclock_profile)
    mclock_profile=$(eval echo $mclock_profile)
    test "$mclock_profile" = "high_recovery_ops" || return 1

    # Change the mclock profile to 'custom'
    ceph tell osd.0 config set osd_mclock_profile custom || return 1

    # Verify that the mclock profile is set to 'custom' on the OSDs
    mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_profile | jq .osd_mclock_profile)
    mclock_profile=$(eval echo $mclock_profile)
    test "$mclock_profile" = "custom" || return 1

    # Change a mclock config param and confirm the change
    local client_res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    echo "client_res = $client_res"
    local client_res_new=$(echo "$client_res + 0.1" | bc -l)
    echo "client_res_new = $client_res_new"
    ceph config set osd.0 osd_mclock_scheduler_client_res \
      $client_res_new || return 1

    # Check value in config monitor db
    local res=$(ceph config get osd.0 \
      osd_mclock_scheduler_client_res) || return 1
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi
    # Check value in the in-memory 'values' map
    res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi

    teardown $dir || return 1
}

function TEST_profile_custom_to_builtin() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1

    # Verify the default mclock profile on the OSD
    local def_mclock_profile
    def_mclock_profile=$(ceph config get osd.0 osd_mclock_profile)
    test "$def_mclock_profile" = "balanced" || return 1

    # Verify the running mClock profile
    local orig_mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile |\
      jq .osd_mclock_profile)
    orig_mclock_profile=$(eval echo $orig_mclock_profile)
    test $orig_mclock_profile = "high_recovery_ops" || return 1

    # Change the mclock profile to 'custom'
    ceph tell osd.0 config set osd_mclock_profile custom || return 1

    # Verify that the mclock profile is set to 'custom' on the OSDs
    local mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile | \
      jq .osd_mclock_profile)
    mclock_profile=$(eval echo $mclock_profile)
    test $mclock_profile = "custom" || return 1

    # Save the original client reservations allocated to the OSDs
    local client_res
    client_res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    echo "Original client_res for osd.0 = $client_res"

    # Change a mclock config param and confirm the change
    local client_res_new=$(echo "$client_res + 0.1" | bc -l)
    echo "client_res_new = $client_res_new"
    ceph config set osd osd_mclock_scheduler_client_res \
      $client_res_new || return 1
    # Check value in config monitor db
    local res=$(ceph config get osd.0 \
      osd_mclock_scheduler_client_res) || return 1
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi
    # Check value in the in-memory 'values' map
    res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi

    # Switch the mclock profile back to the original built-in profile.
    # The config subsystem prevents the overwrite of the changed QoS config
    # option above i.e. osd_mclock_scheduler_client_res. This fact is verified
    # before proceeding to remove the entry from the config monitor db. After
    # the config entry is removed, the original value for the config option is
    # restored and is verified.
    ceph tell osd.0 config set osd_mclock_profile $orig_mclock_profile || return 1
    # Verify that the mclock profile is set back to the original on the OSD
    eval mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile | \
      jq .osd_mclock_profile)
    #mclock_profile=$(ceph config get osd.0 osd_mclock_profile)
    test "$mclock_profile" = "$orig_mclock_profile" || return 1

    # Verify that the new value is still in effect
    # Check value in config monitor db
    local res=$(ceph config get osd.0 \
      osd_mclock_scheduler_client_res) || return 1
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi
    # Check value in the in-memory 'values' map
    res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    if (( $(echo "$res != $client_res_new" | bc -l) )); then
      return 1
    fi

    # Remove the changed QoS config option from monitor db
    ceph config rm osd osd_mclock_scheduler_client_res || return 1

    sleep 5 # Allow time for change to take effect

    # Verify that the original values are now restored
    # Check value in config monitor db
    res=$(ceph config get osd.0 \
      osd_mclock_scheduler_client_res) || return 1
    if (( $(echo "$res != 0.0" | bc -l) )); then
      return 1
    fi

    # Check value in the in-memory 'values' map
    res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
      osd.0) config get osd_mclock_scheduler_client_res | \
      jq .osd_mclock_scheduler_client_res | bc)
    if (( $(echo "$res != $client_res" | bc -l) )); then
      return 1
    fi

    teardown $dir || return 1
}

function TEST_recovery_limit_adjustment_mclock() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1
    local recoveries=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_recovery_max_active)
    # Get default value
    echo "$recoveries" | grep --quiet 'osd_recovery_max_active' || return 1

    # Change the recovery limit without setting
    # osd_mclock_override_recovery_settings option. Verify that the recovery
    # limit is retained at its default value.
    ceph config set osd.0 osd_recovery_max_active 10 || return 1
    sleep 2 # Allow time for change to take effect
    local max_recoveries=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_recovery_max_active)
    test "$max_recoveries" = "$recoveries" || return 1

    # Change recovery limit after setting osd_mclock_override_recovery_settings.
    # Verify that the recovery limit is modified.
    ceph config set osd.0 osd_mclock_override_recovery_settings true || return 1
    ceph config set osd.0 osd_recovery_max_active 10 || return 1
    sleep 2 # Allow time for change to take effect
    max_recoveries=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_recovery_max_active)
    test "$max_recoveries" = '{"osd_recovery_max_active":"10"}' || return 1

    teardown $dir || return 1
}

function TEST_backfill_limit_adjustment_mclock() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1
    local backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills | jq .osd_max_backfills | bc)
    # Get default value
    echo "osd_max_backfills: $backfills" || return 1

    # Change the backfill limit without setting
    # osd_mclock_override_recovery_settings option. Verify that the backfill
    # limit is retained at its default value.
    ceph config set osd.0 osd_max_backfills 20 || return 1
    sleep 2 # Allow time for change to take effect
    local max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills | jq .osd_max_backfills | bc)
    test $max_backfills = $backfills || return 1

    # Verify local and async reserver settings are not changed
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .local_reservations.max_allowed | bc)
    test $max_backfills = $backfills || return 1
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .remote_reservations.max_allowed | bc)
    test $max_backfills = $backfills || return 1

    # Change backfills limit after setting osd_mclock_override_recovery_settings.
    # Verify that the backfills limit is modified.
    ceph config set osd.0 osd_mclock_override_recovery_settings true || return 1
    ceph config set osd.0 osd_max_backfills 20 || return 1
    sleep 2 # Allow time for change to take effect
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills | jq .osd_max_backfills | bc)
    test $max_backfills = 20 || return 1

    # Verify local and async reserver settings are changed
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .local_reservations.max_allowed | bc)
    test $max_backfills = 20 || return 1
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .remote_reservations.max_allowed | bc)
    test $max_backfills = 20 || return 1

    # Kill osd and bring it back up.
    # Confirm that the backfill settings are retained.
    kill_daemons $dir TERM osd || return 1
    ceph osd down 0 || return 1
    wait_for_osd down 0 || return 1
    activate_osd $dir 0 --osd-op-queue=mclock_scheduler || return 1

    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        config get osd_max_backfills | jq .osd_max_backfills | bc)
    test $max_backfills = 20 || return 1

    # Verify local and async reserver settings are changed
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .local_reservations.max_allowed | bc)
    test $max_backfills = 20 || return 1
    max_backfills=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path osd.0) \
        dump_recovery_reservations | jq .remote_reservations.max_allowed | bc)
    test $max_backfills = 20 || return 1

    teardown $dir || return 1
}

function TEST_profile_disallow_builtin_params_modify() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1

    # Verify that the default mclock profile is set on the OSD
    local def_mclock_profile=$(ceph config get osd.0 osd_mclock_profile)
    test "$def_mclock_profile" = "balanced" || return 1

    # Verify the running mClock profile
    local cur_mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile |\
      jq .osd_mclock_profile)
    cur_mclock_profile=$(eval echo $cur_mclock_profile)
    test $cur_mclock_profile = "high_recovery_ops" || return 1

    declare -a options=("osd_mclock_scheduler_background_recovery_res"
      "osd_mclock_scheduler_client_res")

    local retries=10
    local errors=0
    for opt in "${options[@]}"
    do
      # Try and change a mclock config param and confirm that no change occurred
      local opt_val_orig=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get $opt | jq .$opt | bc)
      local opt_val_new=$(echo "$opt_val_orig + 0.1" | bc -l)
      ceph config set osd.0 $opt $opt_val_new || return 1

      # Check configuration values
      for count in $(seq 0 $(expr $retries - 1))
      do
        errors=0
        sleep 2 # Allow time for changes to take effect

        echo "Check configuration values - Attempt#: $count"
        # Check configuration value on Mon store (or the default) for the osd
        local res=$(ceph config get osd.0 $opt) || return 1
        echo "Mon db (or default): osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) )); then
          errors=$(expr $errors + 1)
        fi

        # Check running configuration value using "config show" cmd
        res=$(ceph config show osd.0 | grep $opt |\
          awk '{ print $2 }' | bc ) || return 1
        echo "Running config: osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) || \
              $(echo "$res != $opt_val_orig" | bc -l)  )); then
          errors=$(expr $errors + 1)
        fi

        # Check value in the in-memory 'values' map is unmodified
        res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
          osd.0) config get $opt | jq .$opt | bc)
        echo "Values map: osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) || \
              $(echo "$res != $opt_val_orig" | bc -l) )); then
          errors=$(expr $errors + 1)
        fi

        # Check if we succeeded or exhausted retry count
        if [ $errors -eq 0 ]
        then
          break
        elif [ $count -eq $(expr $retries - 1) ]
        then
          return 1
        fi
      done
    done

    teardown $dir || return 1
}

function TEST_profile_disallow_builtin_params_override() {
    local dir=$1

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1

    run_osd $dir 0 --osd_op_queue=mclock_scheduler || return 1

    # Verify that the default mclock profile is set on the OSD
    local def_mclock_profile=$(ceph config get osd.0 osd_mclock_profile)
    test "$def_mclock_profile" = "balanced" || return 1

    # Verify the running mClock profile
    local cur_mclock_profile=$(CEPH_ARGS='' ceph --format=json daemon \
      $(get_asok_path osd.0) config get osd_mclock_profile |\
      jq .osd_mclock_profile)
    cur_mclock_profile=$(eval echo $cur_mclock_profile)
    test $cur_mclock_profile = "high_recovery_ops" || return 1

    declare -a options=("osd_mclock_scheduler_background_recovery_res"
      "osd_mclock_scheduler_client_res")

    local retries=10
    local errors=0
    for opt in "${options[@]}"
    do
      # Override a mclock config param and confirm that no change occurred
      local opt_val_orig=$(CEPH_ARGS='' ceph --format=json daemon \
        $(get_asok_path osd.0) config get $opt | jq .$opt | bc)
      local opt_val_new=$(echo "$opt_val_orig + 0.1" | bc -l)
      ceph tell osd.0 config set $opt $opt_val_new || return 1

      # Check configuration values
      for count in $(seq 0 $(expr $retries - 1))
      do
        errors=0
        sleep 2 # Allow time for changes to take effect

        echo "Check configuration values - Attempt#: $count"
        # Check configuration value on Mon store (or the default) for the osd
        local res=$(ceph config get osd.0 $opt) || return 1
        echo "Mon db (or default): osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) )); then
          errors=$(expr $errors + 1)
        fi

        # Check running configuration value using "config show" cmd
        res=$(ceph config show osd.0 | grep $opt |\
          awk '{ print $2 }' | bc ) || return 1
        echo "Running config: osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) || \
              $(echo "$res != $opt_val_orig" | bc -l)  )); then
          errors=$(expr $errors + 1)
        fi

        # Check value in the in-memory 'values' map is unmodified
        res=$(CEPH_ARGS='' ceph --format=json daemon $(get_asok_path \
          osd.0) config get $opt | jq .$opt | bc)
        echo "Values map: osd.0 $opt = $res"
        if (( $(echo "$res == $opt_val_new" | bc -l) || \
              $(echo "$res != $opt_val_orig" | bc -l) )); then
          errors=$(expr $errors + 1)
        fi

        # Check if we succeeded or exhausted retry count
        if [ $errors -eq 0 ]
        then
          break
        elif [ $count -eq $(expr $retries - 1) ]
        then
          return 1
        fi
      done
    done

    teardown $dir || return 1
}

main mclock-config "$@"

# Local Variables:
# compile-command: "cd build ; make -j4 && \
#   ../qa/run-standalone.sh mclock-config.sh"
# End:
