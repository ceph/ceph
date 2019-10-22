# This file is part of the DeepSea integration test suite

#
# helper functions (not to be called directly from test scripts)
#

STAGE_TIMEOUT_DURATION="60m"

function _report_stage_failure {
    STAGE_SUCCEEDED=""
    local stage_num=$1
    local stage_status=$2

    echo "********** Stage $stage_num failed **********"
    test "$stage_status" = "124" && echo "Stage $stage_num timed out after $STAGE_TIMEOUT_DURATION"
    set -ex
    journalctl -r | head -n 2000
    echo "WWWW"
    echo "Finished dumping up to 2000 lines of journalctl"
}

function _run_stage {
    local stage_num=$1

    set +x
    echo ""
    echo "*********************************************"
    echo "********** Running DeepSea Stage $stage_num **********"
    echo "*********************************************"

    STAGE_SUCCEEDED="non-empty string"
    test -n "$CLI" && _run_stage_cli $stage_num || _run_stage_non_cli $stage_num
}

function _run_stage_cli {
    local stage_num=$1
    local deepsea_cli_output_path="/tmp/deepsea.${stage_num}.log"

    set +e
    set -x
    timeout $STAGE_TIMEOUT_DURATION \
        deepsea \
        --log-file=/var/log/salt/deepsea.log \
        --log-level=debug \
        stage \
        run \
        ceph.stage.${stage_num} \
        --simple-output \
        2>&1 | tee $deepsea_cli_output_path
    local stage_status="${PIPESTATUS[0]}"
    set +x
    echo "deepsea exit status: $stage_status"
    echo "WWWW"
    if [ "$stage_status" != "0" ] ; then
        _report_stage_failure $stage_num $stage_status
        return 0
    fi
    if grep -q -F "failed=0" $deepsea_cli_output_path ; then
        echo "********** Stage $stage_num completed successfully **********"
    else
        echo "ERROR: deepsea stage returned exit status 0, yet one or more steps failed. Bailing out!"
        _report_stage_failure $stage_num $stage_status
    fi
    set -ex
}

function _run_stage_non_cli {
    local stage_num=$1
    local stage_log_path="/tmp/stage.${stage_num}.log"

    set +e
    set -x
    timeout $STAGE_TIMEOUT_DURATION \
        salt-run \
        --no-color \
        state.orch \
        ceph.stage.${stage_num} \
        2>/dev/null | tee $stage_log_path
    local stage_status="${PIPESTATUS[0]}"
    set +x
    echo "WWWW"
    if [ "$stage_status" != "0" ] ; then
        _report_stage_failure $stage_num $stage_status
        return 0
    fi
    STAGE_FINISHED=$(grep -F 'Total states run' $stage_log_path)
    if [ "$STAGE_FINISHED" ]; then
        FAILED=$(grep -F 'Failed: ' $stage_log_path | sed 's/.*Failed:\s*//g' | head -1)
        if [ "$FAILED" -gt "0" ]; then
            echo "ERROR: salt-run returned exit status 0, yet one or more steps failed. Bailing out!"
            _report_stage_failure $stage_num $stage_status
        else
            echo "********** Stage $stage_num completed successfully **********"
        fi
    else
        echo "ERROR: salt-run returned exit status 0, yet Stage did not complete. Bailing out!"
        _report_stage_failure $stage_num $stage_status
    fi
    set -ex
}

function _client_node {
    salt --static --out json -C 'not I@roles:storage' test.ping 2>/dev/null | jq -r 'keys[0]'
}

function _master_has_role {
    local ROLE=$1
    echo "Asserting that master minion has role ->$ROLE<-"
    salt $MASTER_MINION pillar.get roles 2>/dev/null
    salt $MASTER_MINION pillar.get roles 2>/dev/null | grep -q "$ROLE"
    echo "Yes, it does."
}

function _first_x_node {
    local ROLE=$1
    salt --static --out json -C "I@roles:$ROLE" test.ping 2>/dev/null | jq -r 'keys[0]'
}

function _first_storage_only_node {
    local COMPOUND_TARGET="I@roles:storage"
    local NOT_ROLES="mon
mgr
mds
rgw
igw
ganesha
"
    local ROLE=
    for ROLE in $NOT_ROLES ; do
        COMPOUND_TARGET="$COMPOUND_TARGET and not I@roles:$ROLE"
    done
    local MAYBEJSON=$(salt --static --out json -C "$COMPOUND_TARGET" test.ping 2>/dev/null)
    echo $MAYBEJSON | jq --raw-output 'keys[0]'
}

function _run_test_script_on_node {
    local TESTSCRIPT=$1 # on success, TESTSCRIPT must output the exact string
                        # "Result: OK" on a line by itself, otherwise it will
                        # be considered to have failed
    local TESTNODE=$2
    local ASUSER=$3
    salt-cp $TESTNODE $TESTSCRIPT $TESTSCRIPT 2>/dev/null
    local LOGFILE=/tmp/test_script.log
    local STDERR_LOGFILE=/tmp/test_script_stderr.log
    local stage_status=
    if [ -z "$ASUSER" -o "x$ASUSER" = "xroot" ] ; then
      salt $TESTNODE cmd.run "sh $TESTSCRIPT" 2>$STDERR_LOGFILE | tee $LOGFILE
      stage_status="${PIPESTATUS[0]}"
    else
      salt $TESTNODE cmd.run "sudo su $ASUSER -c \"bash $TESTSCRIPT\"" 2>$STDERR_LOGFILE | tee $LOGFILE
      stage_status="${PIPESTATUS[0]}"
    fi
    local RESULT=$(grep -o -P '(?<=Result: )(OK)$' $LOGFILE) # since the script
                                  # is run by salt, the output appears indented
    test "x$RESULT" = "xOK" && return
    echo "The test script that ran on $TESTNODE failed. The stderr output was as follows:"
    cat $STDERR_LOGFILE
    exit 1
}

function _grace_period {
    local SECONDS=$1
    echo "${SECONDS}-second grace period"
    sleep $SECONDS
}

function _root_fs_is_btrfs {
    stat -f / | grep -q 'Type: btrfs'
}

function _ping_minions_until_all_respond {
    local RESPONDING=""
    for i in {1..20} ; do
        sleep 10
        RESPONDING=$(salt '*' test.ping 2>/dev/null | grep True 2>/dev/null | wc --lines)
        echo "Of $TOTAL_NODES total minions, $RESPONDING are responding"
        test "$TOTAL_NODES" -eq "$RESPONDING" && break
    done
}

function _ceph_cluster_running {
    ceph status >/dev/null 2>&1
}

