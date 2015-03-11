#!/bin/bash -ex

OBJ=blkin_qa_obj
FILE=/etc/passwd
TMP=/tmp/$(basename FILE).copy
POOL=blkin_qa_pool
TRACE=blkin_qa_trace

cleanup() {
    rados -p $POOL rm $OBJ || true
    rados rmpool $POOL $POOL --yes-i-really-really-mean-it || true
    rm $TMP || true
}

begin_tracing() {
    lttng destroy $TRACE || true
    lttng create $TRACE
    lttng enable-event --userspace zipkin:timestamp
    lttng enable-event --userspace zipkin:keyval
    lttng start
}

end_tracing() {
    lttng stop
    lttng view | grep zipkin
    lttng destroy $TRACE
}

test_rados() {
    cleanup
    rados mkpool $POOL
    rados -p $POOL put $OBJ $FILE
    rados -p $POOL ls
    rados -p $POOL get $OBJ $TMP
    cmp $TMP $FILE
    cleanup
}

begin_tracing
test_rados
end_tracing

echo OK
exit 0
