#!/usr/bin/env bash

# INITIALIZATION

mydir=$(dirname $0)
data_pool=default.rgw.buckets.data
orphan_list_out=/tmp/orphan_list.$$
radoslist_out=/tmp/radoslist.$$
rados_ls_out=/tmp/rados_ls.$$
diff_out=/tmp/diff.$$

rgw_host="$(hostname --fqdn)"
echo "INFO: fully qualified domain name: $rgw_host"

export RGW_ACCESS_KEY="0555b35654ad1656d804"
export RGW_SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
export RGW_HOST="${RGW_HOST:-$rgw_host}"

# random argument determines if multipart is aborted or completed 50/50
outcome=$((RANDOM % 2))
if [ $outcome -eq 0 ] ;then
    echo "== TESTING *ABORTING* MULTIPART UPLOAD WITH RE-UPLOADS =="
else
    echo "== TESTING *COMPLETING* MULTIPART UPLOAD WITH RE-UPLOADS =="
fi

# random argument determines if multipart is aborted or completed 50/50
versioning=$((RANDOM % 2))
if [ $versioning -eq 0 ] ;then
    echo "== TESTING NON-VERSIONED BUCKET =="
else
    echo "== TESTING VERSIONED BUCKET =="
fi

# create a randomized bucket name
bucket="reupload-bkt-$((RANDOM % 899999 + 100000))"


# SET UP PYTHON VIRTUAL ENVIRONMENT

# install boto3
python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install boto3


# CREATE RGW USER IF NECESSARY

if radosgw-admin user info --access-key $RGW_ACCESS_KEY 2>/dev/null ;then
    echo INFO: user already exists
else
    echo INFO: creating user
    radosgw-admin user create --uid testid \
		  --access-key $RGW_ACCESS_KEY \
		  --secret $RGW_SECRET_KEY \
		  --display-name 'M. Tester' \
		  --email tester@ceph.com 2>/dev/null
fi


# RUN REUPLOAD TEST

$mydir/bin/python3 ${mydir}/test_rgw_s3_mp_reupload.py $bucket $outcome $versioning


# ANALYZE FOR ERRORS
# (NOTE: for now we're choosing not to use the rgw-orphan-list tool)

# force garbage collection to remove extra parts
radosgw-admin gc process --include-all 2>/dev/null

marker=$(radosgw-admin metadata get bucket:$bucket 2>/dev/null | grep bucket_id | sed 's/.*: "\(.*\)".*/\1/')

# determine expected rados objects
radosgw-admin bucket radoslist --bucket=$bucket 2>/dev/null | sort >$radoslist_out
echo "radosgw-admin bucket radoslist:"
cat $radoslist_out

# determine found rados objects
rados ls -p $data_pool 2>/dev/null | grep "^$marker" | sort >$rados_ls_out
echo "rados ls:"
cat $rados_ls_out

# compare expected and found
diff $radoslist_out $rados_ls_out >$diff_out
if [ $(cat $diff_out | wc -l) -ne 0 ] ;then
    error=1
    echo "ERROR: Found differences between expected and actual rados objects for test bucket."
    echo "    note: indicators: '>' found but not expected; '<' expected but not found."
    cat $diff_out
fi


# CLEAN UP

deactivate

rm -f $orphan_list_out $radoslist_out $rados_ls_out $diff_out


# PRODUCE FINAL RESULTS

if [ -n "$error" ] ;then
    echo "== FAILED =="
    exit 1
fi

echo "== PASSED =="
exit 0
