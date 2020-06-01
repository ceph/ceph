#!/usr/bin/env bash

set -ex

# if defined, debug messages will be displayed and prepended with the string
# debug="DEBUG"

huge_size=2222 # in megabytes
big_size=6 # in megabytes

huge_obj=/tmp/huge_obj.temp.$$
big_obj=/tmp/big_obj.temp.$$
empty_obj=/tmp/empty_obj.temp.$$

fifo=/tmp/orphan-fifo.$$
awscli_dir=${HOME}/awscli_temp
export PATH=${PATH}:${awscli_dir}

rgw_host=$(hostname --fqdn)
rgw_port=80

echo "Fully Qualified Domain Name: $rgw_host"

success() {
    echo OK.
    exit 0
}

########################################################################
# INSTALL AND CONFIGURE TOOLING

install_awscli() {
    # NB: this does verify authenticity and integrity of downloaded
    # file; see
    # https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html
    here="$(pwd)"
    cd "$HOME"
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    mkdir -p $awscli_dir
    ./aws/install -i $awscli_dir
    cd "$here"
}

uninstall_awscli() {
    here="$(pwd)"
    cd "$HOME"
    rm -rf $awscli_dir ./aws awscliv2.zip
    cd "$here"
}

sudo dnf install -y s3cmd

sudo yum install python3-setuptools
sudo yum -y install python3-pip
sudo pip3 install --upgrade setuptools
sudo pip3 install python-swiftclient

# get ready for transition from s3cmd to awscli
if false ;then
    install_awscli
    aws --version
    uninstall_awscli
fi

s3config=/tmp/s3config.$$

# do not include the port when it is 80; the host base is used in the
# v4 signature and it needs to follow this convention for signatures
# to match
if [ "$rgw_port" -ne 80 ] ;then
    s3_host_base="${rgw_host}:${rgw_port}"
else
    s3_host_base="$rgw_host"
fi

cat >${s3config} <<EOF
[default]
host_base = $s3_host_base
access_key = 0555b35654ad1656d804
secret_key = h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==
bucket_location = us-east-1
check_ssl_certificate = True
check_ssl_hostname = True
default_mime_type = binary/octet-stream
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
follow_symlinks = False
force = False
guess_mime_type = True
host_bucket = anything.with.three.dots
multipart_chunk_size_mb = 15
multipart_max_chunks = 10000
recursive = False
recv_chunk = 65536
send_chunk = 65536
signature_v2 = False
socket_timeout = 300
use_https = False
use_mime_magic = True
verbosity = WARNING
EOF


# set up swift authentication
export ST_AUTH=http://${rgw_host}:${rgw_port}/auth/v1.0
export ST_USER=test:tester
export ST_KEY=testing

create_users() {
    # Create S3 user
    local akey='0555b35654ad1656d804'
    local skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    radosgw-admin user create --uid testid \
		  --access-key $akey --secret $skey \
		  --display-name 'M. Tester' --email tester@ceph.com

    # Create Swift user
    radosgw-admin user create --subuser=test:tester \
		  --display-name=Tester-Subuser --key-type=swift \
		  --secret=testing --access=full
}

myswift() {
    if [ -n "$debug" ] ;then
	echo "${debug}: swift --verbose --debug $@"
    fi
    swift --verbose --debug "$@"
    local code=$?
    if [ $code -ne 0 ] ;then
	echo "ERROR: code = $code ; command = s3cmd --config=${s3config} --verbose --debug "$@""
	exit $code
    fi
}

mys3cmd() {
    if [ -n "$debug" ] ;then
	echo "${debug}: s3cmd --config=${s3config} --verbose --debug $@"
    fi
    s3cmd --config=${s3config} --verbose --debug "$@"
    local code=$?
    if [ $code -ne 0 ] ;then
	echo "ERROR: code = $code ; command = s3cmd --config=${s3config} --verbose --debug "$@""
	exit $code
    fi
}

mys3uploadkill() {
    if [ $# -ne 5 ] ;then
	echo "$0: error expecting 5 arguments"
	exit 1
    fi

    set -v
    local_file="$1"
    remote_bkt="$2"
    remote_obj="$3"
    fifo="$4"
    stop_part="$5"
    
    mkfifo $fifo

    s3cmd --config=${s3config} put $local_file \
	  s3://${remote_bkt}/${remote_obj} \
	  --progress \
	  --multipart-chunk-size-mb=5 >$fifo &
    set +e # don't allow errors to stop script
    while read line ;do
        echo "$line" | grep --quiet "part $stop_part "
        if [ ${PIPESTATUS[1]} -eq 0 ] ;then
	    kill -9 $(jobs -p)
            break
        fi
    done <$fifo
    set -e

    rm -f $fifo
}

mys3upload() {
    obj=$1
    bucket=$2
    dest_obj=$3

    mys3cmd put -q $obj s3://${bucket}/$dest_obj
}

########################################################################
# PREP

create_users
dd if=/dev/urandom of=$big_obj bs=1M count=${big_size}
dd if=/dev/urandom of=$huge_obj bs=1M count=${huge_size}
touch $empty_obj

quick_tests() {
    echo TRY A SWIFT COMMAND
    myswift upload swift-plain-ctr $big_obj --object-name swift-obj-test
    myswift list
    myswift list swift-plain-ctr

    echo TRY A RADOSGW-ADMIN COMMAND
    radosgw-admin bucket list # make sure rgw is up and running
}

########################################################################
# S3 TESTS

####################################
# regular multipart test

mys3cmd mb s3://multipart-bkt
mys3upload $huge_obj multipart-bkt multipart-obj
mys3cmd ls
mys3cmd ls s3://multipart-bkt

####################################
# multipart test with incomplete uploads

bkt="incomplete-mp-bkt-1"

mys3cmd mb s3://$bkt
mys3uploadkill $huge_obj $bkt incomplete-mp-obj-1 $fifo 20
mys3uploadkill $huge_obj $bkt incomplete-mp-obj-2 $fifo 100

####################################
# resharded bucket

bkt=resharded-bkt-1

mys3cmd mb s3://$bkt

for f in $(seq 8) ; do
    dest_obj="reshard-obj-${f}"
    mys3cmd put -q $big_obj s3://${bkt}/$dest_obj
done

radosgw-admin bucket reshard --num-shards 3 --bucket=$bkt --yes-i-really-mean-it
radosgw-admin bucket reshard --num-shards 5 --bucket=$bkt --yes-i-really-mean-it

####################################
# versioned bucket

if true ;then
    echo "WARNING: versioned bucket test currently turned off"
else
    bkt=versioned-bkt-1

    mys3cmd mb s3://$bkt

    # bucket-enable-versioning $bkt

    for f in $(seq 3) ;do
	for g in $(seq 10) ;do
	    dest_obj="versioned-obj-${g}"
	    mys3cmd put -q $big_obj s3://${bkt}/$dest_obj
	done
    done

    for g in $(seq 1 2 10) ;do
	dest_obj="versioned-obj-${g}"
	mys3cmd rm s3://${bkt}/$dest_obj
    done
fi

############################################################
# copy small objects

o_bkt="orig-bkt-1"
d_bkt="copy-bkt-1"
mys3cmd mb s3://$o_bkt

for f in $(seq 4) ;do
    dest_obj="orig-obj-$f"
    mys3cmd put -q $big_obj s3://${o_bkt}/$dest_obj
done

mys3cmd mb s3://$d_bkt

mys3cmd cp s3://${o_bkt}/orig-obj-1 s3://${d_bkt}/copied-obj-1
mys3cmd cp s3://${o_bkt}/orig-obj-3 s3://${d_bkt}/copied-obj-3

for f in $(seq 5 6) ;do
    dest_obj="orig-obj-$f"
    mys3cmd put -q $big_obj s3://${d_bkt}/$dest_obj
done

############################################################
# copy small objects and delete original

o_bkt="orig-bkt-2"
d_bkt="copy-bkt-2"

mys3cmd mb s3://$o_bkt

for f in $(seq 4) ;do
    dest_obj="orig-obj-$f"
    mys3cmd put -q $big_obj s3://${o_bkt}/$dest_obj
done

mys3cmd mb s3://$d_bkt

mys3cmd cp s3://${o_bkt}/orig-obj-1 s3://${d_bkt}/copied-obj-1
mys3cmd cp s3://${o_bkt}/orig-obj-3 s3://${d_bkt}/copied-obj-3

for f in $(seq 5 6) ;do
    dest_obj="orig-obj-$f"
    mys3cmd put -q $big_obj s3://${d_bkt}/$dest_obj
done

mys3cmd rb --recursive s3://${o_bkt}

############################################################
# copy multipart objects

o_bkt="orig-mp-bkt-3"
d_bkt="copy-mp-bkt-3"

mys3cmd mb s3://$o_bkt

for f in $(seq 2) ;do
    dest_obj="orig-multipart-obj-$f"
    mys3cmd put -q $huge_obj s3://${o_bkt}/$dest_obj
done

mys3cmd mb s3://$d_bkt

mys3cmd cp s3://${o_bkt}/orig-multipart-obj-1 \
	s3://${d_bkt}/copied-multipart-obj-1

for f in $(seq 5 5) ;do
    dest_obj="orig-multipart-obj-$f"
    mys3cmd put -q $huge_obj s3://${d_bkt}/$dest_obj
done


############################################################
# copy multipart objects and delete original

o_bkt="orig-mp-bkt-4"
d_bkt="copy-mp-bkt-4"

mys3cmd mb s3://$o_bkt

for f in $(seq 2) ;do
    dest_obj="orig-multipart-obj-$f"
    mys3cmd put -q $huge_obj s3://${o_bkt}/$dest_obj
done

mys3cmd mb s3://$d_bkt

mys3cmd cp s3://${o_bkt}/orig-multipart-obj-1 \
	s3://${d_bkt}/copied-multipart-obj-1

for f in $(seq 5 5) ;do
    dest_obj="orig-multipart-obj-$f"
    mys3cmd put -q $huge_obj s3://${d_bkt}/$dest_obj
done

mys3cmd rb --recursive s3://$o_bkt

########################################################################
# SWIFT TESTS

# 600MB
segment_size=629145600

############################################################
# plain test

for f in $(seq 4) ;do
    myswift upload swift-plain-ctr $big_obj --object-name swift-obj-$f
done

############################################################
# zero-len test

myswift upload swift-zerolen-ctr $empty_obj --object-name subdir/
myswift upload swift-zerolen-ctr $big_obj --object-name subdir/abc1
myswift upload swift-zerolen-ctr $empty_obj --object-name subdir/empty1
myswift upload swift-zerolen-ctr $big_obj --object-name subdir/xyz1

############################################################
# dlo test

# upload in 300MB segments
myswift upload swift-dlo-ctr $huge_obj --object-name dlo-obj-1 \
      -S $segment_size

############################################################
# slo test

# upload in 300MB segments
myswift upload swift-slo-ctr $huge_obj --object-name slo-obj-1 \
      -S $segment_size --use-slo

############################################################
# large object copy test

# upload in 300MB segments
o_ctr=swift-orig-ctr
o_obj=slo-orig-obj-1
d_ctr=swift-copy-ctr
d_obj=slo-copy-obj-1
myswift upload $o_ctr $big_obj --object-name $o_obj

myswift copy --destination /${d_ctr}/${d_obj} \
      $o_ctr $o_obj

myswift delete $o_ctr $o_obj

############################################################
# huge dlo object copy test

o_ctr=swift-orig-dlo-ctr-1
o_obj=dlo-orig-dlo-obj-1
d_ctr=swift-copy-dlo-ctr-1
d_obj=dlo-copy-dlo-obj-1

myswift upload $o_ctr $huge_obj --object-name $o_obj \
      -S $segment_size

myswift copy --destination /${d_ctr}/${d_obj} \
      $o_ctr $o_obj

############################################################
# huge dlo object copy and orig delete

o_ctr=swift-orig-dlo-ctr-2
o_obj=dlo-orig-dlo-obj-2
d_ctr=swift-copy-dlo-ctr-2
d_obj=dlo-copy-dlo-obj-2

myswift upload $o_ctr $huge_obj --object-name $o_obj \
      -S $segment_size

myswift copy --destination /${d_ctr}/${d_obj} \
      $o_ctr $o_obj

myswift delete $o_ctr $o_obj

############################################################
# huge slo object copy test

o_ctr=swift-orig-slo-ctr-1
o_obj=slo-orig-slo-obj-1
d_ctr=swift-copy-slo-ctr-1
d_obj=slo-copy-slo-obj-1
myswift upload $o_ctr $huge_obj --object-name $o_obj \
      -S $segment_size --use-slo

myswift copy --destination /${d_ctr}/${d_obj} $o_ctr $o_obj

############################################################
# huge slo object copy test and orig delete

o_ctr=swift-orig-slo-ctr-2
o_obj=slo-orig-slo-obj-2
d_ctr=swift-copy-slo-ctr-2
d_obj=slo-copy-slo-obj-2
myswift upload $o_ctr $huge_obj --object-name $o_obj \
      -S $segment_size --use-slo

myswift copy --destination /${d_ctr}/${d_obj} $o_ctr $o_obj

myswift delete $o_ctr $o_obj

########################################################################
# FORCE GARBAGE COLLECTION

sleep 6 # since for testing age at which gc can happen is 5 secs
radosgw-admin gc process --include-all


########################################
# DO ORPHAN LIST

pool="default.rgw.buckets.data"

rgw-orphan-list $pool

# we only expect there to be one output file, but loop just in case
ol_error=""
for f in orphan-list-*.out ; do
    if [ -s "$f"  ] ;then # if file non-empty
	ol_error="${ol_error}:$f"
	echo "One ore more orphans found in $f:"
	cat "$f"
    fi
done

if [ -n "$ol_error" ] ;then
    echo "ERROR: orphans found when none expected"
    exit 1
fi

########################################################################
# CLEAN UP

rm -f $empty_obj $big_obj $huge_obj $s3config

success
