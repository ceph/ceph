#!/bin/bash

sudo systemctl daemon-reload

INST=`echo $1 | awk -F@ '{print $1}'`
MON_DIR="/etc/systemd/system/ceph-mon@.service.d"
OSD_DIR="/etc/systemd/system/ceph-osd@.service.d"
ARG_DIR="/etc/systemd/system/"$INST"@.service.d"

add_config(){
	for dir in "$@"
	do
		sudo mkdir $dir
		touch $dir/override.conf
		if ! grep -q "[Service]" $dir/override.conf ; then
			sudo echo "[Service]"| sudo tee -a $dir/override.conf
		fi
		if grep -q "PrivateDevices" $dir/override.conf; then                                #so we don't add the same line twice
			sudo sed -i -e 's/PrivateDevices=yes/PrivateDevices=no/g' $dir/override.conf
		else                                                                                    #add line if doesn't exist
			sudo echo "PrivateDevices=no" | sudo tee -a $dir/override.conf
		fi
		if ! grep -q "LimitMEMLOCK=infinity" $dir/override.conf ; then                      #so we don't add the same line twice
			sudo echo "LimitMEMLOCK=infinity"| sudo tee -a $dir/override.conf
		fi
	done
}

rm_config(){
	for dir in "$@"                                                                            #removing is easier since it will only revert what already exists
	do
		sudo sed -i -e 's/PrivateDevices=no/PrivateDevices=yes/g' $dir/override.conf
		sudo sed -i '/LimitMEMLOCK=infinity/d' $dir/override.conf
	done
}

if [[ $(ceph-conf ms_type) = *rdma* ]] || [[ $(ceph-conf ms_public_type) = *rdma* ]] || [[ $(ceph-conf ms_cluster_type) = *rdma* ]] ;then
	if [ "$2" != "clean" ]; then
		add_config $MON_DIR $ARG_DIR $OSD_DIR
	else
		rm_config $MON_DIR $ARG_DIR $OSD_DIR
	fi
fi

sudo systemctl daemon-reload
exit 0
