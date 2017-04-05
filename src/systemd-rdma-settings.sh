#!/bin/sh

SERV=`echo $1 | awk -F@ '{print $1}'`
FILE="/usr/lib/systemd/system/"$SERV"@.service"
ARG_DIR=/run/systemd/system/$1
NAME=$1
FLAG=false

add_config(){
	if [ -f "/run/systemd/system/ceph-osd.target.wants/"$NAME"" ]; then
		sudo rm -rf "/run/systemd/system/ceph-osd.target.wants/"$NAME""
	fi

	for CONF in "$@"
	do
		if [ ! -f $CONF ]; then
			touch $CONF
			FLAG=true
			cat $FILE > $CONF
		fi
		if ! grep -q "[Service]" $CONF ; then
			echo "[Service]"| sudo tee -a $CONF
			FLAG=true
		fi
		if grep -q "PrivateDevices=yes" $CONF; then
			sudo sed -i 's/PrivateDevices=yes/PrivateDevices=no/g' $CONF
			FLAG=true
		fi
		if ! grep -q "LimitMEMLOCK=infinity" $CONF ; then
			sudo sed -i '/\[Service\]/a LimitMEMLOCK=infinity' $CONF
			FLAG=true
		fi
	done
}

rm_config(){
	if [ -f "/run/systemd/system/ceph-osd.target.wants/"$NAME"" ]; then
		sudo rm -rf "/run/systemd/system/ceph-osd.target.wants/"$NAME""
	fi

	for CONF in "$@"
	do
		if [ -f $CONF ]; then
			sudo rm -f $CONF
			FLAG=true
		fi
	done
}

if [ "$2" != "clean" ]; then
	add_config $ARG_DIR
	if [ "$FLAG" = true ]; then
		sudo systemctl daemon-reload
	fi
else
	rm_config $ARG_DIR
	if [ "$FLAG" = true ]; then
		sudo systemctl daemon-reload
	fi
fi
