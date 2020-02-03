if [ -n $1 ] && [ -n $2 ]
then
cat >> /home/ubuntu/dg.yaml << EOF
  placement:
  service_type: osd
    host_pattern: "$1"*
    data_devices:
      all: 'true'
      limit: "$2"
EOF
else
        echo "Either hostname or OSD count is missing"
	return 1
fi

