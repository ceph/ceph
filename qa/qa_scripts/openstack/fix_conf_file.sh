#/bin/bash -fv
source ./copy_func.sh
#
# Take a templated file, modify a local copy, and write it to the
# remote site.
#
# Usage: fix_conf_file <remote-site> <file-name> <remote-location> [<rbd-secret>]
#      <remote-site> -- site where we want this modified file stored.
#      <file-name> -- name of the remote file.
#      <remote-location> -- directory where the file will be stored
#      <rbd-secret> -- (optional) rbd_secret used by libvirt
#
function fix_conf_file() {
    if [[ $# < 3 ]]; then
        echo 'fix_conf_file: Too few parameters' 
        exit 1
    fi
    openstack_node_local=${1}
    cp files/${2}.template.conf ${2}.conf
    hostname=`ssh $openstack_node_local hostname`
    inet4addr=`ssh $openstack_node_local hostname -i`
    sed -i s/VARHOSTNAME/$hostname/g ${2}.conf
    sed -i s/VARINET4ADDR/$inet4addr/g ${2}.conf
    if [[ $# == 4 ]]; then
        sed -i s/RBDSECRET/${4}/g ${2}.conf
    fi
    copy_file ${2}.conf $openstack_node_local ${3} 0644 "root:root"
    rm ${2}.conf
}
