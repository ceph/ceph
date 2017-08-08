#
# copy_file(<filename>, <node>, <directory>, [<permissions>], [<owner>]
# 
# copy a file -- this is needed because passwordless ssh does not
#                work when sudo'ing.
#    <file> -- name of local file to be copied
#    <node> -- node where we want the file
#    <directory> -- location where we want the file on <node>
#    <permissions> -- (optional) permissions on the copied file
#    <owner> -- (optional) owner of the copied file
#
function copy_file() {
    fname=`basename ${1}`
    scp ${1} ${2}:/tmp/${fname}
    ssh ${2} sudo cp /tmp/${fname} ${3}
    if [ $# -gt 3 ]; then
       ssh ${2} sudo chmod ${4} ${3}/${fname}
    fi
    if [ $# -gt 4 ]; then
       ssh ${2} sudo chown ${5} ${3}/${fname}
    fi
}
