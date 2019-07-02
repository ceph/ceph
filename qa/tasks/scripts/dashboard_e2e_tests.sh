# dashboard_e2e_tests.sh
#

set -ex

useradd -m farm
echo "farm ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

script_path='/usr/share/ceph/dashboard-e2e/dashboard_e2e_tests.sh'
cat <<EOM
Assuming that the ceph-dashboard-e2e package has already been installed,
there should be a script ->$script_path<-
in the system. Now executing that script as a normal user....
EOM
su - farm -c "$script_path"
