# clobber_repositories.sh
#
# args: some repositories in format name:url
#
# TODO: priority

set -ex

if [ -d /etc/zypp/repos.d.bck ] ; then
    echo "Repos were already clobbered. Doing nothing." 2>/dev/null
    exit 0
fi

echo "Repos BEFORE clobber" 2>/dev/null
zypper lr -upEP

cp -a /etc/zypp/repos.d /etc/zypp/repos.d.bck
rm -f /etc/zypp/repos.d/*

for repo_spec in "$@" ; do
    repo_name=$(echo $repo_spec | sed -e 's/\:.*$//')
    repo_url=$(echo $repo_spec | sed -e 's/^.*\:\(http:.*$\)/\1/')
    zypper \
        --non-interactive \
        addrepo \
        --refresh \
        --no-gpgcheck \
        $repo_url \
        $repo_name
done

zypper --non-interactive --no-gpg-checks refresh

echo "Repos AFTER clobber" 2>/dev/null
zypper lr -upEP
