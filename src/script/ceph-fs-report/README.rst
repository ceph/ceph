ceph-fs-report
==============

ceph-fs-report is a command line tool that analyses the activity
of the Ceph project to help publish reports to figure out how
it evolves over time::

     $ ceph-fs-report \
          --repo $(git rev-parse --show-toplevel) \
          --token $github_token > ages.js
     $ firefox ages.html

Home page : https://pypi.python.org/pypi/ceph-fs-report

Hacking
=======

* Get the code : git clone https://git.ceph.com/ceph.git
* Run the unit tests : tox

 - version=1.0.0 ; perl -pi -e "s/^version.*/version='$version',/" setup.py ; do python setup.py sdist ; amend=$(git log -1 --oneline | grep --quiet "version $version" && echo --amend) ; git commit $amend -m "version $version" setup.py ; git tag -a -f -m "version $version" $version ; done

* Publish a new version

 - python setup.py sdist upload --sign
 - git push ; git push --tags

* pypi maintenance

 - python setup.py register # if the project does not yet exist
 - trim old versions at https://pypi.python.org/pypi/ceph-fs-report
