# Ceph - a scalable distributed storage system

Please see http://ceph.io/ for current info.

## Building the Documentation

### Prerequisites

The list of package dependencies for building the documentation can be
found in `doc_deps.deb.txt`:

	sudo apt-get install `cat doc_deps.deb.txt`

### Building the Documentation

To build the documentation, ensure that you are in the top-level
`/ceph` directory, and execute the build script. For example:

	./admin/build-doc

The built documentation will be available at buid-doc/output. There you will have access to the man and html pages.

Please see https://docs.ceph.com/docs/master/dev/generatedocs/ for detailed info.
