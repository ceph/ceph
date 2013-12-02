CEPHFS_TAR = ../ceph-$(VERSION).tar.gz

.PHONY: all

all:
	@echo "To build RPMS run 'make cephrpms'"

.PHONY: cephrpms prep srcrpm clean

cephrpms: prep srcrpm rpms
	-rm -rf rpmbuild

prep:
	if [ ! -e $(CEPHFS_TAR) ]; then \
		$(MAKE) -C .. dist; \
	fi
	-mkdir -p rpmbuild/SPECS
	-mkdir -p rpmbuild/RPMS
	-mkdir -p rpmbuild/SRPMS
	-rm -rf rpmbuild/SOURCES
	mkdir -p ./rpmbuild/SOURCES
	cp ../*.tar.gz ./rpmbuild/SOURCES
	cp ../ceph.spec ./rpmbuild/SPECS

srcrpm:
	rpmbuild --define '_topdir $(shell pwd)/rpmbuild' -bs rpmbuild/SPECS/ceph.spec
	mv rpmbuild/SRPMS/* .

rpms:
	rpmbuild --define '_topdir $(shell pwd)/rpmbuild' -bb rpmbuild/SPECS/ceph.spec
	mv rpmbuild/RPMS/*/* .

clean:
	-rm -rf rpmbuild
	-rm -f *.rpm
