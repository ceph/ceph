#include <string>
#include <include/fs_types.h>
#include <mds/mdstypes.h>
#include <include/cephfs/libcephfs.h>

#define MAX_CEPH_FILES	1000
#define DIRNAME		"ino_release_cb"

static volatile bool cb_done = false;

static void cb(void *hdl, vinodeno_t vino)
{
	cb_done = true;
}

int main(int argc, char *argv[])
{
	inodeno_t inos[MAX_CEPH_FILES];
	struct ceph_mount_info *cmount = NULL;

	ceph_create(&cmount, "admin");
	ceph_conf_read_file(cmount, NULL);
	ceph_init(cmount);

	[[maybe_unused]] int ret = ceph_mount(cmount, NULL);
	assert(ret >= 0);
	ret = ceph_mkdir(cmount, DIRNAME, 0755);
	assert(ret >= 0);
	ret = ceph_chdir(cmount, DIRNAME);
	assert(ret >= 0);

	/* Create a bunch of files, get their inode numbers and close them */
	int i;
	for (i = 0; i < MAX_CEPH_FILES; ++i) {
		int fd;
		struct ceph_statx stx;

		string name = std::to_string(i);

		fd = ceph_open(cmount, name.c_str(), O_RDWR|O_CREAT, 0644);
		assert(fd >= 0);

		ret = ceph_fstatx(cmount, fd, &stx, CEPH_STATX_INO, 0);
		assert(ret >= 0);

		inos[i] = stx.stx_ino;
		ceph_close(cmount, fd);
	}

	/* Remount */
	ceph_unmount(cmount);
	ceph_release(cmount);
	ceph_create(&cmount, "admin");
	ceph_conf_read_file(cmount, NULL);
	ceph_init(cmount);

	struct ceph_client_callback_args args = { 0 };
	args.ino_release_cb = cb;
	ceph_ll_register_callbacks(cmount, &args);

	ret = ceph_mount(cmount, NULL);
	assert(ret >= 0);

	Inode	*inodes[MAX_CEPH_FILES];

	for (i = 0; i < MAX_CEPH_FILES; ++i) {
		/* We can stop if we got a callback */
		if (cb_done)
			break;

		ret = ceph_ll_lookup_inode(cmount, inos[i], &inodes[i]);
		assert(ret >= 0);
	}

	assert(cb_done);
	return 0;
}
