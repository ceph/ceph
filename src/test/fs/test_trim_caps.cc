#define _FILE_OFFSET_BITS 64
#if defined(__linux__)
#include <features.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <include/cephfs/libcephfs.h>

int main(int argc, char *argv[]) 
{
	char buf;
	int pipefd[2];
	int rc [[maybe_unused]] = pipe(pipefd);
        assert(rc >= 0);

	pid_t pid = fork();
	assert(pid >= 0);
	if (pid == 0)
		close(pipefd[1]);
	else
		close(pipefd[0]);

	struct ceph_mount_info *cmount = NULL;

	ceph_create(&cmount, "admin");
	ceph_conf_read_file(cmount, NULL);

	int ret [[maybe_unused]] = ceph_mount(cmount, NULL);
	assert(ret >= 0);

	if (pid == 0) {
		ret = read(pipefd[0], &buf, 1);
		assert(ret == 1);

		ret = ceph_rename(cmount, "1", "3");
		assert(ret >= 0);

		ret = ceph_rename(cmount, "2", "1");
		assert(ret >= 0);

		printf("child exits\n");
	} else {
		ret = ceph_mkdirs(cmount, "1/2", 0755);
		assert(ret >= 0);

		struct ceph_statx stx;
		ret = ceph_statx(cmount, "1", &stx, 0, 0);
		assert(ret >= 0);
		uint64_t orig_ino [[maybe_unused]] = stx.stx_ino;


		ret = ceph_mkdir(cmount, "2", 0755);
		assert(ret >= 0);

		ret = write(pipefd[1], &buf, 1);
		assert(ret == 1);

		int wstatus;
		ret = waitpid(pid, &wstatus, 0);
		assert(ret >= 0);
		assert(wstatus == 0);

		// make origin '1' no parent dentry
		ret = ceph_statx(cmount, "1", &stx, 0, 0);
		assert(ret >= 0);
		assert(orig_ino != stx.stx_ino);

		// move root inode's cap_item to tail of session->caps
		ret = ceph_statx(cmount, ".", &stx, 0, 0);
		assert(ret >= 0);

		printf("waiting for 60 seconds to see whether will it crash\n");
		sleep(60);
		printf("parent exits\n");
	}
	ceph_unmount(cmount);
	return 0;
}
