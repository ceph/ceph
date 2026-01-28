#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/sysmacros.h>
#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include <sys/queue.h>

struct listhead *headp;
struct fopen {
	char fname[80];
	struct rgw_file_handle rgw_fh;
	LIST_ENTRY(fopen) entries;
};

LIST_HEAD(listhead, fopen) head;

uint64_t g_r_whence;
char g_whence[80];

int close_file(struct rgw_fs *rgw_fs, const char *path, struct rgw_file_handle *rgw_fh);

int is_valid_handle(struct rgw_file_handle rgw_fh)
{
	struct rgw_file_handle rgw_empty_fh = {0};
	return (memcmp(&rgw_empty_fh, &rgw_fh, sizeof(rgw_fh))? 1 : 0);
}

struct rgw_file_handle get_handle(const char *fname)
{
	struct fopen *op = NULL;
	struct rgw_file_handle rgw_empty_fh = {0};

	for (op = head.lh_first; op != NULL; op = op->entries.le_next) {
		if (strcmp(op->fname, fname) == 0) {
			return op->rgw_fh;
		}
	}
	return rgw_empty_fh;
}

void free_all_opens(struct rgw_fs *rgw_fs)
{
	struct fopen *op = NULL;
	int rc = 0;

	for (op = head.lh_first; op != NULL; op = op->entries.le_next) {
		LIST_REMOVE(op, entries);
		rc = rgw_close(rgw_fs, &op->rgw_fh, RGW_CLOSE_FLAG_NONE);
		if (rc < 0) {
			printf("Unable to close file:%s rc:%d\n", op->fname, rc);
			/* Falling through */
		}
		free(op);
	}
	return;
}

int add_to_opens(const char *fname, struct rgw_file_handle rgw_fh)
{
	struct fopen *op = NULL;

	op = malloc(sizeof(*op));
	if (op == NULL) {
		printf("Not enough memory for new open\n");
		return -ENOMEM;
	}
	strncpy(op->fname, fname, sizeof(op->fname)-1);
	op->rgw_fh = rgw_fh;
	LIST_INSERT_HEAD(&head, op, entries);
	return 0;
}

void remove_from_opens(const char *fname)
{
	struct fopen *op = NULL;
	struct rgw_file_handle rgw_fh = get_handle(fname);

	if(!is_valid_handle(rgw_fh)) {
		printf("Invalid handle for file:%s\n", fname);
		return;
	}

	for (op = head.lh_first; op != NULL; op = op->entries.le_next) {
		if (memcmp(&op->rgw_fh, &rgw_fh, sizeof(rgw_fh)) == 0) {
			LIST_REMOVE(op, entries);
			free(op);
			return;
		}
	}
	return;
}

/* 
 * args: 
 *	user_str: string
 *	user_access_key: string
 *	user_secret_access_key: string
 *	path (bucket name): string
 */
void usage(void) 
{
	printf(	"Usage:\n"
		"myrgw <user-id-string> <user-access-key> <user-secret-access-key> <bucket-name> <debug_flag>\n"
		"user-id-string: String specifying user id\n"
		"user-access-key: String specifying access key\n"
		"user-secret-access-key: String specifying secret key\n"
		"bucket_name/path: bucket name or path like /bucket-name or /bucket-name/dir-object\n"
		"debug_flag: debug=yes/debug=no\n");
	return;
}

/*
    "fsal": {
      "access_key_id": "s1ngt3l",
      "name": "RGW",
      "secret_access_key": "s1ngt3l",
      "user_id": "user1"
    },
*/

/* /var/lib/ceph/4c33fbb4-a9a7-11f0-a3b6-525400d4e97f/config/ceph.client.admin.keyring 
 * /var/lib/ceph/4c33fbb4-a9a7-11f0-a3b6-525400d4e97f/config/ceph.conf
 * */

int lookup_path(struct rgw_fs *rgw_fs, struct rgw_file_handle *out_handle, const char *path)
{
	int rc = 0;
	struct rgw_file_handle *o_handle;
	struct stat st = {0};

	rc = rgw_lookup(rgw_fs, rgw_fs->root_fh, path, &o_handle, &st, 0, 0);
        if (rc < 0) {
		printf("Error looking up path:%s with rgw_fs=%p handle=%p. rc:%d\n",
			path, rgw_fs, rgw_fs->root_fh, rc);
		memset(out_handle, 0, sizeof(*out_handle));
		return rc;
	}

	*out_handle = *o_handle;
	return 0;
}

int release_handle(struct rgw_fs *rgw_fs, struct rgw_file_handle *rgw_fh)
{
	(void)rgw_fh_rele(rgw_fs, rgw_fh, RGW_FH_RELE_FLAG_NONE);
	return 0;
}


int open_file(struct rgw_fs *rgw_fs, const char *path)
{
	int rc = 0;
	struct rgw_file_handle rgw_fh;

	printf("Open Entry: path=%s\n", path);
	rc = lookup_path(rgw_fs, &rgw_fh, path);
	if (rc < 0) {
		printf("Open for %s failed to return handle from lookup. Err:%d Errno:%d\n",
			path, rc, errno);
		return rc;
	}

	rc = rgw_open(rgw_fs, &rgw_fh, 0, RGW_OPEN_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to open %s. Err:%d Errno:%d\n", path, rc, errno);
		release_handle(rgw_fs, &rgw_fh);
		return rc;
	}

	(void)release_handle(rgw_fs, &rgw_fh); /* release handle from lookup operation */
	if (add_to_opens(path, rgw_fh) < 0) {
		printf("Unable to add open to list\n");
		rc = close_file(rgw_fs, path, &rgw_fh);
		if (rc < 0) {
			printf("Unable to close file:%s rc:%d\n", path, rc);
			return -1;
		}
	}
	printf("Open successful for path=%s\n", path);
	return 0;
}

int close_file(struct rgw_fs *rgw_fs, const char *path, struct rgw_file_handle *rgw_fh)
{
	int rc = 0;
	struct rgw_file_handle rgw_fh_fname = {0};

	/* close with just filename */
	if (rgw_fh == NULL) {
		rgw_fh_fname = get_handle(path);
		if (!is_valid_handle(rgw_fh_fname)) {
			printf("Unable to find file handle for file:%s\n", path);
			return -ENOENT;
		}
		rgw_fh = &rgw_fh_fname;
	}
	rc = rgw_close(rgw_fs, rgw_fh, RGW_CLOSE_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to close file:%s rc:%d\n", path, rc);
		return rc;
	}
	remove_from_opens(path);
	printf("Closed.\n");

	return 0;
}

int read_file(struct rgw_fs *rgw_fs, const char *fname, const char *dst_fname)
{
	int rc = 0;
	int fd = -1;
	uint64_t offset = 0;
	size_t length = 1048576; /* TODO: Get length from stats */
	uint8_t *buffer = NULL;
	uint8_t *pbuffer;
	size_t nread = 0;
	struct rgw_file_handle rgw_fh;

	rgw_fh = get_handle(fname);
	if (!is_valid_handle(rgw_fh)) {
		printf("No handle found for file:%s\n", fname);
		return -ENOENT;
	}

	errno = 0;
	fd = open(dst_fname, O_CREAT | O_WRONLY, S_IRWXU);
	if (fd < 0) {
		printf("Error opening file to dump object data. fd:%d Errno:%d\n", fd, errno);
		return fd;
	}

	buffer = malloc(length);
	if (buffer == NULL) {
		printf("Not enough memory to read buffer\n");
		rc = -ENOMEM;
		goto out_close;
	}
	pbuffer = buffer;

	while(nread != length) {
		size_t bytes_read = 0;

		rc = rgw_read(rgw_fs, &rgw_fh, offset, length, &bytes_read, pbuffer, RGW_READ_FLAG_NONE);
		if (rc < 0) {
			printf("Unable to read from offset:%lu with length:%zu bytes_read:%zu\n",
				offset, length, bytes_read);
			goto out_buffer;
		}
		if (bytes_read == 0) {
			printf("End of file reached.\n");
		}
		offset += bytes_read;
		nread += bytes_read;
		pbuffer += bytes_read;
	}

	rc = write(fd, buffer, length);
	if (rc < 0) {
		printf("Error dumping read data. rc=%d Errno:%d\n", rc, errno);
		goto out_buffer;
	}

	printf("Read success. Data dumped in %s\n", dst_fname);

out_buffer:
	free(buffer);
out_close:
	close(fd);
	return rc;

}

struct rgw_rd_arg {
	struct rgw_fs *rgw_fs;
	char fname[256];
	bool eof;
};

void dump_stats(struct stat *st)
{
	struct stat sb = *st;

	printf("Dump Stats Begin.\n");
	printf("ID of containing device:  [%x,%x]\n",
			major(sb.st_dev),
			minor(sb.st_dev));

	printf("File type:                ");

	switch (sb.st_mode & S_IFMT) {
		case S_IFBLK:  printf("block device\n");            break;
		case S_IFCHR:  printf("character device\n");        break;
		case S_IFDIR:  printf("directory\n");               break;
		case S_IFIFO:  printf("FIFO/pipe\n");               break;
		case S_IFLNK:  printf("symlink\n");                 break;
		case S_IFREG:  printf("regular file\n");            break;
		case S_IFSOCK: printf("socket\n");                  break;
		default:       printf("unknown?\n");                break;
	}

	printf("I-node number:            %ju\n", (uintmax_t) sb.st_ino);

	printf("Mode:                     %jo (octal)\n",
			(uintmax_t) sb.st_mode);

	printf("Link count:               %ju\n", (uintmax_t) sb.st_nlink);
	printf("Ownership:                UID=%ju   GID=%ju\n",
			(uintmax_t) sb.st_uid, (uintmax_t) sb.st_gid);

	printf("Preferred I/O block size: %jd bytes\n",
			(intmax_t) sb.st_blksize);
	printf("File size:                %jd bytes\n",
			(intmax_t) sb.st_size);
	printf("Blocks allocated:         %jd\n",
			(intmax_t) sb.st_blocks);

	printf("Last status change:       %s", ctime(&sb.st_ctime));
	printf("Last file access:         %s", ctime(&sb.st_atime));
	printf("Last file modification:   %s", ctime(&sb.st_mtime));

	printf("Dump Stats End.\n");

}

int rgw_rd_cb(const char *name, void *arg, uint64_t offset, struct stat *st, uint32_t mask, uint32_t flags)
{
	struct rgw_rd_arg *cb_arg = (struct rgw_rd_arg *)arg;

	printf("Object-name: %s offset=%lu mask=%u flags=%u\n", name, offset, mask, flags);

	if (cb_arg->eof == true) {
		/* Its end of dir listing, return 0 */
		return 0;
	}

	/* Since its not end of dir listing, return non-zero value to continue
	 * listing.
	 */
	return 1;
}

int list_files(struct rgw_fs *rgw_fs)
{
	int rc = 0;
	const char *r_whence = NULL;
	struct rgw_rd_arg *cb_arg = NULL;

	printf("Listing objects\n");
	cb_arg = malloc(sizeof(*cb_arg));
	if (cb_arg == NULL) {
		printf("Not enough memory for callback argument\n");
		return -ENOMEM;
	}
	cb_arg->rgw_fs = rgw_fs;
	cb_arg->eof = false;

	rc = rgw_readdir2(rgw_fs, rgw_fs->root_fh, r_whence, rgw_rd_cb,
                          cb_arg, &cb_arg->eof, RGW_READDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Failed to call readdir. rc=%d\n", rc);
		return rc;
	}

	printf("Readdir complete.\n");
	return 0;
}

int create_dir(struct rgw_fs *rgw_fs, const char *name)
{
	int rc = 0;
	struct rgw_file_handle *rgw_fh;
	struct stat st = {0};
	uint32_t create_mask;

	st.st_uid = 0;
	st.st_gid = 0;
	st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;

	create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;
	rc = rgw_mkdir(rgw_fs, rgw_fs->root_fh, name, &st, create_mask,
		       &rgw_fh, RGW_MKDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to create directory:%s. Err:%d\n", name, rc);
		return -1;
	}

	printf("Successfully created directory:%s.\n", name);
	rc = close_file(rgw_fs, name, rgw_fh);

	return 0;
}

int rename_file(struct rgw_fs *rgw_fs, const char *old_name, const char *new_name)
{
	int rc = 0;
	struct rgw_file_handle *bkt_fh = NULL;

	rc = rgw_lookup(rgw_fs, rgw_fs->root_fh, "bkt2", &bkt_fh, NULL, 0, RGW_LOOKUP_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to lookup bucket. rc = %d\n", rc);
		return rc;
	}

	printf("root_fh=%p bkt_fh=%p\n", rgw_fs->root_fh, bkt_fh);

	rc = rgw_rename(rgw_fs,
			bkt_fh, old_name,
			bkt_fh, new_name,
			RGW_RENAME_FLAG_NONE);
	if (rc < 0) {
		printf("failed to rename [%s] to [%s]. rc=%d\n",
			old_name, new_name, rc);
		return rc;
	}

	rgw_fh_rele(rgw_fs, bkt_fh, RGW_FH_RELE_FLAG_NONE);

	printf("Rename successful\n");
	return rc;
}

int nested_dir(struct rgw_fs *rgw_fs)
{
	int rc = 0;
	int  i = 0;
	uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;
	struct rgw_file_handle *rgw_fh = NULL;
	struct rgw_file_handle *parent_fh = NULL;
	struct rgw_file_handle *dir_rgw_fh = NULL;
	char *dir_names[] = 	{
					"testdir/",
					"testdir/d1",
					"testdir/d1/d2",
					NULL
				};
	struct stat st = {0};
	char *cdir_names[] = 	{
					"cdir/",
					"cdir/d1",
					"cdir/d1/d2",
					NULL
				};

	printf("root_fh = %p\n", rgw_fs->root_fh);

	/* lookup all paths */
	do {
		if (dir_names[i] == NULL) {
			break;
		}
		printf("Looking up: [%s]\n", dir_names[i]);

		rc = rgw_lookup(rgw_fs, rgw_fs->root_fh, dir_names[i], &rgw_fh, &st, 0, 0);
		if (rc < 0) {
			printf("lookup failed for [%s]. rc=%d\n", dir_names[i], rc);
		} else {
			printf("lookup passed for [%s]. handle=%p\n", dir_names[i], rgw_fh);
			release_handle(rgw_fs, rgw_fh);
		}

		i++;
	} while(dir_names[i] != NULL);

	/* lookup and create dir */
	parent_fh = rgw_fs->root_fh;
	printf("parent_fh = %p\n", parent_fh);
	i = 0;
	do {
		if (cdir_names[i] == NULL) {
			break;
		}
		printf("Looking up: [%s]\n", cdir_names[i]);

		rc = rgw_lookup(rgw_fs, rgw_fs->root_fh, cdir_names[i], &rgw_fh, &st, 0, 0);
		if (rc < 0) {
			printf("lookup failed for [%s]. rc=%d\n", cdir_names[i], rc);
		} else {
			printf("lookup passed for [%s]. handle=%p\n", cdir_names[i], rgw_fh);
		}

		printf("Trying to create dir[%s] with handle as [%p]\n", cdir_names[i], parent_fh);
		rc = rgw_mkdir(rgw_fs, parent_fh, cdir_names[i], &st, create_mask, &dir_rgw_fh, RGW_MKDIR_FLAG_NONE);
		if (rc < 0) {
			printf("Failed to create dir[%s]. rc=%d\n", cdir_names[i], rc);
		} else {
			printf("Success creating dir[%s]. handle=%p\n", cdir_names[i], dir_rgw_fh);
			rc = rgw_close(rgw_fs, dir_rgw_fh, RGW_CLOSE_FLAG_NONE);
			if (rc < 0) {
				printf("Unable to close[%s]. rc=%d\n", cdir_names[i], rc);
			} else {
				printf("Closed [%s] with handle=%p\n", cdir_names[i], dir_rgw_fh);
			}
		}
		i++;
	} while(cdir_names[i] != NULL);

	return 0;
}

int test_mkdir_relative_path(struct rgw_fs *rgw_fs)
{
	/* Test relative paths */
	int rc = 0;
	char *rdir = "testdir";
	char *d1 = "d3";
	char *d2 = "d3/d4";
	struct rgw_file_handle *rdir_fh = NULL;
	struct rgw_file_handle *d1_fh = NULL;
	struct rgw_file_handle *d2_fh = NULL;
	struct rgw_file_handle *parent_fh = rgw_fs->root_fh;
	struct stat st = {0};
	uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;
	int ch = 0;

	/*
	 * Test:
	 * Directory create using absolute paths.
	 * 1. Open rdir get handle as rdir_fh
	 * 2. try creating d1 under rdir with handle rdir_fh and path as rdir/d1
	 * 3. try creating d2 under rdir/d1 with handle as rdir_fh but path as
	 * d1/d2
	 * 4. try creating d2 under rdir/d1 with handle as d1_fh but path as
	 * d1/d2
	 */

	rc = rgw_lookup(rgw_fs, parent_fh, rdir, &rdir_fh, &st, 0, RGW_LOOKUP_TYPE_FLAGS);
	if (rc < 0) {
		printf("Error looking up [%s].\n", rdir);
		return rc;
	}
	printf("Looked up [%s] with parent handle as [%p], returned handle[%p]\n",
		rdir, parent_fh, rdir_fh);

	/* create d1 under rdir with handle rdir_fh and path as rdir/d1 */
	parent_fh = rdir_fh;
	rc = rgw_mkdir(rgw_fs, parent_fh, d1, &st, create_mask, &d1_fh, RGW_MKDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Error creating dir [%s]. rc= %d \n", d1, rc);
		return rc;
	}
	printf("Created dir [%s] under [%s] with parent handle [%p], returned handle[%p]\n",
		d1, rdir, parent_fh, d1_fh);
	printf("Enter 0 to continue:");
	scanf("%d", &ch);

	/* try creating d2 under rdir/d1 with handle as rdir_fh but path as
	 * d1/d2
	 */
	parent_fh = rdir_fh;
	rc = rgw_mkdir(rgw_fs, parent_fh, d2, &st, create_mask, &d2_fh, RGW_MKDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Error creating dir [%s]. rc= %d \n", d2, rc);
		printf("Creating dir [%s] under [%s] with parent handle [%p] failed\n",
			d2, rdir, parent_fh);
		return rc;
	}
	printf("Created dir [%s] under [%s] with parent handle [%p], returned handle[%p]\n",
			d2, rdir, parent_fh, d2_fh);
	printf("Enter 0 to continue:");
	scanf("%d", &ch);

	/*
	 * 4. try creating d2 under rdir/d1 with handle as d1_fh but path as
	 * d1/d2
	 */
	parent_fh = d1_fh;
	rc = rgw_mkdir(rgw_fs, parent_fh, d2, &st, create_mask, &d2_fh, RGW_MKDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Error creating dir [%s]. rc= %d \n", d2, rc);
		return rc;
	}
	printf("Created dir [%s] under [%s%s] with parent handle [%p], returned handle[%p]\n",
			d2, rdir, d1, parent_fh, d2_fh);

	return 0;
}

int write_to_object(struct rgw_fs *rgw_fs)
{

	int rc = 0;
	struct rgw_file_handle *write_fh = NULL;
	struct stat st = {0};
	uint32_t mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;
	char *buffer = "hello";
	uint64_t offset = 0;
	size_t len = strlen(buffer)+1;
	size_t bytes_written = 0;

	rc = rgw_getattr(rgw_fs,
			rgw_fs->root_fh,
			&st,
			RGW_GETATTR_FLAG_NONE);

	if (rc < 0) {
		printf("Unable to get attributes for bucket. rc=%d\n", rc);
		return -1;
	}

	rc = rgw_create(rgw_fs,
			rgw_fs->root_fh,
			"write_test",
			&st,
			mask,
			&write_fh,
			O_CREAT|O_RDWR|O_APPEND|O_TRUNC,
			RGW_CREATE_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to create object. rc=%d\n", rc);
		return -1;
	}

	rc = rgw_open(rgw_fs,
		      write_fh,
		      O_WRONLY|O_APPEND,
		      RGW_OPEN_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to open file. rc=%d\n", rc);
		return -1;
	}

	rc = rgw_write(rgw_fs,
			write_fh,
			offset,
			len,
			&bytes_written,
			buffer,
			RGW_WRITE_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to write to object. rc=%d\n", rc);
		rgw_close(rgw_fs, write_fh, RGW_CLOSE_FLAG_NONE);
		return -1;
	}

	rgw_close(rgw_fs, write_fh, RGW_CLOSE_FLAG_NONE);
	printf("Wrote %lu bytes\n", bytes_written);
	return 0;
}

int lookup_object(struct rgw_fs *rgw_fs, const char *fname)
{
	int rc = 0;
	struct rgw_file_handle *o_handle;
	struct stat st = {0};
	uint32_t mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

	rc = rgw_lookup(rgw_fs, rgw_fs->root_fh, fname, &o_handle, &st, mask, RGW_LOOKUP_TYPE_FLAGS);
        if (rc < 0) {
		printf("Error looking up path:%s with rgw_fs=%p handle=%p. rc:%d\n",
			fname, rgw_fs, rgw_fs->root_fh, rc);
		return rc;
	}

	printf("Look up success.\n");

	rgw_fh_rele(rgw_fs, o_handle, RGW_FH_RELE_FLAG_NONE);

	return 0;
}

int create_file(struct rgw_fs *rgw_fs, char *name)
{
	int rc = 0;
	struct rgw_file_handle *fh = NULL;
	struct stat st = {0};
	uint32_t mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;
	uint32_t mode = O_CREAT;
	size_t len = 0;

	len = strlen(name);
	if (name[len-1] == '/') {
		mode |= O_DIRECTORY;
	} else {
		mode |= O_RDWR;
	}

	errno = 0;
	st.st_mode = 0;
	mode = 0;
	rc = rgw_create(rgw_fs,
			rgw_fs->root_fh,
			name,
			&st,
			mask,
			&fh,
			mode,
			RGW_CREATE_FLAG_NONE);
	if (rc < 0) {
		printf("Unable to create object. rc=%d\n", rc);
		return -1;
	}

	printf("Created file: %s rc:%d errno:%d\n", name, rc, errno);

	return rc;
}

int rgw_rd_cb_1(const char *name, void *arg, uint64_t offset, struct stat *st, uint32_t mask, uint32_t flags)
{
	struct rgw_rd_arg *cb_arg = (struct rgw_rd_arg *)arg;

	printf("Object-name: %s offset=%lu mask=%u flags=%u\n", name, offset, mask, flags);

	if (cb_arg->eof == true) {
		/* Its end of dir listing, return 0 */
		printf("CB: End of file  reached\n");
		return 0;
	}
	strncpy(g_whence, name, sizeof(g_whence));

	/* Since its not end of dir listing, return non-zero value to continue
	 * listing.
	 */
	return 0;
}

int list_files_with_rgw_readdir2(struct rgw_fs *rgw_fs)
{
	int rc = 0;
	int c = 0;
	struct rgw_rd_arg *cb_arg = NULL;

	printf("Listing objects with whence\n\n");
	cb_arg = malloc(sizeof(*cb_arg));
	if (cb_arg == NULL) {
		printf("Not enough memory for callback argument\n");
		return -ENOMEM;
	}
	cb_arg->rgw_fs = rgw_fs;
	cb_arg->eof = false;
	strncpy(g_whence, "", sizeof(g_whence));

	while(cb_arg->eof != true) {
		if (c == 0) {
			c++;
			rc = rgw_readdir2(rgw_fs, rgw_fs->root_fh, NULL, rgw_rd_cb_1,
					cb_arg, &cb_arg->eof, RGW_READDIR_FLAG_NONE);
		} else {

			rc = rgw_readdir2(rgw_fs, rgw_fs->root_fh, g_whence, rgw_rd_cb_1,
					cb_arg, &cb_arg->eof, RGW_READDIR_FLAG_NONE);
		}
		if (rc < 0) {
			printf("Failed to call readdir. rc=%d\n", rc);
			return rc;
		}
		printf("Returned rc=%d\n", rc);
	}

	printf("\n\nReaddir complete.\n\n");
	return 0;

}

int rgw_rd_cb_2(const char *name, void *arg, uint64_t offset, struct stat *st, uint32_t mask, uint32_t flags)
{
	//struct rgw_rd_arg *cb_arg = (struct rgw_rd_arg *)arg;

	printf("Object-name: %s offset=%lu mask=%u flags=%u\n", name, offset, mask, flags);
	g_r_whence = offset;

	return 0;
}

int list_files_with_rgw_readdir(struct rgw_fs *rgw_fs)
{
	int rc = 0;
	struct rgw_rd_arg *cb_arg = NULL;

	printf("Listing objects with whence\n\n");
	cb_arg = malloc(sizeof(*cb_arg));
	if (cb_arg == NULL) {
		printf("Not enough memory for callback argument\n");
		return -ENOMEM;
	}
	cb_arg->rgw_fs = rgw_fs;
	cb_arg->eof = false;

	rc = rgw_readdir(rgw_fs, rgw_fs->root_fh, &g_r_whence, rgw_rd_cb_2,
			cb_arg, &cb_arg->eof, RGW_READDIR_FLAG_NONE);
	if (rc < 0) {
		printf("Failed to call readdir. rc=%d\n", rc);
		return rc;
	}

	if (cb_arg->eof == true) {
		/* Its end of dir listing, return 0 */
		printf("CB: End of file  reached\n");
		return 1;
	}

	free(cb_arg);
	printf("\n\nReaddir complete.\n\n");
	return 0;

}

int get_choice(void)
{
	int choice = 0;

	printf("1.  Open file\n"
	       "2.  Close file\n"
	       "3.  List files\n"
	       "4.  Create directory\n"
	       "5.  Read file\n"
	       "6.  Dump file stats\n"
	       "7.  Rename\n"
	       "8.  Nested dir create\n"
	       "9.  Nested dir relative create\n"
	       "10. Write to object\n"
	       "11. Lookup object\n"
	       "12. Create file/dir\n"
	       "13. List files with readdir2 r_whence\n"
	       "14. List files with readdir r_whence\n"
	       "99. Exit\n"
	       "Choice:");
	scanf("%d", &choice);
	fflush(stdin);
	return choice;
}

int main(int argc, char *argv[])
{
	int rc = 0;
	int nparams = 0;
	char *params[] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
	const char *pgm_name = argv[0];
	const char *user_id_str = argv[1];
	const char *user_access_key = argv[2];
	const char *user_secret_access_key = argv[3];
	const char *bkt_name = argv[4];
	const char *debug_flag = argv[5];
	const char *cluster_config_file = "/home/mbenjamin/dev/ceph-cp/build/ceph.conf";
	const char *rgw_name = "client.admin";
        const char* keyring = "/home/mbenjamin/dev/ceph-cp/build/keyring";
	const char *cluster = "ceph";
	char fname[80];
	char dst_fname[80];
	int len = 0;
	int choice = 0;

	/* librgw related objects */
	librgw_t hrgw = {0};
	struct rgw_fs *rgw_fs;
	LIST_INIT(&head);

	if (argc != 6) {
		printf("Invalid argument.\n");
		usage();
		return -EINVAL;
	}

	/* Init parameters for librgw_create() */
	len = strlen(pgm_name) + 1;
	params[nparams] = malloc(len);
	if (params[nparams] == NULL) {
		printf("Not enough memory to prepare first arg\n");
		return -ENOMEM;
	}
	snprintf(params[nparams], len, "%s", pgm_name);
	nparams++;

	len = strlen("--conf=") + strlen(cluster_config_file) + 1;
	params[nparams] = malloc(len);
	if (params[nparams] == NULL) {
		printf("Not enough memory to prepare config file arg\n");
		goto out_free;
	}
	snprintf(params[nparams], len, "--conf=%s", cluster_config_file);
	nparams++;

	len = strlen("--name=") + strlen(rgw_name) + 1;
	params[nparams] = malloc(len);
	if (params[nparams] == NULL) {
		printf("Not enough memory to prepare arg --name\n");
		goto out_free;
	}
	snprintf(params[nparams], len, "--name=%s", rgw_name);
	nparams++;

	len = strlen("--cluster=") + strlen(cluster) + 1;
	params[nparams] = malloc(len);
	if (params[nparams] == NULL) {
		printf("Not enough memory to prepare arg --cluster\n");
		goto out_free;
	}
	snprintf(params[nparams], len, "--cluster=%s", cluster);
	nparams++;

	len = strlen("--keyring=") + strlen(keyring) + 1;
	params[nparams] = malloc(len);
	if (params[nparams] == NULL) {
		printf("Not enough memory to prepare arg --keyring\n");
		return -ENOMEM;
	}
	snprintf(params[nparams], len, "--keyring=%s", keyring);
	nparams++;

	if (strcmp(debug_flag, "debug=yes") == 0) {
		len = strlen("-d --debug-rgw=20") + strlen("-d --debug-rgw=20") + 1;
		params[nparams] = malloc(len);
		if (params[nparams] == NULL) {
			printf("Not enough memory to prepare arg -d --debug-rgw=20\n");
			return -ENOMEM;
		}
		snprintf(params[nparams], len, "%s", "-d --debug-rgw=20");
		nparams++;
	}

	printf("Trying to mount bucket:%s with user:%s access key=%s secret access key=%s on cluster=%s\n",
		bkt_name, user_id_str, user_access_key, user_secret_access_key, debug_flag);

	rc = librgw_create(&hrgw, nparams, params);
	if (rc != 0) {
		printf("Failed to init librgw. rc=%d. Errno:%d\n", rc, errno);
		goto init_fail;
	}

	printf("rgw library initialised.\n");

        rc = rgw_mount2(hrgw,
			user_id_str,
			user_access_key,
			user_secret_access_key,
			"/",
			&rgw_fs,
			RGW_MOUNT_FLAG_NONE);
	if (rc != 0) {
		printf("Unable to mount bucket=%s.Err=%d\n", bkt_name, rc);
		if (rc == -EINVAL) {
			printf("Unable to authorise user=%s\n", user_id_str);
		}
		goto out_free;
	}

	printf("Mounted bucket=%s successfully.\n", bkt_name);

#if 0
	while(choice != 99) {
		choice = get_choice();
		switch(choice) {
			case 99:
				goto out_unmount;
			case 1:
				printf("Enter Filename:");
				scanf("%s", fname);
				fflush(stdin);
				rc = open_file(rgw_fs, fname);
				break;

			case 2:
				printf("Enter Filename:");
				scanf("%s", fname);
				fflush(stdin);
				rc = close_file(rgw_fs, fname, NULL);
				break;

			case 3:
				list_files(rgw_fs);
				break;

			case 4:
				printf("Enter Dirname:");
				scanf("%s", fname);
				fflush(stdin);
				create_dir(rgw_fs, fname);
				break;

			case 5:
				printf("Enter Source file:");
				scanf("%s", fname);
				fflush(stdin);
				printf("Enter Destination file:");
				scanf("%s", dst_fname);
				fflush(stdin);

				rc = read_file(rgw_fs, fname, dst_fname);
				break;

			case 6:
				printf("Dumping stats not supported\n");
				break;

			case 7:
				printf("Enter old name (file/dir):");
				scanf("%s", fname);
				fflush(stdin);
				printf("Enter new name (file/dir):");
				scanf("%s", dst_fname);
				fflush(stdin);
				rc = rename_file(rgw_fs, fname, dst_fname);
				break;

			case 8:
				rc = nested_dir(rgw_fs);
				break;

			case 9:
				rc = test_mkdir_relative_path(rgw_fs);
				break;

			case 10:
				rc = write_to_object(rgw_fs);
				break;

			case 11:
				printf("Enter name (file/dir):");
				scanf("%s", fname);
				fflush(stdin);
				rc = lookup_object(rgw_fs, fname);
				break;

			case 12:
				printf("Enter name (file/dir):");
				scanf("%s", fname);
				fflush(stdin);
				rc = create_file(rgw_fs, fname);
				break;

			case 13:
				rc = list_files_with_rgw_readdir2(rgw_fs);
				break;

			case 14:
				while(1) {
					rc = list_files_with_rgw_readdir(rgw_fs);
					if (rc == 1) {
						rc = 0;
						break;
					}
				}
				break;

			default:
				printf("Invalid choice\n");
				break;
		}

	} /* End of while */
#else
	{
	  char* fname;
	  char* dst_fname;
	  fname = strdup("passwd");
	  dst_fname = strdup("dweezil");
	  rc = rename_file(rgw_fs, fname, dst_fname);
	}
#endif

out_unmount:
	rc = rgw_umount(rgw_fs, RGW_UMOUNT_FLAG_NONE);
	if (rc != 0) {
		printf("Unable to un-mount bucket=%s. Err=%d\n", bkt_name, rc);
		goto out_free;
	}

	printf("Un-Mounted bucket=%s successfully.\n", bkt_name);

out_free:
	for (; nparams >=0; nparams--) {
		if (params[nparams]) {
			free(params[nparams]);
			params[nparams] = NULL;
		}
	}
init_fail:
	if (hrgw) {
		librgw_shutdown(hrgw);
	}
	return rc;
}
