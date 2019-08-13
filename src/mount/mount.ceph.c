#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/mount.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <wait.h>
#include <cap-ng.h>

#include "common/module.h"
#include "common/secret.h"
#include "include/addr_parsing.h"

#ifndef MS_RELATIME
# define MS_RELATIME (1<<21)
#endif

#define MAX_SECRET_LEN 1000
#define MAX_SECRET_OPTION_LEN (MAX_SECRET_LEN + 7)

bool verboseflag = false;
bool skip_mtab_flag = false;
static const char * const EMPTY_STRING = "";

/* TODO duplicates logic from kernel */
#define CEPH_AUTH_NAME_DEFAULT "guest"

#include "mtab.c"
#include "keyring.h"

static void block_signals (int how)
{
     sigset_t sigs;

     sigfillset (&sigs);
     sigdelset(&sigs, SIGTRAP);
     sigdelset(&sigs, SIGSEGV);
     sigprocmask (how, &sigs, (sigset_t *) 0);
}

void mount_ceph_debug(const char *fmt, ...)
{
	if (verboseflag) {
		va_list args;

		va_start(args, fmt);
		vprintf(fmt, args);
		va_end(args);
	}
}

static char *mount_resolve_src(const char *orig_str)
{
	int len, pos;
	char *mount_path;
	char *src;
	char *buf = strdup(orig_str);
	if (!buf) {
		fprintf(stderr, "%s: failed to allocate memory\n", __func__);
		return NULL;
	}

	mount_path = strstr(buf, ":/");
	if (!mount_path) {
		fprintf(stderr, "source mount path was not specified\n");
		free(buf);
		return NULL;
	}
	if (mount_path == buf) {
		fprintf(stderr, "server address expected\n");
		free(buf);
		return NULL;
	}

	*mount_path = '\0';
	mount_path++;

	if (!*mount_path) {
		fprintf(stderr, "incorrect source mount path\n");
		free(buf);
		return NULL;
	}

	src = resolve_addrs(buf);
	if (!src) {
		free(buf);
		return NULL;
	}

	len = strlen(src);
	pos = safe_cat(&src, &len, len, ":");
	safe_cat(&src, &len, pos, mount_path);

	free(buf);
	return src;
}

static int
drop_capabilities()
{
	capng_setpid(getpid());
	capng_clear(CAPNG_SELECT_BOTH);
	if (capng_update(CAPNG_ADD, CAPNG_PERMITTED, CAP_DAC_READ_SEARCH)) {
		fprintf(stderr, "Unable to update permitted capability set.\n");
		return EX_SYSERR;
	}
	if (capng_update(CAPNG_ADD, CAPNG_EFFECTIVE, CAP_DAC_READ_SEARCH)) {
		fprintf(stderr, "Unable to update effective capability set.\n");
		return EX_SYSERR;
	}
	if (capng_apply(CAPNG_SELECT_BOTH)) {
		fprintf(stderr, "Unable to apply new capability set.\n");
		return EX_SYSERR;
	}
	return 0;
}

/*
 * Attempt to grab the mount secret out of the ceph keyring file. Starting up
 * a CephContext may involve activity that is dangerous for a privileged
 * process, so we fork a child process and have it drop privileges before
 * doing so.
 *
 * If the child finds a secret, it will be copied into the mmap'ed buffer
 * and the child will exit with a status of 0. If the child doesn't exit with
 * a 0 status, or the resulting string doesn't pass sanity checks, return an
 * error so the caller knows not to use it.
 */
static int fetch_keyring_secret(const char *name, char *dst)
{
	int ret = 0;
	pid_t pid;
	char *secret;

	secret = mmap((void *)0, MAX_SECRET_LEN, PROT_READ | PROT_WRITE,
			MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if (secret == (char *)-1) {
		fprintf(stderr, "Unable to allocate memory: %s\n",
				strerror(errno));
		return EX_SYSERR;
	}

	pid = fork();
	if (pid < 0) {
		fprintf(stderr, "fork() failure: %s\n", strerror(errno));
		ret = EX_SYSERR;
		goto out;
	}

	if (pid == 0) {
		/* child */
		ret = drop_capabilities();
		if (!ret)
			ret = get_keyring_secret(name, secret, MAX_SECRET_LEN);
		exit(ret ? 1 : 0);
	} else {
		/* parent */
		size_t len;
		pid = wait(&ret);
		if (!WIFEXITED(ret)) {
			fprintf(stderr, "Child process terminated abnormally.\n");
			ret = EX_SYSERR;
			goto out;
		}
		ret = WEXITSTATUS(ret);
		if (ret) {
			fprintf(stderr, "Child exited with status %d\n", ret);
			ret = EX_SYSERR;
			goto out;
		}

		len = strnlen(secret, MAX_SECRET_LEN) + 1;
		if (len > MAX_SECRET_LEN)
			ret = EX_SYSERR;
		else
			memcpy(dst, secret, len);
	}
out:
	munmap(secret, MAX_SECRET_LEN);
	return ret;
}

/*
 * this one is partially based on parse_options() from cifs.mount.c
 */
static char *parse_options(const char *data, int *filesys_flags)
{
	char * next_keyword = NULL;
	char * out = NULL;
	int out_len = 0;
	int pos = 0;
	char *name = NULL;
	int name_len = 0;
	int name_pos = 0;
	char secret[MAX_SECRET_LEN];
	char *saw_name = NULL;
	char *saw_secret = NULL;

	mount_ceph_debug("parsing options: %s\n", data);

	do {
		char * value = NULL;
		bool skip = true;

		/*  check if ends with trailing comma */
		if(*data == 0)
			break;
		next_keyword = strchr(data,',');
	
		/* temporarily null terminate end of keyword=value pair */
		if(next_keyword)
			*next_keyword++ = 0;

		/* temporarily null terminate keyword to make keyword and value distinct */
		if ((value = strchr(data, '=')) != NULL) {
			*value = '\0';
			value++;
		}


		if (strncmp(data, "ro", 2) == 0) {
			*filesys_flags |= MS_RDONLY;
		} else if (strncmp(data, "rw", 2) == 0) {
			*filesys_flags &= ~MS_RDONLY;
		} else if (strncmp(data, "nosuid", 6) == 0) {
			*filesys_flags |= MS_NOSUID;
		} else if (strncmp(data, "suid", 4) == 0) {
			*filesys_flags &= ~MS_NOSUID;
		} else if (strncmp(data, "dev", 3) == 0) {
			*filesys_flags &= ~MS_NODEV;
		} else if (strncmp(data, "nodev", 5) == 0) {
			*filesys_flags |= MS_NODEV;
		} else if (strncmp(data, "noexec", 6) == 0) {
			*filesys_flags |= MS_NOEXEC;
		} else if (strncmp(data, "exec", 4) == 0) {
			*filesys_flags &= ~MS_NOEXEC;
                } else if (strncmp(data, "sync", 4) == 0) {
                        *filesys_flags |= MS_SYNCHRONOUS;
                } else if (strncmp(data, "remount", 7) == 0) {
                        *filesys_flags |= MS_REMOUNT;
                } else if (strncmp(data, "mandlock", 8) == 0) {
                        *filesys_flags |= MS_MANDLOCK;
		} else if ((strncmp(data, "nobrl", 5) == 0) || 
			   (strncmp(data, "nolock", 6) == 0)) {
			*filesys_flags &= ~MS_MANDLOCK;
		} else if (strncmp(data, "noatime", 7) == 0) {
			*filesys_flags |= MS_NOATIME;
		} else if (strncmp(data, "nodiratime", 10) == 0) {
			*filesys_flags |= MS_NODIRATIME;
		} else if (strncmp(data, "relatime", 8) == 0) {
			*filesys_flags |= MS_RELATIME;

		} else if (strncmp(data, "noauto", 6) == 0) {
			skip = true;  /* ignore */
		} else if (strncmp(data, "_netdev", 7) == 0) {
			skip = true;  /* ignore */
		} else if (strncmp(data, "nofail", 6) == 0) {
			skip = true;  /* ignore */

		} else if (strncmp(data, "secretfile", 10) == 0) {
			if (!value || !*value) {
				fprintf(stderr, "keyword secretfile found, but no secret file specified\n");
				free(saw_name);
				return NULL;
			}

			if (read_secret_from_file(value, secret, sizeof(secret)) < 0) {
				fprintf(stderr, "error reading secret file\n");
				return NULL;
			}

			/* see comment for "secret" */
			saw_secret = secret;
			skip = true;
		} else if (strncmp(data, "secret", 6) == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option secret requires a value.\n");
				free(saw_name);
				return NULL;
			}

			/* secret is only added to kernel options as
			   backwards compatibility, if add_key doesn't
			   recognize our keytype; hence, it is skipped
			   here and appended to options on add_key
			   failure */
			size_t len = sizeof(secret);
			strncpy(secret, value, len-1);
			secret[len-1] = '\0';
			saw_secret = secret;
			skip = true;
		} else if (strncmp(data, "name", 4) == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option name requires a value.\n");
				return NULL;
			}

			/* take a copy of the name, to be used for
			   naming the keys that we add to kernel; */
			free(saw_name);
			saw_name = strdup(value);
			if (!saw_name) {
				fprintf(stderr, "out of memory.\n");
				return NULL;
			}
			skip = false;
		} else {
			skip = false;
			mount_ceph_debug("mount.ceph: unrecognized mount option \"%s\", passing to kernel.\n",
					data);
		}

		/* Copy (possibly modified) option to out */
		if (!skip) {
			if (pos)
				pos = safe_cat(&out, &out_len, pos, ",");

			if (value) {
				pos = safe_cat(&out, &out_len, pos, data);
				pos = safe_cat(&out, &out_len, pos, "=");
				pos = safe_cat(&out, &out_len, pos, value);
			} else {
				pos = safe_cat(&out, &out_len, pos, data);
			}
			
		}
		data = next_keyword;
	} while (data);

	name_pos = safe_cat(&name, &name_len, name_pos, "client.");
	if (!saw_name) {
		name_pos = safe_cat(&name, &name_len, name_pos, CEPH_AUTH_NAME_DEFAULT);
	} else {
		name_pos = safe_cat(&name, &name_len, name_pos, saw_name);
	}

	if (!saw_secret && !is_kernel_secret(name)) {
		int ret = fetch_keyring_secret(name, secret);

		/*
		 * If fetching the secret doesn't work, then just try to
		 * mount without it.
		 */
		if (ret) {
			mount_ceph_debug("fetch_keyring_secret returned %d\n",
					 ret);
		} else {
			mount_ceph_debug("found key %s\n", secret);
			saw_secret = secret;
		}
	}

	if (saw_secret || is_kernel_secret(name)) {
		int ret;
		char secret_option[MAX_SECRET_OPTION_LEN];
		ret = get_secret_option(saw_secret, name, secret_option, sizeof(secret_option));
		if (ret < 0) {
			free(saw_name);
			return NULL;
		} else {
			if (pos) {
				pos = safe_cat(&out, &out_len, pos, ",");
			}
			pos = safe_cat(&out, &out_len, pos, secret_option);
		}
	}

	free(saw_name);
	if (!out)
		return strdup(EMPTY_STRING);
	return out;
}


static int parse_arguments(int argc, char *const *const argv,
		const char **src, const char **node, const char **opts)
{
	int i;

	if (argc < 2) {
		// There were no arguments. Just show the usage.
		return 1;
	}
	if ((!strcmp(argv[1], "-h")) || (!strcmp(argv[1], "--help"))) {
		// The user asked for help.
		return 1;
	}

	// The first two arguments are positional
	if (argc < 3)
		return -EINVAL;
	*src = argv[1];
	*node = argv[2];

	// Parse the remaining options
	*opts = EMPTY_STRING;
	for (i = 3; i < argc; ++i) {
		if (!strcmp("-h", argv[i]))
			return 1;
		else if (!strcmp("-n", argv[i]))
			skip_mtab_flag = true;
		else if (!strcmp("-v", argv[i]))
			verboseflag = true;
		else if (!strcmp("-o", argv[i])) {
			++i;
			if (i >= argc) {
				fprintf(stderr, "Option -o requires an argument.\n\n");
				return -EINVAL;
			}
			*opts = argv[i];
		}
		else {
			fprintf(stderr, "Can't understand option: '%s'\n\n", argv[i]);
			return -EINVAL;
		}
	}
	return 0;
}

/* modprobe failing doesn't necessarily prevent from working, so this
   returns void */
static void modprobe(void)
{
	int r;

	r = module_load("ceph", NULL);
	if (r)
		printf("failed to load ceph kernel module (%d)\n", r);
}

static void usage(const char *prog_name)
{
	printf("usage: %s [src] [mount-point] [-n] [-v] [-o ceph-options]\n",
		prog_name);
	printf("options:\n");
	printf("\t-h: Print this help\n");
	printf("\t-n: Do not update /etc/mtab\n");
	printf("\t-v: Verbose\n");
	printf("\tceph-options: refer to mount.ceph(8)\n");
	printf("\n");
}

int main(int argc, char *argv[])
{
	const char *src, *node, *opts;
	char *rsrc = NULL;
	char *popts = NULL;
	int flags = 0;
	int retval = 0;

	retval = parse_arguments(argc, argv, &src, &node, &opts);
	if (retval) {
		usage(argv[0]);
		exit((retval > 0) ? 0 : EX_USAGE);
	}

	rsrc = mount_resolve_src(src);
	if (!rsrc) {
		fprintf(stderr, "failed to resolve source\n");
		exit(EX_USAGE);
	}

	/* Ensure the ceph key_type is available */
	modprobe();

	popts = parse_options(opts, &flags);
	if (!popts) {
		fprintf(stderr, "failed to parse ceph_options\n");
		exit(EX_USAGE);
	}

	block_signals(SIG_BLOCK);

	if (mount(rsrc, node, "ceph", flags, popts)) {
		retval = EX_FAIL;
		switch (errno) {
		case ENODEV:
			fprintf(stderr, "mount error: ceph filesystem not supported by the system\n");
			break;
		default:
			fprintf(stderr, "mount error %d = %s\n",errno,strerror(errno));
		}
	} else {
		if (!skip_mtab_flag) {
			update_mtab_entry(rsrc, node, "ceph", popts, flags, 0, 0);
		}
	}

	block_signals(SIG_UNBLOCK);

	free(popts);
	free(rsrc);
	exit(retval);
}

