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
#include "mount.ceph.h"

#ifndef MS_RELATIME
# define MS_RELATIME (1<<21)
#endif

bool verboseflag = false;
bool skip_mtab_flag = false;
static const char * const EMPTY_STRING = "";

/* TODO duplicates logic from kernel */
#define CEPH_AUTH_NAME_DEFAULT "guest"

#include "mtab.c"

struct ceph_mount_info {
	unsigned long	cmi_flags;
	char		*cmi_name;
	char		*cmi_path;
	char		*cmi_mons;
	char		*cmi_conf;
	char		*cmi_opts;
	int		cmi_opts_len;
	char 		cmi_secret[SECRET_BUFSIZE];
};

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

static int parse_src(const char *orig_str, struct ceph_mount_info *cmi)
{
	size_t len;
	char *mount_path;

	mount_path = strstr(orig_str, ":/");
	if (!mount_path) {
		fprintf(stderr, "source mount path was not specified\n");
		return -EINVAL;
	}

	len = mount_path - orig_str;
	if (len != 0) {
		cmi->cmi_mons = strndup(orig_str, len);
		if (!cmi->cmi_mons)
			return -ENOMEM;
	}

	mount_path++;
	cmi->cmi_path = strdup(mount_path);
	if (!cmi->cmi_path)
		return -ENOMEM;
	return 0;
}

static char *finalize_src(struct ceph_mount_info *cmi)
{
	int pos, len;
	char *src;

	src = resolve_addrs(cmi->cmi_mons);
	if (!src)
		return NULL;

	len = strlen(src);
	pos = safe_cat(&src, &len, len, ":");
	safe_cat(&src, &len, pos, cmi->cmi_path);

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
 * Attempt to fetch info from the local config file, if one is present. Since
 * this involves activity that may be dangerous for a privileged task, we
 * fork(), have the child drop privileges and do the processing and then hand
 * back the results via memory shared with the parent.
 */
static int fetch_config_info(struct ceph_mount_info *cmi)
{
	int ret = 0;
	pid_t pid;
	struct ceph_config_info *cci;

	/* Don't do anything if we already have requisite info */
	if (cmi->cmi_secret[0] && cmi->cmi_mons)
		return 0;

	cci = mmap((void *)0, sizeof(*cci), PROT_READ | PROT_WRITE,
		   MAP_ANONYMOUS | MAP_SHARED, -1, 0);
	if (cci == MAP_FAILED) {
		mount_ceph_debug("Unable to allocate memory: %s\n",
				 strerror(errno));
		return EX_SYSERR;
	}

	pid = fork();
	if (pid < 0) {
		mount_ceph_debug("fork() failure: %s\n", strerror(errno));
		ret = EX_SYSERR;
		goto out;
	}

	if (pid == 0) {
		/* child */
		ret = drop_capabilities();
		if (ret)
			exit(1);
		mount_ceph_get_config_info(cmi->cmi_conf, cmi->cmi_name, cci);
		exit(0);
	} else {
		/* parent */
		pid = wait(&ret);
		if (!WIFEXITED(ret)) {
			mount_ceph_debug("Child process terminated abnormally.\n");
			ret = EX_SYSERR;
			goto out;
		}
		ret = WEXITSTATUS(ret);
		if (ret) {
			mount_ceph_debug("Child exited with status %d\n", ret);
			ret = EX_SYSERR;
			goto out;
		}

		/*
		 * Copy values from MAP_SHARED buffer to cmi if we didn't
		 * already find anything and we got something from the child.
		 */
		size_t len;
		if (!cmi->cmi_secret[0] && cci->cci_secret[0]) {

			len = strnlen(cci->cci_secret, SECRET_BUFSIZE);
			if (len < SECRET_BUFSIZE) {
				memcpy(cmi->cmi_secret, cci->cci_secret, len + 1);
			} else {
				mount_ceph_debug("secret is too long (len=%zu max=%zu)!\n", len, SECRET_BUFSIZE);
			}
		}
		if (!cmi->cmi_mons && cci->cci_mons[0]) {
			len = strnlen(cci->cci_mons, MON_LIST_BUFSIZE);
			if (len < MON_LIST_BUFSIZE)
				cmi->cmi_mons = strndup(cci->cci_mons, len + 1);
		}
	}
out:
	munmap(cci, sizeof(*cci));
	return ret;
}

/*
 * this one is partially based on parse_options() from cifs.mount.c
 */
static int parse_options(const char *data, struct ceph_mount_info *cmi)
{
	char * next_keyword = NULL;
	int pos = 0;
	char *name = NULL;
	int name_len = 0;
	int name_pos = 0;

	if (data == EMPTY_STRING)
		goto out;

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

		if (strcmp(data, "ro") == 0) {
			cmi->cmi_flags |= MS_RDONLY;
		} else if (strcmp(data, "rw") == 0) {
			cmi->cmi_flags &= ~MS_RDONLY;
		} else if (strcmp(data, "nosuid") == 0) {
			cmi->cmi_flags |= MS_NOSUID;
		} else if (strcmp(data, "suid") == 0) {
			cmi->cmi_flags &= ~MS_NOSUID;
		} else if (strcmp(data, "dev") == 0) {
			cmi->cmi_flags &= ~MS_NODEV;
		} else if (strcmp(data, "nodev") == 0) {
			cmi->cmi_flags |= MS_NODEV;
		} else if (strcmp(data, "noexec") == 0) {
			cmi->cmi_flags |= MS_NOEXEC;
		} else if (strcmp(data, "exec") == 0) {
			cmi->cmi_flags &= ~MS_NOEXEC;
                } else if (strcmp(data, "sync") == 0) {
                        cmi->cmi_flags |= MS_SYNCHRONOUS;
                } else if (strcmp(data, "remount") == 0) {
                        cmi->cmi_flags |= MS_REMOUNT;
                } else if (strcmp(data, "mandlock") == 0) {
                        cmi->cmi_flags |= MS_MANDLOCK;
		} else if ((strcmp(data, "nobrl") == 0) ||
			   (strcmp(data, "nolock") == 0)) {
			cmi->cmi_flags &= ~MS_MANDLOCK;
		} else if (strcmp(data, "noatime") == 0) {
			cmi->cmi_flags |= MS_NOATIME;
		} else if (strcmp(data, "nodiratime") == 0) {
			cmi->cmi_flags |= MS_NODIRATIME;
		} else if (strcmp(data, "relatime") == 0) {
			cmi->cmi_flags |= MS_RELATIME;
		} else if (strcmp(data, "strictatime") == 0) {
			cmi->cmi_flags |= MS_STRICTATIME;
		} else if (strcmp(data, "noauto") == 0) {
			/* ignore */
		} else if (strcmp(data, "_netdev") == 0) {
			/* ignore */
		} else if (strcmp(data, "nofail") == 0) {
			/* ignore */
		} else if (strcmp(data, "fs") == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option fs requires a value.\n");
				return -EINVAL;
			}
			data = "mds_namespace";
			skip = false;
		} else if (strcmp(data, "secretfile") == 0) {
			int ret;

			if (!value || !*value) {
				fprintf(stderr, "keyword secretfile found, but no secret file specified\n");
				return -EINVAL;
			}
			ret = read_secret_from_file(value, cmi->cmi_secret, sizeof(cmi->cmi_secret));
			if (ret < 0) {
				fprintf(stderr, "error reading secret file: %d\n", ret);
				return ret;
			}
		} else if (strcmp(data, "secret") == 0) {
			size_t len;

			if (!value || !*value) {
				fprintf(stderr, "mount option secret requires a value.\n");
				return -EINVAL;
			}

			len = strnlen(value, sizeof(cmi->cmi_secret)) + 1;
			if (len <= sizeof(cmi->cmi_secret))
				memcpy(cmi->cmi_secret, value, len);
		} else if (strcmp(data, "conf") == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option conf requires a value.\n");
				return -EINVAL;
			}
			/* keep pointer to value */
			cmi->cmi_conf = strdup(value);
			if (!cmi->cmi_conf)
				return -ENOMEM;
		} else if (strcmp(data, "name") == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option name requires a value.\n");
				return -EINVAL;
			}
			/* keep pointer to value */
			name = value;
			skip = false;
		} else {
			/* unrecognized mount options, passing to kernel */
			skip = false;
		}

		/* Copy (possibly modified) option to out */
		if (!skip) {
			if (pos)
				pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, ",");

			if (value) {
				pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, data);
				pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, "=");
				pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, value);
			} else {
				pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, data);
			}
		}
		data = next_keyword;
	} while (data);

out:
	name_pos = safe_cat(&cmi->cmi_name, &name_len, name_pos, "client.");
	name_pos = safe_cat(&cmi->cmi_name, &name_len, name_pos,
			    name ? name : CEPH_AUTH_NAME_DEFAULT);

	if (cmi->cmi_opts)
		mount_ceph_debug("mount.ceph: options \"%s\" will pass to kernel.\n",
						 cmi->cmi_opts);

	if (!cmi->cmi_opts) {
		cmi->cmi_opts = strdup(EMPTY_STRING);
		if (!cmi->cmi_opts)
			return -ENOMEM;
	}
	return 0;
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

/*
 * The structure itself lives on the stack, so don't free it. Just the
 * pointers inside.
 */
static void ceph_mount_info_free(struct ceph_mount_info *cmi)
{
	free(cmi->cmi_opts);
	free(cmi->cmi_name);
	free(cmi->cmi_path);
	free(cmi->cmi_mons);
	free(cmi->cmi_conf);
}

static int append_key_or_secret_option(struct ceph_mount_info *cmi)
{
	int pos = strlen(cmi->cmi_opts);

	if (!cmi->cmi_secret[0] && !is_kernel_secret(cmi->cmi_name))
		return 0;

	if (pos)
		pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, ",");

	/* when parsing kernel options (-o remount) we get '<hidden>' as the secret */
	if (cmi->cmi_secret[0] && (strcmp(cmi->cmi_secret, "<hidden>") != 0)) {
		int ret = set_kernel_secret(cmi->cmi_secret, cmi->cmi_name);
		if (ret < 0) {
			if (ret == -ENODEV || ret == -ENOSYS) {
				/* old kernel; fall back to secret= in options */
				pos = safe_cat(&cmi->cmi_opts,
					       &cmi->cmi_opts_len, pos,
					       "secret=");
				pos = safe_cat(&cmi->cmi_opts,
					       &cmi->cmi_opts_len, pos,
					       cmi->cmi_secret);
				return 0;
			}
			fprintf(stderr, "adding ceph secret key to kernel failed: %s\n",
				strerror(-ret));
			return ret;
		}
	}

	pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, "key=");
	pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, pos, cmi->cmi_name);

	return 0;
}

int main(int argc, char *argv[])
{
	const char *src, *node, *opts;
	char *rsrc = NULL;
	int retval;
	struct ceph_mount_info cmi = { 0 };

	retval = parse_arguments(argc, argv, &src, &node, &opts);
	if (retval) {
		usage(argv[0]);
		retval = (retval > 0) ? 0 : EX_USAGE;
		goto out;
	}

	retval = parse_options(opts, &cmi);
	if (retval) {
		fprintf(stderr, "failed to parse ceph_options: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	retval = parse_src(src, &cmi);
	if (retval) {
		fprintf(stderr, "unable to parse mount source: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	/* We don't care if this errors out, since this is best-effort */
	fetch_config_info(&cmi);

	if (!cmi.cmi_mons) {
		fprintf(stderr, "unable to determine mon addresses\n");
		retval = EX_USAGE;
		goto out;
	}

	rsrc = finalize_src(&cmi);
	if (!rsrc) {
		fprintf(stderr, "failed to resolve source\n");
		retval = EX_USAGE;
		goto out;
	}

	/* Ensure the ceph key_type is available */
	modprobe();

	retval = append_key_or_secret_option(&cmi);
	if (retval) {
		fprintf(stderr, "couldn't append secret option: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	block_signals(SIG_BLOCK);

	if (mount(rsrc, node, "ceph", cmi.cmi_flags, cmi.cmi_opts)) {
		retval = EX_FAIL;
		switch (errno) {
		case ENODEV:
			fprintf(stderr, "mount error: ceph filesystem not supported by the system\n");
			break;
		case EHOSTUNREACH:
			fprintf(stderr, "mount error: no mds server is up or the cluster is laggy\n");
			break;
		default:
			fprintf(stderr, "mount error %d = %s\n",errno,strerror(errno));
		}
	} else {
		if (!skip_mtab_flag) {
			update_mtab_entry(rsrc, node, "ceph", cmi.cmi_opts, cmi.cmi_flags, 0, 0);
		}
	}

	block_signals(SIG_UNBLOCK);
out:
	ceph_mount_info_free(&cmi);
	free(rsrc);
	return retval;
}

