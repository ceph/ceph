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
bool v2_addrs = true;
bool no_fallback = false;
bool ms_mode_specified = false;
bool mon_addr_specified = false;
static const char * const EMPTY_STRING = "";

/* TODO duplicates logic from kernel */
#define CEPH_AUTH_NAME_DEFAULT "guest"

/* path to sysfs for ceph */
#define CEPH_SYS_FS_PATH "/sys/module/ceph/"
#define CEPH_SYS_FS_PARAM_PATH CEPH_SYS_FS_PATH"/parameters"

/*
 * mount support hint from kernel -- we only need to check
 * v2 support for catching bugs.
 */
#define CEPH_V2_MOUNT_SUPPORT_PATH CEPH_SYS_FS_PARAM_PATH"/mount_syntax_v2"

#define CEPH_DEFAULT_V2_MS_MODE "prefer-crc"

#include "mtab.c"

enum mount_dev_format {
  MOUNT_DEV_FORMAT_OLD = 0,
  MOUNT_DEV_FORMAT_NEW = 1,
};

struct ceph_mount_info {
	unsigned long	cmi_flags;
	char		*cmi_name;
	char		*cmi_fsname;
	char		*cmi_fsid;
	char		*cmi_path;
	char		*cmi_mons;
	char		*cmi_conf;
	char		*cmi_opts;
	int		cmi_opts_len;
	char 		cmi_secret[SECRET_BUFSIZE];

	/* mount dev syntax format */
	enum mount_dev_format format;
};

static void mon_addr_as_resolve_param(char *mon_addr)
{
	for (; *mon_addr; ++mon_addr)
		if (*mon_addr == '/')
			*mon_addr = ',';
}

static void resolved_mon_addr_as_mount_opt(char *mon_addr)
{
	for (; *mon_addr; ++mon_addr)
		if (*mon_addr == ',')
			*mon_addr = '/';
}

static void resolved_mon_addr_as_mount_dev(char *mon_addr)
{
	for (; *mon_addr; ++mon_addr)
		if (*mon_addr == '/')
			*mon_addr = ',';
}

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

/*
 * append a key value pair option to option string.
 */
static void append_opt(const char *key, const char *value,
		       struct ceph_mount_info *cmi, int *pos)
{
	if (*pos != 0)
		*pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, *pos, ",");

	if (value) {
		*pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, *pos, key);
		*pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, *pos, "=");
		*pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, *pos, value);
	} else {
		*pos = safe_cat(&cmi->cmi_opts, &cmi->cmi_opts_len, *pos, key);
	}
}

/*
 * remove a key value pair from option string. caller should ensure that the
 * key value pair is separated by "=".
 */
static int remove_opt(struct ceph_mount_info *cmi, const char *key, char **value)
{
	char *key_start = strstr(cmi->cmi_opts, key);
	if (!key_start) {
		return -ENOENT;
	}

	/* key present -- try to split */
	char *key_sep = strstr(key_start, "=");
	if (!key_sep) {
		return -ENOENT;
	}

	if (strncmp(key, key_start, key_sep - key_start) != 0) {
		return -ENOENT;
	}

	++key_sep;
	char *value_end = strstr(key_sep, ",");
	if (!value_end)
		value_end = key_sep + strlen(key_sep);

	if (value_end != key_sep && value) {
		size_t len1 = value_end - key_sep;
		*value = strndup(key_sep, len1+1);
		if (!*value)
			return -ENOMEM;
		(*value)[len1] = '\0';
	}

	/* purge it */
	size_t len2 = strlen(value_end);
	if (len2) {
		++value_end;
		memmove(key_start, value_end, len2);
	} else {
                /* last kv pair - swallow the comma */
		if (*(key_start - 1) == ',') {
			--key_start;
		}
		*key_start = '\0';
	}

	return 0;
}

static void record_name(const char *name, struct ceph_mount_info *cmi)
{
	int name_pos = 0;
	int name_len = 0;

	name_pos = safe_cat(&cmi->cmi_name, &name_len, name_pos, name);
}

/*
 * parse old device string of format: <mon_addr>:/<path>
 */
static int parse_old_dev(const char *dev_str, struct ceph_mount_info *cmi,
			 int *opt_pos)
{
	size_t len;
	char *mount_path;

	mount_path = strstr(dev_str, ":/");
	if (!mount_path) {
		fprintf(stderr, "source mount path was not specified\n");
		return -EINVAL;
	}

	len = mount_path - dev_str;
	if (len != 0) {
		free(cmi->cmi_mons);
		/* overrides mon_addr passed via mount option (if any) */
		cmi->cmi_mons = strndup(dev_str, len);
		if (!cmi->cmi_mons)
			return -ENOMEM;
		mon_addr_specified = true;
	} else {
		/* reset mon_addr=<> mount option */
		mon_addr_specified = false;
	}

	mount_path++;
	cmi->cmi_path = strdup(mount_path);
	if (!cmi->cmi_path)
		return -ENOMEM;
	if (!cmi->cmi_name)
		record_name(CEPH_AUTH_NAME_DEFAULT, cmi);

	cmi->format = MOUNT_DEV_FORMAT_OLD;
	return 0;
}

/*
 * parse new device string of format: name@<fsid>.fs_name=/path
 */
static int parse_new_dev(const char *dev_str, struct ceph_mount_info *cmi,
			 int *opt_pos)
{
	size_t len;
	char *name;
	char *name_end;
	char *dot;
	char *fs_name;

	name_end = strstr(dev_str, "@");
	if (!name_end) {
		mount_ceph_debug("invalid new device string format\n");
		return -ENODEV;
	}

	len = name_end - dev_str;
	if (!len) {
		fprintf(stderr, "missing <name> in device\n");
		return -EINVAL;
	}

	name = (char *)alloca(len+1);
	memcpy(name, dev_str, len);
	name[len] = '\0';

	if (cmi->cmi_name && strcmp(cmi->cmi_name, name)) {
		fprintf(stderr, "mismatching ceph user in mount option and device string\n");
		return -EINVAL;
	}

	/* record name and store in option string */
	if (!cmi->cmi_name) {
		record_name(name, cmi);
		append_opt("name", name, cmi, opt_pos);
	}

	++name_end;
	/* check if an fsid is included in the device string */
	dot = strstr(name_end, ".");
	if (!dot) {
		fprintf(stderr, "invalid device string format\n");
		return -EINVAL;
	}
	len = dot - name_end;
	if (len) {
		/* check if this _looks_ like a UUID */
		if (len != CLUSTER_FSID_LEN - 1) {
			fprintf(stderr, "invalid device string format\n");
			return -EINVAL;
		}

		cmi->cmi_fsid = strndup(name_end, len);
		if (!cmi->cmi_fsid)
			return -ENOMEM;
	}

	++dot;
	fs_name = strstr(dot, "=");
	if (!fs_name) {
		fprintf(stderr, "invalid device string format\n");
		return -EINVAL;
	}
	len = fs_name - dot;
	if (!len) {
		fprintf(stderr, "missing <fs_name> in device\n");
		return -EINVAL;
	}
	cmi->cmi_fsname = strndup(dot, len);
	if (!cmi->cmi_fsname)
		return -ENOMEM;

	++fs_name;
	if (strlen(fs_name)) {
		cmi->cmi_path = strdup(fs_name);
		if (!cmi->cmi_path)
			return -ENOMEM;
	}

	/* new-style dev - force using v2 addrs first */
	if (!ms_mode_specified && !mon_addr_specified)
	  append_opt("ms_mode", CEPH_DEFAULT_V2_MS_MODE, cmi,
		     opt_pos);

	cmi->format = MOUNT_DEV_FORMAT_NEW;
	return 0;
}

static int parse_dev(const char *dev_str, struct ceph_mount_info *cmi,
		     int *opt_pos)
{
	int ret;

	ret = parse_new_dev(dev_str, cmi, opt_pos);
	if (ret < 0 && ret != -ENODEV)
		return -EINVAL;
	if (ret)
		ret = parse_old_dev(dev_str, cmi, opt_pos);
	if (ret < 0)
		fprintf(stderr, "error parsing device string\n");
	return ret;
}

/* resolve monitor host and optionally record in option string.
 * use opt_pos to determine if the caller wants to record the
 * resolved address in mount option (c.f., mount_old_device_format).
 */
static int finalize_src(struct ceph_mount_info *cmi, int *opt_pos,
			char **resolved_addr)
{
	char *src;
        size_t len = strlen(cmi->cmi_mons);
        char *addr = alloca(len+1);

        memcpy(addr, cmi->cmi_mons, len+1);
        mon_addr_as_resolve_param(addr);

	src = resolve_addrs(addr);
	if (!src)
		return -1;

	mount_ceph_debug("mount.ceph: resolved to: \"%s\"\n", src);
	if (opt_pos) {
		resolved_mon_addr_as_mount_opt(src);
		append_opt("mon_addr", src, cmi, opt_pos);
	} else if (resolved_addr) {
                *resolved_addr = strdup(src);
        }
	free(src);
	return 0;
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
	if (cmi->cmi_secret[0] && cmi->cmi_mons && cmi->cmi_fsid)
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
		char *entity_name = NULL;
		int name_pos = 0;
		int name_len = 0;

		/* child */
		ret = drop_capabilities();
		if (ret)
			exit(1);

		name_pos = safe_cat(&entity_name, &name_len, name_pos, "client.");
		name_pos = safe_cat(&entity_name, &name_len, name_pos, cmi->cmi_name);
		mount_ceph_get_config_info(cmi->cmi_conf, entity_name, v2_addrs, cci);
		free(entity_name);
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
		if (!cmi->cmi_fsid) {
			len = strnlen(cci->cci_fsid, CLUSTER_FSID_LEN);
			if (len < CLUSTER_FSID_LEN)
				cmi->cmi_fsid = strndup(cci->cci_fsid, len + 1);
		}
	}
out:
	munmap(cci, sizeof(*cci));
	return ret;
}

/*
 * this one is partially based on parse_options() from cifs.mount.c
 */
static int parse_options(const char *data, struct ceph_mount_info *cmi,
			 int *opt_pos)
{
	char * next_keyword = NULL;
	char *name = NULL;

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
		} else if (strcmp(data, "nofallback") == 0) {
			no_fallback = true;
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
		} else if (strcmp(data, "ms_mode") == 0) {
			if (!value || !*value) {
				fprintf(stderr, "mount option ms_mode requires a value.\n");
				return -EINVAL;
			}
			/* Only legacy ms_mode needs v1 addrs */
			v2_addrs = strcmp(value, "legacy");
			skip = false;
			ms_mode_specified = true;
		} else if (strcmp(data, "mon_addr") == 0) {
			/* monitor address to use for mounting */
			if (!value || !*value) {
				fprintf(stderr, "mount option mon_addr requires a value.\n");
				return -EINVAL;
			}
			cmi->cmi_mons = strdup(value);
			if (!cmi->cmi_mons)
				return -ENOMEM;
			mon_addr_specified = true;
		} else {
			/* unrecognized mount options, passing to kernel */
			skip = false;
		}

		/* Copy (possibly modified) option to out */
		if (!skip)
                        append_opt(data, value, cmi, opt_pos);
		data = next_keyword;
	} while (data);

out:
	/*
	 * set ->cmi_name conditionally -- this gets checked when parsing new
	 * device format. for old device format, ->cmi_name is set to default
	 * user name when name option is not passed in.
	 */
	if (name)
		record_name(name, cmi);
	if (cmi->cmi_opts)
		mount_ceph_debug("mount.ceph: options \"%s\".\n",  cmi->cmi_opts);

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
                } else {
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
	free(cmi->cmi_fsname);
	free(cmi->cmi_fsid);
	free(cmi->cmi_path);
	free(cmi->cmi_mons);
	free(cmi->cmi_conf);
}

static int mount_new_device_format(const char *node, struct ceph_mount_info *cmi)
{
	int r;
	char *rsrc = NULL;
	int pos = 0;
	int len = 0;

	if (!cmi->cmi_fsid) {
		fprintf(stderr, "missing ceph cluster-id");
		return -EINVAL;
	}

	pos = safe_cat(&rsrc, &len, pos, cmi->cmi_name);
	pos = safe_cat(&rsrc, &len, pos, "@");
	pos = safe_cat(&rsrc, &len, pos, cmi->cmi_fsid);
	pos = safe_cat(&rsrc, &len, pos, ".");
	pos = safe_cat(&rsrc, &len, pos, cmi->cmi_fsname);
	pos = safe_cat(&rsrc, &len, pos, "=");
	if (cmi->cmi_path)
		safe_cat(&rsrc, &len, pos, cmi->cmi_path);

	mount_ceph_debug("mount.ceph: trying mount with new device syntax: %s\n",
			 rsrc);
	if (cmi->cmi_opts)
		mount_ceph_debug("mount.ceph: options \"%s\" will pass to kernel\n",
				 cmi->cmi_opts);
	r = mount(rsrc, node, "ceph", cmi->cmi_flags, cmi->cmi_opts);
	if (r)
		r = -errno;
	free(rsrc);
	return r;
}

static int mount_old_device_format(const char *node, struct ceph_mount_info *cmi)
{
	int r;
	int len = 0;
	int pos = 0;
	char *mon_addr = NULL;
	char *rsrc = NULL;

	r = remove_opt(cmi, "mon_addr", &mon_addr);
	if (r) {
		fprintf(stderr, "failed to switch using old device format\n");
		return -EINVAL;
	}

	/* if we reach here and still have a v2 addr, we'd need to
	 * refresh with v1 addrs, since we'll be not passing ms_mode
	 * with the old syntax.
	 */
	if (v2_addrs && !ms_mode_specified && !mon_addr_specified) {
		mount_ceph_debug("mount.ceph: switching to using v1 address with old syntax\n");
		v2_addrs = false;
		free(mon_addr);
		free(cmi->cmi_mons);
		mon_addr = NULL;
		cmi->cmi_mons = NULL;
		fetch_config_info(cmi);
		if (!cmi->cmi_mons) {
			fprintf(stderr, "unable to determine (v1) mon addresses\n");
			return -EINVAL;
		}
		r = finalize_src(cmi, NULL, &mon_addr);
		if (r) {
			fprintf(stderr, "failed to resolve (v1) mon addresses\n");
			return -EINVAL;
		}
		remove_opt(cmi, "ms_mode", NULL);
	}

	pos = strlen(cmi->cmi_opts);
        if (cmi->cmi_fsname)
                append_opt("mds_namespace", cmi->cmi_fsname, cmi, &pos);
	if (cmi->cmi_fsid)
		append_opt("fsid", cmi->cmi_fsid, cmi, &pos);

	pos = 0;
	resolved_mon_addr_as_mount_dev(mon_addr);
	pos = safe_cat(&rsrc, &len, pos, mon_addr);
	pos = safe_cat(&rsrc, &len, pos, ":");
	if (cmi->cmi_path)
		safe_cat(&rsrc, &len, pos, cmi->cmi_path);

	mount_ceph_debug("mount.ceph: trying mount with old device syntax: %s\n",
			 rsrc);
	if (cmi->cmi_opts)
		mount_ceph_debug("mount.ceph: options \"%s\" will pass to kernel\n",
				 cmi->cmi_opts);

	r = mount(rsrc, node, "ceph", cmi->cmi_flags, cmi->cmi_opts);
	free(mon_addr);
	free(rsrc);

	return r;
}

/*
 * check whether to fall-back to using old-style mount syntax (called
 * when new-style mount syntax fails). this is mostly to catch any
 * new-style (v2) implementation bugs in the kernel and is primarly
 * used in teuthology tests.
 */
static bool should_fallback()
{
	int ret;
	struct stat stbuf;

	if (!no_fallback)
		return true;

	ret = stat(CEPH_V2_MOUNT_SUPPORT_PATH, &stbuf);
	if (ret) {
		mount_ceph_debug("mount.ceph: v2 mount support check returned %d\n",
				 errno);
		if (errno == ENOENT)
			mount_ceph_debug("mount.ceph: kernel does not support v2"
					 " syntax\n");
		/* fallback on *all* errors */
		return true;
	}

	fprintf(stderr, "mount.ceph: kernel BUG!\n");
	return false;
}

static int do_mount(const char *dev, const char *node,
		    struct ceph_mount_info *cmi) {
	int pos = 0;
	int retval= -EINVAL;
	bool fallback = true;

        /* no v2 addresses available via config - try v1 addresses */
	if (v2_addrs &&
	    !cmi->cmi_mons &&
	    !ms_mode_specified &&
	    !mon_addr_specified) {
		mount_ceph_debug("mount.ceph: switching to using v1 address\n");
		v2_addrs = false;
		fetch_config_info(cmi);
		remove_opt(cmi, "ms_mode", NULL);
	}

	if (!cmi->cmi_mons) {
		fprintf(stderr, "unable to determine mon addresses\n");
		return -EINVAL;
	}

	pos = strlen(cmi->cmi_opts);
	retval = finalize_src(cmi, &pos, NULL);
	if (retval) {
		fprintf(stderr, "failed to resolve source\n");
		return -EINVAL;
	}

	retval = -1;
	if (cmi->format == MOUNT_DEV_FORMAT_NEW) {
		retval = mount_new_device_format(node, cmi);
		if (retval)
			fallback = (should_fallback() && retval == -EINVAL && cmi->cmi_fsid);
	}

	/* pass-through or fallback to old-style mount device */
	if (retval && fallback)
		retval = mount_old_device_format(node, cmi);
	if (retval) {
		retval = EX_FAIL;
		switch (errno) {
		case ENODEV:
			fprintf(stderr, "mount error: ceph filesystem not supported by the system\n");
			break;
		case EHOSTUNREACH:
			fprintf(stderr, "mount error: no mds (Metadata Server) is up. "
			"The cluster might be laggy, or you may not be authorized\n");
			break;
		default:
			fprintf(stderr, "mount error %d = %s\n", errno, strerror(errno));
		}
	}

	if (!retval && !skip_mtab_flag) {
		update_mtab_entry(dev, node, "ceph", cmi->cmi_opts, cmi->cmi_flags, 0, 0);
	}

	return retval;
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
	int opt_pos = 0;
	const char *dev, *node, *opts;
	int retval;
	struct ceph_mount_info cmi = { 0 };

	retval = parse_arguments(argc, argv, &dev, &node, &opts);
	if (retval) {
		usage(argv[0]);
		retval = (retval > 0) ? 0 : EX_USAGE;
		goto out;
	}

	retval = parse_options(opts, &cmi, &opt_pos);
	if (retval) {
		fprintf(stderr, "failed to parse ceph_options: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	retval = parse_dev(dev, &cmi, &opt_pos);
	if (retval) {
		fprintf(stderr, "unable to parse mount device string: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	/*
	 * We don't care if this errors out, since this is best-effort.
	 * note that this fetches v1 or v2 addr depending on @v2_addr
	 * flag.
	 */
	fetch_config_info(&cmi);

	/* Ensure the ceph key_type is available */
	modprobe();

	retval = append_key_or_secret_option(&cmi);
	if (retval) {
		fprintf(stderr, "couldn't append secret option: %d\n", retval);
		retval = EX_USAGE;
		goto out;
	}

	block_signals(SIG_BLOCK);
	retval = do_mount(dev, node, &cmi);
	block_signals(SIG_UNBLOCK);
out:
	ceph_mount_info_free(&cmi);
	return retval;
}

