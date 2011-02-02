#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <errno.h>
#include <sys/mount.h>

#ifndef MS_RELATIME
# define MS_RELATIME (1<<21)
#endif

#define BUF_SIZE 128

int verboseflag = 0;
const static char * const EMPTY_STRING = "";

#include "mtab.c"

static void block_signals (int how)
{
     sigset_t sigs;

     sigfillset (&sigs);
     sigdelset(&sigs, SIGTRAP);
     sigdelset(&sigs, SIGSEGV);
     sigprocmask (how, &sigs, (sigset_t *) 0);
}


static int safe_cat(char **pstr, int *plen, int pos, const char *str2)
{
	int len2 = strlen(str2);

	while (*plen < pos + len2 + 1) {
		*plen += BUF_SIZE;
		*pstr = realloc(*pstr, (size_t)*plen);

		if (!*pstr) {
			printf("Out of memory\n");
			exit(1);
		}
	}

	strcpy((*pstr)+pos, str2);

	return pos + len2;
}

static char *mount_resolve_src(const char *orig_str)
{
	char *new_str;
	char *mount_path;
	char *tok, *p, *port_str;
	int len, pos;
	char buf[strlen(orig_str) + 1];
	strcpy(buf, orig_str);

	mount_path = strrchr(buf, ':');
	if (!mount_path) {
		printf("source mount path was not specified\n");
		return NULL;
	}
	if (mount_path == buf) {
		printf("server address expected\n");
		return NULL;
	}

	*mount_path = '\0';
	mount_path++;

	if (!*mount_path) {
		printf("incorrect source mount path\n");
		return NULL;
	}

	len = BUF_SIZE;
	new_str = (char *)malloc(len);

	p = new_str;
	pos = 0;

	tok = strtok(buf, ",");

	while (tok) {
		struct addrinfo hint;
		struct addrinfo *res, *ores;
		char *firstcolon, *lastcolon, *bracecolon;
		int r;
		int brackets = 0;

		firstcolon = strchr(tok, ':');
		lastcolon = strrchr(tok, ':');
		bracecolon = strstr(tok, "]:");

		port_str = 0;
		if (firstcolon && firstcolon == lastcolon) {
			/* host:port or a.b.c.d:port */
			*firstcolon = 0;
			port_str = firstcolon + 1;
		} else if (bracecolon) {
			/* {ipv6addr}:port */
			port_str = bracecolon + 1;
			*port_str = 0;
			port_str++;
		}
		if (port_str && !*port_str)
			port_str = NULL;

		if (*tok == '[' &&
		    tok[strlen(tok)-1] == ']') {
			tok[strlen(tok)-1] = 0;
			tok++;
			brackets = 1;
		}			

		/*printf("name '%s' port '%s'\n", tok, port_str);*/

		memset(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_protocol = IPPROTO_TCP;

		r = getaddrinfo(tok, port_str, &hint, &res);
		if (r < 0) {
			printf("server name not found: %s (%s)\n", tok, strerror(errno));
			free(new_str);
			return 0;
		}

		/* build resolved addr list */
		ores = res;
		while (res) {
			char host[40], port[40];
			getnameinfo(res->ai_addr, res->ai_addrlen,
				    host, sizeof(host),
				    port, sizeof(port),
				    NI_NUMERICSERV | NI_NUMERICHOST);
			/*printf(" host %s port %s flags %d family %d socktype %d proto %d sanonname %s\n",
			       host, port,
			       res->ai_flags, res->ai_family, res->ai_socktype, res->ai_protocol,
			       res->ai_canonname);*/
			if (res->ai_family == AF_INET6)
				brackets = 1;  /* always surround ipv6 addrs with brackets */
			if (brackets)
				pos = safe_cat(&new_str, &len, pos, "[");
			pos = safe_cat(&new_str, &len, pos, host);
			if (brackets)
				pos = safe_cat(&new_str, &len, pos, "]");
			if (port_str) {
				pos = safe_cat(&new_str, &len, pos, ":");
				pos = safe_cat(&new_str, &len, pos, port);
			}
			res = res->ai_next;
			if (res)
				pos = safe_cat(&new_str, &len, pos, ",");
		}
		freeaddrinfo(ores);

		tok = strtok(NULL, ",");
		if (tok)
			pos = safe_cat(&new_str, &len, pos, ",");

	}

	pos = safe_cat(&new_str, &len, pos, ":");
	pos = safe_cat(&new_str, &len, pos, mount_path);

	/*printf("new_str is '%s'\n", new_str);*/
	return new_str;
}

/*
 * this one is partialy based on parse_options() from cifs.mount.c
 */
static char *parse_options(const char *data, int *filesys_flags)
{
	char * value = NULL;
	char * next_keyword = NULL;
	char * out = NULL;
	int out_len = 0;
	int word_len;
	int skip;
	int pos = 0;
	char *newdata = 0;
	char secret[1000];

	if(verboseflag)
		printf("parsing options: %s\n", data);

	do {
		/*  check if ends with trailing comma */
		if(*data == 0)
			break;
		next_keyword = strchr(data,',');
		newdata = 0;
	
		/* temporarily null terminate end of keyword=value pair */
		if(next_keyword)
			*next_keyword++ = 0;

		/* temporarily null terminate keyword to make keyword and value distinct */
		if ((value = strchr(data, '=')) != NULL) {
			*value = '\0';
			value++;
		}

		skip = 1;

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
			skip = 1;  /* ignore */
		} else if (strncmp(data, "_netdev", 7) == 0) {
			skip = 1;  /* ignore */

		} else if (strncmp(data, "secretfile", 7) == 0) {
			char *fn = value;
			char *end = fn;
			int fd;
			int len;

			if (!fn || !*fn) {
				printf("keyword secretfile found, but no secret file specified\n");
				return NULL;
			}

			while (*end)
				end++;
			fd = open(fn, O_RDONLY);
			if (fd < 0) {
				perror("unable to read secretfile");
				return NULL;
			}
			len = read(fd, secret, 1000);
			if (len <= 0) {
				perror("unable to read secret from secretfile");
				return NULL;
			}
			end = secret;
			while (end < secret + len && *end && *end != '\n' && *end != '\r')
				end++;
			*end = '\0';
			close(fd);

			if (verboseflag)
				printf("read secret of len %d from %s\n", len, fn);
			data = "secret";
			value = secret;
			skip = 0;
		} else {
			skip = 0;
			if (verboseflag)
				printf("ceph: Unknown mount option %s\n",data);
		}

		/* Copy (possibly modified) option to out */
		if (!skip) {
			word_len = strlen(data);
			if (value)
				word_len += 1 + strlen(value);

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
		else if (!strcmp("-v", argv[i]))
			verboseflag = 1;
		else if (!strcmp("-o", argv[i])) {
			++i;
			if (i >= argc) {
				printf("Option -o requires an argument.\n\n");
				return -EINVAL;
			}
			*opts = argv[i];
		}
		else {
			printf("Can't understand option: '%s'\n\n", argv[i]);
			return -EINVAL;
		}
	}
	return 0;
}

static void usage(const char *prog_name)
{
	printf("usage: %s [src] [mount-point] [-v] [-o ceph-options]\n",
		prog_name);
	printf("options:\n");
	printf("\t-h: Print this help\n");
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
		exit((retval > 0) ? EXIT_SUCCESS : EXIT_FAILURE);
	}

	rsrc = mount_resolve_src(src);
	if (!rsrc) {
		printf("failed to resolve source\n");
		exit(1);
	}

	popts = parse_options(opts, &flags);
	if (!popts) {
		printf("failed to parse ceph_options\n");
		exit(1);
	}

	block_signals(SIG_BLOCK);

	if (mount(rsrc, node, "ceph", flags, popts)) {
		retval = errno;
		switch (errno) {
		case ENODEV:
			printf("mount error: ceph filesystem not supported by the system\n");
			break;
		default:
			printf("mount error %d = %s\n",errno,strerror(errno));
		}
	} else {
		update_mtab_entry(rsrc, node, "ceph", popts, flags, 0, 0);
	}

	block_signals(SIG_UNBLOCK);

	free(popts);
	free(rsrc);
	exit(retval);
}

