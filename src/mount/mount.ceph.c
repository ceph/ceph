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

#include "mtab.c"

void
block_signals (int how) {
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

char *mount_resolve_dest(char *orig_str)
{
	char *new_str;
	char *mount_path;
	char *tok, *p, *port_str;
	int len, pos;

	mount_path = strrchr(orig_str, ':');
	if (!mount_path) {
		printf("source mount path was not specified\n");
		return NULL;
	}
	if (mount_path == orig_str) {
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

	tok = strtok(orig_str, ",");

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
static int parse_options(char ** optionsp, int * filesys_flags)
{
	const char * data;
	char * value = NULL;
	char * next_keyword = NULL;
	char * out = NULL;
	int out_len = 0;
	int word_len;
	int skip;
	int pos = 0;
	char *newdata = 0;

	if (!optionsp || !*optionsp)
		return 1;
	data = *optionsp;

	if(verboseflag)
		printf("parsing options: %s\n", data);

	while(data != NULL) {
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
			char secret[1000];
			int len;

			while (*end && *end != ',')
				end++;
			fd = open(fn, O_RDONLY);
			if (fd < 0) {
				perror("unable to read secretfile");
				return -1;
			}
			len = read(fd, secret, 1000);
			if (len <= 0) {
				perror("unable to read secret from secretfile");
				return -1;
			}
			end = secret;
			while (end < secret + len && *end && *end != '\n' && *end != '\r')
				end++;
			*end = '\0';
			close(fd);

			//printf("read secret of len %d from %s\n", len, fn);
			data = "secret";
			value = secret;
			skip = 0;
		} else {
			skip = 0;
			/* printf("ceph: Unknown mount option %s\n",data); */
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
	}

	*optionsp = out;
	return 0;
}


int main(int argc, char *argv[])
{
	int i;
	char **new_argv;
	int flags = 0;
	int options_pos = 0;

	if (argc < 5)
		exit(1);

	new_argv = (char **)malloc(sizeof(char *)*argc);

	for (i=0; i<argc; i++) {
		new_argv[i] = argv[i];
		if (strcmp(new_argv[i], "-o") == 0) {
			options_pos = i+1;
			if (options_pos >= argc) {
				printf("usage error\n");
				exit(1);
			}		
		} else if (strcmp(new_argv[i], "-v") == 0) {
			verboseflag = 1;
		}
	}

	new_argv[1] = mount_resolve_dest(argv[1]);

	parse_options(&new_argv[options_pos], &flags);

	block_signals(SIG_BLOCK);

	if (mount(new_argv[1], new_argv[2], "ceph", flags, new_argv[options_pos])) {
		switch (errno) {
		case ENODEV:
			printf("mount error: ceph filesystem not supported by the system\n");
			break;
		default:
			printf("mount error %d = %s\n",errno,strerror(errno));
		}
	} else {
		update_mtab_entry(new_argv[1], new_argv[2], "ceph", new_argv[options_pos], flags, 0, 0);
	}

	block_signals(SIG_UNBLOCK);

	free(new_argv);	
	exit(0);
}

