#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <errno.h>
#include <sys/mount.h>

#define BUF_SIZE 128

int verboseflag = 0;

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
		struct hostent *ent;
		char addr[16];

		port_str = strchr(tok, ':');
		*port_str = 0;
		port_str++;
		if (!*port_str)
			port_str = NULL;

		ent = gethostbyname(tok);

		if (!ent) {
			printf("server name not found: %s\n", tok);
			free(new_str);
			return 0;
		}

		snprintf(addr, sizeof(addr), "%u.%u.%u.%u", 
			(unsigned char)ent->h_addr[0], 
			(unsigned char)ent->h_addr[1], 
			(unsigned char)ent->h_addr[2], 
			(unsigned char)ent->h_addr[3]);

		pos = safe_cat(&new_str, &len, pos, addr);

		if (port_str) {
			pos = safe_cat(&new_str, &len, pos, ":");
			pos = safe_cat(&new_str, &len, pos, port_str);
		}

		tok = strtok(NULL, ",");
		if (tok)
			pos = safe_cat(&new_str, &len, pos, ",");

	}

	pos = safe_cat(&new_str, &len, pos, ":");
	pos = safe_cat(&new_str, &len, pos, mount_path);

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
	
		/* temporarily null terminate end of keyword=value pair */
		if(next_keyword)
			*next_keyword++ = 0;

		/* temporarily null terminate keyword to make keyword and value distinct */
		if ((value = strchr(data, '=')) != NULL) {
			*value = '\0';
			value++;
		}

		skip = 1;

		if (strncmp(data, "nosuid", 6) == 0) {
			*filesys_flags |= MS_NOSUID;
		} else if (strncmp(data, "suid", 4) == 0) {
			*filesys_flags &= ~MS_NOSUID;
		} else if (strncmp(data, "nodev", 5) == 0) {
			*filesys_flags |= MS_NODEV;
		} else if ((strncmp(data, "nobrl", 5) == 0) || 
			   (strncmp(data, "nolock", 6) == 0)) {
			*filesys_flags &= ~MS_MANDLOCK;
		} else if (strncmp(data, "dev", 3) == 0) {
			*filesys_flags &= ~MS_NODEV;
		} else if (strncmp(data, "noexec", 6) == 0) {
			*filesys_flags |= MS_NOEXEC;
		} else if (strncmp(data, "exec", 4) == 0) {
			*filesys_flags &= ~MS_NOEXEC;
		} else if (strncmp(data, "ro", 2) == 0) {
			*filesys_flags |= MS_RDONLY;
		} else if (strncmp(data, "rw", 2) == 0) {
			*filesys_flags &= ~MS_RDONLY;
                } else if (strncmp(data, "remount", 7) == 0) {
                        *filesys_flags |= MS_REMOUNT;
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

	if (mount(new_argv[1], new_argv[2], "ceph", flags, new_argv[options_pos])) {
		switch (errno) {
		case ENODEV:
			printf("mount error: ceph filesystem not supported by the system\n");
			break;
		default:
			printf("mount error %d = %s\n",errno,strerror(errno));
		}
	}

	free(new_argv);	
	exit(0);
}

