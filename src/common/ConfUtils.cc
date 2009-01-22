#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <map>
#include <list>
#include <string>

#include "ConfUtils.h"

using namespace std;

struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};


struct conf_line {
	char *prefix;
	char *var;
	char *mid;
	char *val;
	char *suffix;
	char *section;
};

static const char *_def_delim=" \t\n\r";
static const char *_eq_delim="= \t\n\r";
static const char *_eol_delim="\n\r";
static const char *_pr_delim="[] \t\n\r";


static int is_delim(char c, const char *delim)
{
	while (*delim) {
		if (c==*delim)
			return 1;
		delim++;
	}

	return 0;
}

char *get_next_tok(char *str, const char *delim, int alloc, char **p)
{
	char *tok;
	int i=0;
	char *out;
	int is_str = 0;

	while (*str && is_delim(*str, delim)) {
		str++;
	}

	if (*str == '"') {
		while (str[i] && !is_delim(str[i], _eol_delim)) {
			i++;
			if (str[i] == '"') {
				i++;
				break;
			}
		}		
	} else {
		while (str[i] && !is_delim(str[i], delim)) {
			i++;
		}
	}

	if (alloc) {
		out = (char *)malloc(i+1);
		memcpy(out, str, i);
		out[i] = '\0';
	} else {
		out = str;
	}

	if (p)
		*p = &str[i];

	return out;
}

#define MAX_LINE 256

char *get_next_delim(char *str, const char *delim, int alloc, char **p)
{
	char *tok;
	int i=0;
	char *out;

	while (str[i] && is_delim(str[i], delim)) {
		i++;
	}

	if (alloc) {
		out = (char *)malloc(i+1);
		memcpy(out, str, i);
		out[i] = '\0';
	} else {
		out = str;
	}

	*p = &str[i];

	return out;
}

static int _parse_section(char *str, struct conf_line *parsed)
{
	char *open, *close;
	char *name = NULL;
	char *p;
	int ret = 0;
	char line[MAX_LINE];

	char *start, *end;

	start = index(str, '[');
	end = strchr(str, ']');	

	if (!start || !end)
		return 0;

	start++;
	*end = '\0';

	if (end <= start)
		return 0;
	

	p = start;
	line[0] ='\0';

	do {
		if (name)
			free(name);
		name = get_next_tok(p, _def_delim, 1, &p);

		printf("name='%s' line='%s'\n", name, line);

		if (*name) {
			if (*line)
				snprintf(line, MAX_LINE, "%s %s", line, name);
			else
				snprintf(line, MAX_LINE, "%s", name);
		}
	} while (*name);
	
	if (*line)	
		parsed->section = strdup(line);

out:
	return ret;
}

int parse_line(char *line, struct conf_line *parsed)
{
	char *dup=strdup(line);
	char *p = NULL;
	char *tok;
	int i;
	char *eq;
	int ret = 0;

	memset(parsed, 0, sizeof(struct conf_line));

	parsed->prefix = get_next_delim(dup, _def_delim, 1, &p);

	if (!*p)
		goto out;

	switch (*p) {
		case '#':
			parsed->suffix = strdup(p);
			goto out;
		case '[':
			parsed->suffix = strdup(p);
			return _parse_section(p, parsed);
	}

	parsed->var = get_next_tok(p, _def_delim, 1, &p);
	if (!*p)
		goto out;

	parsed->mid = get_next_delim(p, _eq_delim, 1, &p);
	if (!*p)
		goto out;

	eq =  get_next_tok(parsed->mid, _def_delim, 0, NULL);
	if (*eq != '=') {
		goto out;
	}

	parsed->val = get_next_tok(p, _def_delim, 1, &p);
	if (!*p)
		goto out;

	ret = 1;

	parsed->suffix = strdup(p);
out:
	free(dup);
	return ret;
}

static int _str_cat(char *str1, int max, char *str2)
{
	int len;

	if (max)
		len = snprintf(str1, max, "%s", str2);

	if (len < 0)
		len = 0;

	return len;
}

void ConfFile::dump()
{
	SectionList::iterator sec_iter, sec_end;
	 struct conf_line *cl;
	char line[MAX_LINE];
	int len = 0;
	char *p;
	

	sec_end=sections_list.end();

	printf("------ config starts here ------\n");

	for (sec_iter=sections_list.begin(); sec_iter != sec_end; ++sec_iter) {
		ConfList::iterator iter, end;
		ConfSection *sec;
		sec = *sec_iter;

		end = sec->conf_list.end();

		for (iter = sec->conf_list.begin(); iter != end; ++iter) {
			cl = *iter;
			p = line;
			len = 0;

			if (cl) {
				line[0] = '\0';
				if (cl->prefix)
					len += _str_cat(&line[len], MAX_LINE-len, cl->prefix);
				if (cl->var)
					len += _str_cat(&line[len], MAX_LINE-len, cl->var);
				if (cl->mid)
					len += _str_cat(&line[len], MAX_LINE-len, cl->mid);
				if (cl->val)
					len += _str_cat(&line[len], MAX_LINE-len, cl->val);
				if (cl->suffix)
					len += _str_cat(&line[len], MAX_LINE-len, cl->suffix);
				printf("%s\n", line);
			}
		}
	}
	printf("------  config ends here  ------\n");
}

int ConfFile::parse()
{
	char *buf;
	int len, i, l, map_index;
	char line[MAX_LINE];
	struct conf_line *cl;
	ConfSection *section;

	section = new ConfSection("global");
	sections["global"] = section;
	sections_list.push_back(section);
#define BUF_SIZE 4096
	fd = open(filename, O_RDWR);
	if (fd < 0)
		return 0;

	l = 0;
	map_index = 0;

	buf = (char *)malloc(BUF_SIZE);
	do {
		len = ::read(fd, buf, BUF_SIZE);

		for (i=0; i<len; i++) {
			switch (buf[i]) {
			case '\r' :
				continue;
			case '\n' :
				line[l] = '\0';
				cl = (struct conf_line *)malloc(sizeof(struct conf_line));
				parse_line(line, cl);
				if (cl->var) {
					section->conf_map[cl->var] = cl;
					printf("cl->var <---- '%s'\n", cl->var);
				} else if (cl->section) {
					printf("cur_map <---- '%s'\n", cl->section);
					map_index = 0;
					section = new ConfSection(cl->section);
					sections[cl->section] = section;
					sections_list.push_back(section);
				}
				global_list.push_back(cl);
				section->conf_list.push_back(cl);
				l = 0;
				break;
			default:
				line[l++] = buf[i];
			}
		}
	} while (len);

	return 1;
}

struct conf_line *ConfFile::_find_var(const char *section, const char* var)
{
	SectionMap::iterator iter = sections.find(section);
	ConfSection *sec;
	ConfMap::iterator cm_iter;
	struct conf_line *cl;

	if (iter == sections.end() )
		goto notfound;

	sec = iter->second;
	cm_iter = sec->conf_map.find(var);

	if (cm_iter == sec->conf_map.end())
		goto notfound;

	cl = cm_iter->second;

	return cl;
notfound:
	return 0;
}

struct conf_line *ConfFile::_add_var(const char *section, const char* var)
{
	SectionMap::iterator iter = sections.find(section);
	ConfSection *sec;
	ConfMap::iterator cm_iter;
	struct conf_line *cl;
	char buf[128];

	if (iter == sections.end() ) {
		sec = new ConfSection(section);
		sections[section] = sec;
		sections_list.push_back(sec);

		cl = (struct conf_line *)malloc(sizeof(struct conf_line));
		memset(cl, 0, sizeof(struct conf_line));
		snprintf(buf, sizeof(buf), "[%s]", section);
		cl->prefix = strdup(buf);
		sec->conf_list.push_back(cl);
	} else {
		sec = iter->second;
	}

	cl = (struct conf_line *)malloc(sizeof(struct conf_line));
	memset(cl, 0, sizeof(struct conf_line));

	cl->prefix = strdup("\t");
	cl->var = strdup(var);
	cl->mid = strdup(" = ");

	sec->conf_map[var] = cl;
	sec->conf_list.push_back(cl);

	return cl;
}

template<typename T>
static void _conf_copy(T *dst_val, T def_val)
{
	*dst_val = def_val;
}

static void _conf_copy(char **dst_val, char *def_val)
{
	*dst_val = strdup(def_val);
}


static void _conf_decode(int *dst_val, char *str_val)
{
	*dst_val = atoi(str_val);
}

static void _conf_decode(bool *dst_val, char *str_val)
{
	if (strcasecmp(str_val, "true")==0) {
		*dst_val = true;
	} else if (strcasecmp(str_val, "false")==0) {
		*dst_val = false;
	} else {
		*dst_val = atoi(str_val);
	}
}

static void _conf_decode(char **dst_val, char *str_val)
{
	int len;

	len = strlen(str_val);

	if (*str_val == '"') {
		str_val++;
		for (len-1; len > 0; len--) {
			if (str_val[len] == '"')
				break;
		}
	}

	*dst_val = (char *)malloc(len + 1);
	strncpy(*dst_val, str_val, len);
}

static void _conf_decode(float *dst_val, char *str_val)
{
	*dst_val = atof(str_val);
}

static void _conf_encode(char *dst_str, int len, int val)
{
	snprintf(dst_str, len, "%d", val);
}

static void _conf_encode(char *dst_str, int len, float val)
{
	snprintf(dst_str, len, "%f", val);
}

static void _conf_encode(char *dst_str, int len, bool val)
{
	snprintf(dst_str, len, "%s", (val ? "true" : "false"));
}

static void _conf_encode(char *dst_str, int len, char *val)
{
	int have_delim = 0;
	int i;
	len = strlen(val);

	for (i=0; i<len; i++) {
		if (is_delim(val[i], _def_delim)) {
			have_delim = 1;
			break;
		}
	}

	if (have_delim)
		snprintf(dst_str, len, """%s""", val);
	else
		snprintf(dst_str, len, "%s", val);

	return;
}

template<typename T>
int ConfFile::_read(const char *section, const char *var, T *val, T def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	_conf_decode(val, cl->val);

	return 1;
notfound:
	_conf_copy<T>(val, def_val);
	return 0;
}

template<typename T>
int ConfFile::_write(const char *section, const char *var, T val)
{
	struct conf_line *cl;
	char line[MAX_LINE];

	cl = _find_var(section, var);
	if (!cl)
		cl = _add_var(section, var);

	if (cl->val)
		free(cl->val);

	_conf_encode(line, MAX_LINE, val);
	cl->val = strdup(line);
	
	return 1;
}

int ConfFile::read(const char *section, const char *var, int *val, int def_val)
{
	return _read<int>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, bool *val, bool def_val)
{
	return _read<bool>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, char **val, char *def_val)
{
	return _read<char *>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, float *val, float def_val)
{
	return _read<float>(section, var, val, def_val);
}

int ConfFile::write(const char *section, const char *var, int val)
{
	return _write<int>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, bool val)
{
	return _write<bool>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, float val)
{
	return _write<float>(section, var, val);
}


int ConfFile::write(const char *section, const char *var, char *val)
{
	return _write<char *>(section, var, val);
}

void parse_test(char *line)
{
	struct conf_line cl;
	int rc;

	rc = parse_line(line, &cl);
	printf("ret=%d\n", rc);	
	printf("pre: '%s'\n", cl.prefix);
	printf("var: '%s'\n", cl.var);
	printf("mid: '%s'\n", cl.mid);
	printf("val: '%s'\n", cl.val);
	printf("suf: '%s'\n", cl.suffix);
	printf("section: '%s'\n", cl.section);

}

int main(int argc, char *argv[])
{
	ConfFile cf(argv[1]);
	int val;
	cf.parse();
	cf.dump();
	cf.read("core", "repositoryformatversion", &val, 12);
	cf.write("core", "lola", 15);
	cf.write("zore", "lola", 15);
	cf.dump();

	printf("read val=%d\n", val);

	return 0;
}
