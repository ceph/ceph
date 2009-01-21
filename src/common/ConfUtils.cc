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

	while (*str && is_delim(*str, delim)) {
		str++;
	}

	while (str[i] && !is_delim(str[i], delim)) {
		i++;
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
#if 0
	std::list<struct conf_line *>::iterator iter, end;
	struct conf_line *cl;
	char line[MAX_LINE];
	int len = 0;
	char *p;
	

	end=list.end();

	for (iter=list.begin(); iter != end; ++iter) {
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
			printf("line=%s\n", line);
		}
	}
#endif
	ConfMap *cur_map;
	ConfMap::iterator map_iter, map_end;
	SectionMap::iterator sec_iter, sec_end;

	sec_end = sections.end();

	for (sec_iter = sections.begin(); sec_iter != sec_end; ++sec_iter) {
		cur_map = sec_iter->second;
		map_end = cur_map->end();

		for (map_iter = cur_map->begin(); map_iter != map_end; ++map_iter) {
			
		}
	}

}

int ConfFile::parse()
{
	char *buf;
	int len, i, l;
	char line[MAX_LINE];
	struct conf_line *cl;
	ConfMap *cur_map;

	cur_map = new ConfMap;
	sections["global"] = cur_map;
#define BUF_SIZE 4096
	fd = open(filename, O_RDWR);
	if (fd < 0)
		return 0;

	l = 0;
	buf = (char *)malloc(BUF_SIZE);
	do {
		len = read(fd, buf, BUF_SIZE);

		for (i=0; i<len; i++) {
			switch (buf[i]) {
			case '\r' :
				continue;
			case '\n' :
				line[l] = '\0';
				cl = (struct conf_line *)malloc(sizeof(struct conf_line));
				parse_line(line, cl);
				list.push_back(cl);
				if (cl->var) {
					(*cur_map)[cl->var] = cl;
					printf("cl->var <---- '%s'\n", cl->var);
				} else if (cl->section) {
					printf("cur_map <---- '%s'\n", cl->section);
					cur_map = new ConfMap;
					sections[cl->section] = cur_map;
				}
				l = 0;
				break;
			default:
				line[l++] = buf[i];
			}
		}
	} while (len);

	return 1;
}

struct conf_line *ConfFile::_find_var(char *section, char* var)
{
	std::map<string, ConfMap *>::iterator iter = sections.find(section);
	ConfMap *map;
	ConfMap::iterator cm_iter;
	struct conf_line *cl;

	if (iter == sections.end() )
		goto notfound;

	map = iter->second;
	cm_iter = map->find(var);

	if (cm_iter == map->end())
		goto notfound;

	cl = cm_iter->second;

	return cl;
notfound:
	return 0;
}

int ConfFile::read_int(char *section, char *var, int *val, int def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	*val = atoi(cl->val);
	
	return 1;
notfound:
	*val = def_val;
	return 0;
}

int ConfFile::write_int(char *section, char *var, int val)
{
	struct conf_line *cl;
	char line[MAX_LINE];

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	free(cl->val);

	sprintf(line, "%d", val);
	cl->val = strdup(line);
	
	
	return 1;
notfound:
	return 0;
}

int ConfFile::read_bool(char *section, char *var, bool *val, bool def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	if (strcmp(cl->val, "true")==0) {
		*val = true;
	} else if (strcmp(cl->val, "false")==0) {
		*val = false;
	} else {
		*val = atoi(cl->val);
	}
	
	return 1;
notfound:
	*val = def_val;
	return 0;
}

int ConfFile::read_float(char *section, char *var, float *val, float def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	*val = atof(cl->val);
	
	return 1;
notfound:
	*val = def_val;
	return 0;
}

int ConfFile::read_str(char *section, char *var, char *val, int size, char *def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	strncpy(val, cl->val, size);
	
	return 1;
notfound:
	strncpy(val, def_val, size);
	return 0;
}

int ConfFile::read_str_alloc(char *section, char *var, char **val, char *def_val)
{
	struct conf_line *cl;

	cl = _find_var(section, var);
	if (!cl || !cl->val)
		goto notfound;

	*val = strdup(cl->val);

	return 1;
notfound:
	*val = strdup(def_val);
	return 0;
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
	cf.read_int("core", "repositoryformatversion", &val, 12);

	printf("read val=%d\n", val);

	return 0;
}
