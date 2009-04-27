#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <map>
#include <list>
#include <string>

#include "ConfUtils.h"
#include "dyn_snprintf.h"

using namespace std;

struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};

static const char *_def_delim=" \t\n\r";
static const char *_eq_delim="= \t\n\r";
static const char *_eq_nospace_delim="=\t\n\r";
static const char *_eol_delim="\n\r";
/* static const char *_pr_delim="[] \t\n\r"; */


static int is_delim(char c, const char *delim)
{
	while (*delim) {
		if (c==*delim)
			return 1;
		delim++;
	}

	return 0;
}

static char *get_next_tok(char *str, const char *delim, int alloc, char **p)
{
	int i=0;
	char *out;

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

static char *get_next_name(char *str, int alloc, char **p)
{
	int i=0;
	char *out;

	while (*str && is_delim(*str, _def_delim)) {
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
		while (str[i] && !is_delim(str[i], _eq_nospace_delim)) {
			i++;
		}

		i--;

		while (str[i] && is_delim(str[i], _def_delim)) {
			i--;
		}

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
#if 0
static char *str_trim(char *str)
{
	char *head = str;
	char *tail;

	while (*head && is_delim(*head, _def_delim))
		++head;

	head = strdup(head);

	if (!(*head))
		goto done;

	tail = &head[strlen(head)-1];

	while (*tail && is_delim(*tail, _def_delim))
		--tail;

	*(tail + 1) = '\0';

done:
	return head;
}
#endif

/*
 * normalizes a var name, removes extra spaces; e.g., 'foo  bar' -> 'foo bar'
 */
static char *normalize_name(const char *name)
{
	char *newname, *p;
	int i, len;
	int last_delim = 0, had_delim = 0, had_non_delim = 0;

	if (!name)
		return NULL;

	len = strlen(name);
	newname = (char *)malloc(len + 1);

	p = newname;

	for (i=0; i < len; i++) {
		int now_delim = is_delim(name[i], _def_delim);

		if (!now_delim) {

			if (had_delim && had_non_delim)
				*p++ = ' ';

			*p++ = name[i];

			had_non_delim = 1;
			had_delim = 0;
		} else {
			had_delim = 1;
		}

		last_delim = now_delim;
	}
	*p = '\0';

	return newname;
}

#define MAX_LINE 256

static char *get_next_delim(char *str, const char *delim, int alloc, char **p)
{
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

static int _parse_section(char *str, ConfLine *parsed)
{
	char *name = NULL;
	char *p;
	int ret = 0;
	char *line;
	size_t max_line = MAX_LINE;

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
	line = (char *)malloc(max_line);
	line[0] ='\0';

	do {
		if (name)
			free(name);
		name = get_next_tok(p, _def_delim, 1, &p);

		if (*name) {
			if (*line)
				dyn_snprintf(&line, &max_line, 2, "%s %s", line, name);
			else
				dyn_snprintf(&line, &max_line, 1, "%s", name);
		}
	} while (*name);
	
	if (*line)	
		parsed->set_section(line);

	free(line);

	return ret;
}

int parse_line(char *line, ConfLine *parsed)
{
	char *dup=strdup(line);
	char *p = NULL;
	char *eq;
	int ret = 0;

	memset(parsed, 0, sizeof(ConfLine));

	parsed->set_prefix(get_next_delim(dup, _def_delim, 1, &p));

	if (!*p)
		goto out;

	switch (*p) {
		case ';':
		case '#':
			parsed->set_suffix(p);
			goto out;
		case '[':
			parsed->set_suffix(p);
			return _parse_section(p, parsed);
	}

	parsed->set_var(get_next_name(p, 1, &p));
	if (!*p)
		goto out;

	parsed->set_mid(get_next_delim(p, _eq_delim, 1, &p));
	if (!*p)
		goto out;

	eq = get_next_tok(parsed->get_mid(), _def_delim, 0, NULL);
	if (*eq != '=') {
		goto out;
	}

	parsed->set_val(get_next_tok(p, _def_delim, 1, &p));
	if (!*p)
		goto out;

	ret = 1;

	parsed->set_suffix(p);
out:
	free(dup);
	return ret;
}

static int _str_cat(char *str1, int max, char *str2)
{
	int len = 0;

	if (max)
		len = snprintf(str1, max, "%s", str2);

	if (len < 0)
		len = 0;

	return len;
}

ConfSection::~ConfSection()
{
	ConfList::iterator conf_iter, conf_end;
	ConfLine *cl;

	conf_end = conf_list.end();

	for (conf_iter = conf_list.begin(); conf_iter != conf_end; ++conf_iter) {
		cl = *conf_iter;

		delete cl;
	}
}

void ConfLine::_set(char **dst, const char *val)
{
	if (*dst)
		free(*dst);

	if (!val)
		*dst = NULL;
	else
		*dst = strdup(val);
}

void ConfLine::set_prefix(const char *val)
{
	_set(&prefix, val);
}

void ConfLine::set_var(const char *val)
{
	_set(&var, val);

	if (norm_var) {
		free(norm_var);
		norm_var = NULL;
	}
}

void ConfLine::set_mid(const char *val)
{
	_set(&mid, val);
}

void ConfLine::set_val(const char *val)
{
	_set(&this->val, val);
}

void ConfLine::set_suffix(const char *val)
{
	_set(&suffix, val);
}

void ConfLine::set_section(const char *val)
{
	_set(&section, val);
}

char *ConfLine::get_norm_var()
{
	if (!norm_var)
		norm_var = normalize_name(var);

	return norm_var;
}


ConfLine::~ConfLine()
{
	if (prefix)
		free(prefix);
	if(var)
		free(var);
	if (mid)
		free(mid);
	if (val)
		free(val);
	if (suffix)
		free(suffix);
	if (section)
		free(section);
}

ConfFile::~ConfFile()
{
	SectionList::iterator sec_iter, sec_end;
	ConfSection *sec;

	free(filename);

	sec_end = sections_list.end();

	for (sec_iter = sections_list.begin(); sec_iter != sec_end; ++sec_iter) {
		sec = *sec_iter;

		delete sec;
	}
}

int ConfLine::output(char *line, int max_len)
{
	int len = 0;

	if (!max_len)
		return 0;

	line[0] = '\0';
	if (prefix)
		len += _str_cat(&line[len], max_len-len, prefix);
	if (var)
		len += _str_cat(&line[len], max_len-len, var);
	if (mid)
		len += _str_cat(&line[len], max_len-len, mid);
	if (val)
		len += _str_cat(&line[len], max_len-len, val);
	if (suffix)
		len += _str_cat(&line[len], max_len-len, suffix);

	return len;
}


void ConfFile::_dump(int fd)
{
	SectionList::iterator sec_iter, sec_end;
	 ConfLine *cl;
	char *line;
	size_t max_line = MAX_LINE;
	size_t len;
	char *p;

	line = (char *)malloc(max_line);

	sec_end=sections_list.end();

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
				do {
					if (len >= max_line) {
						max_line *= 2;
						free(line);
						line = (char *)malloc(max_line);
					}

					len = cl->output(line, max_line);
				} while (len == max_line);
				::write(fd, line, strlen(line));
				::write(fd, "\n", 1);
			}
		}
	}

	free(line);
}

void ConfFile::dump()
{
	SectionList::iterator sec_iter, sec_end;

	sec_end=sections_list.end();

	_dump(STDOUT_FILENO);
}

ConfSection *ConfFile::_add_section(const char *section, ConfLine *cl)
{
	ConfSection *sec;
#define BUF_SIZE 4096
	char *buf;

	buf = (char *)malloc(BUF_SIZE);

	sec = new ConfSection(section);
	sections[section] = sec;
	sections_list.push_back(sec);

	if (!cl) {
		cl = new ConfLine();
		snprintf(buf, BUF_SIZE, "[%s]", section);
		cl->set_prefix(buf);
	}
	sec->conf_list.push_back(cl);

	free(buf);

	return sec;
}


bool ConfFile::_parse(char *filename, ConfSection **psection)
{
	char *buf;
	int len, i, l;
	char *line;
	ConfLine *cl;
	ConfSection *section = *psection;
	int fd;
	int max_line = MAX_LINE;

	line = (char *)malloc(max_line);

	

	fd = open(filename, O_RDONLY);
	if (fd < 0)
		return 0;

	l = 0;

	buf = (char *)malloc(BUF_SIZE);
	do {
		len = ::read(fd, buf, BUF_SIZE);

		for (i=0; i<len; i++) {
			switch (buf[i]) {
			case '\r' :
				continue;
			case '\n' :
				if (l > 0 && line[l-1] == '\\')
					continue;
				line[l] = '\0';
				cl = new ConfLine();
				parse_line(line, cl);
				if (cl->get_var()) {
					if (strcmp(cl->get_var(), "include") == 0) {
						if (!_parse(cl->get_val(), &section)) {
							printf("error parsing %s\n", cl->get_val());
						}
					} else {
						char *norm_var = cl->get_norm_var();
						section->conf_map[norm_var] = cl;
						free(norm_var);
						global_list.push_back(cl);
						section->conf_list.push_back(cl);
					}
				} else if (cl->get_section()) {
					section = _add_section(cl->get_section(), cl);
				} else {
					delete cl;
				}
				l = 0;
				break;
			default:
				line[l++] = buf[i];

				if (l == max_line-1) {
					max_line *= 2;
					line = (char *)realloc(line, max_line);
				}
			}
		}
	} while (len);

	free(buf);

	*psection = section;

	free(line);

	return 1;
}

bool ConfFile::parse()
{
	ConfSection *section;

	section = new ConfSection("global");
	sections["global"] = section;
	sections_list.push_back(section);

	return _parse(filename, &section);
}

int ConfFile::flush()
{
	int rc;
	int fd;

	fd = open(filename, O_RDWR | O_CREAT, 0644);

	if (fd < 0) {
		printf("error opening file %s errno=%d\n", filename, errno);
		return 0;
	}

	_dump(fd);
	rc = close(fd);

	if (rc < 0)
		return 0;

	return 1;
}

ConfLine *ConfFile::_find_var(const char *section, const char* var)
{
	SectionMap::iterator iter = sections.find(section);
	ConfSection *sec;
	ConfMap::iterator cm_iter;
	ConfLine *cl;
	char *norm_var;

	if (iter == sections.end() )
		goto notfound;

	sec = iter->second;
	norm_var = normalize_name(var);

	cm_iter = sec->conf_map.find(norm_var);
	free(norm_var);

	if (cm_iter == sec->conf_map.end())
		goto notfound;

	cl = cm_iter->second;

	return cl;
notfound:
	return 0;
}

ConfLine *ConfFile::_add_var(const char *section, const char* var)
{
	SectionMap::iterator iter = sections.find(section);
	ConfSection *sec;
	ConfMap::iterator cm_iter;
	ConfLine *cl;

	if (iter == sections.end() ) {
		sec = _add_section(section, NULL);
	} else {
		sec = iter->second;
	}

	cl = new ConfLine();

	cl->set_prefix("\t");
	cl->set_var(var);
	cl->set_mid(" = ");

	sec->conf_map[var] = cl;
	sec->conf_list.push_back(cl);

	return cl;
}

template<typename T>
static void _conf_copy(T *dst_val, T def_val)
{
	*dst_val = def_val;
}

template<>
void _conf_copy<char *>(char **dst_val, char *def_val)
{
	if (def_val)
		*dst_val = strdup(def_val);
	else
		*dst_val = NULL;
}

static void _conf_decode(int *dst_val, char *str_val)
{
	*dst_val = atoi(str_val);
}

static void _conf_decode(unsigned int *dst_val, char *str_val)
{
	*dst_val = strtoul(str_val, NULL, 0);
}

static void _conf_decode(unsigned long long *dst_val, char *str_val)
{
	*dst_val = strtoull(str_val, NULL, 0);
}
static void _conf_decode(long long *dst_val, char *str_val)
{
	*dst_val = strtoll(str_val, NULL, 0);
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
		for (len = len-1; len > 0; len--) {
			if (str_val[len] == '"')
				break;
		}
	}

	*dst_val = (char *)malloc(len + 1);
	strncpy(*dst_val, str_val, len);
	(*dst_val)[len] = '\0';
}

static int _conf_decode(float *dst_val, char *str_val)
{
	*dst_val = atof(str_val);

	return 1;
}

static int _conf_decode(double *dst_val, char *str_val)
{
	*dst_val = atof(str_val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, int val)
{
	snprintf(dst_str, len, "%d", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, unsigned int val)
{
	snprintf(dst_str, len, "%u", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, long long val)
{
	snprintf(dst_str, len, "%lld", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, unsigned long long val)
{
	snprintf(dst_str, len, "%llu", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, float val)
{
	snprintf(dst_str, len, "%g", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, double val)
{
	snprintf(dst_str, len, "%lg", val);

	return 1;
}

static int _conf_encode(char *dst_str, int len, bool val)
{
	snprintf(dst_str, len, "%s", (val ? "true" : "false"));

	return 1;
}

static int _conf_encode(char *dst_str, int max_len, char *val)
{
	int have_delim = 0;
	int i;
	int len;

	if (!val)
		return 0;

	len = strlen(val);

	for (i=0; i<len; i++) {
		if (is_delim(val[i], _def_delim)) {
			have_delim = 1;
			break;
		}
	}

	if (have_delim)
		snprintf(dst_str, max_len, "\"%s\"", val);
	else
		snprintf(dst_str, max_len, "%s", val);

	return 1;
}

template<typename T>
int ConfFile::_read(const char *section, const char *var, T *val, T def_val)
{
	ConfLine *cl;
	char *str_val;

	cl = _find_var(section, var);
	if (!cl || !cl->get_val())
		goto notfound;

	str_val = cl->get_val();

	if (post_process_func) {
		str_val = post_process_func(str_val);
	}

	_conf_decode(val, str_val);

	return 1;
notfound:
	_conf_copy<T>(val, def_val);

	if (auto_update)
		_write<T>(section, var, def_val);

	return 0;
}

template<typename T>
int ConfFile::_write(const char *section, const char *var, T val)
{
	ConfLine *cl;
	char line[MAX_LINE];

	if (!_conf_encode(line, MAX_LINE, val))
		return 0;

	cl = _find_var(section, var);
	if (!cl)
		cl = _add_var(section, var);

	cl->set_val(line);
	
	return 1;
}

int ConfFile::read(const char *section, const char *var, int *val, int def_val)
{
	return _read(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, unsigned int *val, unsigned int def_val)
{
	return _read<unsigned int>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, long long *val, 
		   long long def_val)
{
	return _read<long long>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, unsigned long long *val, 
		   unsigned long long def_val)
{
	return _read<unsigned long long>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, bool *val, bool def_val)
{
	return _read<bool>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, char **val, const char *def_val)
{
	return _read<char *>(section, var, val, (char *)def_val);
}

int ConfFile::read(const char *section, const char *var, float *val, float def_val)
{
	return _read<float>(section, var, val, def_val);
}

int ConfFile::read(const char *section, const char *var, double *val, double def_val)
{
	return _read<double>(section, var, val, def_val);
}

int ConfFile::write(const char *section, const char *var, int val)
{
	return _write<int>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, unsigned int val)
{
	return _write<unsigned int>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, long long val)
{
	return _write<long long>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, unsigned long long val)
{
	return _write<unsigned long long>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, bool val)
{
	return _write<bool>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, float val)
{
	return _write<float>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, double val)
{
	return _write<double>(section, var, val);
}

int ConfFile::write(const char *section, const char *var, char *val)
{
	return _write<char *>(section, var, val);
}
#if 0
void parse_test(char *line)
{
	ConfLine cl;
	int rc;

	rc = parse_line(line, &cl);
	printf("ret=%d\n", rc);	
	printf("pre: '%s'\n", cl.get_prefix());
	printf("var: '%s'\n", cl.get_var());
	printf("mid: '%s'\n", cl.get_mid());
	printf("val: '%s'\n", cl.get_val());
	printf("suf: '%s'\n", cl.get_suffix());
	printf("section: '%s'\n", cl.get_section());
}

int main(int argc, char *argv[])
{
	ConfFile cf(argv[1]);
	int val;
	char *str_val;
	float fval;
	bool bval;
	char *s;


	s = "  hello    world=   kaka  ";

	printf("s'%s'\n", s);
	s = get_next_name(s, 1, NULL);

	printf("str_trim='%s'\n", s);

	cf.parse();
	cf.dump();



	cf.set_auto_update(true);

	cf.read("core", "repositoryformatversion", &val, 12);
	cf.read("foo", "lala1", &val, 10);
	cf.read("foo", "lala2", &val, 11);
	cf.read("foo", "lala3", &val, 12);
	cf.read("foo2", "lala4", &val, 13);
	cf.read("foo", "lala5", &val, 14);
	cf.read("foo", "lala6", &fval, 14.2);
	cf.read("foo", "lala7", &bval, false);

	cf.read("foo", "str", &str_val, "hello world2");
	cf.read("foo", "  two  words   are  good  ", &str_val, "hello world5");

	printf("read str=%s\n", str_val);
	printf("read bool=%d\n", bval);
	printf("read float=%f\n", fval);
	cf.dump();
	cf.flush();

	printf("read val=%d\n", val);

	return 0;
}
#endif
