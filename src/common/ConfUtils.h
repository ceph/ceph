#ifndef __CONFUTILS_H
#define __CONFUTILS_H


#include <string.h>
#include <map>
#include <string>
#include <list>

typedef std::map<std::string, struct conf_line *> ConfMap;
typedef std::list<struct conf_line *> ConfList;

class ConfFile;

class ConfSection
{
	friend class ConfFile;

	std::string name;
	ConfList conf_list;
	ConfMap conf_map;
public:
	ConfSection(std::string sec_name) : name(sec_name) { }
};

typedef std::map<std::string, ConfSection *> SectionMap;
typedef std::list<ConfSection *> SectionList;

class ConfFile {
	int fd;
	char *filename;

	SectionMap sections;
	SectionList sections_list;
	ConfList global_list;

	struct conf_line *_find_var(char *section, char* var);
	struct conf_line *_add_var(char *section, char* var);
public:
	ConfFile(char *fname) : filename(strdup(fname)) {}
	~ConfFile() { free(filename); }

	int parse();
	int read_int(char *section, char *var, int *val, int def_val);
	int read_bool(char *section, char *var, bool *val, bool def_val);
	int read_str(char *section, char *var, char *val, int size, char *def_val);
	int read_str_alloc(char *section, char *var, char **val, char *def_val);
	int read_float(char *section, char *var, float *val, float def_val);

	int write_int(char *section, char *var, int val);

	void dump();
};

#endif
