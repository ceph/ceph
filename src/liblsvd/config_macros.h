/*
 * file:        config_macros.h
 * description: really simple config file / env var parsing
 *
 * config file keyword = variable name
 * environment variable = LSVD_(uppercase keyword)
 * handles four types of values:
 *   - string
 *   - int
 *   - human-readable int (e.g. 10m, 20G)
 *   - table (actually std::map<char,...>) lookup
 *
 * to use: 
 *   F_CONFIG_XXX(input, arg, name) - if input==name, set name=arg
 *   ENV_CONFIG_XXX(name) - if LSVD_<name> is set, set name=<val>
 *
 */

#include <algorithm>
#include <cctype>

static long parseint(const char *_s)
{
    char *s = (char*)_s;
    long val = strtol(s, &s, 0);
    if (toupper(*s) == 'G')
        val *= (1024*1024*1024);
    if (toupper(*s) == 'M')
        val *= (1024*1024);
    if (toupper(*s) == 'K')
        val *= 1024;
    return val;
}

static long parseint(std::string &s)
{
    return parseint(s.c_str());
}

#define CONFIG_HDR(name)    \
    const char *val = NULL; \
    std::string env = "LSVD_" #name; \
    std::transform(env.begin(), env.end(), env.begin(), \
		   [](unsigned char c){ return std::toupper(c);});

#define F_CONFIG_STR(input, arg, name) { \
    if (input == #name) \
	name = arg; \
    }

#define ENV_CONFIG_STR(name) { \
    CONFIG_HDR(name) \
    if ((val = getenv(env.c_str()))) \
	name = std::string(val); \
    }

#define F_CONFIG_INT(input, arg, name) { \
    if (input == #name) \
	name = atoi(arg.c_str()); \
    }

#define ENV_CONFIG_INT(name) { \
    CONFIG_HDR(name) \
    if ((val = getenv(env.c_str()))) \
	name = atoi(val); \
    }

#define F_CONFIG_H_INT(input, arg, name) { \
    if (input == #name) \
	name = parseint(arg); \
    }

#define ENV_CONFIG_H_INT(name) { \
    CONFIG_HDR(name) \
    if ((val = getenv(env.c_str()))) \
	name = parseint(val); \
    }

#define F_CONFIG_TABLE(input, arg, name, table) { \
    if (input == #name) \
	name = table[arg]; \
    }

#define ENV_CONFIG_TABLE(name, table) { \
    CONFIG_HDR(name) \
    if ((val = getenv(env.c_str()))) \
	name = table[std::string(val)]; \
    }
