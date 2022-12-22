/*
 * file:        config.cc
 * description: quick and dirty config file parser
 *              env var overrides modeled on github.com/spf13/viper
 * 
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <stdlib.h>
#include <ctype.h>
#include <uuid/uuid.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <filesystem>
namespace fs = std::filesystem;

#include "config.h"

std::vector<std::string> cfg_path(
    {"lsvd.conf", "/usr/local/etc/lsvd.conf"});

static void split(std::string s, std::vector<std::string> &words) {
    std::string w = "";
    for (auto c : s) {
	if (!isspace(c))
	    w = w + c;
	else {
	    words.push_back(w);
	    w = "";
	}
    }
    if (w.size() > 0)
	words.push_back(w);
}

static std::map<std::string,cfg_backend> m = {{"file", BACKEND_FILE},
					      {"rados", BACKEND_RADOS}};


/* fancy-ass macros to parse config file lines. 
 *   config keyword = field name
 *   environment var = LSVD_ + uppercase(field name)
 *   skips blank lines and lines that don't match a keyword
 */
#include "config_macros.h"

int lsvd_config::read() {
    auto explicit_cfg = getenv("LSVD_CONFIG_FILE");
    if (explicit_cfg) {
	std::string f(explicit_cfg);
	cfg_path.insert(cfg_path.begin(), f);
    }
    for (auto f : cfg_path) {
	std::ifstream fp(f);
	if (!fp.is_open())
	    continue;
	std::string line;
	while (getline(fp, line)) {
	    if (line[0] == '#')
		continue;
	    std::vector<std::string> words;
	    split(line, words);
	    if (words.size() != 2)
		continue;
	    F_CONFIG_H_INT(words[0], words[1], batch_size);
	    F_CONFIG_INT(words[0], words[1], wcache_batch);
	    F_CONFIG_H_INT(words[0], words[1], wcache_chunk);
	    F_CONFIG_STR(words[0], words[1], cache_dir);
	    F_CONFIG_INT(words[0], words[1], xlate_window);
	    F_CONFIG_TABLE(words[0], words[1], backend, m);
	    F_CONFIG_H_INT(words[0], words[1], cache_size);
	    F_CONFIG_INT(words[0], words[1], hard_sync);
	    F_CONFIG_INT(words[0], words[1], ckpt_interval);
	    F_CONFIG_INT(words[0], words[1], flush_msec);
	    F_CONFIG_INT(words[0], words[1], gc_threshold);
	}
	fp.close();
	break;
    }
    
    ENV_CONFIG_H_INT(batch_size);
    ENV_CONFIG_INT(wcache_batch);
    ENV_CONFIG_H_INT(wcache_chunk);
    ENV_CONFIG_STR(cache_dir);
    ENV_CONFIG_INT(xlate_window);
    ENV_CONFIG_TABLE(backend, m);
    ENV_CONFIG_H_INT(cache_size);
    ENV_CONFIG_INT(hard_sync);
    ENV_CONFIG_INT(ckpt_interval);
    ENV_CONFIG_INT(flush_msec);
    ENV_CONFIG_INT(gc_threshold);

    return 0;			// success
}

std::string lsvd_config::cache_filename(uuid_t &uuid, const char *name) {
    char buf[256]; // PATH_MAX
    std::string file(name);
    file = fs::path(file).filename();
    
    sprintf(buf, "%s/%s.cache", cache_dir.c_str(), file.c_str());
    if (access(buf, R_OK|W_OK) == 0)
	return std::string((const char*)buf);

    char uuid_s[64];
    uuid_unparse(uuid, uuid_s);
    sprintf(buf, "%s/%s.cache", cache_dir.c_str(), uuid_s);
    return std::string((const char*)buf);
}

#if 0
int main(int argc, char **argv) {
    auto cfg = new lsvd_config;
    cfg->read();

    printf("batch: %d\n", cfg->batch_size); // h_int
    printf("wc batch %d\n", cfg->wcache_batch); // int
    printf("cache: %s\n", cfg->cache_dir.c_str()); // str
    printf("backend: %d\n", (int)cfg->backend);	   // table

    uuid_t uu;
    std::cout << cfg->cache_filename(uu, "foobar") << "\n";
}
#endif
