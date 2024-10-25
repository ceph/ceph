// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#ifdef _WIN32
#include "include/compat.h" // for cpu_set_t
#endif

#include <sched.h>
#include <cstddef> // for size_t
#include <set>
#include <string>

int parse_cpu_set_list(const char *s,
		       size_t *cpu_set_size,
		       cpu_set_t *cpu_set);
std::string cpu_set_to_str_list(size_t cpu_set_size,
				const cpu_set_t *cpu_set);
std::set<int> cpu_set_to_set(size_t cpu_set_size,
			     const cpu_set_t *cpu_set);

int get_numa_node_cpu_set(int node,
			  size_t *cpu_set_size,
			  cpu_set_t *cpu_set);

int set_cpu_affinity_all_threads(size_t cpu_set_size,
				 cpu_set_t *cpu_set);
