#include "util.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <cctype>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ceph_exporter

BlockTimer::BlockTimer(std::string file, std::string function)
	: file(file), function(function), stopped(false) {
	t1 = std::chrono::high_resolution_clock::now();
}
BlockTimer::~BlockTimer() {
  dout(20) << file << ":" << function << ": " << ms.count() << "ms" << dendl;
}

// useful with stop
double BlockTimer::get_ms() {
	return ms.count();
}

// Manually stop the timer as you might want to get the time
void BlockTimer::stop() {
	if (!stopped) {
		stopped = true;
		t2 = std::chrono::high_resolution_clock::now();
		ms = t2 - t1;
	}
}

bool string_is_digit(std::string s) {
	size_t i = 0;
	while (std::isdigit(s[i]) && i < s.size()) {
		i++;
	}
	return i >= s.size();
}

std::string read_file_to_string(std::string path) {
	std::ifstream is(path);
	std::stringstream buffer;
	buffer << is.rdbuf();
	return buffer.str();
}

// Must be kept in sync with promethize() in src/pybind/mgr/prometheus/module.py
void promethize(std::string &name) {
  if (name[name.size() - 1] == '-') {
    name[name.size() - 1] = '_';
    name += "minus";
  }

  auto should_be_underscore = [](char ch) {
    return ch == '.' || ch == '/' || ch == ' ' || ch == '-';
  };
  std::replace_if(name.begin(), name.end(), should_be_underscore, '_');

  boost::replace_all(name, "::", "_");
  boost::replace_all(name, "+", "_plus");

  name = "ceph_" + name;
}
