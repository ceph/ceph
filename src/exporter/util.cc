#include "util.h"
#include <boost/algorithm/string/classification.hpp>
#include <cctype>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

BlockTimer::BlockTimer(std::string file, std::string function)
	: file(file), function(function), stopped(false) {
	t1 = std::chrono::high_resolution_clock::now();
}
BlockTimer::~BlockTimer() {
	std::cout << file << ":" << function << ": " << ms.count() << "ms\n";
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
	// FIXME: snails are faster than this
	std::ifstream is(path);
	std::stringstream buffer;
	buffer << is.rdbuf();
	return buffer.str();
}
