#include "common/hostname.h"
#include <chrono>
#include <string>

class BlockTimer {
 public:
	BlockTimer(std::string file, std::string function);
	~BlockTimer();
	void stop();
	double get_ms() const;
 private:
	std::chrono::duration<double, std::milli> ms;
	std::string file, function;
	bool stopped;
	using clock_t = std::chrono::steady_clock;
	clock_t::time_point t1;
	clock_t::time_point t2;
};

std::string read_file_to_string(std::string path);

void promethize(std::string &name);
