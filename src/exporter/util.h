#include "common/hostname.h"
#include <chrono>
#include <string_view>

class BlockTimer {
 public:
	BlockTimer(std::string_view file, std::string_view function);
	~BlockTimer();
	void stop();
	double get_ms() const;
 private:
	const std::string_view file;
	const std::string_view function;
	bool stopped = false;
	using clock_t = std::chrono::steady_clock;
	clock_t::time_point t1;
	clock_t::time_point t2;
};

std::string read_file_to_string(std::string path);

void promethize(std::string &name);
