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
	std::chrono::time_point<std::chrono::high_resolution_clock> t1, t2;
};

std::string read_file_to_string(std::string path);

void promethize(std::string &name);
