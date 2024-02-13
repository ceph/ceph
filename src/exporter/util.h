#include "common/hostname.h"
#include <chrono>
#include <string>

#define TIMED_FUNCTION() BlockTimer timer(__FILE__, __FUNCTION__) 

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

bool string_is_digit(std::string s);
std::string read_file_to_string(std::string path);
std::string get_hostname(std::string path);

void promethize(std::string &name);
