// Shared library for the trivial task of adding up all the bytes in a file

//#include "inet.h"
#include "common.h"
#include "utility.h"
#include "client/Client.h"


extern "C" void start_trivial_task (const char* ceph_filename,
				    const char* local_filename, 
				    off_t offset, off_t length);

