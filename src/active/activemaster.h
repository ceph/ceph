/*
 * This is the master executable to start up
 * a compute task across several nodes.
 *
 *
 */  


//#include <sys/stat.h>
#include "utility.h"

int start_map_task(const char* command, const char* input_filename, 
		   long start, long length, tcpaddr_t ip_address);

void usage(const char* name);

//Client* startCephClient();
//void kill_client(Client* client);
