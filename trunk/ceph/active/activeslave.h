#include <dlfcn.h>
#include <string>
#include <sstream>
#include "inet.h"
#include "common.h"
#include "utility.h"
#include "client/Client.h"

// The port number is "osdd" on a telephone keypad.
#define SERV_TCP_PORT 6733

#define MAXLINE 512

void str_echo(int sockfd);
void str_ack(int sockfd);
void str_run(int sockfd);
void str_getmsg(int sockfd);
void process_request(int newsockfd);
void process_ping(int fd);
void process_start_task(int fd);
void process_get_local(int fd);
void process_shipcode(int fd);

void start_trivial_task(const char* ceph_filename, const char* local_filename,
			long offset, long length);
