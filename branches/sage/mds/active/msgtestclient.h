#include "inet.h"
#include "common.h"
#include "utility.h"
#include "client/Client.h"

// wait.h MUST NOT be #included before client/Client.h
#include <sys/wait.h>
#include <vector>

  struct request_split {
    tcpaddr_t ip_address;
    off_t start;
    off_t length;
  };


//#define SERV_HOST_ADDR "128.114.57.143" //issdm-8
#define SERV_HOST_ADDR "128.114.57.166" //issdm-31

#define SERV_TCP_PORT 6733
#define MAXLINE 512

void msg_type_sender(int sockfd);


int create_new_connection(const char* ip_address, uint16_t port);
int create_new_connection(in_addr_t ip_address, uint16_t port);
void usage(const char* name);
void ping_test(int fd);
void start_task_test(int fd);

int start_map_task(sockaddr_in ip_address, int taskID,
		   const char* map_command, 
		   const char* input_filename,
		   off_t start, off_t length,
		   const char* output_filename);

void send_start_task_msg(int fd,
			 int taskID,
			 int command_size, const char* command,
			 int inputfilenamesize, const char* inputfilename,
			 off_t offset,
			 off_t length,
			 int outputfilenamesize, const char* outputfilename);
