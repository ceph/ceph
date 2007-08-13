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
