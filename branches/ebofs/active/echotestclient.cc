/*
 * This is merely a test of an echo server; it's an early step in
 * building up the Ceph distributed compute service. This is
 * discardable once the next stage is up and running.
 *
 * Code is based off examples in Stevens' "Unix Network Programming".
 */

#include "echotestclient.h"

int main(int argc, char* argv[]) {
  
  int sockfd;
  struct sockaddr_in serv_addr;

  char* pname = argv[0];

  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(SERV_HOST_ADDR);
  serv_addr.sin_port = htons(SERV_TCP_PORT);

  
  // open a TCP socket
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("client: can't open stream socket");
    exit (-1);
  }

  // connect to the server.
  if (connect(sockfd, (struct sockaddr *) &serv_addr,
	      sizeof(serv_addr)) < 0) {
    printf("client: can't connect to server");
    exit (-1);
  }
  
  // start the test echoer
  str_cli(stdin, sockfd);
      
  
  close (sockfd);
  exit(0);
}


void str_cli(FILE *fp, int sockfd) {

  int n;
  char sendline[MAXLINE], recvline[MAXLINE + 1];

  // read from the fp and write to the socket;
  // then read from the socket and write to stdout
  while (fgets(sendline, MAXLINE, fp) != NULL) {

    n = strlen(sendline);
    if (writen(sockfd, sendline, n) != n) {
      printf("str_cli: writen error on socket");
      exit(-1);
    }
    n = readline(sockfd, recvline, MAXLINE);
    if (n < 0) {
      printf("str_cli: readline error");
      exit(-1);
    }
    recvline[n] = 0;
    fputs(recvline, stdout);
  }

  if (ferror(fp)) {
    printf("str_cli: error reading file");
    exit(-1);
  }

}
