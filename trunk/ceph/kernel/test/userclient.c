#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h> 
#include <arpa/inet.h>
#include <netinet/in.h> 

#define HOST_ADDR "192.168.1.10"
#define PORT 32222

int main(int argc, char **argv) 
{
	int port, sd, len;
	struct hostent *host;
	struct in_addr h_addr;
	struct sockaddr_in saddr;
	char buf[256];


	if (argc != 3) {
		fprintf(stderr,"usage %s hostname port\n", argv[0]);
		exit(1);
	}
	if ((host = gethostbyname(argv[1])) == NULL) {
		fprintf(stderr, "nslookup failed on '%s'\n", argv[1]);
		exit(1);
	}
	h_addr.s_addr = *((unsigned long *) host->h_addr_list[0]);
	fprintf(stdout, "%s\n", inet_ntoa(h_addr));
	port = atoi(argv[2]);
	printf("port num = %d\n", port);

	sd = socket(AF_INET, SOCK_STREAM, 0);
	if (sd < 0) {
		fprintf(stderr,"socket open error");
		exit(1);
	}
	printf("created socket\n");
        bzero((char *) &saddr, sizeof(saddr));
        saddr.sin_family = AF_INET;
        bcopy((char *)host->h_addr, (char *)&saddr.sin_addr.s_addr,
                        host->h_length);
        saddr.sin_port = htons(port);
        if (connect(sd,(struct sockaddr *)&saddr,sizeof(saddr)) < 0) {
                fprintf(stderr,"connection error\n");
                exit(1);
        }
        printf("connected to kernel host\n");
        bzero(buf,256);
        len = read(sd,buf,255);
        if (len < 0) {
                fprintf(stderr,"read error\n");
                exit(1);
        }
        printf("Message received: %s\n",buf);

        printf("Please enter the message: ");
        bzero(buf,256);
        fgets(buf,255,stdin);
        len = write(sd,buf,strlen(buf));
        if (len < 0) {
                fprintf(stderr,"write error\n");
                exit(1);
        }
        bzero(buf,256);
        len = read(sd,buf,255);
        if (len < 0) {
                fprintf(stderr,"write error\n");
                exit(1);
        }
        printf("received message %s\n",buf);
	close(sd);
	exit(0);
}
