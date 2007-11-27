#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 

int main(int argc, char *argv[])
{
	int sd, port,len;
	struct sockaddr_in saddr;
	struct hostent *server;
	char buf[256];

	if (argc < 3) {
		fprintf(stderr,"usage %s hostname port\n", argv[0]);
		exit(1);
	}
	port = atoi(argv[2]);
	printf("port num = %d\n", port);
	sd = socket(AF_INET, SOCK_STREAM, 0);
	if (sd < 0) {
		fprintf(stderr,"socket open error");
		exit(1);
	}
	server = gethostbyname(argv[1]);
	printf("server name  = %s\n", server->h_name);
	printf("server ip  = %s\n", *(server->h_addr));
	if (server == NULL) {
		fprintf(stderr,"get host error\n");
		exit(1);
	}
	bzero((char *) &saddr, sizeof(saddr));
	saddr.sin_family = AF_INET;
	bcopy((char *)server->h_addr, (char *)&saddr.sin_addr.s_addr, 
			server->h_length);
	saddr.sin_port = htons(port);
	if (connect(sd,(struct sockaddr *)&saddr,sizeof(saddr)) < 0) {
		fprintf(stderr,"connection error\n");
		exit(1);	
	}
	printf("connected to kernel server\n");
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
