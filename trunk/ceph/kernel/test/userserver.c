/* note:  port number is passed as an argument */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>

int main(int argc, char *argv[])
{
	struct sockaddr_in saddr, caddr;
	int sd, new_sd, port, clen, numbytes;
	char buf[256];

	if (argc < 2) {
		fprintf(stderr,"error must enter port \n");
		exit(1);
	}
	printf("starting server\n");

	sd = socket(AF_INET, SOCK_STREAM, 0);
	if (sd < 0) {
		fprintf(stderr,"error creating socket\n");
		exit(1);
	}
	bzero((char *) &saddr, sizeof(struct sockaddr_in));
	port = atoi(argv[1]);
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = INADDR_ANY;
	saddr.sin_port = htons(port);
	if (bind(sd, (struct sockaddr *) &saddr, sizeof(saddr)) < 0) {
		fprintf(stderr,"bind error\n");
		exit(1);
	}
	listen(sd,5);
	printf("started listening\n");
	clen = sizeof(caddr);
	new_sd = accept(sd, (struct sockaddr *) &caddr, &clen);
	if (new_sd < 0) {
		fprintf(stderr,"accept error\n");
		exit(1);
	}
	printf("accepted connection\n");
	bzero(buf,256);
	numbytes = read(new_sd,buf,255);
	if (numbytes < 0) {
		fprintf(stderr,"read error\n");
		exit(1);
	}
	printf("Message received: %s\n",buf);
	strcpy(buf,"server received your message");
	numbytes = write(new_sd, buf, 255);
	if (numbytes < 0){
		fprintf(stderr,"write error\n");
		exit(1);
	} 
	close(new_sd);
	close(sd);
	exit(0); 
}

