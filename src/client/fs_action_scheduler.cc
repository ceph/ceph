#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/un.h>
#include <sys/socket.h>

// A normal C function that is executed as a thread when its name
// is specified in pthread_create()
int sock;
struct sockaddr_un server;
char *unix_sock = "/var/run/ceph.sock";

char* concat(const char *s1,const char *s2)
{
    char *result = (char *)malloc(strlen(s1)+strlen(s2)+1);//+1 for the zero-terminator
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}

void log_ceph_sock1(char *string){
    FILE *fptr;
	fptr = fopen("program.txt", "a");
	if (fptr == NULL) {
		printf("Error!");
		exit(1);
	}
	fprintf(fptr, "%s\n", string);
	fclose(fptr);
}

void initialize_socket() {
	log_ceph_sock1("inside init socket\n");
	sock = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock < 0) {
		perror("opening stream socket");
		exit(1);
	}
	log_ceph_sock1("got sock\n");
	server.sun_family = AF_UNIX;
	strcpy(server.sun_path, unix_sock);
	log_ceph_sock1(server.sun_path);
	if (connect(sock, (struct sockaddr *) &server, sizeof(struct sockaddr_un)) < 0) {
		close(sock);
		perror(unix_sock);
		perror("connecting stream socket");
		log_ceph_sock1("connecting stream socket\n");
		exit(1);
	}
	log_ceph_sock1("finishing socket creation\n");
}

void write_to_sock(char *hooks_type,const char *name) {
	log_ceph_sock1("writing to sock\n");
	log_ceph_sock1(hooks_type);
	char * result = concat(hooks_type,name);
	if (write(sock, result, strlen(result)) < 0)
		perror("writing on stream socket");
}

void close_socket() {
	close(sock);
}

//void *myThreadFun(void *vargp) {
//	sleep(1);
//	printf("Printing GeeksQuiz from Thread \n");
//	return NULL;
//}
//
//int main() {
//	pthread_t tid;
//	printf("Before Thread\n");
//	pthread_create(&tid, NULL, myThreadFun, NULL);
//	pthread_join(tid, NULL);
//	printf("After Thread\n");
//	exit(0);
//}
