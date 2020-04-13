#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char *argv[])
{
	char obuf[32], ibuf[1024];
	int n, max = 0;
	
	if (argc > 2)
		max = atoi(argv[2]);
	if (!max)
		max = 600;
	
	memset(obuf, 0xff, sizeof(obuf));
	
	for (n = 1; n <= max; ++n) {
		int fd, ret;
		fd = open(argv[1], O_RDWR | O_CREAT | O_TRUNC, 0644);
		printf("%d/%d: open fd = %d\n", n, max, fd);
		
		ret = write(fd, obuf, sizeof(obuf));
		printf("write ret = %d\n", ret);
		
		sleep(1);
		
		ret = write(fd, obuf, sizeof(obuf));
		printf("write ret = %d\n", ret);
		
		ret = pread(fd, ibuf, sizeof(ibuf), 0);
		printf("pread ret = %d\n", ret);
		
		if (memcmp(obuf, ibuf, sizeof(obuf))) {
			printf("mismatch\n");
			close(fd);
			break;
		}
		close(fd);
	}
	return 0;
}
