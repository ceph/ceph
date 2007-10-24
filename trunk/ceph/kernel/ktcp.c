#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "kmsg.h"
#include "ktcp.h"


int  _kconnect(struct sockaddr *saddr, struct socket **sd)
{
	int ret;

	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, sd);
        if (ret < 0) {
		printk(KERN_INFO "sock_create_kern error: %d\n", ret);
	} else {
	/* or could call kernel_connect(), opted to reduce call overhead */
		ret = (*sd)->ops->connect(*sd, (struct sockaddr *) saddr,
					  sizeof (struct sockaddr_in),0);
        	if (ret < 0) {
			printk(KERN_INFO "kernel_connect error: %d\n", ret);
			sock_release(*sd);
		}
	}
	return(ret);
}

struct socket * _klisten(struct sockaddr *saddr)
{
	int ret;
	struct socket *sd = NULL;
	struct sockaddr_in *in_addr = (struct sockaddr_in *)saddr;


	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sd);
        if (ret < 0) {
		printk(KERN_INFO "sock_create_kern error: %d\n", ret);
		return(sd);
	}

	/* no user specified address given so create */
	if (!in_addr->sin_addr.s_addr) {
		in_addr->sin_family = AF_INET;
		in_addr->sin_addr.s_addr = htonl(INADDR_ANY);
		in_addr->sin_port = htons(CEPH_PORT);  /* known port for now */
	}

/* TBD: set sock options... */
	/*  ret = kernel_setsockopt(sd, SOL_SOCKET, SO_REUSEADDR,
				(char *)optval, optlen); 
	if (ret < 0) {
		printk("Failed to set SO_REUSEADDR: %d\n", ret);
	}  */
	ret = sd->ops->bind(sd, saddr, sizeof(saddr));
/* TBD: probaby want to lessen the backlog queue to prevent chewing up resources.. */
	ret = sd->ops->listen(sd, SOMAXCONN);
	if (ret < 0) {
		printk(KERN_INFO "kernel_listen error: %d\n", ret);
		sock_release(sd);
		sd = NULL;
	}
	return(sd);
}

void _kaccept(struct sockaddr *saddr, struct socket **sd)
{
	return;
}

/*
 * receive a message this may return after partial send
 */
int _krecvmsg(struct socket *sd, void *buf, size_t len, unsigned msgflags)
{
	struct kvec iov = {buf, len};
	struct msghdr msg = {.msg_flags = msgflags};
	int rlen = 0;		/* length read */

	printk(KERN_INFO "entered krevmsg\n");
	msg.msg_flags |= MSG_DONTWAIT | MSG_NOSIGNAL;

	/* receive one kvec for now...  */
	rlen = kernel_recvmsg(sd, &msg, &iov, 1, len, msg.msg_flags);
        if (rlen < 0) {
		printk(KERN_INFO "kernel_recvmsg error: %d\n", rlen);
        }
	return(rlen);

}

/*
 * Send a message this may return after partial send
 */
int _ksendmsg(struct socket *sd, struct kvec *iov, 
		size_t len, size_t kvlen, unsigned msgflags)
{
	struct msghdr msg = {.msg_flags = msgflags};
	int rlen = 0;

	printk(KERN_INFO "entered ksendmsg\n");
	msg.msg_flags |=  MSG_DONTWAIT | MSG_NOSIGNAL;

	rlen = kernel_sendmsg(sd, &msg, iov, kvlen, len);
        if (rlen < 0) {
		printk(KERN_INFO "kernel_sendmsg error: %d\n", rlen);
        }
	return(rlen);
}
