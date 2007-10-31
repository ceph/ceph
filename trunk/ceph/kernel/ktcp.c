#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "kmsg.h"
#include "ktcp.h"


struct socket * _kconnect(struct sockaddr *saddr)
{
	int ret;
	struct socket *sd = NULL;

/* TBD: somewhere check for a connection already established to this node? */
/*      if we are keeping connections alive for a period of time           */
	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sd);
        if (ret < 0) {
		printk(KERN_INFO "sock_create_kern error: %d\n", ret);
	} else {
	/* or could call kernel_connect(), opted to reduce call overhead */
		ret = sd->ops->connect(sd, (struct sockaddr *) saddr,
					  sizeof (struct sockaddr_in),0);
        	if (ret < 0) {
			printk(KERN_INFO "kernel_connect error: %d\n", ret);
			sock_release(sd);
		}
	}
	return(sd);
}

struct socket * _klisten(struct sockaddr *saddr)
{
	int ret;
	struct socket *sd = NULL;
	struct sockaddr_in *in_addr = (struct sockaddr_in *)saddr;


	ret = sock_create_kern(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sd);
        if (ret < 0) {
		printk(KERN_INFO "sock_create_kern error: %d\n", ret);
		return(NULL);
	}

	/* no user specified address given so create, will allow arg to mount */
	if (!in_addr->sin_addr.s_addr) {
		in_addr->sin_family = AF_INET;
		in_addr->sin_addr.s_addr = htonl(INADDR_ANY);
		in_addr->sin_port = htons(CEPH_PORT);  /* known port for now */
		/* in_addr->sin_port = htons(0); */  /* any port */
	}

/* TBD: set sock options... */
	/*  ret = kernel_setsockopt(sd, SOL_SOCKET, SO_REUSEADDR,
				(char *)optval, optlen); 
	if (ret < 0) {
		printk("Failed to set SO_REUSEADDR: %d\n", ret);
	}  */
	ret = sd->ops->bind(sd, saddr, sizeof(saddr));
/* TBD: probaby want to tune the backlog queue .. */
	ret = sd->ops->listen(sd, NUM_BACKUP);
	if (ret < 0) {
		printk(KERN_INFO "kernel_listen error: %d\n", ret);
		sock_release(sd);
		sd = NULL;
	}
	return(sd);
}

/*
 * Note: Maybe don't need this, or make inline... keep for now for debugging..
 * we may need to add more functionality
 */
struct socket *_kaccept(struct socket *sd)
{
	struct socket *new_sd = NULL;
	int ret;


	ret = kernel_accept(sd, &new_sd, sd->file->f_flags);
        if (ret < 0) {
		printk(KERN_INFO "kernel_accept error: %d\n", ret);
		return(new_sd);
	}
/* TBD:  shall we check name for validity?  */
	return(new_sd);
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
		size_t kvlen, size_t len, unsigned msgflags)
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

struct sockaddr *_kgetname(struct socket *sd)
{
	struct sockaddr *saddr = NULL;
	int len;
	int ret;

	if ((ret = sd->ops->getname(sd, (struct sockaddr *)saddr,
					&len, 2) < 0)) {
		printk(KERN_INFO "kernel getname error: %d\n", ret);
	}
	return(saddr);

}
