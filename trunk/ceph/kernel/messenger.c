#include <linux/socket.h>
#include <linux/net.h>
#include <net/tcp.h>
#include <linux/string.h>
#include "kmsg.h"

/* note: early stages, doesn't build... */
extern struct ceph_message *ceph_read_message()
{
	int ret;
	int received = 0;
	kvec *iov = message->payload->b_kv;

	while (received < len) {
		_krecvmsg(socket, iov->iov_base, iov->iov_len);
	}
}

extern int ceph_send_message(struct ceph_message *message)
{
	int ret;
	int sent = 0;
	int len = message->bufferlist->b_kvlen;
	kvec *iov = message->payload->b_kv;

	/* while (num left to send > 0) { */
	while (sent < len) {
		ret = _ksendmsg(socket, iov->iov_base + sent, iov->iov_len, len - sent);
		sent += ret;
	}
	return sent;
}

struct ceph_accepter ceph_accepter_init()
{
	struct socket *sd;
	struct sockaddr saddr;

	memset(&saddr, 0, sizeof(saddr));
	/* if .ceph.hosts file get host info from file */
		/* make my address from user specified address, fill in saddr */
	sd = _klisten(&saddr);
}

void ceph_dispatch(ceph_message *msg)
{
}

void make_addr(struct sockaddr *saddr, struct ceph_entity_addr *v)
{
	struct sockaddr_in *in_addr = (struct sockaddr_in *)saddr;

	memset(in_addr,0,sizof(in_addr));
	in_addr.sin_family = AF_INET;
	memcpy((char*)in_addr.sin_addr.s_addr, (char*)v.ipq, 4);
	in_addr.sin_port = htons(v.port);
}
void set_addr()
{
}
