
#include <stdio.h>
#include <unistd.h>
#include <sys/uio.h>

#include "proxy_link.h"
#include "proxy_manager.h"
#include "proxy_helpers.h"
#include "proxy_log.h"

typedef struct _proxy_version {
	uint16_t major;
	uint16_t minor;
} proxy_version_t;

#define DEFINE_VERSION(_num) [_num] = NEG_VERSION_SIZE(_num)

static uint32_t negotiation_sizes[PROXY_LINK_NEGOTIATE_VERSION + 1] = {
	DEFINE_VERSION(1)

	/* NEG_VERSION: Add newly defined versions above this comment. */
};

static int32_t iov_length(struct iovec *iov, int32_t count)
{
	int32_t len;

	len = 0;
	while (count > 0) {
		len += iov->iov_len;
		iov++;
		count--;
	}

	return len;
}

static int32_t proxy_link_prepare(struct sockaddr_un *addr, const char *path)
{
	struct sigaction action;
	int32_t sd, len, err;

	memset(&action, 0, sizeof(action));
	action.sa_handler = SIG_IGN;
	err = proxy_signal_set(SIGPIPE, &action, NULL);
	if (err < 0) {
		return err;
	}

	memset(addr, 0, sizeof(*addr));
	addr->sun_family = AF_UNIX;
	len = snprintf(addr->sun_path, sizeof(addr->sun_path), "%s", path);
	if (len < 0) {
		return proxy_log(LOG_ERR, EINVAL,
				 "Failed to copy Unix socket path");
	}
	if (len >= sizeof(addr->sun_path)) {
		return proxy_log(LOG_ERR, ENAMETOOLONG,
				 "Unix socket path too long");
	}

	sd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sd < 0) {
		return proxy_log(LOG_ERR, errno,
				 "Failed to create a Unix socket");
	}

	return sd;
}

static int32_t proxy_link_read(proxy_link_t *link, int32_t sd, void *buffer,
			       int32_t size)
{
	ssize_t len;

	while (size > 0) {
		len = read(sd, buffer, size);
		if (len < 0) {
			if (errno == EINTR) {
				if (link->stop(link)) {
					return -EINTR;
				}
				continue;
			}
			return proxy_log(LOG_ERR, errno,
					 "Failed to read from socket");
		}
		if (len == 0) {
			return proxy_log(LOG_ERR, EPIPE, "Partial read");
		}

		buffer = (char *)buffer + len;
		size -= len;
	}

	return 0;
}

static int32_t proxy_link_write(proxy_link_t *link, int32_t sd, void *buffer,
				int32_t size)
{
	ssize_t len;

	while (size > 0) {
		len = write(sd, buffer, size);
		if (len < 0) {
			if (errno == EINTR) {
				if (link->stop(link)) {
					return -EINTR;
				}
				continue;
			}
			return proxy_log(LOG_ERR, errno,
					 "Failed to write to socket");
		}
		if (len == 0) {
			return proxy_log(LOG_ERR, EPIPE, "Partial write");
		}

		buffer = (char *)buffer + len;
		size -= len;
	}

	return 0;
}

int32_t proxy_link_ctrl_send(int32_t sd, void *data, int32_t size, int32_t type,
			     void *ctrl, int32_t ctrl_size)
{
	char buffer[CMSG_SPACE(ctrl_size)];
	struct msghdr msg;
	struct iovec iov;
	struct cmsghdr *cmsg;
	ssize_t len;

	msg.msg_name = NULL;
	msg.msg_namelen = 0;

	iov.iov_base = data;
	iov.iov_len = size;
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	cmsg = (struct cmsghdr *)buffer;
	cmsg->cmsg_len = CMSG_LEN(ctrl_size);
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = type;
	memcpy(CMSG_DATA(cmsg), ctrl, ctrl_size);
	msg.msg_control = buffer;
	msg.msg_controllen = sizeof(buffer);

	msg.msg_flags = 0;

	len = sendmsg(sd, &msg, MSG_NOSIGNAL);
	if (len < 0) {
		return proxy_log(LOG_ERR, errno,
				 "Failed to send a control message");
	}

	if (len != size) {
		return proxy_log(LOG_ERR, 0,
				 "Control message not sent completely");
	}

	return 0;
}

int32_t proxy_link_ctrl_recv(int32_t sd, void *data, int32_t size, int32_t type,
			     void *ctrl, int32_t *ctrl_size)
{
	char buffer[CMSG_SPACE(*ctrl_size)];
	struct msghdr msg;
	struct iovec iov;
	struct cmsghdr *cmsg;
	ssize_t len;
	int32_t clen;

	msg.msg_name = NULL;
	msg.msg_namelen = 0;

	iov.iov_base = data;
	iov.iov_len = size;
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	msg.msg_control = buffer;
	msg.msg_controllen = sizeof(buffer);

	msg.msg_flags = 0;

	len = recvmsg(sd, &msg, MSG_NOSIGNAL | MSG_CMSG_CLOEXEC);
	if (len < 0) {
		return proxy_log(LOG_ERR, errno,
				 "Failed to received a control message");
	}

	if (len != size) {
		return proxy_log(LOG_ERR, 0,
				 "Message data not received completely");
	}

	if ((msg.msg_flags & MSG_CTRUNC) != 0) {
		return proxy_log(LOG_ERR, ENOSPC,
				 "Message data is larger than expected");
	}

	cmsg = CMSG_FIRSTHDR(&msg);
	if (cmsg == NULL) {
		*ctrl_size = 0;
		return len;
	}

	if (cmsg->cmsg_level != SOL_SOCKET) {
		return proxy_log(LOG_ERR, EINVAL,
				 "Unexpected control message level");
	}

	if (cmsg->cmsg_type != type) {
		return proxy_log(LOG_ERR, EINVAL,
				 "Unexpected control message type");
	}

	clen = cmsg->cmsg_len - CMSG_LEN(0);
	if (clen != *ctrl_size) {
		return proxy_log(LOG_ERR, EINVAL,
				 "Unexpected control message size");
	}

	if (CMSG_NXTHDR(&msg, cmsg) != NULL) {
		return proxy_log(LOG_ERR, EINVAL, "Too many control messages");
	}

	memcpy(ctrl, CMSG_DATA(cmsg), clen);

	return len;
}

static int32_t proxy_link_negotiate_read(proxy_link_t *link, int32_t sd,
					 proxy_link_negotiate_t *neg)
{
	char buffer[128];
	char *ptr;
	uint32_t size, len;
	int32_t err;

	memset(neg, 0, sizeof(proxy_link_negotiate_t));

	ptr = (char *)neg;

	err = proxy_link_read(link, sd, ptr, sizeof(neg->v0));
	if (err < 0) {
		return err;
	}

	ptr += sizeof(neg->v0);
	size = neg->v0.size;

	/* Version 0 is only used to represent legacy peers that don't use any
	 * negotiation requests. */
	if (neg->v0.version == 0) {
		return proxy_log(LOG_ERR, EINVAL, "Invalid negotiate version");
	}

	/* Ignore data from future versions. */
	if (neg->v0.version > PROXY_LINK_NEGOTIATE_VERSION) {
		neg->v0.version = PROXY_LINK_NEGOTIATE_VERSION;
		neg->v0.size = NEG_VERSION_SIZE(PROXY_LINK_NEGOTIATE_VERSION);
	}

	if (negotiation_sizes[neg->v0.version] != neg->v0.size) {
		return proxy_log(LOG_ERR, EINVAL,
				 "Negotiation structure doesn't have the "
				 "expected size");
	}

	if (neg->v0.version < neg->v0.min_version) {
		return proxy_log(LOG_ERR, ENOTSUP,
				 "Negotiation requires an unsupported version");
	}

	/* Read remaining negotiation data. */
	err = proxy_link_read(link, sd, ptr, neg->v0.size - sizeof(neg->v0));
	if (err < 0) {
		return err;
	}

	/* Ignore any additional data belonging to unsupported negotiation
	 * structures. */
	size -= neg->v0.size;
	while (size > 0) {
		len = sizeof(buffer);
		if (len > size) {
			len = size;
		}
		err = proxy_link_read(link, sd, buffer, len);
		if (err < 0) {
			return err;
		}
		size -= len;
	}

	return 0;
}

static int32_t proxy_link_negotiate_check(proxy_link_negotiate_t *local,
					  proxy_link_negotiate_t *remote,
					  proxy_link_negotiate_cbk_t cbk)
{
	uint32_t supported, enabled;
	int32_t err;

	if (local->v0.version > remote->v0.version) {
		local->v0.version = remote->v0.version;
		local->v0.size = remote->v0.size;
	}

	if (remote->v0.version == 0) {
		/* Legacy peer. If we require any feature, the peer won't
		 * support it, so we can't continue */
		if (local->v1.required != 0) {
			return proxy_log(LOG_ERR, ENOTSUP,
					 "The peer doesn't support any "
					 "features");
		}

		/* The peer is running the old version, but it is compatible
		 * with us, so everything is fine and the connection can be
		 * completed successfully with all features disabled. */

		local->v1.enabled = 0;

		proxy_log(LOG_INFO, 0,
			  "Connected to legacy peer. No features enabled");

		goto validate;
	}

	supported = local->v1.supported & remote->v1.supported;
	local->v1.supported = supported;

	if ((local->v1.required & ~supported) != 0) {
		return proxy_log(LOG_ERR, ENOTSUP,
				 "Required features are not supported by the "
				 "peer");
	}
	if ((remote->v1.required & ~supported) != 0) {
		return proxy_log(LOG_ERR, ENOTSUP,
				 "The peer requires some features that are "
				 "not supported");
	}

	/* For now just combine the desired features from each side. In the
	 * future, if there are two features to implement the same, they may
	 * be chosen here and enable just one of them. */

	enabled = (local->v1.enabled | remote->v1.enabled) & supported;
	local->v1.enabled = enabled;

	/* NEG_VERSION: Implement handling of negotiate extensions. */

validate:
	if (cbk != NULL) {
		err = cbk(local);
		if (err < 0) {
			return err;
		}
	}

	return 0;
}

static int32_t proxy_link_negotiate_client(proxy_link_t *link, int32_t sd,
					   proxy_link_negotiate_t *neg,
					   proxy_link_negotiate_cbk_t cbk)
{
	proxy_link_negotiate_t remote;
	int32_t err;

	err = proxy_link_write(link, sd, neg, neg->v0.size);
	if (err < 0) {
		return err;
	}

	err = proxy_link_negotiate_read(link, sd, &remote);
	if (err < 0) {
		return err;
	}

	err = proxy_link_negotiate_check(neg, &remote, cbk);
	if (err < 0) {
		return err;
	}

	if (remote.v0.version == 0) {
		return 0;
	}

	/* For version 1 and higher, send the agreed enabled features. */
	err =  proxy_link_write(link, sd, &neg->v1.enabled,
				sizeof(neg->v1.enabled));
	if (err < 0) {
		return err;
	}

	return 0;
}

static int32_t proxy_link_negotiate_server(proxy_link_t *link, int32_t sd,
					   proxy_link_negotiate_t *neg,
					   proxy_link_negotiate_cbk_t cbk)
{
	proxy_link_negotiate_t remote;
	int32_t err, version;

	version = proxy_link_negotiate_read(link, sd, &remote);
	if (version < 0) {
		return version;
	}

	err = proxy_link_negotiate_check(neg, &remote, cbk);
	if (err < 0) {
		return err;
	}

	err = proxy_link_write(link, sd, neg, neg->v0.size);
	if (err < 0) {
		return err;
	}

	if (remote.v0.version > 0) {
		/* Read finally enabled features from client. */
		err = proxy_link_read(link, sd, &neg->v1.enabled,
				      sizeof(neg->v1.enabled));
		if (err < 0) {
			return err;
		}

		if ((neg->v1.enabled & ~neg->v1.supported) != 0) {
			return proxy_log(LOG_ERR, EINVAL,
					 "The client tried to enable an "
					 "unsupported feature");
		}

		if ((neg->v1.required & ~neg->v1.enabled) != 0) {
			return proxy_log(LOG_ERR, EINVAL,
					 "The client tried to disable a "
					 "required feature");
		}
	}

	/* NEG_VERSION: Implement any required handling for new negotiate
	 *              extensions. */

	if (cbk != NULL) {
		err = cbk(neg);
		if (err < 0) {
			return err;
		}
	}

	return 0;
}

int32_t proxy_link_handshake_client(proxy_link_t *link, int32_t sd,
				    proxy_link_negotiate_t *neg,
				    proxy_link_negotiate_cbk_t cbk)
{
	proxy_link_negotiate_t legacy;
	proxy_version_t version;
	uint32_t id;
	int32_t err;

	/* To make negotiation backward compatible, we send the same data that
	 * the previous version was sending, so if the server is still running
	 * the old version it will get the correct message and accept the
	 * connection.
	 *
	 * We use ancillary data to tell the server that further negotiation
	 * will happen. If the server runs the old version, it will ignore
	 * this data and return the normal answer, containing an old version
	 * number. If the server supports negotiation, it will answer with the
	 * new version number.
	 */

	id = LIBCEPHFS_LIB_CLIENT;

	err = proxy_link_ctrl_send(sd, &id, sizeof(id), SCM_RIGHTS, &sd,
				   sizeof(sd));
	if (err < 0) {
		return err;
	}

	err = proxy_link_read(link, sd, &version, sizeof(version));
	if (err < 0) {
		return proxy_log(LOG_ERR, -err,
				 "Failed to get initial answer from server");
	}

	if (version.major != LIBCEPHFSD_MAJOR) {
		return proxy_log(LOG_ERR, ENOTSUP,
				 "Unsupported major version received from "
				 "server");
	}

	if (version.minor == LIBCEPHFSD_MINOR) {
		/* The server doesn't support negotiation. */
		proxy_link_negotiate_init_v0(&legacy, 0, 0);

		return proxy_link_negotiate_check(neg, &legacy, cbk);
	}

	if (version.minor != LIBCEPHFSD_MINOR_NEG) {
		return proxy_log(LOG_ERR, ENOTSUP,
				 "Unsupported minor version recevied from "
				 "server");
	}

	/* The server supports negotiation. Let's do it. */

	return proxy_link_negotiate_client(link, sd, neg, cbk);
}

int32_t proxy_link_handshake_server(proxy_link_t *link, int32_t sd,
				    proxy_link_negotiate_t *neg,
				    proxy_link_negotiate_cbk_t cbk)
{
	proxy_link_negotiate_t legacy;
	proxy_version_t version;
	uint32_t id;
	int32_t err, size, dummy;

	/* To make negotiation backward compatible, we try to receive the same
	 * data that the previous version was sending, so if the client is still
	 * running the old version, we will get the correct message and accept
	 * the connection. In this case we'll disable all features and return
	 * the old version.
	 *
	 * We'll check if the first message contains ancilliary data. If so, it
	 * means that the client understands negotiation, so we'll reply with
	 * the new version and start the negotiation procedure.
	 */

	dummy = -1;
	size = sizeof(dummy);
	err = proxy_link_ctrl_recv(sd, &id, sizeof(id), SCM_RIGHTS, &dummy,
				   &size);
	if (err < 0) {
		return err;
	}

	/* If the client has passed a file descriptor, just close it. We don't
	 * need it for anything, only to know that it supports negotiation. */
	if (size > 0) {
		close(dummy);
	}

	if (id != LIBCEPHFS_LIB_CLIENT) {
		return proxy_log(LOG_ERR, EINVAL, "Unsupported client id");
	}

	if (size == 0) {
		proxy_link_negotiate_init_v0(&legacy, 0, 0);

		err = proxy_link_negotiate_check(neg, &legacy, cbk);
		if (err < 0) {
			return err;
		}

		version.major = LIBCEPHFSD_MAJOR;
		version.minor = LIBCEPHFSD_MINOR;

		return proxy_link_write(link, sd, &version, sizeof(version));
	}

	version.major = LIBCEPHFSD_MAJOR;
	version.minor = LIBCEPHFSD_MINOR_NEG;

	err = proxy_link_write(link, sd, &version, sizeof(version));
	if (err < 0) {
		return err;
	}

	/* The client supports negotiation. Let's do it. */

	return proxy_link_negotiate_server(link, sd, neg, cbk);
}

int32_t proxy_link_client(proxy_link_t *link, const char *path,
			  proxy_link_stop_t stop)
{
	struct sockaddr_un addr;
	int32_t sd, err;

	link->stop = stop;
	link->sd = -1;

	sd = proxy_link_prepare(&addr, path);
	if (sd < 0) {
		return sd;
	}

	err = 0;
	while (err >= 0) {
		if (connect(sd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
			if (errno == EINTR) {
				continue;
			}

			err = proxy_log(LOG_ERR, errno,
					"Failed to connect to libcephfsd");
		} else {
			link->sd = sd;
			return sd;
		}
	}

	close(sd);

	return err;
}

void proxy_link_close(proxy_link_t *link)
{
	close(link->sd);
	link->sd = -1;
}

int32_t proxy_link_server(proxy_link_t *link, const char *path,
			  proxy_link_start_t start, proxy_link_stop_t stop)
{
	struct sockaddr_un addr;
	socklen_t len;
	int32_t cd, err;

	link->stop = stop;
	link->sd = -1;

	err = proxy_link_prepare(&addr, path);
	if (err < 0) {
		return err;
	}
	link->sd = err;

	if ((unlink(path) < 0) && (errno != ENOENT) && (errno != ENOTDIR)) {
		err = proxy_log(LOG_ERR, errno,
				"Failed to remove existing socket");
		goto done;
	}

	if (bind(link->sd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		err = proxy_log(LOG_ERR, errno, "Failed to bind Unix socket");
		goto done;
	}

	if (listen(link->sd, SOMAXCONN) < 0) {
		err = proxy_log(LOG_ERR, errno,
				"Failed to listen from Unix socket");
		goto done;
	}

	while (!stop(link)) {
		len = sizeof(addr);
		cd = accept(link->sd, (struct sockaddr *)&addr, &len);
		if (cd < 0) {
			if (errno != EINTR) {
				proxy_log(LOG_ERR, errno,
					  "Failed to accept a connection");
			}
		} else {
			start(link, cd);
		}
	}

	err = 0;

done:
	close(link->sd);

	return err;
}

int32_t proxy_link_send(int32_t sd, struct iovec *iov, int32_t count)
{
	struct iovec iov_copy[count];
	ssize_t len;
	int32_t total;

	memcpy(iov_copy, iov, sizeof(struct iovec) * count);
	iov = iov_copy;

	total = 0;
	while (count > 0) {
		len = writev(sd, iov, count);
		if (len < 0) {
			return proxy_log(LOG_ERR, errno, "Failed to send data");
		}
		if (len == 0) {
			return proxy_log(LOG_ERR, ENOBUFS, "Partial write");
		}
		total += len;

		while ((count > 0) && (iov->iov_len <= len)) {
			len -= iov->iov_len;
			iov++;
			count--;
		}

		if (count > 0) {
			iov->iov_base = (char *)iov->iov_base + len;
			iov->iov_len -= len;
		}
	}

	return total;
}

int32_t proxy_link_recv(int32_t sd, struct iovec *iov, int32_t count)
{
	struct iovec iov_copy[count];
	ssize_t len;
	int32_t total;

	memcpy(iov_copy, iov, sizeof(struct iovec) * count);
	iov = iov_copy;

	total = 0;
	while (count > 0) {
		len = readv(sd, iov, count);
		if (len < 0) {
			return proxy_log(LOG_ERR, errno,
					 "Failed to receive data");
		}
		if (len == 0) {
			if (total > 0) {
				return proxy_log(LOG_ERR, EPIPE,
						 "Partial read");
			}
			return 0;
		}
		total += len;

		while ((count > 0) && (iov->iov_len <= len)) {
			len -= iov->iov_len;
			iov++;
			count--;
		}

		if (count > 0) {
			iov->iov_base = (char *)iov->iov_base + len;
			iov->iov_len -= len;
		}
	}

	return total;
}

int32_t proxy_link_req_send(int32_t sd, int32_t op, struct iovec *iov,
			    int32_t count)
{
	proxy_link_req_t *req;

	req = iov[0].iov_base;

	req->header_len = iov[0].iov_len;
	req->op = op;
	req->data_len = iov_length(iov + 1, count - 1);

	return proxy_link_send(sd, iov, count);
}

int32_t proxy_link_req_recv(int32_t sd, struct iovec *iov, int32_t count)
{
	proxy_link_req_t *req;
	void *buffer;
	int32_t err, len, total;

	len = iov->iov_len;
	iov->iov_len = sizeof(proxy_link_req_t);
	err = proxy_link_recv(sd, iov, 1);
	if (err <= 0) {
		return err;
	}
	total = err;

	req = iov->iov_base;

	if (req->data_len > 0) {
		if (count == 1) {
			return proxy_log(LOG_ERR, ENOBUFS,
					 "Request data is too long");
		}
		if (iov[1].iov_len < req->data_len) {
			buffer = proxy_malloc(req->data_len);
			if (buffer == NULL) {
				return -ENOMEM;
			}
			iov[1].iov_base = buffer;
		}
		iov[1].iov_len = req->data_len;
	} else {
		count = 1;
	}

	if (req->header_len > sizeof(proxy_link_req_t)) {
		if (len < req->header_len) {
			return proxy_log(LOG_ERR, ENOBUFS,
					 "Request is too long");
		}
		iov->iov_base = (char *)iov->iov_base + sizeof(proxy_link_req_t);
		iov->iov_len = req->header_len - sizeof(proxy_link_req_t);
	} else {
		iov++;
		count--;
		if (count == 0) {
			return total;
		}
	}

	err = proxy_link_recv(sd, iov, count);
	if (err <= 0) {
		if (err == 0) {
			return proxy_log(LOG_ERR, EPIPE, "Partial read");
		}
		return err;
	}

	return total + err;
}

int32_t proxy_link_ans_send(int32_t sd, int32_t result, struct iovec *iov,
			    int32_t count)
{
	proxy_link_ans_t *ans;

	ans = iov->iov_base;

	ans->header_len = iov->iov_len;
	ans->flags = 0;
	ans->result = result;
	ans->data_len = iov_length(iov + 1, count - 1);

	return proxy_link_send(sd, iov, count);
}

int32_t proxy_link_ans_recv(int32_t sd, struct iovec *iov, int32_t count)
{
	proxy_link_ans_t *ans;
	int32_t err, len, total;

	len = iov->iov_len;
	iov->iov_len = sizeof(proxy_link_ans_t);
	err = proxy_link_recv(sd, iov, 1);
	if (err <= 0) {
		if (err == 0) {
			return proxy_log(LOG_ERR, EPIPE,
					 "Connection closed while waiting for "
					 "an answer");
		}
		return err;
	}
	total = err;

	ans = iov->iov_base;

	if (ans->data_len > 0) {
		if ((count == 1) || (iov[1].iov_len < ans->data_len)) {
			return proxy_log(LOG_ERR, ENOBUFS,
					 "Answer data is too long");
		}
		iov[1].iov_len = ans->data_len;
	} else {
		count = 1;
	}

	if (ans->header_len > sizeof(proxy_link_ans_t)) {
		if (len < ans->header_len) {
			return proxy_log(LOG_ERR, ENOBUFS,
					 "Answer is too long");
		}
		iov->iov_base = (char *)iov->iov_base + sizeof(proxy_link_ans_t);
		iov->iov_len = ans->header_len - sizeof(proxy_link_ans_t);
	} else {
		iov++;
		count--;
		if (count == 0) {
			return total;
		}
	}

	err = proxy_link_recv(sd, iov, count);
	if (err <= 0) {
		if (err == 0) {
			return proxy_log(LOG_ERR, EPIPE, "Partial read");
		}
		return err;
	}

	return total + err;
}

int32_t proxy_link_request(int32_t sd, int32_t op, struct iovec *req_iov,
			   int32_t req_count, struct iovec *ans_iov,
			   int32_t ans_count)
{
	int32_t err;

	err = proxy_link_req_send(sd, op, req_iov, req_count);
	if (err < 0) {
		return err;
	}

	return proxy_link_ans_recv(sd, ans_iov, ans_count);
}
