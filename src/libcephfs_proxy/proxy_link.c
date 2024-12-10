
#include <stdio.h>
#include <unistd.h>
#include <sys/uio.h>

#include "proxy_link.h"
#include "proxy_manager.h"
#include "proxy_helpers.h"
#include "proxy_log.h"

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

int32_t proxy_link_read(proxy_link_t *link, int32_t sd, void *buffer,
			int32_t size)
{
	ssize_t len;

	do {
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
	} while (len < 0);

	return len;
}

int32_t proxy_link_write(proxy_link_t *link, int32_t sd, void *buffer,
			 int32_t size)
{
	ssize_t len;
	int32_t total;

	total = size;
	while (total > 0) {
		len = write(sd, buffer, total);
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
			return proxy_log(LOG_ERR, ENOBUFS,
					 "No data written to socket");
		}

		buffer += len;
		total -= len;
	}

	return size;
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
			iov->iov_base += len;
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
			return proxy_log(LOG_ERR, ENODATA, "Partial read");
		}
		total += len;

		while ((count > 0) && (iov->iov_len <= len)) {
			len -= iov->iov_len;
			iov++;
			count--;
		}

		if (count > 0) {
			iov->iov_base += len;
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
	if (err < 0) {
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
		iov->iov_base += sizeof(proxy_link_req_t);
		iov->iov_len = req->header_len - sizeof(proxy_link_req_t);
	} else {
		iov++;
		count--;
		if (count == 0) {
			return total;
		}
	}

	err = proxy_link_recv(sd, iov, count);
	if (err < 0) {
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
	if (err < 0) {
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
		iov->iov_base += sizeof(proxy_link_ans_t);
		iov->iov_len = ans->header_len - sizeof(proxy_link_ans_t);
	} else {
		iov++;
		count--;
		if (count == 0) {
			return total;
		}
	}

	err = proxy_link_recv(sd, iov, count);
	if (err < 0) {
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
