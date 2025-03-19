
#include <unistd.h>

#include "include/cephfs/libcephfs.h"

#include "proxy_async.h"
#include "proxy_requests.h"
#include "proxy_log.h"

typedef int32_t (*proxy_cbk_handler_t)(proxy_async_t *, proxy_cbk_t *, void *,
				       uint32_t);

static int32_t libcephfsd_cbk_nonblocking_rw(proxy_async_t *async,
					     proxy_cbk_t *cbk, void *data,
					     uint32_t size)
{
	struct ceph_ll_io_info *info;
	const struct iovec *iov;
	int32_t err, count;

	err = ptr_check(&async->random, cbk->ll_nonblocking_rw.info,
			(void **)&info);
	if (err < 0) {
		return err;
	}

	info->result = cbk->ll_nonblocking_rw.res;

	if (!info->write) {
		iov = info->iov;
		count = info->iovcnt;
		while (size > 0) {
			if (count == 0) {
				proxy_log(LOG_ERR, 0, "Excess data received");
				break;
			}
			if (size < iov->iov_len) {
				memcpy(iov->iov_base, data, size);
				break;
			}

			memcpy(iov->iov_base, data, iov->iov_len);
			data += iov->iov_len;
			size -= iov->iov_len;
			iov++;
			count--;
		}
	}

	info->callback(info);

	return 0;
}

static proxy_cbk_handler_t libcephfsd_cbk_handlers[LIBCEPHFSD_CBK_TOTAL_OPS] = {
	[LIBCEPHFSD_CBK_LL_NONBLOCKING_RW] = libcephfsd_cbk_nonblocking_rw,
};

static void *
proxy_async_worker(void *arg)
{
	proxy_cbk_t cbk;
	proxy_async_t *async;
	struct iovec iov[2];
	int32_t err;

	async = arg;

	while (true) {
		iov[0].iov_base = &cbk;
		iov[0].iov_len = sizeof(cbk);
		iov[1].iov_base = NULL;
		iov[1].iov_len = 0;

		err = proxy_link_req_recv(async->fd, iov, 2);
		if (err <= 0) {
			break;
		}

		if (cbk.header.op >= LIBCEPHFSD_CBK_TOTAL_OPS) {
			proxy_log(LOG_ERR, ENOSYS,
				  "Unknown asynchronous callback");
		} else if (libcephfsd_cbk_handlers[cbk.header.op] == NULL) {
			proxy_log(LOG_ERR, EOPNOTSUPP,
				  "Unsupported asynchronous callback");
		} else {
			err = libcephfsd_cbk_handlers[cbk.header.op](
				async, &cbk, iov[1].iov_base, iov[1].iov_len);
		}

		if (iov[1].iov_base != NULL) {
			proxy_free(iov[1].iov_base);
		}

		if (err < 0) {
			break;
		}
	}

	if (err < 0) {
		proxy_log(LOG_ERR, -err, "Asynchronous worker found an error");

		/* Force disconnection from main connection. */
		proxy_link_close(async->link);
	}

	proxy_log(LOG_INFO, 0, "Asynchronous worker stopped");

	return NULL;
}

int32_t proxy_async_client(proxy_async_t *async, proxy_link_t *link, int32_t sd)
{
	int32_t err, data, fd, size;

	size = sizeof(fd);
	err = proxy_link_ctrl_recv(sd, &data, sizeof(data), SCM_RIGHTS, &fd,
				   &size);
	if (err < 0) {
		return err;
	}
        if (size < sizeof(fd)) {
                return proxy_log(LOG_ERR, ENODATA,
                                 "Failed to receive the asynchronous socket");
        }

	random_init(&async->random);
	async->fd = fd;
        async->link = link;

	err = pthread_create(&async->thread, NULL, proxy_async_worker, async);
	if (err < 0) {
		close(fd);
		return proxy_log(LOG_ERR, err,
				 "Failed to create asynchronous worker");
	}

	return 0;
}

int32_t proxy_async_server(proxy_async_t *async, int32_t sd)
{
	int32_t fds[2];
	int32_t err, data;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
		return proxy_log(LOG_ERR, errno,
				 "Failed to create a socket pair");
	}

	data = 0;
	err = proxy_link_ctrl_send(sd, &data, sizeof(data), SCM_RIGHTS, fds,
				   sizeof(int32_t));
	close(fds[0]);
	if (err < 0) {
		goto failed;
	}

	async->fd = fds[1];

	return 0;

failed:
	close(fds[1]);

	return err;
}
