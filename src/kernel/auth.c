
#include <linux/module.h>

#include "ceph_debug.h"
#include "ceph_ver.h"
#include "auth.h"
#include "super.h"




struct ceph_x_auth {
	int state;
	u64 client_challenge;
	u64 server_challenge;
	void *request;
} __attribute__ ((packed));

struct ceph_x_response {
	__le64 server_challenge;
} __attribute__ ((packed));

struct ceph_x_request {
	__le64 client_challenge;
	__le64 key;
} __attribute__ ((packed));

static int ceph_x_auth_init(struct ceph_auth_data *data)
{
	struct ceph_x_auth *auth_data;

	auth_data = (struct ceph_x_auth *)kmalloc(sizeof(struct ceph_x_auth), GFP_KERNEL);
	if (!auth_data)
		return -ENOMEM;
	data->private_data = auth_data;

	auth_data->state = 0;
	auth_data->request = NULL;

	return 0;
}

static int ceph_x_auth_handle_response(struct ceph_client *client,
				       struct ceph_auth_data *data,
				       const char *blob,
				       int err,
				       int len)
{
	struct ceph_x_auth *auth_data = (struct ceph_x_auth *)data->private_data;
	const struct ceph_x_response *response;
	if (err) {
		return err;
	}

	switch (auth_data->state) {
	case 0:
		if (len != sizeof(struct ceph_x_response))
			return -EINVAL;

		response = (const struct ceph_x_response *)blob;
		auth_data->state++;
		auth_data->server_challenge = le64_to_cpu(response->server_challenge);
		break;
	case 1:
		if (len == 0)
			break;
	default:
		return -EINVAL;
	}

	return 0;	
}
					

static int ceph_x_auth_create_request(struct ceph_client *client,
				     struct ceph_auth_data *data, char **blob, int *len)
{
	struct ceph_x_request *request;
	struct ceph_x_auth *auth_data = (struct ceph_x_auth *)data->private_data;

	switch (auth_data->state) {
	case 0:
		request = kmalloc(sizeof(struct ceph_x_request), GFP_KERNEL);
		if (!request) {
			return -ENOMEM;
		}
		auth_data->client_challenge = 0x123456789ABCDEF0;
		request->client_challenge = cpu_to_le64(auth_data->client_challenge);
		request->key =
			cpu_to_le64(auth_data->client_challenge ^ auth_data->server_challenge);

		*blob = (char *)request;
		*len = sizeof(struct ceph_x_request);
		break;
	default:
		return -EINVAL;
	}

	return 0;
}

static void ceph_x_auth_finalize(struct ceph_auth_data *data)
{
	struct ceph_x_auth *auth_data = (struct ceph_x_auth *)data->private_data;

	if (auth_data) {
		kfree(auth_data->request);
		kfree(auth_data);
	}

	data->private_data = NULL;
}

static struct ceph_auth_ops ceph_x_auth_ops = {
	init:			ceph_x_auth_init,
	create_request:		ceph_x_auth_create_request,
	handle_response:	ceph_x_auth_handle_response,
	finalize:		ceph_x_auth_finalize,
};

struct ceph_auth_ops *ceph_x_auth_get_ops(void)
{
	return &ceph_x_auth_ops;
}

