
#include <linux/module.h>

#include "ceph_debug.h"
#include "ceph_ver.h"
#include "auth.h"
#include "decode.h"
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

static int ceph_auth_err(int err)
{
	return (err < 0 && err != -EAGAIN);
}

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
	u32 blob_len;
	int ret = 0;

	printk(KERN_ERR "ceph_x_auth_handle_response len=%d\n", len);
	if (ceph_auth_err(err)) {
		return err;
	}

	if (len < sizeof(u32))
		return -EINVAL;

	ceph_decode_32_safe(&blob, blob + len, blob_len, bad);

	switch (auth_data->state) {
	case 0:
		printk(KERN_ERR "blob_len=%d sizeof=%ld\n", blob_len, sizeof(struct ceph_x_response));
		if (blob_len != sizeof(struct ceph_x_response))
			return -EINVAL;

		response = (const struct ceph_x_response *)blob;
		auth_data->state++;
		auth_data->server_challenge = le64_to_cpu(response->server_challenge);
		printk(KERN_ERR "server_challenge=%llx\b", auth_data->server_challenge);
		ret = -EAGAIN;
		break;
	case 1:
		auth_data->state++;
		if (blob_len == 0)
			break;
	default:
		ret = -EINVAL;
	}

	return ret;
bad:
	return err;
}
					

static int ceph_x_auth_create_request(struct ceph_client *client,
				     struct ceph_auth_data *data, char **blob, int *len)
{
	struct ceph_x_request *request;
	struct ceph_x_auth *auth_data = (struct ceph_x_auth *)data->private_data;

	switch (auth_data->state) {
	case 1:
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

static int ceph_generic_auth_init(struct ceph_auth_data *data)
{
	return 0;
}

static int ceph_supported_auth[] = { CEPH_AUTH_NONE,
				     CEPH_AUTH_CEPH };

static int ceph_generic_auth_create_request(struct ceph_client *client,
				     struct ceph_auth_data *data, char **blob, int *len)
{
	struct ceph_mon_auth_init_req *req;
	int i;
	int max_auth_types = sizeof(ceph_supported_auth) / sizeof(ceph_supported_auth[0]);
	int rlen = sizeof (struct ceph_mon_auth_init_req) +
			max_auth_types * sizeof(struct ceph_auth_type);

	req = (struct ceph_mon_auth_init_req *)kmalloc(rlen, GFP_KERNEL);
	if (!req) {
		return -ENOMEM;
	}

	req->num_auth = max_auth_types;

	for (i = 0; i < max_auth_types; i++) {
		req->auth_type[i].type = cpu_to_le32(ceph_supported_auth[i]);
	}

	*blob = (char *)req;
	*len = rlen;

	return 0;
}

static int ceph_generic_auth_handle_response(struct ceph_client *client,
				       struct ceph_auth_data *data,
				       const char *blob,
				       int err,
				       int len)
{
	printk(KERN_ERR "ceph_auth err=%d\n", err);

	if (ceph_auth_err(err))
		return err;

	/* FIXME: reset aops according to to the first response */
	client->aops = ceph_x_auth_get_ops();

	err = client->aops->init(&client->auth_data);
	if (err < 0)
		goto out;

	err = client->aops->handle_response(client, &client->auth_data, blob, err, len);
out:
	return err;
}

static void ceph_generic_auth_finalize(struct ceph_auth_data *data)
{
	return;
}

static struct ceph_auth_ops ceph_generic_auth_ops = {
	init:			ceph_generic_auth_init,
	create_request:		ceph_generic_auth_create_request,
	handle_response:	ceph_generic_auth_handle_response,
	finalize:		ceph_generic_auth_finalize,
};

struct ceph_auth_ops *ceph_auth_get_generic_ops(void)
{
	return &ceph_generic_auth_ops;
}


