


struct ceph_message {
	struct ceph_message_header m_hdr; /* header */
	struct iovec *m_iov;  /* payload */
	int m_iovlen;
	struct list_head m_list_head;    /* i'll sit in a queue */
};



/* dispatch method type */
typedef void (*ceph_kmsg_dispatch_t)(void *h, struct ceph_message *message);

struct ceph_kmsg {
 	ceph_kmsg_dispatch_t m_dispatch; /* where incoming messages go */
	void *m_parent;                  /* passed to dispatch method */
	
	struct ceph_kmsg_threadpool *m_threadpool;  /* pool of threads */
	/* possibly shared among multiple kmsg instances? */

	/* other nodes i talk to */
	struct radix_tree_root m_pipes;   /* key: dest addr, value: ceph_kmsg_pipe */
	
	/* ... */
};


struct ceph_kmsg_pipe {
	int p_sd;         /* socket descriptor */
	__u64 p_out_seq;  /* last message sent */
	__u64 p_in_seq;   /* last message received */

	/* out queue */
	struct list_head p_out_queue;
	struct ceph_message *p_out_partial;  /* partially sent message */
	int p_out_partial_pos;
	struct list_head p_out_sent;  /* sent but unacked; may need resend if connection drops */

	/* partially read message contents */
	struct iovec *p_in_partial_iov;   /* hrm, this probably isn't what we want */
	int p_in_partial_iovlen;
	int p_in_parital_iovmax;  /* size of currently allocated m_iov array */
	/* .. or something like that? .. */

};



