// Check that MTMessenger properly dispatches replies to the correct
// thread.  Processes with mutliple threads of clients send a
// "request" to a server, which then sends back a "reply".  The client
// checks that it received the correct reply for its request.  The
// request and reply are both an MClientRequest, which we used because
// it allows us to pass an arbitrary string in the sarg field.  In the
// request, the sarg field contains a string "rN:tN:mN" which uniquely
// identifies a request by rank (process), thread and message.  The
// server sends the reply with the sarg field set to "rN:tN:mN reply",
// and the client can the verify it receive the correct reply for its
// request.

#include <pthread.h>
#include "mpi.h"

#include "messages/MClientRequest.h"
#include "msg/MTMessenger.h"
#include "include/error.h"

#define SARG_SIZE 64
#define SERVER_RANK 0
#define NTHREADS 11        // number of threads per rank
#define NMESSAGES 31        // number of messages per thread

static void server_loop(MTMessenger &msgr, int world_size)
{
    // we expect this many messages from clients, then we quit
    // (world_size-1 since server is one of the processes).
    int totmsg = NTHREADS * NMESSAGES * (world_size - 1);
    int nmsg = 0;

    char buf[SARG_SIZE];

    while(nmsg < totmsg) {
    MClientRequest *req = (MClientRequest*)msgr.recvreq();
    ASSERT(req->get_type() == MSG_CLIENT_REQUEST);
    
    //cout << "Server acknowledging " << req->get_sarg() << endl;

    sprintf(buf, "%s reply", req->get_sarg().c_str());
    MClientRequest resp(0, 0);
    resp.set_sarg(buf);
    msgr.sendresp(req, &resp);

    delete req;
    nmsg++;
    }

    cout << "Server successful" << endl;
}

// arguments for client thread start function (see pthread_create)
struct client_arg
{
    MTMessenger *msgr;
    int rank;
    int thread;
};

static void *client_session(void *_carg)
{
    client_arg *carg = (client_arg *)_carg;

    char buf[SARG_SIZE];

    // repeat some number (arbitrary really) of rounds
    for (int i = 0; i < NMESSAGES; i++) {

    // send the message, receive the reply and check reply is as
    // expected

    MClientRequest request(0, 0);
    sprintf(buf, "r%d:t%d:m%d", carg->rank, carg->thread, i);
    request.set_sarg(buf);

    //cout << "Client sending " << request.get_sarg() << endl;

    MClientRequest *resp =
        (MClientRequest*)carg->msgr->sendrecv(&request, SERVER_RANK);

    ASSERT(resp->get_type() == MSG_CLIENT_REQUEST);
    sprintf(buf, "r%d:t%d:m%d reply", carg->rank, carg->thread, i);
    ASSERT(strcmp(buf, resp->get_sarg().c_str()) == 0);

    //cout << "Client verified " << resp->get_sarg() << endl;

    delete resp;
    }

    cout << "Client (" << carg->rank << "," << carg->thread
     <<  ") successful" << endl;

    delete carg;
    return NULL;
}

static void launch_clients(MTMessenger &msgr, int rank)
{
    pthread_t tid[NTHREADS];

    // launch some number (arbitrary really) of threads
    for (int i = 0; i < NTHREADS; i++) {

    client_arg *carg = (client_arg*)malloc(sizeof(client_arg));
    ASSERT(carg);
    carg->msgr = &msgr;
    carg->rank = rank;
    carg->thread = i;

    if (pthread_create(&tid[i], NULL, client_session, carg) < 0)
        SYSERROR();
    }

    // we must wait for all the threads to exit before returning,
    // otherwise we shutdown MPI before while the threads are
    // chatting.
    for (int i = 0; i < NTHREADS; i++) {
    void *retval;

    if (pthread_join(tid[i], &retval) < 0)
        SYSERROR();
    }
}

int main(int argc, char **argv)
{
    MTMessenger msgr(argc, argv);

    int rank;
    ASSERT(MPI_Comm_rank(MPI_COMM_WORLD, &rank) == MPI_SUCCESS);
    int world_size;
    ASSERT(MPI_Comm_size(MPI_COMM_WORLD, &world_size) == MPI_SUCCESS);

    if (rank == SERVER_RANK)
    server_loop(msgr, world_size);
    else
    launch_clients(msgr, rank);

    return 0;
}
