#!/usr/bin/env python3
import sys
import asyncio

import utils
import ctx
from async_client import AsyncClient
from response_handler import make_response_handler

if __name__ == "__main__":
    utils.assert_py_version()
    client_ctx = ctx.make_ctx(sys.argv[1])
    resp_handler = make_response_handler(client_ctx)
    client_ctx.set_up_logging()
    client = AsyncClient(response_handler = resp_handler,
                         auth_type = client_ctx.auth_type,
                         auth_creds = client_ctx.auth_creds
    )

    ev_loop = asyncio.get_event_loop()
    futures = [ asyncio.ensure_future(client.run(**kwargs)) for kwargs in client_ctx.arg_list ]
    ev_loop.run_until_complete(asyncio.gather(*futures))
    resp_handler.print_stats()
