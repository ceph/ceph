from raven import Client

client = None

def get_client(ctx):
    global client
    if client:
        return client
    dsn = ctx.teuthology_config.get('sentry_dsn')
    if dsn:
        client = Client(dsn=dsn)
        return client

