import json
import web

from config import DB

def load_machine(name):
    results = list(DB.select('machine', what='*',
                             where='name = $name',
                             vars=dict(name=name)))
    if not results:
        raise web.NotFound()
    return results[0]

class MachineLock:
    def GET(self, name):
        row = load_machine(name)
        row.locked_since = row.locked_since.isoformat()
        web.header('Content-type', 'text/json')
        return json.dumps(row)

    def DELETE(self, name):
        user = web.input('user')['user']
        machine = load_machine(name)
        if not machine.locked:
            raise web.BadRequest()
        if machine.locked_by != user:
            raise web.Forbidden()

        res = DB.update('machine',
                        where='locked = true AND name = $name AND locked_by = $user',
                        vars=dict(name=name, user=user),
                        locked=False, locked_by=None, description=None)
        assert res == 1, 'Failed to unlock machine {name}'.format(name=name)
        print user, 'unlocked', name

    def POST(self, name):
        user = web.input('user')['user']
        desc = web.input(desc=None)['desc']
        machine = load_machine(name)
        if machine.locked:
            raise web.Forbidden()
        res = DB.update('machine', where='name = $name AND locked = false',
                        vars=dict(name=name),
                        locked=True,
                        description=desc,
                        locked_by=user,
                        locked_since=web.db.SQLLiteral('NOW()'))
        assert res == 1, 'Failed to lock machine {name}'.format(name=name)
        print user, 'locked single machine', name

    def PUT(self, name):
        desc = web.input(desc=None)['desc']
        status = web.input(status=None)['status']
        sshpubkey = web.input(sshpubkey=None)['sshpubkey']

        updated = {}
        if desc is not None:
            updated['description'] = desc
        if status is not None:
            updated['up'] = (status == 'up')
        if sshpubkey is not None:
            updated['sshpubkey'] = sshpubkey

        if not updated:
            raise web.BadRequest()
        DB.update('machine', where='name = $name',
                  vars=dict(name=name), **updated)
        print 'updated', name, 'with', updated

class Lock:
    def GET(self):
        rows = list(DB.select('machine', what='*'))
        if not rows:
            raise web.NotFound()
        for row in rows:
            row.locked_since = row.locked_since.isoformat()
        web.header('Content-type', 'text/json')
        return json.dumps(rows)

    def POST(self):
        user = web.input('user')['user']
        desc = web.input(desc=None)['desc']
        num = int(web.input('num')['num'])
        machinetype = dict(machinetype=(web.input(machinetype='plana')['machinetype']))

        if num < 1:
            raise web.BadRequest()

        tries = 0
        while True:
            try:
                # transaction will be rolled back if an exception is raised
                with DB.transaction():
                    results = list(DB.select('machine', machinetype, what='name, sshpubkey',
                                             where='locked = false AND up = true AND type =$machinetype',
                                             limit=num))
                    if len(results) < num:
                        raise web.HTTPError(status='503 Service Unavailable')
                    name_keys = {}
                    for row in results:
                        name_keys[row.name] = row.sshpubkey
                    where_cond = web.db.sqlors('name = ', name_keys.keys()) \
                        + ' AND locked = false AND up = true'
                    num_locked = DB.update('machine',
                                           where=where_cond,
                                           locked=True,
                                           locked_by=user,
                                           description=desc,
                                           locked_since=web.db.SQLLiteral('NOW()'))
                    assert num_locked == num, 'Failed to lock machines'
            except:
                tries += 1
                if tries < 10:
                    continue
                raise
            else:
                break

        print user, 'locked', name_keys.keys()

        web.header('Content-type', 'text/json')
        return json.dumps(name_keys)
