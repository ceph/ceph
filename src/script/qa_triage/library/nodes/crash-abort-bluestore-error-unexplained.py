import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at
from qa_triage.tree.crash import OPS, frames

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'lflores-2026-07-17_23:28:16-rados-wip-lflores-testing-3-2026-07-17-1417-distro-default-trial/374634':
        'crash/abort/bluestore-error/unexplained/enoent-on-object-removed-as-divergent',
}

NOT_HANDLED = r'_txc_add_transaction error \((-?\d+)\) (.+?) not handled on operation (\d+)'
# How many lines before the error the one that names its object may be.
WINDOW = 1000


@node('crash/abort/bluestore-error/unexplained',
      'Which error did BlueStore not handle, on which object, and why was it missing?')
def bluestore_error_unexplained(ctx):
    who = ctx.facts.get('daemon', '')
    if not who:
        return unknown('the tree did not name the daemon')
    log = f'remote/*/log/ceph-{who}.log.gz'
    # The node before reads the first few _txc_add_transaction lines, which in a
    # long log are debug "got ENOENT" lines that BlueStore handled; the error it
    # did not handle is the one it logs as such.
    found = ctx.search(log, NOT_HANDLED, 'not handled on operation', max_hits=1)
    if not found:
        return unknown(f'{who}\'s log does not say which error BlueStore did not handle')
    error = found[0]
    code, text, number = re.search(NOT_HANDLED, error.text).groups()
    if code.lstrip('-') != '2':
        return unknown(f'BlueStore did not handle error {code} ({text}), not one this node '
                       'knows', error)
    op = OPS.get(int(number), f'op {number}')
    thread = re.match(r'\S+ ([0-9a-f]+) ', error.text)
    thread = thread.group(1) if thread else ''
    got = [h for h in ctx.search(log, thread + r'\s+\d+ bluestore\(\S*\) _txc_add_transaction op ' +
                                 number + r' got ENOENT on (\S+)',
                                 f'_txc_add_transaction op {number} got ENOENT', max_hits=50)
           if h.file == error.file and error.line - WINDOW < h.line < error.line]
    if not got:
        return unknown(f'{who}\'s log does not name the object of the ENOENT', error)
    obj = re.search(r'got ENOENT on (\S+)', got[-1].text).group(1)
    # 1#3:45cfbf7e:::name:95# is shard 1 of the object PGLog names 3:45cfbf7e:::name:95.
    hobj = re.match(r'(?:[0-9a-f]+#)?(.+?)#[0-9a-f]*$', obj)
    if not hobj:
        return unknown(f'{obj} is not an object name this node reads', got[-1], error)
    hobj = hobj.group(1)
    removed = [h for h in ctx.search(log, r'_merge_object_divergent_entries: hoid ' +
                                     re.escape(hobj) + r' cannot roll back, removing and '
                                     r'adding to missing', hobj,
                                     'cannot roll back, removing and adding to missing')
               if h.file == error.file and h.line < error.line]
    if not removed:
        return unknown(f'BlueStore got ENOENT in {op} of {obj}, but {who} did not log removing '
                       'it as a divergent entry', got[-1], error)
    top = frames(ctx, who, 0)
    caller = next((f for f in top if 'BlueStore' not in f and 'ObjectStore' not in f), '')
    name = re.sub(r'\(.*', '', caller)
    return done('enoent-on-object-removed-as-divergent',
                f'BlueStore on {who} got ENOENT{at(error)} in {op} of {obj}' +
                (f', a write sent by {name}' if name else '') +
                f': {who} had removed that object{at(removed[-1])}, as a divergent log '
                'entry it could not roll back, and added it to its missing set, but the '
                'primary still sent it the write as if the shard had the object',
                removed[-1], got[-1], error,
                key=f'ENOENT in {op} of an object removed as divergent' +
                    (f', from {name}' if name else ''),
                error=code, op=op, caller=caller, object=obj)
