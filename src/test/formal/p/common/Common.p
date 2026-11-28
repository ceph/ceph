/*
 * Shared by the RGW models: RGW's return codes, the clients that run a
 * scenario, and helpers to write its script.
 *
 * A model that includes this file defines:
 * - tCfg, its configuration;
 * - tReq, a request as a client sends it;
 * - tInit, the Store's starting state;
 * - tOut, what an answered request hands to later ones (such as the
 *   credentials an STS request issues), and InSlot(r) and OutSlot(r): the
 *   slot request r takes its input from, and the slot its answer fills, 0
 *   for none;
 * - FirstRid(), the first request's ID; the next ones count up from it, so
 *   that a model can keep request IDs apart from its other IDs;
 * - machine Store, created with (cfg: tCfg, init: tInit), which answers
 *   eQuiesce with eQuiesced once it has finished its own end-of-run work;
 * - machine Rgw, created with (cfg: tCfg, store: machine, driver: machine,
 *   rid: int, req: tReq, input: tOut), which sends the driver eDone once
 *   it has answered its request or died.
 */

// the errnos and RGW errors the models' RADOS ops and handlers return
enum tRc { OK, EEXIST, ECANCELED, ENOENT, EBUSY, EIO, EINVAL,
           ERR_PRECONDITION_FAILED, ERR_NO_SUCH_UPLOAD, ERR_INVALID_PART, ERR_INTERNAL_ERROR,
           ERR_CONDITIONAL_REQUEST_CONFLICT, EUSERS }

event eDone: (rid: int, crashed: bool, out: tOut);
// to the specs: a client sent request rid, with the input from its slot
event eLaunch: (rid: int, req: tReq, input: tOut);
event eQuiesce: machine;
event eQuiesced;

/*
 * Clients. A script is a list of phases; the requests of a phase run
 * concurrently, and a phase starts once every request of the one before
 * has been answered or its RGW has died. Then the Store finishes, and the
 * specs see the final state.
 */
machine Driver {
  var cfg: tCfg;
  var store: machine;
  var script: seq[seq[tReq]];
  var phase: int;
  var outstanding: int;
  var nextRid: int;
  var slots: map[int, tOut];
  var outSlot: map[int, int];

  start state Run {
    entry (p: (cfg: tCfg, init: tInit, script: seq[seq[tReq]])) {
      cfg = p.cfg;
      script = p.script;
      nextRid = FirstRid();
      store = new Store((cfg = cfg, init = p.init));
      Launch();
    }

    on eDone do (d: (rid: int, crashed: bool, out: tOut)) {
      if (!d.crashed && outSlot[d.rid] != 0) {
        slots[outSlot[d.rid]] = d.out;
      }
      outstanding = outstanding - 1;
      if (outstanding == 0) {
        phase = phase + 1;
        if (phase < sizeof(script)) {
          Launch();
        } else {
          send store, eQuiesce, this;
        }
      }
    }

    ignore eQuiesced;
  }

  fun Launch() {
    var r: tReq;
    var input: tOut;
    foreach (r in script[phase]) {
      input = default(tOut);
      if (InSlot(r) in slots) {
        input = slots[InSlot(r)];
      }
      outSlot[nextRid] = OutSlot(r);
      announce eLaunch, (rid = nextRid, req = r, input = input);
      new Rgw((cfg = cfg, store = store, driver = this, rid = nextRid, req = r, input = input));
      nextRid = nextRid + 1;
      outstanding = outstanding + 1;
    }
  }
}

// a phase of one, two or three concurrent requests
fun One(a: tReq): seq[tReq] {
  var s: seq[tReq];
  s += (0, a);
  return s;
}
fun Two(a: tReq, b: tReq): seq[tReq] {
  var s: seq[tReq];
  s += (0, a);
  s += (1, b);
  return s;
}
fun Three(a: tReq, b: tReq, c: tReq): seq[tReq] {
  var s: seq[tReq];
  s += (0, a);
  s += (1, b);
  s += (2, c);
  return s;
}
