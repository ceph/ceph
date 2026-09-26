/*
 * Clients. A script is a list of phases; the requests of a phase run
 * concurrently, and a phase starts once every request of the one before
 * has been answered or its RGW has died. Then the specs see the final
 * state.
 */
machine Driver {
  var cfg: tCfg;
  var store: machine;
  var script: seq[seq[tSpec]];
  var phase: int;
  var outstanding: int;
  var nextRid: int;

  start state Run {
    entry (p: (cfg: tCfg, objects: set[int], twins: bool, uploads: set[int], script: seq[seq[tSpec]])) {
      cfg = p.cfg;
      script = p.script;
      nextRid = 2;
      store = new Store((cfg = cfg, objects = p.objects, twins = p.twins, uploads = p.uploads));
      Launch();
    }

    on eDone do (d: (rid: int, crashed: bool)) {
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
    var s: tSpec;
    foreach (s in script[phase]) {
      new Rgw((cfg = cfg, store = store, driver = this, rid = nextRid, req = s));
      nextRid = nextRid + 1;
      outstanding = outstanding + 1;
    }
  }
}
