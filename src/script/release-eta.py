#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyyaml"]
# ///
#
# SPDX-License-Identifier: LGPL-2.1-or-later
"""Estimate when the next upstream Ceph point release (and EOLs) will land.

Redmine versions and GitHub milestones are rarely kept current, so instead of
trusting their due dates we look at where each release actually is in the
upstream release pipeline and apply historical lags for that stage:

  backports  -> last release + typical gap between point releases
  qe         -> <name>-release branch frozen / ceph.io notes PR opened
                -> freeze date + typical QE-to-announcement lag
  tagged     -> vX.Y.Z tag pushed -> tag date + typical tag-to-announcement lag
  released   -> announced (ceph.io notes merged or doc/releases/releases.yml)

Usage:
  src/script/release-eta.py                 # text summary of all active releases
  src/script/release-eta.py squid           # just one release
  src/script/release-eta.py --json out.json --html out.html

Set GITHUB_TOKEN (or have `gh` logged in) to avoid API rate limits.
"""

import argparse
import datetime as dt
import html
import json
import os
import re
import statistics
import subprocess
import sys
import urllib.parse
import urllib.request

import yaml

GH = "https://api.github.com"
TRACKER = "https://tracker.ceph.com"
RELEASES_YML = "repos/ceph/ceph/contents/doc/releases/releases.yml?ref=main"
TODAY = dt.date.today()
HISTORY = 8  # how many recent samples to use for each lag distribution
CADENCE = 56  # upstream aims for a point release every 8 weeks
ACTIVE_MAJORS = 2  # upstream supports two stable majors at a time


# ---------------------------------------------------------------- fetching

def _token():
    tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if tok:
        return tok
    try:
        return subprocess.run(["gh", "auth", "token"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


TOKEN = _token()


def gh(path, raw=False, method="GET", body=None):
    req = urllib.request.Request(path if path.startswith("http") else f"{GH}/{path}",
                                 method=method, data=body)
    req.add_header("Accept", "application/vnd.github.raw" if raw
                   else "application/vnd.github+json")
    if TOKEN:
        req.add_header("Authorization", f"Bearer {TOKEN}")
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    return data.decode() if raw else json.loads(data)


def gh_graphql(query):
    out = gh("graphql", method="POST", body=json.dumps({"query": query}).encode())
    if "errors" in out:
        raise RuntimeError(out["errors"])
    return out["data"]


def gh_paginate(path):
    page, out = 1, []
    while True:
        sep = "&" if "?" in path else "?"
        batch = gh(f"{path}{sep}per_page=100&page={page}")
        out += batch
        if len(batch) < 100:
            return out
        page += 1


def date(s):
    return dt.date.fromisoformat(str(s)[:10]) if s else None


def vkey(v):
    return tuple(int(x) for x in v.lstrip("v").split("."))


# ---------------------------------------------------------------- data model

def collect():
    """Pull every signal we use into one dict (also handy for debugging)."""
    yml = yaml.safe_load(gh(RELEASES_YML, raw=True))

    # Tag dates. Annotated tags carry a tagger date; lightweight ones fall back
    # to the commit date. Vendor tags (e.g. -pre.ibm) are ignored.
    tags = {}
    cursor = None
    for _ in range(3):  # newest 300 tags is plenty
        after = f', after:"{cursor}"' if cursor else ""
        d = gh_graphql(f"""{{repository(owner:"ceph",name:"ceph"){{
          refs(refPrefix:"refs/tags/",first:100{after},
               orderBy:{{field:TAG_COMMIT_DATE,direction:DESC}}){{
            pageInfo{{endCursor hasNextPage}}
            nodes{{name target{{... on Tag{{tagger{{date}}}}
                               ... on Commit{{committedDate}}}}}}}}}}}}""")
        refs = d["repository"]["refs"]
        for n in refs["nodes"]:
            if re.fullmatch(r"v\d+\.\d+\.\d+", n["name"]):
                t = n["target"]
                tags[n["name"][1:]] = date((t.get("tagger") or {}).get("date")
                                           or t.get("committedDate"))
        if not refs["pageInfo"]["hasNextPage"]:
            break
        cursor = refs["pageInfo"]["endCursor"]

    aliases = " ".join(f'm{m}:ref(qualifiedName:"refs/tags/v{m}.0.0")'
                       f"{{target{{... on Tag{{tagger{{date}}}} ... on Commit{{committedDate}}}}}}"
                       for m in range(16, 30))
    for k, ref in gh_graphql(f'{{repository(owner:"ceph",name:"ceph"){{{aliases}}}}}')[
            "repository"].items():
        if ref:
            t = ref["target"]
            tags[f"{k[1:]}.0.0"] = date((t.get("tagger") or {}).get("date") or t.get("committedDate"))

    branches = {}
    for b in gh_paginate("repos/ceph/ceph/branches"):
        if re.fullmatch(r"[a-z]+(-release)?", b["name"]):
            branches[b["name"]] = b["commit"]["sha"]

    # ceph.io release-notes PRs: opened ~ when QE starts, merged = announcement.
    notes = []
    q = "repo:ceph/ceph.io is:pr in:title release notes"
    for it in gh(f"search/issues?q={urllib.parse.quote(q)}&per_page=100&sort=created")["items"]:
        m = re.search(r"\bv?(\d+\.\d+\.\d+)\b", it["title"])
        if not m or not re.search(r"release notes", it["title"], re.I):
            continue
        merged = (it.get("pull_request") or {}).get("merged_at")
        notes.append({"version": m.group(1), "number": it["number"],
                      "url": it["html_url"], "state": it["state"],
                      "created": date(it["created_at"]), "merged": date(merged)})

    milestones = {m["title"].lstrip("v"): m for m in
                  gh("repos/ceph/ceph/milestones?state=open&per_page=100")}

    # Redmine versions: the vX.0.0 entry names the next major (e.g. "Vampire")
    # and its due date is the planned dev kickoff.
    try:
        with urllib.request.urlopen(f"{TRACKER}/projects/ceph/versions.json?limit=100",
                                    timeout=60) as r:
            redmine = {v["name"].lstrip("v"): v for v in json.load(r)["versions"]
                       if v["status"] != "locked"}
    except Exception:
        redmine = {}

    kickoffs = [{"title": it["title"], "number": it["number"], "url": it["html_url"],
                 "state": it["state"], "created": date(it["created_at"]),
                 "merged": date((it.get("pull_request") or {}).get("merged_at"))}
                for it in gh("search/issues?per_page=50&q=" + urllib.parse.quote(
                    "repo:ceph/ceph is:pr in:title kickoff"))["items"]]

    return {"yml": yml, "tags": tags, "branches": branches, "notes": notes,
            "milestones": milestones, "redmine": redmine, "kickoffs": kickoffs}


def branch_head_date(sha):
    return date(gh(f"repos/ceph/ceph/commits/{sha}")["commit"]["committer"]["date"])


def ahead_by(base, head):
    try:
        return gh(f"repos/ceph/ceph/compare/{base}...{head}")["ahead_by"]
    except Exception:
        return None


# ---------------------------------------------------------------- statistics

def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def dist(xs):
    xs = [x for x in xs if x is not None and x >= 0][-HISTORY:]
    if not xs:
        return None
    return {"n": len(xs), "p25": round(pct(xs, 25)), "p50": round(statistics.median(xs)),
            "p75": round(pct(xs, 75)), "max": max(xs)}


def announced_dates(data):
    """version -> announcement date, from releases.yml plus merged notes PRs
    (releases.yml itself sometimes lags the announcement)."""
    out = {}
    for series in data["yml"]["releases"].values():
        for r in series["releases"]:
            out[str(r["version"])] = date(r["released"])
    for n in data["notes"]:
        if n["merged"]:
            out.setdefault(n["version"], n["merged"])
    return out


def lag_models(data, announced):
    """Historical lags (days) for each pipeline stage, newest samples last."""
    stable = sorted((v for v in announced if re.fullmatch(r"\d+\.2\.\d+", v)), key=vkey)

    tag_to_ann = [(announced[v] - data["tags"][v]).days
                  for v in sorted(announced, key=lambda v: announced[v])
                  if v in data["tags"] and v in stable]
    notes_to_ann = [(n["merged"] - n["created"]).days
                    for n in sorted(data["notes"], key=lambda n: n["created"])
                    if n["merged"] and n["version"] in stable]
    # Point release cadence across all stable series (per-series cadence is
    # computed separately and preferred when there is enough history).
    gaps = []
    by_major = {}
    for v in stable:
        by_major.setdefault(vkey(v)[0], []).append(v)
    for vs in by_major.values():
        gaps += [(announced[b] - announced[a]).days for a, b in zip(vs, vs[1:])]
    # Major release: first RC tag (x.1.0) and last RC tag -> GA announcement (x.2.0)
    rc_to_ga, last_rc_to_ga, gas = [], [], []
    for m in sorted(by_major):
        if f"{m}.2.0" not in announced:
            continue
        ga = announced[f"{m}.2.0"]
        gas.append(ga)
        rcs = sorted((v for v in data["tags"] if re.fullmatch(rf"{m}\.1\.\d+", v)), key=vkey)
        if rcs:
            rc_to_ga.append((ga - data["tags"][rcs[0]]).days)
            last_rc_to_ga.append((ga - data["tags"][rcs[-1]]).days)
    ga_gap = [(b - a).days for a, b in zip(gas, gas[1:])]
    # Dev kickoff (x.0.0 tag, main becomes the new release) -> GA
    dev0_to_ga = [(announced[f"{m}.2.0"] - data["tags"][f"{m}.0.0"]).days
                  for m in sorted(by_major)
                  if f"{m}.2.0" in announced and f"{m}.0.0" in data["tags"]]

    return {"tag_to_announce": dist(tag_to_ann), "qe_to_announce": dist(notes_to_ann),
            "point_release_gap": dist(gaps), "rc_to_ga": dist(rc_to_ga),
            "last_rc_to_ga": dist(last_rc_to_ga), "major_release_gap": dist(ga_gap),
            "kickoff_to_ga": dist(dev0_to_ga),
            "latest_ga": max(gas), "by_major": by_major}


def window(start, d, label):
    return {"basis": label, "earliest": start + dt.timedelta(d["p25"]),
            "likely": start + dt.timedelta(d["p50"]),
            "latest": start + dt.timedelta(d["p75"]), "samples": d["n"]}


# ---------------------------------------------------------------- estimation

def estimate_series(name, series, data, announced, models):
    versions = sorted((str(r["version"]) for r in series["releases"]), key=vkey)
    major = vkey(versions[0])[0]
    # Pick up releases announced on ceph.io but not yet in releases.yml
    versions = sorted(set(versions) | {v for v in announced if vkey(v)[0] == major
                                       and re.fullmatch(r"\d+\.2\.\d+", v)}, key=vkey)
    last = versions[-1]
    a, b, c = vkey(last)
    nxt = f"{a}.{b}.{c + 1}"
    target_eol = date(series.get("target_eol"))
    actual_eol = date(series.get("actual_eol"))

    res = {"name": name, "major": major, "last": {"version": last, "date": announced[last]},
           "next": nxt, "target_eol": target_eol, "actual_eol": actual_eol,
           "evidence": [], "links": [], "note": ""}
    ev, links = res["evidence"], res["links"]

    if actual_eol:
        res.update(stage="eol", summary=f"{name.title()} reached end of life on {actual_eol}. "
                   f"No further releases are expected; the final release was {last}.")
        return res

    # Per-series cadence when we have >= 3 gaps, else the global one
    own = [(announced[y] - announced[x]).days for x, y in zip(versions, versions[1:])]
    cadence = dist(own) if len(own) >= 3 else models["point_release_gap"]

    notes_pr = next((n for n in data["notes"] if n["version"] == nxt), None)
    rel_branch = data["branches"].get(f"{name}-release")
    rel_date = None
    if rel_branch:
        rel_date = branch_head_date(rel_branch)
        if data["tags"].get(last) and rel_date <= data["tags"][last]:
            rel_date = None  # branch is left over from the previous release

    ms = data["milestones"].get(nxt)
    if ms:
        tot = ms["open_issues"] + ms["closed_issues"]
        ev.append(f"GitHub milestone v{nxt}: {ms['closed_issues']}/{tot} PRs merged"
                  + (f" ({ms['open_issues']} still open)" if ms["open_issues"] else ""))
        links.append((f"milestone v{nxt}", ms["html_url"]))
    n_back = ahead_by(f"v{last}", name)
    if n_back is not None:
        ev.append(f"{n_back} commits on the {name} branch since v{last}")

    if nxt in data["tags"]:
        tag = data["tags"][nxt]
        res["stage"] = "tagged"
        ev.insert(0, f"v{nxt} was tagged on {tag}; it is built but not yet announced")
        res["eta"] = window(tag, models["tag_to_announce"], "tag → announcement")
        started = tag
    elif notes_pr or rel_date:
        started = min(d for d in (notes_pr and notes_pr["created"], rel_date) if d)
        res["stage"] = "qe"
        if rel_date:
            ev.insert(0, f"{name}-release branch was frozen for QA on {rel_date}")
        if notes_pr:
            ev.insert(0, f"Release notes PR ceph.io#{notes_pr['number']} opened {notes_pr['created']}")
            links.insert(0, (f"ceph.io#{notes_pr['number']}", notes_pr["url"]))
        res["eta"] = window(started, models["qe_to_announce"], "QA freeze → announcement")
    else:
        res["stage"] = "backports"
        started = announced[last]
        planned = started + dt.timedelta(CADENCE)
        # Not frozen yet, so it still needs a full QA cycle first
        likely = max(planned, TODAY + dt.timedelta(models["qe_to_announce"]["p50"]))
        res["eta"] = {"basis": "8-week cadence", "likely": likely,
                      "earliest": max(TODAY + dt.timedelta(models["qe_to_announce"]["p25"]),
                                      min(planned, started + dt.timedelta(cadence["p25"]))),
                      "latest": max(likely, started + dt.timedelta(cadence["p75"])),
                      "samples": cadence["n"]}
        ev.insert(0, f"Next point release is scheduled 8 weeks after v{last}; in practice "
                     f"{name} releases have been {cadence['p50']} days apart (middle half "
                     f"{cadence['p25']}–{cadence['p75']})")

    eta = res["eta"]
    age = (TODAY - started).days
    if eta["latest"] < TODAY:
        # Past the usual window: whatever is holding it up isn't visible in the
        # data, so say so rather than invent a date in the past.
        hist_max = (models["qe_to_announce"] if res["stage"] == "qe" else
                    models["tag_to_announce"] if res["stage"] == "tagged" else cadence)["max"]
        eta.update(overdue=True, earliest=TODAY, likely=None,
                   latest=max(TODAY, started + dt.timedelta(hist_max)))
        ev.append(f"Running late: {age} days in this stage, usual is "
                  f"{(models['qe_to_announce'] if res['stage'] == 'qe' else cadence)['p50']} "
                  f"days (longest seen {hist_max})")

    stage_txt = {"backports": "collecting backports",
                 "qe": "in QA / release validation",
                 "tagged": "tagged, awaiting announcement"}[res["stage"]]
    when = (f"overdue, could land any day (historically up to ~{eta['latest']})"
            if eta.get("overdue") else
            f"most likely around {eta['likely']} (range {eta['earliest']} – {eta['latest']})")
    res["summary"] = f"{name.title()} {nxt} is {stage_txt}: {when}."
    return res


def estimate_dev(data, announced, models):
    """The release currently in RC (x.1.y tags, no x.2.0 yet)."""
    known = {vkey(str(s["releases"][0]["version"]))[0] for s in data["yml"]["releases"].values()}
    majors = sorted({vkey(v)[0] for v in data["tags"]} - known)
    majors = [m for m in majors if f"{m}.1.0" in data["tags"] and f"{m}.2.0" not in announced]
    if not majors:
        return None
    m = majors[0]
    # Release names are alphabetical, so the dev branch starts with the letter
    # after the newest named release (tentacle -> umbrella).
    newest = max(data["yml"]["releases"].items(),
                 key=lambda kv: vkey(str(kv[1]["releases"][0]["version"])))[0]
    letter = chr(ord(newest[0]) + 1)
    name = next((b for b in data["branches"] if b.startswith(letter) and "-" not in b), f"v{m}")
    rcs = sorted((v for v in data["tags"] if re.fullmatch(rf"{m}\.1\.\d+", v)), key=vkey)
    ga = f"{m}.2.0"
    res = {"name": name, "major": m, "stage": "rc", "next": ga, "evidence": [], "links": [], "note": "",
           "last": {"version": rcs[-1], "date": data["tags"][rcs[-1]]},
           "target_eol": None, "actual_eol": None}
    ev = res["evidence"]
    ev.append(f"Release candidates tagged so far: {', '.join('v' + v for v in rcs)} "
              f"(first on {data['tags'][rcs[0]]})")
    if ga in data["tags"]:
        res["stage"] = "tagged"
        ev.insert(0, f"v{ga} tagged on {data['tags'][ga]}")
        res["eta"] = window(data["tags"][ga], models["tag_to_announce"], "tag → announcement")
    else:
        res["eta"] = window(data["tags"][rcs[0]], models["rc_to_ga"], "first RC → GA")
    next_rc = f"{m}.1.{vkey(rcs[-1])[2] + 1}"
    rc_pending = (ms := data["milestones"].get(next_rc)) and ms["open_issues"]
    for v in (next_rc, ga):
        if ms := data["milestones"].get(v):
            ev.append(f"GitHub milestone v{v}: {ms['closed_issues']}/"
                      f"{ms['open_issues'] + ms['closed_issues']} PRs merged")
            res["links"].append((f"milestone v{v}", ms["html_url"]))
    if res["stage"] == "rc" and res["eta"]["latest"] < TODAY:
        late = (TODAY - data["tags"][rcs[0]]).days
        ev.append(f"GA is later than usual: {late} days since the first RC, "
                  f"typical is {models['rc_to_ga']['p50']}")
        if rc_pending:
            # Another RC is still to come; GA usually follows the last RC by this much.
            res["eta"] = window(TODAY, models["last_rc_to_ga"], "next RC → GA")
            res["note"] = f"One more release candidate ({next_rc}) comes first."
            ev.append(f"Another RC ({next_rc}) is still collecting fixes; GA has historically "
                      f"come {models['last_rc_to_ga']['p50']} days after the last RC")
        else:
            res["eta"].update(overdue=True, earliest=TODAY, likely=None, latest=max(
                TODAY, data["tags"][rcs[-1]] + dt.timedelta(models["last_rc_to_ga"]["max"])))
    eta = res["eta"]
    when = ("overdue, could land any day" if eta.get("overdue")
            else f"most likely around {eta['likely']} (range {eta['earliest']} – {eta['latest']})")
    res["summary"] = (f"{name.title()} {ga} (the next major release) is in release "
                      f"candidates, latest {rcs[-1]}: GA is {when}.")
    return res


def estimate_planned(data, announced, models, dev):
    """The major after the one in RC: still being kicked off on main."""
    m = (dev["major"] if dev else max(models["by_major"])) + 1
    rv = data["redmine"].get(f"{m}.0.0") or {}
    name = (rv.get("description") or "").strip().lower() or f"v{m}"
    ga = f"{m}.2.0"
    res = {"name": name, "major": m, "stage": "planning", "next": ga, "evidence": [],
           "links": [], "last": None, "target_eol": None, "actual_eol": None,
           "note": "Rough estimate: the release kickoff hasn't merged yet."}
    ev, links = res["evidence"], res["links"]
    k2g, gap = models["kickoff_to_ga"], models["major_release_gap"]

    kick = next((k for k in data["kickoffs"] if name in k["title"].lower()), None)
    if kick:
        ev.append(f"Release kickoff PR #{kick['number']} \"{kick['title']}\" opened {kick['created']}"
                  + (f", merged {kick['merged']}" if kick["merged"] else ", not merged yet"))
        links.append((f"kickoff #{kick['number']}", kick["url"]))
    prev = next((k for k in data["kickoffs"]
                 if dev and dev["name"] in k["title"].lower() and k["merged"]), None)
    if prev:
        ev.append(f"For comparison, the {dev['name'].title()} kickoff took "
                  f"{(prev['merged'] - prev['created']).days} days to merge "
                  f"({prev['created']} → {prev['merged']})")

    windows = []
    if f"{m}.0.0" in data["tags"]:
        res["stage"] = "dev"
        k = data["tags"][f"{m}.0.0"]
        ev.insert(0, f"v{m}.0.0 tagged {k}: development is open on main")
        windows.append(window(k, k2g, "kickoff → GA"))
    elif rv.get("due_date"):
        k = date(rv["due_date"])
        ev.append(f"Redmine version v{m}.0.0 ({name.title()}) is due {k}, the planned kickoff")
        links.append((f"Redmine v{m}.0.0", f"{TRACKER}/versions/{rv['id']}"))
        windows.append(window(k, k2g, "kickoff → GA"))
    if k2g:
        ev.append(f"Kickoff (x.0.0) to GA has historically taken {k2g['p50']} days "
                  f"(middle half {k2g['p25']}–{k2g['p75']})")
    if dev:
        d = dev["eta"]
        windows.append({"earliest": d["earliest"] + dt.timedelta(gap["p25"]),
                        "likely": ga_date(d) + dt.timedelta(gap["p50"]),
                        "latest": d["latest"] + dt.timedelta(gap["p75"])})
        ev.append(f"Major releases come {gap['p50']} days apart (middle half "
                  f"{gap['p25']}–{gap['p75']}); {dev['name'].title()} GA is "
                  + ("overdue" if d.get("overdue") else f"expected around {mid(d)}"))
    if not windows:
        return None
    likely = min(w["likely"] for w in windows) + (
        max(w["likely"] for w in windows) - min(w["likely"] for w in windows)) / 2
    res["eta"] = {"basis": "kickoff and major-release cadence", "likely": likely,
                  "earliest": min(w["earliest"] for w in windows),
                  "latest": max(w["latest"] for w in windows)}
    if len(windows) == 2:
        ev.append(f"The two methods give {windows[0]['likely']} (from kickoff) and "
                  f"{windows[1]['likely']} (from major cadence)")
    res["summary"] = (f"{name.title()} {ga} is the major after {dev['name'].title() if dev else 'the current one'}. "
                      f"It is still being kicked off, so this is a rough estimate: GA around "
                      f"{likely:%B %Y} (range {res['eta']['earliest']:%b %Y} – {res['eta']['latest']:%b %Y}).")
    return res


def mid(w):
    """A single representative date for a window whose 'likely' may be unknown."""
    return w["likely"] or w["earliest"] + (w["latest"] - w["earliest"]) / 2


def ga_date(w):
    """Like mid(), but an overdue release counts as landing today: the middle
    of its remaining window would be a date nothing else on the page shows."""
    return TODAY if w.get("overdue") else mid(w)


def first_slot(start, ga):
    """First 8-weekly release slot (start, start+56, ...) on or after ga -> (k, date)."""
    k = max(0, -(-(ga - start).days // CADENCE))
    return k, start + dt.timedelta(k * CADENCE)


def eol_policy(results, models, announced):
    """Upstream keeps two stable majors. When major M+2 goes GA, major M gets
    one more point release and then reaches end of life."""
    dev = next((r for r in results if r["stage"] in ("rc", "tagged") and r["next"].endswith(".2.0")), None)
    gap = models["major_release_gap"]
    for r in results:
        if r["stage"] == "eol" or r["next"].endswith(".2.0"):
            continue
        m = r["major"]
        successor = m + ACTIVE_MAJORS
        succ_name = next((x["name"].title() for x in results if x["major"] == successor),
                         f"{successor}.2.0")
        # When does the successor that retires this major go GA?
        succ = next((x for x in results if x["major"] == successor and x["next"].endswith(".2.0")), None)
        if f"{successor}.2.0" in announced:
            g = announced[f"{successor}.2.0"]
            ga = {"earliest": g, "likely": g, "latest": g}
        elif succ:
            ga = dict(succ["eta"], likely=ga_date(succ["eta"]))
        else:
            if dev:
                base, n = dev["eta"], successor - dev["major"]
            else:
                base, n = {"earliest": models["latest_ga"], "likely": models["latest_ga"],
                           "latest": models["latest_ga"]}, successor - max(models["by_major"])
            if n < 0:
                continue
            ga = {"earliest": base["earliest"] + dt.timedelta(n * gap["p25"]),
                  "likely": ga_date(base) + dt.timedelta(n * gap["p50"]),
                  "latest": base["latest"] + dt.timedelta(n * gap["p75"])}
        ga_known = f"{successor}.2.0" in announced

        nxt = r["eta"]
        nxt_date = mid(nxt) if not nxt.get("overdue") else TODAY
        a, b, c = vkey(r["next"])
        # The next release lands before the successor's GA -> it isn't the last
        # one; the final release is the first 8-weekly slot after GA.
        k_l, fin_l = first_slot(nxt_date, ga["likely"])
        _, fin_e = first_slot(nxt_date, ga["earliest"])
        _, fin_x = first_slot(nxt_date, ga["latest"])
        if not ga_known and ga["latest"] > nxt_date and k_l == 0:
            k_l = 1  # GA window still open after this release: one more release follows
            fin_l = nxt_date + dt.timedelta(CADENCE)
        final = f"{a}.{b}.{c + k_l}"
        # Releases slip against the 8-week plan; widen by the historical spread.
        pr = models["point_release_gap"]
        r["eol"] = {"final_release": final, "successor": succ_name, "likely": fin_l,
                    "earliest": max(ga["earliest"], nxt_date,
                                    min(fin_e, fin_l) - dt.timedelta(CADENCE - pr["p25"])),
                    "latest": max(fin_x, fin_l) + dt.timedelta(max(0, pr["p75"] - CADENCE)),
                    "far": (fin_l - TODAY).days > 365}
        ev = r["evidence"]
        if r["target_eol"] and r["target_eol"] != fin_l:
            ev.append(f"releases.yml lists target EOL {r['target_eol']}; the two-major policy "
                      f"puts it after {succ_name} GA instead")
        if not r["eol"]["far"]:
            r["note"] = (f"Last {r['name'].title()} release." if final == r["next"] else
                         f"Not the last {r['name'].title()} release: {final} follows {succ_name} GA.")
        if r["eol"]["far"]:
            r["summary"] += (f" EOL: after {succ_name} GA plus one more point release, "
                             f"roughly {fin_l:%B %Y}.")
        elif final == r["next"]:
            r["summary"] += (f" {succ_name} GA {'was ' + str(ga['likely']) if ga_known else 'comes first'}, "
                             f"so {final} is expected to be the last {r['name']} release and "
                             f"{r['name'].title()} goes EOL with it.")
        else:
            ga_when = ("overdue, could land any day" if ga.get("overdue")
                       else f"expected around {ga['likely']}")
            r["summary"] += (f" Not the last one: {succ_name} isn't GA yet ({ga_when}), and {r['name']} gets one more point release after "
                             f"that. Expect the final release ({final}) and EOL around {fin_l} "
                             f"(range {r['eol']['earliest']} – {r['eol']['latest']}).")


def run():
    data = collect()
    announced = announced_dates(data)
    models = lag_models(data, announced)
    results = []
    for name, series in data["yml"]["releases"].items():
        if vkey(str(series["releases"][0]["version"]))[0] < 18:
            continue  # ancient history
        results.append(estimate_series(name, series, data, announced, models))
    dev = estimate_dev(data, announced, models)
    if dev:
        results.append(dev)
    if planned := estimate_planned(data, announced, models, dev):
        results.append(planned)
    eol_policy(results, models, announced)
    # Oldest active first, EOL releases at the end
    results.sort(key=lambda r: (r["stage"] == "eol", r["major"]))
    models.pop("by_major")
    models.pop("latest_ga")
    return {"generated": dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes"),
            "models": models, "releases": results}


# ---------------------------------------------------------------- output

def to_json(out):
    return json.dumps(out, indent=2, default=str)


def to_text(out, only=None):
    lines = []
    for r in out["releases"]:
        if only and only.lower() not in (r["name"], str(r["major"])):
            continue
        lines.append(r["summary"])
        for e in r["evidence"]:
            lines.append(f"  - {e}")
        lines.append("")
    if not lines:
        return f"No release matching {only!r}. Known: " + ", ".join(r["name"] for r in out["releases"])
    m = out["models"]
    lines.append("Historical lags (days, median [p25–p75], last {} samples): ".format(HISTORY)
                 + "; ".join(f"{k.replace('_', ' ')} {v['p50']} [{v['p25']}–{v['p75']}]"
                             for k, v in m.items() if v))
    return "\n".join(lines)


STAGES = ["backports", "qe", "tagged", "released"]
STAGE_LABEL = {"backports": "Backports", "qe": "QA", "tagged": "Tagged", "released": "Announced"}


def to_html(out, fragment=False):
    """fragment=True omits the document skeleton, for hosts that add their own."""
    e = html.escape
    cards = []
    for r in out["releases"]:
        eta = r.get("eta") or {}
        if r["stage"] == "eol":
            big, sub = "End of life", f"since {r['actual_eol']} · final release {r['last']['version']}"
        elif r["stage"] in ("planning", "dev"):
            big = f"~{eta['likely']:%B %Y}"
            sub = f"rough range {eta['earliest']:%b %Y} – {eta['latest']:%b %Y}"
        elif eta.get("overdue"):
            big, sub = "Any day now", f"running late · usually by {eta['latest']}"
        else:
            big = eta["likely"].strftime("%-d %b %Y")
            sub = f"likely range {eta['earliest']:%-d %b} – {eta['latest']:%-d %b}"
        if r["stage"] in STAGES:
            cur = STAGES.index(r["stage"])
            pipe = "".join(f'<li class="{"done" if i < cur else "now" if i == cur else ""}">'
                           f"{STAGE_LABEL[s]}</li>" for i, s in enumerate(STAGES))
            pipe = f'<ol class="pipe">{pipe}</ol>'
        elif r["stage"] in ("rc", "dev", "planning"):
            cur = ["planning", "dev", "rc"].index(r["stage"])
            pipe = "".join(f'<li class="{"done" if i < cur else "now" if i == cur else ""}">{s}</li>'
                           for i, s in enumerate(["Kickoff", "Dev", "RC", "GA"]))
            pipe = f'<ol class="pipe">{pipe}</ol>'
        else:
            pipe = ""
        eol = ""
        if x := r.get("eol"):
            eol = (f"<div class=meta>EOL after {e(x['successor'])} GA, ~<b>{x['likely']:%b %Y}</b></div>"
                   if x["far"] else
                   f"<div class=meta>EOL with <b>{e(x['final_release'])}</b>, ~<b>{x['likely']}</b></div>")
        ev = "".join(f"<li>{e(x)}</li>" for x in r["evidence"])
        lk = " · ".join(f'<a href="{e(u)}" target="_blank" rel="noopener">{e(t)}</a>'
                        for t, u in r["links"])
        cards.append(f"""
<section class="card {e(r['stage'])}">
  <header><h2>{e(r['name'].title())} <span>{r['major']}</span></h2>
  <div class=next>next: <b>{e(r['next'])}</b>{f" · last {e(r['last']['version'])} ({r['last']['date']})" if r['last'] else " · not yet released"}</div></header>
  <div class=eta><div class=big>{e(big)}</div><div class=sub>{e(sub)}</div></div>
  {pipe}{eol}
  <p class=summary>{e(r['summary'])}</p>
  <details><summary>Why</summary><ul>{ev}</ul>{f'<p class=links>{lk}</p>' if lk else ''}</details>
</section>""")
    page = f"""<title>Ceph Release ETA</title>
<style>
:root{{--bg:#f7f7f5;--card:#fff;--fg:#1d1d1b;--mute:#6b6b66;--line:#e3e2dc;--accent:#d2410f;--ok:#2f7d4f;--warn:#b26b00}}
@media (prefers-color-scheme:dark){{:root:not([data-theme=light]){{--bg:#161615;--card:#1f1f1d;--fg:#ecebe6;--mute:#9a9992;--line:#34332f;--accent:#ff7a45;--ok:#5fbf86;--warn:#e0a03a;color-scheme:dark}}}}
:root[data-theme=dark]{{--bg:#161615;--card:#1f1f1d;--fg:#ecebe6;--mute:#9a9992;--line:#34332f;--accent:#ff7a45;--ok:#5fbf86;--warn:#e0a03a;color-scheme:dark}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,-apple-system,sans-serif;padding:16px;padding-block:20px}}
h1{{font-size:18px;margin:0 0 2px}}.top{{max-width:1100px;margin:0 auto 14px}}.top p{{margin:0;color:var(--mute);font-size:13px}}
.grid{{max-width:1100px;margin:0 auto;display:grid;gap:14px;grid-template-columns:repeat(auto-fill,minmax(300px,1fr))}}
.card{{min-width:0;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px}}
.card.eol{{opacity:.6}}h2{{margin:0;font-size:20px}}h2 span{{color:var(--mute);font-weight:400;font-size:15px}}
.next,.meta,.sub{{color:var(--mute);font-size:13px}}.eta{{margin:12px 0}}.big{{font-size:26px;font-weight:650;letter-spacing:-.01em}}
.qe .big,.tagged .big{{color:var(--accent)}}
.pipe{{display:flex;list-style:none;padding:0;margin:8px 0;gap:4px;font-size:12px}}
.pipe li{{flex:1;text-align:center;padding:3px 0;border-radius:4px;background:var(--line);color:var(--mute)}}
.pipe li.done{{background:color-mix(in srgb,var(--ok) 25%,transparent);color:var(--fg)}}
.pipe li.now{{background:var(--accent);color:#fff;font-weight:600}}
.summary{{font-size:14px;margin:10px 0 6px}}details{{font-size:13px;color:var(--mute)}}summary{{cursor:pointer}}
details ul{{padding-left:18px;margin:6px 0}}a{{color:var(--accent)}}
</style>
<div class=top><h1>When is the next Ceph release?</h1>
<p>Estimated from upstream git tags, release branches, ceph.io release-notes PRs and historical lags, not from tracker due dates. Updated {e(out['generated'][:16].replace('T', ' '))} UTC.</p></div>
<div class=grid>{''.join(cards)}</div>"""
    if fragment:
        return page
    return ("<!doctype html><html lang=en><head><meta charset=utf-8>"
            '<meta name=viewport content="width=device-width,initial-scale=1">'
            + page.replace("</style>", "</style></head><body>", 1) + "</body></html>")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("release", nargs="?", help="release name or major, e.g. squid or 19")
    ap.add_argument("--json", metavar="FILE")
    ap.add_argument("--html", metavar="FILE")
    ap.add_argument("--fragment", action="store_true",
                    help="write --html without <html>/<head>/<body> (for hosts that wrap it)")
    args = ap.parse_args()
    out = run()
    if args.json:
        open(args.json, "w").write(to_json(out))
    if args.html:
        open(args.html, "w").write(to_html(out, args.fragment))
    if not (args.json or args.html) or args.release:
        print(to_text(out, args.release))


if __name__ == "__main__":
    sys.exit(main())
