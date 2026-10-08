#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2021"

[dependencies]
---
//! Stand up NooBaa's NSFS gateway over a data root, so that the
//! conversion corpus can be run against *their* implementation and the
//! result compared with RGW.
//!
//! This is the workflow driver.  It does not test anything itself:  it
//! brings a pinned NooBaa up, hands off, and tears down.  The corpus
//! lives in `../s3-tests-rs/tests/test_corpus/` and is run against
//! whichever endpoint is up.
//!
//! See `README.md` beside this file for the decisions behind the
//! shape.  The short version:
//!
//!   * **Source, not a container.**  Containers are not helpful during
//!     development, and the data root has to be a plain directory that
//!     RGW can open afterwards with the same uid.
//!   * **Sequential handoff against one tree.**  NooBaa writes, stops,
//!     then RGW opens the same directory.  That is the adoption story
//!     -- drain, then upgrade -- and simultaneous access is a separate
//!     feature.
//!   * **A git worktree at the pin**, never a checkout of the
//!     developer's own clone.  Reading noobaa-core's source at some other
//!     commit is a thing we do constantly;  this must not disturb it.
//!
//! Run `standup.rs list` to see the steps, `standup.rs <step>` to run
//! one, `standup.rs up` to run every step through serving.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/* ---------------------------------------------------------------- */

/// Where everything lives.  Every one is overridable, because the
/// pin in particular is a hypothesis rather than a fact -- see
/// `docs/NSFS_ADOPTION_WORK_PLAN.md` and the Scale team question
/// about which commit 5.23 actually ships.
struct Env {
    /// the developer's own clone, read only -- never checked out
    src: PathBuf,
    /// the commit to stand up
    pin: String,
    /// the worktree, the config store, the data root, the logs.
    /// **Required** -- see `Env::load`
    work: PathBuf,
    /// what `rgw-vstart.sh --data-root` is given.  The tree itself is
    /// `data/root`, because that is the layout that flag expects
    data: PathBuf,
    /// the ceph build directory, for the handoff.  **Required** for
    /// `adopt`, and only for `adopt`
    ceph_build: Option<PathBuf>,
    /// the NooBaa HTTP port
    port: u16,
    /// the account NooBaa serves as
    account: String,
    /// the one bucket, which both gateways will serve
    bucket: String,
    /// fixed, so the corpus can be pointed at it without discovery
    access_key: String,
    secret_key: String,
    /// the s3-tests-rs checkout the corpus lives in.  A sibling of
    /// this driver, so it needs no setting in the normal case
    suite: PathBuf,
    /// which corpus tier `capture` runs
    tier: String,
}

/// Every setting, from `interchange.conf` and then the environment.
///
/// `INTERCHANGE_WORK` and `work = ` are the same setting;  the environment
/// wins.
fn settings() -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    let conf = interchange_home().join("interchange.conf");
    if let Ok(text) = std::fs::read_to_string(&conf) {
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once('=') {
                m.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
    }
    for (k, v) in std::env::vars() {
        if let Some(rest) = k.strip_prefix("INTERCHANGE_") {
            m.insert(rest.to_ascii_lowercase(), v);
        }
    }
    m
}

impl Env {
    /// **`work` has no default and this stops without it.**
    ///
    /// It is where a gigabyte of worktree goes *and* where the config
    /// store and the data root live -- the artifacts this exercise
    /// produces.  The first version of this defaulted to
    /// `~/.cache/nsfs-interchange`, which was wrong twice over:  it
    /// put the deliverable somewhere nominally disposable, and it
    /// chose the location without being asked.
    fn load() -> Result<Env, String> {
        let s = settings();
        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".into());

        let Some(work) = s.get("work").filter(|v| !v.is_empty()) else {
            return Err(format!(
                "`work` is not set, and this driver will not choose one for you.\n\n  \
                 It holds the NooBaa worktree -- upwards of a gigabyte -- and the\n  \
                 config store and the data root, which are the artifacts this\n  \
                 produces.  Put it somewhere deliberate, outside any ceph path:\n\n    \
                 INTERCHANGE_WORK=/path/to/interchange standup.rs <step>\n\n  \
                 or, standing, in {}:\n\n    work = /path/to/interchange\n\n  \
                 See interchange.conf.sample.",
                interchange_home().join("interchange.conf").display()
            ));
        };
        let work = PathBuf::from(work);

        let get = |k: &str, d: &str| -> String {
            s.get(k).filter(|v| !v.is_empty()).cloned().unwrap_or_else(|| d.to_string())
        };

        Ok(Env {
            src: get("noobaa_src", &format!("{home}/dev/noobaa-core")).into(),
            pin: get("pin", "08d063f86"),
            /* `rgw-vstart.sh --data-root DIR` serves DIR/root, so the
             * tree NooBaa writes has to be data/root for the handoff to
             * line up without copying anything. */
            data: get("data", &work.join("data").display().to_string()).into(),
            ceph_build: s
                .get("ceph_build")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            work,
            /* HTTP rather than HTTPS deliberately:  the corpus talks to
             * this with the Rust SDK and a self-signed certificate is a
             * problem worth not having. */
            port: get("port", "6001").parse().unwrap_or(6001),
            account: get("account", "interchange"),
            bucket: get("bucket", "interchange"),
            /* Fixed rather than generated:  the corpus has to be
             * pointed at this endpoint, and a credential that changes
             * per run means a config file that changes per run. */
            access_key: get("access_key", "INTERCHANGEACCESSKEY"),
            secret_key: get("secret_key", "interchangesecretkeyinterchangesecretkey"),
            /* `../s3-tests-rs` from here.  The corpus is a modality of
             * that suite rather than a separate tool, so the two move
             * together and the default is right whenever this driver
             * is run from the tree it was committed to. */
            suite: get(
                "suite",
                &interchange_home()
                    .parent()
                    .unwrap_or(&interchange_home())
                    .join("s3-tests-rs")
                    .display()
                    .to_string(),
            )
            .into(),
            tier: get("tier", "coverage"),
        })
    }

    /// The tree itself:  what both gateways open.
    fn tree(&self) -> PathBuf {
        self.data.join("root")
    }

    fn worktree(&self) -> PathBuf {
        self.work.join(format!("noobaa-{}", self.pin))
    }

    /// The NC configuration directory -- the config store the
    /// account and bucket records live in.  Separate from the data
    /// root:  the records describe the tree, they are not in it.
    fn config_root(&self) -> PathBuf {
        self.work.join("config")
    }

    fn node(&self) -> String {
        env_or("INTERCHANGE_NODE", "node")
    }

    fn pid_file(&self) -> PathBuf {
        self.work.join("nsfs.pid")
    }

    fn log_file(&self) -> PathBuf {
        self.work.join("nsfs.log")
    }

    /// Where a capture's artifacts go.
    ///
    /// **The side is in the path, never in the artifacts.**  The
    /// comparison between two gateways is `diff`, so a fact saying
    /// which gateway produced it would differ on every line and the
    /// diff would stop saying anything.  `capture/noobaa` here, and
    /// `capture/rgw` for the run after `adopt`.
    fn capture_dir(&self, side: &str) -> PathBuf {
        self.work.join("capture").join(side)
    }

    fn capture_conf(&self) -> PathBuf {
        self.work.join("s3tests-noobaa.conf")
    }

    fn capture_log(&self) -> PathBuf {
        self.work.join("capture").join("noobaa.log")
    }
}

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

/* ---------------------------------------------------------------- */

/// The step model.
///
/// Listed in order, each independently runnable, because a stand-up
/// that can only be run end to end is one you cannot debug.  A step is
/// expected to be idempotent:  running it twice should be a no-op the
/// second time, not an error.
const STEPS: &[(&str, &str)] = &[
    ("pin",      "resolve the pin and verify the commit is present"),
    ("worktree", "create or refresh a git worktree at the pin"),
    ("patch",    "apply the patches this pin needs to build"),
    ("build",    "install the dependency tree, and build NooBaa's native addon"),
    ("config",   "write the NooBaa account and bucket records over the data root"),
    ("serve",    "start the NooBaa NSFS endpoint"),
    ("capture",  "run the corpus against the NooBaa endpoint"),
    ("stop",     "stop the endpoint, leaving the tree"),
    ("adopt",    "start RGW against the same tree"),
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("list");

    /* `list` is how you find out what to set, so it must work before
     * anything is set. */
    if cmd == "list" {
        list();
        return;
    }

    let r = Env::load().and_then(|env| match cmd {
        "pin" => pin(&env),
        "worktree" => worktree(&env),
        "patch" => patch(&env),
        "build" => build(&env),
        "config" => config(&env),
        "serve" => serve(&env),
        "capture" => capture(&env),
        "stop" => stop(&env),
        "adopt" => adopt(&env),
        "up" => ["pin", "worktree", "patch", "build", "config"]
            .iter()
            .try_for_each(|s| match *s {
                "pin" => pin(&env),
                "worktree" => worktree(&env),
                "patch" => patch(&env),
                "build" => build(&env),
                _ => config(&env),
            }),
        other => Err(format!("unknown step {other:?};  try `list`")),
    });

    if let Err(e) = r {
        eprintln!("\x1b[31mstandup: {e}\x1b[0m");
        std::process::exit(1);
    }
}

fn list() {
    println!("steps:");
    for (name, what) in STEPS {
        println!("  {name:10} {what}");
    }

    println!("\nEach setting is an INTERCHANGE_<NAME> environment variable, or the");
    println!("same name in interchange.conf beside this driver;  the environment");
    println!("wins.\n");

    let s = settings();
    let shown = [
        ("work", "REQUIRED -- the worktree, config store, data root, logs"),
        ("ceph_build", "REQUIRED for `adopt` -- the ceph build directory"),
        ("noobaa_src", "the noobaa-core clone, read only"),
        ("pin", "the commit to stand up"),
        ("data", "what --data-root is given;  the tree is data/root"),
        ("port", "the NooBaa HTTP port"),
        ("account", ""),
        ("bucket", ""),
        ("suite", "the s3-tests-rs checkout;  defaults to a sibling of this driver"),
        ("tier", "which corpus tier `capture` runs"),
    ];
    for (k, note) in shown {
        match s.get(k) {
            Some(v) if !v.is_empty() => println!("  {k:12} {v}"),
            _ => println!("  {k:12} (unset)  {note}"),
        }
    }
    println!("\n`up` runs through `config`.  `serve`, `capture`, `stop` and `adopt`");
    println!("are each run deliberately:  they start and stop gateways over a tree");
    println!("that is the artifact, and chaining them past a failure is how a tree");
    println!("gets destroyed rather than handed over.");
}

/* ---------------------------------------------------------------- */

/// Resolve the pin against the developer's clone, and say what it is.
///
/// Read only.  The clone's HEAD and index are not touched, because
/// reading noobaa-core's source at some other commit is a thing we do
/// constantly and this must not disturb it.
fn pin(env: &Env) -> Result<(), String> {
    if !env.src.join(".git").exists() {
        return Err(format!(
            "{} is not a git clone of noobaa-core;  set INTERCHANGE_NOOBAA_SRC",
            env.src.display()
        ));
    }
    let full = git(&env.src, &["rev-parse", &format!("{}^{{commit}}", env.pin)])
        .map_err(|_| {
            format!(
                "commit {} is not in {};  fetch it, or set INTERCHANGE_PIN",
                env.pin,
                env.src.display()
            )
        })?;
    let desc = git(&env.src, &["log", "-1", "--format=%ad  %s", "--date=short", &full])?;
    println!("pin      {}", &full[..12.min(full.len())]);
    println!("         {desc}");

    /* The pin is a hypothesis, and saying so here is cheaper than
     * someone rediscovering it.  08d063f86 is the *start* of the
     * window in which noobaa-core carried the 5.23 version string;
     * whether Scale ships that surface is an open question with the
     * Scale team, and Matt's standing estimate is 85%. */
    if env.pin.starts_with("08d063f8") {
        println!("         NOTE: this is the 5.23 window *start*, a hypothesis");
        println!("               and not a confirmed pin.  See the work plan.");
    }
    Ok(())
}

/// A worktree at the pin, created once and reused.
fn worktree(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    if wt.join(".git").exists() {
        let at = git(&wt, &["rev-parse", "HEAD"])?;
        println!("worktree {} (already at {})", wt.display(), &at[..12.min(at.len())]);
        return Ok(());
    }
    std::fs::create_dir_all(&env.work)
        .map_err(|e| format!("cannot create {}: {e}", env.work.display()))?;
    /* --detach, because a named branch in someone else's clone is a
     * side effect they did not ask for. */
    git(
        &env.src,
        &[
            "worktree",
            "add",
            "--detach",
            &wt.display().to_string(),
            &env.pin,
        ],
    )?;
    println!("worktree {}", wt.display());
    Ok(())
}

/// Apply the patches this pin needs in order to build.
///
/// One so far:  `fs_napi.cpp`'s GPFS request builder wrote the
/// attribute name through `payload.buffer[0]`, a zero-length array at
/// the end of `payload`, reaching the sibling `buffer` member only
/// because the two happen to be adjacent.  GCC 15 reports
/// `-Warray-bounds` on it and their own `-Werror` then refuses the
/// build.  The diagnostic is right:  the write is out of bounds as far
/// as the language is concerned, and the compiler may optimise on
/// that.  The patch writes through the member that has the storage,
/// which is where the reply is read from anyway.
///
/// **Patched rather than silenced.**  Demoting the warning was the
/// first thing tried and it was wrong -- it would have left a real
/// aliasing hazard in the addon that every capture from NooBaa
/// depends on, where a miscompilation would surface as what looks like
/// a format difference.
///
/// Upstream may well have fixed this in a later commit;  this driver
/// deliberately does not go looking.  The pin is the pin, and carrying
/// a patch against it is cheaper than tracking NooBaa's history.
///
/// Idempotent:  an applied patch is detected by whether it reverses.
fn patch(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    if !wt.exists() {
        return Err("no worktree;  run `worktree` first".into());
    }
    let dir = interchange_home().join("patches");
    let mut found = 0;
    let mut applied = 0;

    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => {
            println!("patch    no patches/ directory;  nothing to apply");
            return Ok(());
        }
    };
    let mut names: Vec<PathBuf> =
        entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
    names.sort();

    for p in names {
        let base = p.file_name().unwrap_or_default().to_string_lossy().to_string();
        if !base.ends_with(".patch") || !base.starts_with(&env.pin) {
            continue;
        }
        found += 1;
        let path = p.display().to_string();
        /* already applied?  then it reverses cleanly */
        if git(&wt, &["apply", "--reverse", "--check", &path]).is_ok() {
            println!("patch    {base} already applied");
            continue;
        }
        git(&wt, &["apply", "--check", &path])
            .map_err(|e| format!("{base} does not apply to this worktree: {e}"))?;
        git(&wt, &["apply", &path])?;
        println!("patch    {base} applied");
        applied += 1;
    }

    if found == 0 {
        println!("patch    no patches for {}", env.pin);
    } else {
        println!("patch    {found} for this pin, {applied} newly applied");
    }
    Ok(())
}

/// Where this driver and its data files live.
fn interchange_home() -> PathBuf {
    std::env::var("INTERCHANGE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| ".".into()))
}

/// Install the dependency tree, from a lock we control.
///
/// **NooBaa's lock at this pin does not `npm ci`.**  `npm ci` is the
/// right verb -- it installs a lock exactly, refuses when the lock and
/// `package.json` disagree, and never rewrites the lock -- and at
/// `08d063f86` it refuses:  `package-lock.json` is missing
/// `@emnapi/core` and `@emnapi/runtime`, which `@napi-rs/wasm-runtime`
/// requires, and carries a stale `@emnapi/wasi-threads`.  The lock is
/// incomplete.  `package.json` declares neither `engines` nor
/// `packageManager`, so there is no npm version to select that makes
/// an incomplete lock complete.
///
/// So the tree is pinned *here* instead.  The first run resolves it
/// with `npm install` and writes the result to `locks/`;  every run
/// after that copies that lock in and uses `npm ci`, which is exact.
/// The resolved lock is committed alongside this driver, so the
/// dependency tree is reproducible for us even though it is not
/// reproducible from NooBaa's commit alone.
///
/// A container would not have helped:  an image bakes in whatever one
/// arbitrary resolution produced, which is this, without saying so.
///
/// Their `package-lock.json` is restored afterwards, so the worktree
/// stays identical to the pin.
fn build(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    if !wt.exists() {
        return Err("no worktree;  run `worktree` first".into());
    }
    lock_is_pristine(&wt, "before")?;

    /* The dependency tree and the native addon are two installs, and
     * skipping the first must not skip the second -- which it did,
     * when this returned early instead of falling through. */
    if wt.join("node_modules").exists() {
        println!("build    node_modules present, skipping the install");
        return native(env);
    }

    let recorded = pinned_lock(env);
    let in_worktree = wt.join("package-lock.json");

    let verb = if recorded.exists() {
        println!("build    npm ci against {}", recorded.display());
        std::fs::copy(&recorded, &in_worktree)
            .map_err(|e| format!("cannot place our lock: {e}"))?;
        "ci"
    } else {
        println!("build    no recorded lock for this pin yet;  resolving with");
        println!("         npm install, then recording it in locks/");
        "install"
    };

    let st = Command::new("npm")
        .args([verb, "--no-audit", "--no-fund"])
        .current_dir(&wt)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("cannot run npm: {e}"))?;

    if !st.success() {
        /* leave the worktree as the pin has it, whatever happened */
        let _ = git(&wt, &["checkout", "--", "package-lock.json"]);
        return Err(format!("npm {verb} failed ({st})"));
    }

    if verb == "install" {
        std::fs::create_dir_all(recorded.parent().unwrap())
            .map_err(|e| format!("cannot create locks/: {e}"))?;
        std::fs::copy(&in_worktree, &recorded)
            .map_err(|e| format!("cannot record our lock: {e}"))?;
        println!("build    recorded {}", recorded.display());
        println!("         commit it:  that is what makes this pin reproducible");
    }

    git(&wt, &["checkout", "--", "package-lock.json"])?;
    lock_is_pristine(&wt, "after")?;
    native(env)
}

/// NooBaa's native addon, which `npm ci` does not build.
///
/// `package.json` overrides the install hook -- *"install hook
/// override to avoid npm default hook calling node-gyp"* -- so
/// installing the dependency tree leaves `build/Release/nb_native.node`
/// absent, and every entry point fails at require time with "Could not
/// locate the bindings file".  `npm run build` is
/// `node-gyp configure && node-gyp build`, and it has to be asked for.
///
/// Needs a C++ toolchain and node headers on the host.  This is the
/// part no lockfile pins, and the one honest argument for a container
/// -- which is about the *toolchain*, not about dependency resolution.
fn native(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    let addon = wt.join("build/Release/nb_native.node");
    if addon.exists() {
        println!("native   nb_native.node present, skipping");
        return Ok(());
    }
    /* No warning is demoted here.  Their -Werror stays as they set
     * it;  the one diagnostic that stopped the build is fixed in the
     * source by `patch`, because it was reporting a real out-of-bounds
     * write rather than a false positive.  See patches/. */
    println!("native   node-gyp configure && build");
    let st = Command::new("npm")
        .args(["run", "build:native"])
        .current_dir(&wt)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("cannot run npm: {e}"))?;
    if !st.success() {
        return Err(format!("node-gyp build failed ({st})"));
    }
    if !addon.exists() {
        return Err(format!(
            "node-gyp reported success and {} is not there",
            addon.display()
        ));
    }
    println!("native   {}", addon.display());
    Ok(())
}

/// Write the NooBaa account and bucket records over the data root.
///
/// Through `manage_nsfs.js`, NooBaa's own CLI, rather than by composing
/// JSON:  the whole point of standing NooBaa up is to get
/// records *they* wrote.  Hand-written records would be our reading of
/// NooBaa's schema again, which is the thing this stage exists to stop
/// relying on.
///
/// **uid and gid are the running user's.**  The handoff is sequential
/// against one tree -- NooBaa writes, stops, RGW opens the same
/// directory -- so the files have to be owned by the identity RGW will
/// run as, or the second half of the exercise fails on permissions
/// rather than on anything interesting.
///
/// Idempotent:  a second run finds the records present and says so.
/// NooBaa's CLI fails a duplicate `add`, which is correct of it and not
/// what this driver wants.
fn config(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    if !wt.join("node_modules").exists() {
        return Err("no dependency tree;  run `build` first".into());
    }
    let (uid, gid) = running_ids()?;
    std::fs::create_dir_all(env.config_root())
        .map_err(|e| format!("cannot create {}: {e}", env.config_root().display()))?;
    /* The tree, not `data`:  `rgw-vstart.sh --data-root DIR` serves
     * DIR/root, so NooBaa has to write into data/root for the handoff
     * to line up without copying anything. */
    std::fs::create_dir_all(env.tree())
        .map_err(|e| format!("cannot create {}: {e}", env.tree().display()))?;

    println!("config   config_root {}", env.config_root().display());
    println!("         tree        {} (uid {uid}, gid {gid})", env.tree().display());

    if nsfs_cli(env, &["account", "status", "--name", &env.account]).is_ok() {
        println!("         account {:?} already present", env.account);
    } else {
        nsfs_cli(
            env,
            &[
                "account", "add",
                "--name", &env.account,
                "--uid", &uid.to_string(),
                "--gid", &gid.to_string(),
                "--new_buckets_path", &env.tree().display().to_string(),
                "--access_key", &env.access_key,
                "--secret_key", &env.secret_key,
            ],
        )?;
        println!("         account {:?} created", env.account);
    }

    let bucket_path = env.tree().join(&env.bucket);
    if nsfs_cli(env, &["bucket", "status", "--name", &env.bucket]).is_ok() {
        println!("         bucket {:?} already present", env.bucket);
    } else {
        std::fs::create_dir_all(&bucket_path)
            .map_err(|e| format!("cannot create {}: {e}", bucket_path.display()))?;
        nsfs_cli(
            env,
            &[
                "bucket", "add",
                "--name", &env.bucket,
                "--owner", &env.account,
                "--path", &bucket_path.display().to_string(),
            ],
        )?;
        println!("         bucket {:?} created at {}", env.bucket, bucket_path.display());
    }

    /* The NC config file, which is where a config override goes.
     *
     * ALLOW_HTTP, because in NC mode the S3 endpoint refuses to serve
     * HTTP at all unless it is set -- `if (nsfs_config_root &&
     * !config.ALLOW_HTTP)` in endpoint.js, which then only
     * `dbg.warn`s "HTTP is not allowed for NC NSFS" and carries on
     * with no listener.  A warning in a log and a closed port is a
     * confusing pair to debug, which is the reason this comment is
     * here.
     *
     * HTTP rather than a certificate because the corpus talks to this
     * with the Rust SDK;  a self-signed cert is a problem worth not
     * having when nothing here leaves the host. */
    let cfg = env.config_root().join("config.json");
    std::fs::write(&cfg, "{\n    \"ALLOW_HTTP\": true\n}\n")
        .map_err(|e| format!("cannot write {}: {e}", cfg.display()))?;
    println!("         wrote {} (ALLOW_HTTP)", cfg.display());

    /* What `capture` will need, written where it can find it rather
     * than rediscovered. */
    let creds = env.work.join("credentials");
    std::fs::write(
        &creds,
        format!(
            "# written by standup.rs config\nendpoint=http://127.0.0.1:{}\n\
             access_key={}\nsecret_key={}\nbucket={}\ntree={}\n",
            env.port,
            env.access_key,
            env.secret_key,
            env.bucket,
            env.tree().display()
        ),
    )
    .map_err(|e| format!("cannot write {}: {e}", creds.display()))?;
    println!("         wrote {}", creds.display());
    Ok(())
}

/// Start the NooBaa S3 endpoint over the data root.
///
/// HTTP only.  `--https_port -1` turns TLS off the way they turn IAM
/// off, and the corpus talks to this with the Rust SDK, for which a
/// self-signed certificate is a problem worth not having.
///
/// Backgrounded with its pid recorded, because the handoff is
/// sequential:  something has to stop this before RGW opens the same
/// tree, and `stop` needs to know what to stop.
///
/// Idempotent:  an endpoint already answering is left alone.
fn serve(env: &Env) -> Result<(), String> {
    if let Some(pid) = running(env) {
        println!("serve    already up, pid {pid}, port {}", env.port);
        return Ok(());
    }
    let wt = env.worktree();
    if !wt.join("build/Release/nb_native.node").exists() {
        return Err("no native addon;  run `build` first".into());
    }
    if !env.config_root().join("buckets").exists() {
        return Err("no bucket records;  run `config` first".into());
    }

    let log = std::fs::File::create(env.log_file())
        .map_err(|e| format!("cannot create {}: {e}", env.log_file().display()))?;
    let errlog = log
        .try_clone()
        .map_err(|e| format!("cannot clone the log handle: {e}"))?;

    let child = Command::new(env.node())
        .args([
            "src/cmd/nsfs.js",
            "--config_root",
            &env.config_root().display().to_string(),
            "--http_port",
            &env.port.to_string(),
            /* Every TLS listener, not only S3's.
             *
             * `if (https_port > 0)` is the test, so a non-positive
             * port disables one -- but STS and Vector have their own
             * defaults (7443, 14443) and keep them unless told
             * otherwise.  Passing -1 for S3 alone left those two
             * starting, and the failure surfaced as `EACCES listen
             * EACCES` from start_https_server, which reads as a
             * permission problem on the port asked for rather than as
             * a port nobody asked for.  IAM already defaults to -1. */
            /* `--flag=-1`, not `--flag -1`.
             *
             * They parse with minimist, which reads a bare `-1` as a
             * short option rather than as a value:  `--https_port -1`
             * leaves https_port `true`, `Number(true) || 6443` is 1,
             * and the endpoint tries to listen on port 1 and gets
             * EACCES.  The error names `0.0.0.0:1`, which is the only
             * clue that the argument never arrived. */
            "--https_port=-1",
            "--https_port_sts=-1",
            "--https_port_vector=-1",
        ])
        .current_dir(&wt)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(errlog))
        .spawn()
        .map_err(|e| format!("cannot start {}: {e}", env.node()))?;

    let pid = child.id();
    std::fs::write(env.pid_file(), pid.to_string())
        .map_err(|e| format!("cannot write {}: {e}", env.pid_file().display()))?;
    println!("serve    started pid {pid}, log {}", env.log_file().display());

    /* Wait for the port rather than for a message in the log:  the
     * question is whether it will answer, and that is what the corpus
     * will ask it. */
    for i in 0..120 {
        if std::net::TcpStream::connect(("127.0.0.1", env.port)).is_ok() {
            println!("serve    answering on http://127.0.0.1:{}", env.port);
            return Ok(());
        }
        if !alive(pid) {
            return Err(format!(
                "it exited before answering;  see {}",
                env.log_file().display()
            ));
        }
        if i == 10 {
            println!("serve    waiting for the port...");
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(format!(
        "it did not answer on port {} within a minute;  see {}",
        env.port,
        env.log_file().display()
    ))
}

/// Run the corpus against the NooBaa endpoint, and keep what it produced.
///
/// This is the step the rest of the stand-up exists for.  Everything
/// before it arranges for a pinned NooBaa to be serving a tree;  this
/// points the conversion corpus at it and keeps the two artifacts the
/// corpus produces per axis -- the S3 manifest and the disk inventory
/// -- under `capture/noobaa`.
///
/// **A failing axis is the finding, not a broken step.**  The corpus
/// asserts what the nsfs driver should do, and NooBaa is a different
/// implementation:  a disagreement is exactly what a capture is for.
/// Every axis writes its manifest and takes its inventory *before* it
/// asserts, which is what makes this usable -- a failed axis still
/// leaves its artifacts behind.  So the corpus is run `--no-fail-fast`
/// and a non-zero exit is reported rather than propagated.  What this
/// step does fail on is producing no artifacts at all, because that
/// means the corpus never reached a write.
///
/// **`CORPUS_OBJECT_MODE=any`.**  The corpus checks that every object
/// is published 0600, which is a statement about the nsfs driver and not
/// about S3.  Left strict, every object NooBaa wrote would be
/// reported as an offender and the differences worth reading would be
/// buried.
///
/// Idempotent:  the artifacts are rewritten, and the buckets the
/// corpus uses have stable names so a second run meets its own.
fn capture(env: &Env) -> Result<(), String> {
    let Some(pid) = running(env) else {
        return Err("the NooBaa endpoint is not running;  run `serve` first".into());
    };
    /* Whether it answers, not whether a pid exists.  The corpus will
     * ask the same question and a clearer error here is cheaper than
     * an SDK timeout there. */
    if std::net::TcpStream::connect(("127.0.0.1", env.port)).is_err() {
        return Err(format!(
            "pid {pid} is alive but nothing accepts a connection on port {};\n  see {}",
            env.port,
            env.log_file().display()
        ));
    }
    if !env.suite.join("Cargo.toml").exists() {
        return Err(format!(
            "{} is not the s3-tests-rs suite.  It is normally a sibling of this\n  \
             driver;  set INTERCHANGE_SUITE, or `suite = ` in {}.",
            env.suite.display(),
            interchange_home().join("interchange.conf").display()
        ));
    }

    let dir = env.capture_dir("noobaa");
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;

    /* Clear the artifacts of a previous capture, and only those.
     *
     * A manifest this run no longer produces would otherwise sit in
     * the directory and read as a fact about this run.  By suffix and
     * one level deep -- not a recursive remove -- because the last
     * thing this project got wrong in exactly this shape deleted a
     * tree it was supposed to be reading. */
    let mut stale = 0usize;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().to_string();
            if name.ends_with(".manifest") || name.ends_with(".inventory") {
                if std::fs::remove_file(e.path()).is_ok() {
                    stale += 1;
                }
            }
        }
    }
    if stale > 0 {
        println!("capture  cleared {stale} artifacts from a previous run");
    }

    let conf = write_capture_conf(env)?;
    println!("capture  endpoint http://127.0.0.1:{}", env.port);
    println!("         conf     {}", conf.display());
    println!("         tier     {}", env.tier);
    println!("         out      {}", dir.display());
    println!("         log      {}", env.capture_log().display());

    /* nextest if it is there, the built-in harness if it is not.  The
     * house standard is nextest and its per-test failure blocks are
     * what makes a multi-axis disagreement readable;  requiring it
     * would make this step fail for a reason that has nothing to do
     * with the interchange. */
    let have_nextest = Command::new("cargo")
        .args(["nextest", "--version"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    /* `--features fails_on_noobaa`.
     *
     * The gates that feature carries are permanent and name what
     * NooBaa's implementation does not do -- the multipart ETag is an
     * identity token rather than a digest with a part count, bucket-
     * level object lock configuration answers NotImplemented, and
     * three scaffolding names are hidden by prefix match.  Without it
     * `capture`'s exit status carries no signal:  a capability fact
     * and a real disagreement look the same.
     *
     * It does not suppress everything that fails.  The bucket-
     * configuration axis still fails here, deliberately, because that
     * one is our instrument looking only where the nsfs driver stores
     * configuration -- gating it would hide a defect of ours behind a
     * fact about NooBaa. */
    let runner = if have_nextest {
        "cargo nextest run --features fails_on_noobaa --test test_corpus \
         --no-fail-fast --test-threads=4"
    } else {
        println!("         (cargo-nextest is not installed;  using cargo test)");
        "cargo test --features fails_on_noobaa --test test_corpus \
         --no-fail-fast -- --test-threads=4"
    };

    /* Through bash so the output reaches the terminal and the log at
     * once:  a capture is slow enough that silence reads as a hang,
     * and a disagreement that only ever existed in a scrollback is
     * not a record.  `pipefail` so the runner's status survives tee.
     *
     * Every path goes through the environment rather than into the
     * command line, so nothing here has to be quoted correctly. */
    let script = format!(
        "set -o pipefail; cd \"$INTERCHANGE_SUITE\" || exit 1; \
         {runner} 2>&1 | tee \"$INTERCHANGE_CAPTURE_LOG\""
    );
    let st = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .env("INTERCHANGE_SUITE", env.suite.display().to_string())
        .env("INTERCHANGE_CAPTURE_LOG", env.capture_log().display().to_string())
        .env("S3TEST_CONF", conf.display().to_string())
        .env("CORPUS_OUT", dir.display().to_string())
        /* The disk half.  Same host by construction -- the NooBaa endpoint
         * is on 127.0.0.1 -- so the inventory can be taken, and it is
         * the half that answers "what did they write". */
        .env("CORPUS_DATA_ROOT", env.tree().display().to_string())
        /* The other place a bucket's configuration can live.  NooBaa
         * keeps tagging, CORS, policy, lifecycle, logging and
         * encryption in the bucket's record and none of them reaches
         * the bucket directory, so an axis reading only the tree
         * concludes they were accepted and dropped. */
        .env("CORPUS_CONFIG_ROOT", env.config_root().display().to_string())
        .env("CORPUS_TIER", &env.tier)
        /* NooBaa publishes its own modes.  See the doc comment. */
        .env("CORPUS_OBJECT_MODE", "any")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("cannot run the corpus: {e}"))?;

    /* What landed.  The artifacts are the product, so they are what
     * gets reported -- and an empty directory is the one outcome that
     * means this step did not happen. */
    let mut artifacts: Vec<(String, usize)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().to_string();
            if !(name.ends_with(".manifest") || name.ends_with(".inventory")) {
                continue;
            }
            let facts = std::fs::read_to_string(e.path())
                .map(|t| t.lines().count())
                .unwrap_or(0);
            artifacts.push((name, facts));
        }
    }
    artifacts.sort();

    if artifacts.is_empty() {
        return Err(format!(
            "the corpus produced nothing in {}.\n  \
             It exited {st}, and every axis writes its manifest before it\n  \
             asserts -- so no artifacts means no axis got as far as writing\n  \
             one.  Read {} from the top:  the first failure is the real one.",
            dir.display(),
            env.capture_log().display()
        ));
    }

    println!();
    println!("capture  {} artifacts in {}", artifacts.len(), dir.display());
    let mut facts = 0usize;
    for (name, n) in &artifacts {
        println!("         {n:6}  {name}");
        facts += n;
    }
    println!("         {facts:6}  total");

    if st.success() {
        println!("capture  every axis agreed with what the corpus expects of the nsfs");
        println!("         driver, which is a stronger result than this step needs.");
    } else {
        println!();
        println!("capture  the corpus exited {st}:  one or more axes disagreed with");
        println!("         NooBaa.  That is the finding -- the artifacts above");
        println!("         are complete either way.  {}", env.capture_log().display());
    }
    Ok(())
}

/// An `s3tests.conf` for the NooBaa endpoint.
///
/// Written rather than borrowed:  the suite's own config points at
/// whatever vstart last brought up, and a capture that quietly ran
/// against RGW while claiming to have run against NooBaa is
/// the worst failure this step could have.
///
/// **Every identity section the loader requires, not only the one the
/// corpus uses.**  `load_config` is all or nothing -- it demands
/// `[s3 main]`, `[s3 alt]`, `[s3 tenant]`, `[iam]`, `[iam root]` and
/// `[iam alt root]`, five keys each, and returns one error for the
/// first it cannot find.  A conf carrying only `[s3 main]` fails every
/// axis at config load, before any of them writes an artifact.
///
/// The identities other than main are placeholders, and deliberately
/// not copies of main's credential:  there is one NC account here, so
/// an axis reaching for a second identity against NooBaa should
/// fail rather than silently act as the first.  Each placeholder names
/// its own section so the failure says which one was asked for.
fn write_capture_conf(env: &Env) -> Result<PathBuf, String> {
    let p = env.capture_conf();

    /* 20 characters, the shape an S3 access key has, so a failure
     * reads as "this identity does not exist" rather than as a
     * malformed credential. */
    let placeholder = |what: &str| -> String {
        let mut k = format!("NOSUCH{}", what.to_ascii_uppercase());
        while k.len() < 20 {
            k.push('X');
        }
        k.truncate(20);
        k
    };
    let unused = |section: &str, what: &str| -> String {
        format!(
            "[{section}]\n\
             # Not an identity on NooBaa.  Required by the suite's config\n\
             # loader, unused by the corpus, and not a copy of main's credential\n\
             # on purpose:  one NC account serves this tree.\n\
             display_name = {what}\n\
             user_id = {what}\n\
             email = {what}@interchange.invalid\n\
             access_key = {ak}\n\
             secret_key = {ak}{ak}\n",
            section = section,
            what = what,
            ak = placeholder(what),
        )
    };

    let text = format!(
        "# Written by standup.rs -- the NooBaa endpoint, not RGW's.\n\
         #\n\
         # NooBaa NSFS at the pin, over {tree}.\n\
         # Regenerated by every `capture`;  edit interchange.conf instead.\n\
         \n\
         [DEFAULT]\n\
         host = 127.0.0.1\n\
         port = {port}\n\
         is_secure = False\n\
         ssl_verify = False\n\
         \n\
         [fixtures]\n\
         # The corpus names its own buckets -- corpus-<tier>-<axis>, derived\n\
         # from what produced them so two captures are diffable -- and does\n\
         # not read this.  Here because the section should exist.\n\
         bucket prefix = interchange-{{random}}-\n\
         \n\
         [s3 main]\n\
         # The one account `config` created, which owns the tree.\n\
         display_name = {account}\n\
         user_id = {account}\n\
         email = {account}@interchange.invalid\n\
         access_key = {ak}\n\
         secret_key = {sk}\n\
         # No storage_classes:  NooBaa has no placement to configure,\n\
         # and the storage-class axis falls back to a name that is valid S3\n\
         # but not STANDARD when nothing is named here.\n\
         \n\
         {alt}\n\
         {tenant}\n\
         {iam}\n\
         {iam_root}\n\
         {iam_alt_root}",
        tree = env.tree().display(),
        port = env.port,
        account = env.account,
        ak = env.access_key,
        sk = env.secret_key,
        alt = unused("s3 alt", "alt"),
        tenant = {
            /* `tenant` is the one extra key in that section */
            let mut t = unused("s3 tenant", "tenant");
            t.push_str("tenant = interchange-tenant\n");
            t
        },
        iam = unused("iam", "iam"),
        iam_root = unused("iam root", "iamroot"),
        iam_alt_root = unused("iam alt root", "iamaltroot"),
    );
    std::fs::write(&p, text).map_err(|e| format!("cannot write {}: {e}", p.display()))?;
    Ok(p)
}

/// Stop the endpoint, leaving the tree.
///
/// The tree is the artifact and must survive:  RGW opens it next.
fn stop(env: &Env) -> Result<(), String> {
    let Some(pid) = running(env) else {
        println!("stop     not running");
        let _ = std::fs::remove_file(env.pid_file());
        return Ok(());
    };
    /* SIGTERM, then wait:  it has a tree open and should be allowed to
     * close it. */
    let _ = Command::new("kill").arg(pid.to_string()).status();
    for _ in 0..60 {
        if !alive(pid) {
            let _ = std::fs::remove_file(env.pid_file());
            println!("stop     pid {pid} stopped;  {} left in place", env.data.display());
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(format!("pid {pid} did not stop within 30s"))
}

/// Start RGW against the tree NooBaa wrote.
///
/// This is the handoff, and the reason it is a step rather than a line
/// in a README:  the discipline it carries is not optional and is easy
/// to get wrong.
///
///   * **NooBaa must be stopped first.**  The model is sequential --
///     drain, then upgrade -- and two gateways writing one tree is a
///     different feature.  Refused rather than warned about.
///   * **`--data-root` is mandatory.**  Without it RGW serves its own
///     build directory and the run silently measures nothing;  and
///     the default location cannot be used anyway, because
///     `vstart.sh` removes `$dev/rgw/$STORE/root` outright on every
///     start, which would delete the tree we came to read.
///   * **`--with-lifecycle`**, because conversion carries bucket
///     configuration and lifecycle is part of it.  NooBaa keeps a
///     bucket's rules in `lifecycle_configuration_rules` in its
///     bucket record;  the nsfs driver keeps them in an xattr.  A run
///     that omits the flag cannot exercise the translation.
///
/// `rgw-vstart.sh` must be run from the build directory, which is why
/// `ceph_build` is a setting rather than a guess.
fn adopt(env: &Env) -> Result<(), String> {
    if let Some(pid) = running(env) {
        return Err(format!(
            "NooBaa is still running (pid {pid}).  The handoff is sequential:  \
             run `stop` first."
        ));
    }
    let Some(build) = env.ceph_build.as_ref() else {
        return Err(format!(
            "`ceph_build` is not set.  `adopt` runs rgw-vstart.sh, which has to \
             be\n  invoked from the ceph build directory:\n\n    \
             INTERCHANGE_CEPH_BUILD=/path/to/ceph/build standup.rs adopt\n\n  \
             or put `ceph_build = /path/to/ceph/build` in {}.",
            interchange_home().join("interchange.conf").display()
        ));
    };
    let script = build.join("../src/script/rgw/rgw-vstart.sh");
    if !script.exists() {
        return Err(format!(
            "{} is not there;  is {} really a ceph build directory?",
            script.display(),
            build.display()
        ));
    }
    let tree = env.tree();
    if !tree.is_dir() {
        return Err(format!(
            "{} does not exist -- there is no tree to adopt.  Run `config` and \
             `serve`, and let something write to it.",
            tree.display()
        ));
    }

    /* Said out loud, because it is the whole point of the exercise and
     * a reader of the log should be able to see what was handed over. */
    let buckets = std::fs::read_dir(&tree)
        .map(|d| d.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    println!("adopt    tree {} ({buckets} entries)", tree.display());
    println!("         rgw-vstart.sh --store nsfs --data-root {} --with-lifecycle",
             env.data.display());

    let st = Command::new("bash")
        .arg(script.display().to_string())
        .args([
            "--store",
            "nsfs",
            "--data-root",
            &env.data.display().to_string(),
            "--with-lifecycle",
        ])
        .current_dir(build)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("cannot run rgw-vstart.sh: {e}"))?;
    if !st.success() {
        return Err(format!("rgw-vstart.sh failed ({st})"));
    }

    /* Confirm what it actually points at rather than trusting the exit
     * code:  a redirect that silently did not take effect is the
     * defect this step exists downstream of. */
    let conf = build.join("ceph.conf");
    let text = std::fs::read_to_string(&conf)
        .map_err(|e| format!("cannot read {}: {e}", conf.display()))?;
    let want = format!("rgw nsfs base path = {}", tree.display());
    if !text.contains(&want) {
        return Err(format!(
            "{} does not say {want:?} -- RGW is not serving the tree",
            conf.display()
        ));
    }
    println!("adopt    ceph.conf confirms the base path");
    Ok(())
}

/// The pid of the NooBaa endpoint this driver started, if it is up.
fn running(env: &Env) -> Option<u32> {
    let pid: u32 = std::fs::read_to_string(env.pid_file())
        .ok()?
        .trim()
        .parse()
        .ok()?;
    alive(pid).then_some(pid)
}

/// Whether a pid is alive, without a libc dependency and without
/// matching on a command line -- which is how three wait loops in this
/// driver\'s own development ended up waiting on themselves.
fn alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

/// NooBaa's CLI.  Quiet unless it fails, because `status` is used as a
/// probe and a missing record is an expected answer.
fn nsfs_cli(env: &Env, args: &[&str]) -> Result<String, String> {
    let wt = env.worktree();
    let mut a: Vec<String> = vec!["src/cmd/manage_nsfs.js".into()];
    a.extend(args.iter().map(|s| s.to_string()));
    a.push("--config_root".into());
    a.push(env.config_root().display().to_string());

    let out = Command::new(env.node())
        .args(&a)
        .current_dir(&wt)
        .output()
        .map_err(|e| format!("cannot run {}: {e}", env.node()))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let o = String::from_utf8_lossy(&out.stdout);
        return Err(format!(
            "manage_nsfs {} failed:\n{}",
            args.join(" "),
            if err.trim().is_empty() { o.trim() } else { err.trim() }
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The uid and gid this process runs as, without a libc dependency.
fn running_ids() -> Result<(u32, u32), String> {
    let one = |flag: &str| -> Result<u32, String> {
        let out = Command::new("id")
            .arg(flag)
            .output()
            .map_err(|e| format!("cannot run id: {e}"))?;
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .map_err(|e| format!("cannot read id {flag}: {e}"))
    };
    Ok((one("-u")?, one("-g")?))
}

/// The lock this project keeps for a given pin.  Committed.
fn pinned_lock(env: &Env) -> PathBuf {
    interchange_home().join("locks").join(format!("{}.package-lock.json", env.pin))
}

/// NooBaa's lockfile must still match the pin.
fn lock_is_pristine(wt: &Path, when: &str) -> Result<(), String> {
    let out = Command::new("git")
        .args(["status", "--porcelain", "--", "package-lock.json"])
        .current_dir(wt)
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if !String::from_utf8_lossy(&out.stdout).trim().is_empty() {
        return Err(format!(
            "package-lock.json is modified ({when} the install);  \
             `git -C {} checkout -- package-lock.json`",
            wt.display()
        ));
    }
    Ok(())
}

/* ---------------------------------------------------------------- */

fn git(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
