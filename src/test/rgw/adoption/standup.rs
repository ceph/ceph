#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2021"

[dependencies]
serde_json = "1"
aws-smithy-xml = "=0.60.15"
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

use aws_smithy_xml::decode::{try_data, Document, ScopedDecoder};
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
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
    /// the port RGW serves on after `adopt` -- `rgw-vstart.sh`'s default
    rgw_port: u16,
    /// NooBaa's IAM port.  HTTPS only -- their IAM listener has no
    /// HTTP form -- with the self-signed certificate they generate
    /// when the config root holds none
    iam_port: u16,
    /// the account NooBaa serves as
    account: String,
    /// the IAM user `iam-user` creates in that account
    iam_user: String,
    /// a credential on the adopted gateway carrying `users=*` and
    /// `nsfs=*`:  `adopt` signs its Admin Ops requests with it, and the
    /// rgw capture conf names it as `[iam]`, which the corpus signs its
    /// Admin Ops requests with.  The default is the `[iam]` user
    /// `vstart.sh` creates
    rgw_admin_key: String,
    rgw_admin_secret: String,
    /// the one bucket, which both gateways will serve
    bucket: String,
    /// fixed, so the corpus can be pointed at it without discovery
    access_key: String,
    secret_key: String,
    /// the s3-tests-rs checkout the corpus lives in.  A sibling of
    /// this driver, so it needs no setting in the normal case
    suite: PathBuf,
    tier: String,
    /// whether NooBaa computes MD5 ETags.  **A property of the tree,
    /// fixed when `config` writes it** -- NooBaa sets
    /// `user.content_md5` at PUT time, so a tree written with MD5 off
    /// cannot be re-captured with it on.  `config` records it in
    /// `work/modality` and the later steps read that rather than this,
    /// so changing the setting cannot relabel an existing tree.
    md5: bool,
    /// whether RGW serves the tree impersonating -- (n) Phase G.  NooBaa
    /// then runs as root, as an installation's does, with its account
    /// at vstart's fixture ids, so the tree is account-owned at
    /// NooBaa's modes;  the corpus captures it as the account, and the
    /// store, root's at `0600`, is not captured (see `capture`)
    impersonate: bool,
    /// the access model RGW is started with under `impersonate`
    access_model: String,
}

/// Every setting, from `adoption.conf` and then the environment.
///
/// `ADOPTION_WORK` and `work = ` are the same setting;  the environment
/// wins.
fn settings() -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    let conf = adoption_home().join("adoption.conf");
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
        if let Some(rest) = k.strip_prefix("ADOPTION_") {
            m.insert(rest.to_ascii_lowercase(), v);
        }
    }
    m
}

impl Env {
    /// **`work` has no default and this stops without it.**
    ///
    /// It is where a gigabyte of worktree goes *and* where the config
    /// store and the data root live -- the artifacts this harness
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
                 ADOPTION_WORK=/path/to/adoption standup.rs <step>\n\n  \
                 or, standing, in {}:\n\n    work = /path/to/adoption\n\n  \
                 See adoption.conf.sample.",
                adoption_home().join("adoption.conf").display()
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
            rgw_port: get("rgw_port", "8000").parse().unwrap_or(8000),
            iam_port: get("iam_port", "7005").parse().unwrap_or(7005),
            account: get("account", "adoption"),
            iam_user: get("iam_user", "adoption-iam-user"),
            rgw_admin_key: get("rgw_admin_key", "ABCDEFGHIJKLMNOPQRST"),
            rgw_admin_secret: get("rgw_admin_secret",
                                  "abcdefghijklmnopqrstuvwxyzabcdefghijklmn"),
            bucket: get("bucket", "adoption"),
            /* Fixed rather than generated:  the corpus has to be
             * pointed at this endpoint, and a credential that changes
             * per run means a config file that changes per run. */
            access_key: get("access_key", "ADOPTIONACCESSKEY001"),
            secret_key: get("secret_key", "adoptionsecretkeyadoptionsecretkeyadopti"),
            /* `../s3-tests-rs` from here.  The corpus is a modality of
             * that suite rather than a separate tool, so the two move
             * together and the default is right whenever this driver
             * is run from the tree it was committed to. */
            suite: get(
                "suite",
                &adoption_home()
                    .parent()
                    .unwrap_or(&adoption_home())
                    .join("s3-tests-rs")
                    .display()
                    .to_string(),
            )
            .into(),
            tier: get("tier", "coverage"),
            md5: matches!(get("md5", "false").to_ascii_lowercase().as_str(),
                          "1" | "on" | "true" | "yes"),
            impersonate: matches!(get("impersonate", "false").to_ascii_lowercase().as_str(),
                                  "1" | "on" | "true" | "yes"),
            access_model: get("access_model", "posix-strict"),
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
    /// Where the tree's modality is written.  See `Env::md5`.
    fn modality_file(&self) -> PathBuf {
        self.work.join("modality")
    }

    /// What the setting asks for, as a label.
    fn modality(&self) -> &'static str {
        if self.md5 { "md5-on" } else { "md5-off" }
    }

    fn config_root(&self) -> PathBuf {
        self.work.join("config")
    }

    fn node(&self) -> String {
        env_or("ADOPTION_NODE", "node")
    }

    fn pid_file(&self) -> PathBuf {
        self.work.join("nsfs.pid")
    }

    fn log_file(&self) -> PathBuf {
        self.work.join("nsfs.log")
    }

    /// The IAM user's credential, which NooBaa generates and shows once.
    fn iam_user_credentials(&self) -> PathBuf {
        self.work.join("iam-user-credentials")
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

    fn capture_conf_for(&self, side: &str) -> PathBuf {
        self.work.join("capture").join(format!("{side}.s3tests.conf"))
    }

    fn capture_conf(&self) -> PathBuf {
        self.work.join("s3tests-noobaa.conf")
    }

    fn capture_log_for(&self, side: &str) -> PathBuf {
        self.work.join("capture").join(format!("{side}.log"))
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
    ("iam-user", "create an IAM user in the account, through NooBaa's IAM API"),
    ("store-tests", "run the nsfs unit tests that read a real NooBaa store, against this one"),
    ("capture",  "run the corpus against the NooBaa endpoint"),
    ("stop",     "stop the endpoint, leaving the tree"),
    ("damage",   "damage one bucket record, so adoption meets a record it cannot carry"),
    ("import",   "RGW over the same tree;  import the identities, buckets left in NooBaa's format"),
    ("adopt",    "adopt every bucket:  import its record and convert its tree"),
    ("compare",  "evaluate the three captures against the expected-difference list"),
    ("conversion", "the conversion profile:  every step above, in order, on a fresh work directory"),
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
        "iam-user" => iam_user(&env),
        "store-tests" => store_tests(&env),
        "capture" => capture(&env, args.get(1).map(String::as_str).unwrap_or("noobaa")),
        "stop" => stop(&env),
        "damage" => damage(&env),
        "import" => adopt(&env, "import"),
        "adopt" => adopt(&env, "adopt"),
        "compare" => compare(&env),
        "conversion" => conversion(&env),
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

    println!("\nEach setting is an ADOPTION_<NAME> environment variable, or the");
    println!("same name in adoption.conf beside this driver;  the environment");
    println!("wins.\n");

    let s = settings();
    let shown = [
        ("work", "REQUIRED -- the worktree, config store, data root, logs"),
        ("ceph_build", "REQUIRED for `adopt` -- the ceph build directory"),
        ("noobaa_src", "the noobaa-core clone, read only"),
        ("pin", "the commit to stand up"),
        ("data", "what --data-root is given;  the tree is data/root"),
        ("port", "the NooBaa HTTP port"),
        ("iam_port", "the NooBaa IAM port, HTTPS"),
        ("rgw_port", "the port RGW serves on after `adopt`"),
        ("account", "the account `config` creates;  default adoption"),
        ("iam_user", "the IAM user `iam-user` creates;  default adoption-iam-user"),
        ("rgw_admin_key", "an Admin Ops key on RGW for `adopt` and the corpus;  default vstart's [iam]"),
        ("bucket", "the bucket `config` creates;  default adoption"),
        ("suite", "the s3-tests-rs checkout;  defaults to a sibling of this driver"),
        ("tier", "which corpus tier `capture` runs"),
        ("md5", "true makes NooBaa compute MD5 ETags;  fixed per tree"),
    ];
    for (k, note) in shown {
        match s.get(k) {
            Some(v) if !v.is_empty() => println!("  {k:12} {v}"),
            _ => println!("  {k:12} (unset)  {note}"),
        }
    }
    println!("\n`up` runs through `config`.  `serve`, `iam-user`, `capture`, `stop` and");
    println!("`adopt` are each run deliberately:  they start and stop gateways");
    println!("over a tree that is the artifact, and chaining them past a failure is");
    println!("how a tree gets destroyed rather than handed over.");
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
            "{} is not a git clone of noobaa-core;  set ADOPTION_NOOBAA_SRC",
            env.src.display()
        ));
    }
    let full = git(&env.src, &["rev-parse", &format!("{}^{{commit}}", env.pin)])
        .map_err(|_| {
            format!(
                "commit {} is not in {};  fetch it, or set ADOPTION_PIN",
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
    let dir = adoption_home().join("patches");
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
fn adoption_home() -> PathBuf {
    std::env::var("ADOPTION_HOME")
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
/// **uid and gid are the running user's**, unless `impersonate`.  The
/// handoff is sequential against one tree -- NooBaa writes, stops, RGW
/// opens the same directory -- so unimpersonated, the files have to be
/// owned by the identity RGW will run as, or the second half fails on
/// permissions rather than on anything interesting.  Under
/// `impersonate` they are vstart's fixture account's, NooBaa runs as
/// root, and RGW acting as the account is the point.
///
/// Idempotent:  a second run finds the records present and says so.
/// NooBaa's CLI fails a duplicate `add`, which is correct of it and not
/// what this driver wants.
fn config(env: &Env) -> Result<(), String> {
    /* The modality check, before this step creates anything.
     *
     * Refusing rather than overwriting:  the modality is fixed in the
     * objects themselves, so a tree written md5-off and relabelled
     * md5-on would claim digests that no object carries, and every
     * later comparison would be against the label rather than the
     * tree.
     * Two modalities are two trees. */
    let recorded = env.modality_file();
    match std::fs::read_to_string(&recorded) {
        Ok(prev) if prev.trim() != env.modality() => {
            return Err(format!(
                concat!(
                    "this tree was written {}, and `md5` now asks for {}.\n",
                    "  A modality is fixed when the objects are written, so\n",
                    "  the two are two trees.  Point `work` at a different\n",
                    "  directory, or remove {} to start over."
                ),
                prev.trim(),
                env.modality(),
                env.work.display()
            ));
        }
        _ => {
            std::fs::write(&recorded, format!("{}\n", env.modality()))
                .map_err(|e| format!("cannot write {}: {e}", recorded.display()))?;
        }
    }
    println!("         modality    {}", env.modality());

    let wt = env.worktree();
    if !wt.join("node_modules").exists() {
        return Err("no dependency tree;  run `build` first".into());
    }
    let (uid, gid) = account_ids(env)?;
    std::fs::create_dir_all(env.config_root())
        .map_err(|e| format!("cannot create {}: {e}", env.config_root().display()))?;
    /* The tree, not `data`:  `rgw-vstart.sh --data-root DIR` serves
     * DIR/root, so NooBaa has to write into data/root for the handoff
     * to line up without copying anything. */
    std::fs::create_dir_all(&env.data)
        .map_err(|e| format!("cannot create {}: {e}", env.data.display()))?;
    account_dir(env, &env.tree(), uid, gid)?;

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

    /* The anonymous account, which NooBaa creates only when asked:  the
     * POSIX identity an unauthenticated request runs as.  A deployment
     * that serves public buckets has one, and the import writes it as an
     * identity row and nothing else (ACCOUNT_IMPORT.md §6.8) -- a record
     * shape and a branch no other record reaches.  Without it here only
     * synthetic fixtures would cover either.  The account's ids, so
     * the identity it names exists. */
    if nsfs_cli(env, &["account", "status", "--anonymous"]).is_ok() {
        println!("         anonymous account already present");
    } else {
        nsfs_cli(
            env,
            &[
                "account", "add", "--anonymous",
                "--uid", &uid.to_string(),
                "--gid", &gid.to_string(),
            ],
        )?;
        println!("         anonymous account created (uid {uid}, gid {gid})");
    }

    let bucket_path = env.tree().join(&env.bucket);
    if nsfs_cli(env, &["bucket", "status", "--name", &env.bucket]).is_ok() {
        println!("         bucket {:?} already present", env.bucket);
    } else {
        account_dir(env, &bucket_path, uid, gid)?;
        /* `--force_md5_etag` is their per-bucket flag, and it is the
         * only one of the three a fixture can reach:  the global
         * `NSFS_CALCULATE_MD5` would need a config.json override and
         * the account flag is not in the account `add` flag set.
         *
         * With it off -- their default -- an object carries no
         * `user.content_md5` and their ETag is a file-instance token
         * that witnesses no bytes.  With it on the ETag is a digest,
         * which is the only modality in which the suite has anything
         * to validate.  DECISIONS.md 2026-10-09. */
        let bp = bucket_path.display().to_string();
        let mut add: Vec<&str> = vec![
            "bucket", "add",
            "--name", &env.bucket,
            "--owner", &env.account,
            "--path", &bp,
        ];
        if env.md5 {
            add.extend_from_slice(&["--force_md5_etag", "true"]);
        }
        nsfs_cli(env, &add)?;
        println!("         bucket {:?} created at {}", env.bucket, bucket_path.display());
    }

    /* The bucket `damage` damages, made as any other is.  It is not a
     * corpus bucket, so no capture and no comparison sees it. */
    let damaged_path = env.tree().join(DAMAGED_BUCKET);
    if nsfs_cli(env, &["bucket", "status", "--name", DAMAGED_BUCKET]).is_ok() {
        println!("         bucket {DAMAGED_BUCKET:?} already present");
    } else {
        account_dir(env, &damaged_path, uid, gid)?;
        let dp = damaged_path.display().to_string();
        nsfs_cli(env, &["bucket", "add", "--name", DAMAGED_BUCKET,
                        "--owner", &env.account, "--path", &dp])?;
        println!("         bucket {DAMAGED_BUCKET:?} created at {dp}, for `damage`");
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
             access_key={}\nsecret_key={}\nbucket={}\ntree={}\nmodality={}\n",
            env.port,
            env.access_key,
            env.secret_key,
            env.bucket,
            env.tree().display(),
            env.modality()
        ),
    )
    .map_err(|e| format!("cannot write {}: {e}", creds.display()))?;
    println!("         wrote {}", creds.display());
    Ok(())
}

/// The modality the tree was actually written with.
///
/// The later steps read this rather than `Env::md5`, so a setting
/// changed after a tree exists cannot relabel it.  `None` means
/// `config` has not run here.
fn tree_modality(env: &Env) -> Option<String> {
    std::fs::read_to_string(env.modality_file())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Start the NooBaa S3 endpoint over the data root.
///
/// S3 over HTTP only.  `--https_port -1` turns its TLS off, and the
/// corpus talks to this with the Rust SDK, for which a self-signed
/// certificate is a problem worth not having.  IAM is the exception:
/// it has no HTTP form, and `iam-user` needs it.
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
    /* Whatever answers on the port below is taken to be this one, so
     * nothing else may hold it:  a NooBaa from an earlier run would
     * answer for one that died on EADDRINUSE. */
    if std::net::TcpStream::connect(("127.0.0.1", env.port)).is_ok() {
        return Err(format!(
            "port {} is already taken, and this work directory runs no NooBaa;  \
             a NooBaa from another work directory?  Stop it, then run again.",
            env.port));
    }

    let log = std::fs::File::create(env.log_file())
        .map_err(|e| format!("cannot create {}: {e}", env.log_file().display()))?;
    let errlog = log
        .try_clone()
        .map_err(|e| format!("cannot clone the log handle: {e}"))?;

    let child = privileged(env, &node_path(env))
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
             * a port nobody asked for.  IAM defaults to -1 in NC;  it
             * is the one listener turned *on*, below. */
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
            /* IAM, because an IAM user is a record only their IAM API
             * writes -- `manage_nsfs.js` cannot address one.  HTTPS is
             * its only form;  with no `certificates/` in the config
             * root they generate a self-signed certificate, which
             * `iam-user` accepts. */
            &format!("--https_port_iam={}", env.iam_port),
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
        /* alive first:  only its own answer counts */
        if !alive(pid) {
            return Err(format!(
                "it exited before answering;  see {}",
                env.log_file().display()
            ));
        }
        /* both listeners:  IAM comes up after S3, once its certificate
         * is generated, and `iam-user` asks it next */
        if std::net::TcpStream::connect(("127.0.0.1", env.port)).is_ok()
            && std::net::TcpStream::connect(("127.0.0.1", env.iam_port)).is_ok() {
            println!("serve    answering on http://127.0.0.1:{} (IAM :{})",
                     env.port, env.iam_port);
            return Ok(());
        }
        if i == 10 {
            println!("serve    waiting for the port...");
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    Err(format!(
        "it did not answer on port {} and IAM port {} within a minute;  see {}",
        env.port,
        env.iam_port,
        env.log_file().display()
    ))
}

/// Create an IAM user in the account, through NooBaa's IAM API.
///
/// **The account's own credential, against their endpoint.**  An IAM
/// user is a record only their IAM API writes -- `manage_nsfs.js`
/// cannot address one -- so this is the one way to get an IAM user that
/// *they* wrote, carrying the fields their code copies into it:
/// `owner`, `iam_path`, and a copy of the account's
/// `nsfs_account_config` (ACCOUNT_IMPORT.md §2.8).
///
/// Signed by `curl --aws-sigv4` rather than an SDK, because this
/// driver has no dependencies and curl is already on every host it
/// runs on.  `-k`, because the certificate is the one they generate.
///
/// **The credential is shown once.**  NooBaa returns the secret from
/// `CreateAccessKey` and never again, so it is written to
/// `iam-user-credentials` as it arrives.  Idempotent on that file:  an
/// IAM user that exists with its credential recorded is left alone;  one
/// that exists without it is given a new key, since the old secret is
/// unrecoverable.
fn iam_user(env: &Env) -> Result<(), String> {
    if running(env).is_none() {
        return Err("the NooBaa endpoint is not running;  run `serve` first".into());
    }
    if std::net::TcpStream::connect(("127.0.0.1", env.iam_port)).is_err() {
        return Err(format!(
            "nothing accepts a connection on port {}, their IAM port.  An \
             endpoint started\n  before `serve` turned IAM on has to be \
             restarted:  `stop`, then `serve`.",
            env.iam_port
        ));
    }

    let exists = iam_call(env, &[("Action", "GetUser"), ("UserName", &env.iam_user)]).is_ok();
    if exists && env.iam_user_credentials().exists() {
        println!("iam-user {:?} already present, credential in {}",
                 env.iam_user, env.iam_user_credentials().display());
        return Ok(());
    }
    if !exists {
        let body = iam_call(env, &[("Action", "CreateUser"), ("UserName", &env.iam_user)])?;
        println!("iam-user {:?} created, NooBaa id {}", env.iam_user,
                 xml_tag(&body, "UserId").unwrap_or_else(|| "?".into()));
    } else {
        println!("iam-user {:?} present without a recorded credential;  \
                  issuing a new one", env.iam_user);
    }

    let body = iam_call(env, &[("Action", "CreateAccessKey"), ("UserName", &env.iam_user)])?;
    let (Some(ak), Some(sk)) = (xml_tag(&body, "AccessKeyId"), xml_tag(&body, "SecretAccessKey"))
    else {
        return Err(format!("CreateAccessKey answered without a key:\n{body}"));
    };
    std::fs::write(
        env.iam_user_credentials(),
        format!("# written by standup.rs iam-user\nuser={}\naccess_key={ak}\nsecret_key={sk}\n",
                env.iam_user),
    )
    .map_err(|e| format!("cannot write {}: {e}", env.iam_user_credentials().display()))?;
    println!("         access key {ak}, recorded in {}", env.iam_user_credentials().display());
    Ok(())
}

/// The IAM user's key and secret, as `iam-user` recorded them.  `None`
/// when that step was not run.
fn iam_user_key(env: &Env) -> Result<Option<(String, String)>, String> {
    let Ok(creds) = std::fs::read_to_string(env.iam_user_credentials()) else {
        return Ok(None);
    };
    let field = |k: &str| -> Option<String> {
        creds.lines().find_map(|l| l.strip_prefix(&format!("{k}=")).map(str::to_string))
    };
    match (field("access_key"), field("secret_key")) {
        (Some(ak), Some(sk)) => Ok(Some((ak, sk))),
        _ => Err(format!("{} carries no key", env.iam_user_credentials().display())),
    }
}

/// One IAM action against NooBaa, signed as the account.
fn iam_call(env: &Env, params: &[(&str, &str)]) -> Result<String, String> {
    let mut form: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    form.push("Version=2010-05-08".into());
    let (status, body) = curl(&[
        "-k",
        "--aws-sigv4", "aws:amz:us-east-1:iam",
        "--user", &format!("{}:{}", env.access_key, env.secret_key),
        "--data", &form.join("&"),
        &format!("https://127.0.0.1:{}/", env.iam_port),
    ])?;
    if status != 200 {
        return Err(format!("{} answered {status}:\n{body}", params[0].1));
    }
    Ok(body)
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
/// **The corpus's mode checks run on both sides.**  nsfs creates in
/// NooBaa's modes, 0660 files and 0770 directories (DECISIONS.md
/// 2026-10-11), so a tree NooBaa wrote and the same tree after RGW
/// adopted it are held to the one expectation.
///
/// Idempotent:  the artifacts are rewritten, and the buckets the
/// corpus uses have stable names so a second run meets its own.
/// Every path under the tree with its size and mtime, sorted.
///
/// An observing capture must not change the tree it describes, and
/// that is checkable rather than reviewable:  take this before and
/// after, and any difference is a write.  Three unguarded writes were
/// found that way -- the versioning state, the copy sources and a
/// cache-warming sentinel -- and none of the three by reading the 33
/// mutating call sites in the axes.
///
/// **ctime as well as mtime.**  Setting an attribute -- a bucket policy
/// is one, on the bucket directory -- changes a file's ctime and not its
/// mtime or size, so a fingerprint without it missed the corpus putting
/// a policy on the log-target bucket.
fn tree_fingerprint(env: &Env) -> Result<Vec<String>, String> {
    /* Whole, or a failure:  a directory skipped here is one the write
     * check cannot see, and it would pass while disabled.  Under
     * `impersonate` taken as the account, which owns the tree. */
    let root = env.tree();
    if env.impersonate {
        let (uid, gid) = account_ids(env)?;
        let out = Command::new("sudo")
            .args(["-n", "-u", &format!("#{uid}"), "-g", &format!("#{gid}"), "find"])
            .arg(&root)
            .args(["-mindepth", "1", "-printf", "%p\t%s\t%T@\t%C@\n"])
            .output()
            .map_err(|e| format!("cannot run find as the account: {e}"))?;
        if !out.status.success() {
            return Err(format!("the account cannot walk {}:  {}", root.display(),
                               String::from_utf8_lossy(&out.stderr).trim()));
        }
        let mut v: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .lines().map(str::to_string).collect();
        v.sort();
        return Ok(v);
    }
    fn walk(d: &PathBuf, out: &mut Vec<String>) -> Result<(), String> {
        let rd = std::fs::read_dir(d)
            .map_err(|e| format!("cannot read {}: {e}", d.display()))?;
        for e in rd.filter_map(Result::ok) {
            let p = e.path();
            let m = e.metadata()
                .map_err(|e| format!("cannot stat {}: {e}", p.display()))?;
            let mtime = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            out.push(format!("{}\t{}\t{}\t{}.{:09}", p.display(), m.len(), mtime,
                             m.ctime(), m.ctime_nsec()));
            if m.is_dir() {
                walk(&p, out)?;
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(&root, &mut out)?;
    out.sort();
    Ok(out)
}

fn capture(env: &Env, side: &str) -> Result<(), String> {
    /* Which gateway is being described, and so which directory the
     * artifacts go in and what the run is allowed to do.
     *
     * `noobaa` generates:  it creates the objects and records them, and
     * the tree it leaves is the artifact the handoff is about.  The rgw
     * sides observe:  the objects are already there, so it records without
     * creating -- and must, because creating would describe its own
     * work rather than NooBaa's, and over a tree served read-only it
     * would be refused outright. */
    let (observe, label) = match side {
        "noobaa" => (false, "noobaa"),
        "rgw-unadopted" => (true, "rgw-unadopted"),
        "rgw-converted" => (true, "rgw-converted"),
        other => {
            return Err(format!(
                "capture takes `noobaa`, `rgw-unadopted` or `rgw-converted`, \
                 not {other:?}.\n  noobaa generates the tree;  the rgw sides \
                 observe it after `import` and after `adopt`."));
        }
    };

    /* The precondition is the side's, because the handoff is
     * sequential.  `noobaa` needs their endpoint up to be described.
     * `rgw` needs it *down* -- RGW is serving the same directory, and
     * two gateways over one tree is a different feature this exercise
     * deliberately does not have. */
    let pid = match (label, running(env)) {
        ("noobaa", None) => {
            return Err("the NooBaa endpoint is not running;  run `serve` \
                        first".into());
        }
        (l, Some(pid)) if l != "noobaa" => {
            return Err(format!(
                "NooBaa is still running (pid {pid}).  The rgw side \
                 describes the tree after the handoff:  run `stop`, then \
                 `adopt`, then this."));
        }
        (_, running) => running.unwrap_or(0),
    };
    /* Whether the side's endpoint answers, not whether a pid exists.
     * The corpus will ask the same question and a clearer error here
     * is cheaper than an SDK timeout there. */
    let port = if label == "noobaa" { env.port } else { env.rgw_port };
    if std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        return Err(if label != "noobaa" {
            format!(
                "nothing accepts a connection on port {port}, where RGW \
                 should be serving the adopted tree;  run `adopt` first")
        } else {
            format!(
                "pid {pid} is alive but nothing accepts a connection on \
                 port {port};\n  see {}",
                env.log_file().display())
        });
    }
    if !env.suite.join("Cargo.toml").exists() {
        return Err(format!(
            "{} is not the s3-tests-rs suite.  It is normally a sibling of this\n  \
             driver;  set ADOPTION_SUITE, or `suite = ` in {}.",
            env.suite.display(),
            adoption_home().join("adoption.conf").display()
        ));
    }

    let dir = env.capture_dir(label);
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
                /* the account's, under `impersonate` */
                let gone = std::fs::remove_file(e.path()).is_ok()
                    || (env.impersonate && privileged(env, "rm").arg("-f").arg(e.path())
                            .status().map(|s| s.success()).unwrap_or(false));
                if gone {
                    stale += 1;
                }
            }
        }
    }
    if stale > 0 {
        println!("capture  cleared {stale} artifacts from a previous run");
    }

    /* The observing side's contract, enforced rather than asserted:
     * taken before the corpus runs and compared after, so a write
     * that slipped past a guard is caught by the step that must not
     * make one. */
    let before = if observe {
        Some(tree_fingerprint(env)?)
    } else {
        None
    };

    let conf = write_capture_conf(env, label)?;
    println!("capture  endpoint http://127.0.0.1:{}", env.port);
    println!("         conf     {}", conf.display());
    println!("         tier     {}", env.tier);
    println!("         out      {}", dir.display());
    println!("         log      {}", env.capture_log_for(label).display());

    /* nextest if it is there, the built-in harness if it is not.  The
     * house standard is nextest and its per-test failure blocks are
     * what makes a multi-axis disagreement readable;  requiring it
     * would make this step fail for a reason that has nothing to do
     * with the adoption harness. */
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
    /* The CORPUS_* variables are listed, with their meanings, in
     * ../s3-tests-rs/src/corpus/env.rs.  The tree's modality is not
     * one of them:  it reaches the corpus as `non_md5_etag` in the
     * capture conf. */
    let mut vars: Vec<(&str, String)> = vec![
        ("S3TEST_CONF", conf.display().to_string()),
        ("CORPUS_OUT", dir.display().to_string()),
        /* The disk half.  Same host by construction -- the NooBaa
         * endpoint is on 127.0.0.1 -- so the inventory can be taken,
         * and it is the half that answers "what did they write". */
        ("CORPUS_DATA_ROOT", env.tree().display().to_string()),
        ("CORPUS_TIER", env.tier.clone()),
        ("CORPUS_OBSERVE", (if observe { "1" } else { "0" }).to_string()),
    ];
    /* The other place a bucket's configuration can live.  NooBaa keeps
     * tagging, CORS, policy, lifecycle, logging and encryption in the
     * bucket's record and none of them reaches the bucket directory,
     * so an axis reading only the tree concludes they were accepted
     * and dropped.
     *
     * Not asked for under `impersonate`.  The store is root's at
     * `0600` there, as an installation's is, and the account cannot
     * read it;  what it holds is NooBaa's format, which impersonation
     * does not change and the unimpersonated run captures. */
    if !env.impersonate {
        vars.push(("CORPUS_CONFIG_ROOT", env.config_root().display().to_string()));
    }

    /* Through bash so the output reaches the terminal and the log at
     * once:  a capture is slow enough that silence reads as a hang,
     * and a disagreement that only ever existed in a scrollback is
     * not a record.  `pipefail` so the runner's status survives tee.
     *
     * Under `impersonate` the corpus runs as the account, which owns
     * the tree and so can read all of it:  the test binary, built
     * here, run through `sudo -u`, with its environment given on the
     * command line because sudo does not pass this one on. */
    let mut cmd = Command::new("bash");
    if env.impersonate {
        let exe = corpus_executable(env)?;
        let (uid, gid) = account_ids(env)?;
        let st = privileged(env, "chown").arg(format!("{uid}:{gid}")).arg(&dir).status()
            .map_err(|e| format!("cannot run chown: {e}"))?;
        if !st.success() {
            return Err(format!("cannot give {} to the account", dir.display()));
        }
        cmd.arg("-c")
            .arg("set -o pipefail; cd \"$ADOPTION_SUITE\" || exit 1; \
                  \"$@\" 2>&1 | tee \"$ADOPTION_CAPTURE_LOG\"")
            .arg("bash")
            .args(["sudo", "-n", "-u", &format!("#{uid}"), "-g", &format!("#{gid}"), "env",
                   "HOME=/tmp"])
            .args(vars.iter().map(|(k, v)| format!("{k}={v}")))
            .arg(exe)
            .arg("--test-threads=4");
    } else {
        cmd.arg("-c")
            .arg(format!("set -o pipefail; cd \"$ADOPTION_SUITE\" || exit 1; \
                          {runner} 2>&1 | tee \"$ADOPTION_CAPTURE_LOG\""))
            .envs(vars.iter().map(|(k, v)| (*k, v.as_str())));
    }
    let st = cmd
        .env("ADOPTION_SUITE", env.suite.display().to_string())
        .env("ADOPTION_CAPTURE_LOG", env.capture_log_for(label).display().to_string())
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
            env.capture_log_for(label).display()
        ));
    }

    println!();
    /* And the contract.  Before the artifacts are reported, because a
     * capture that changed the tree has described something that no
     * longer exists. */
    if let Some(before) = before {
        let after = tree_fingerprint(env)?;
        if after != before {
            let added: Vec<_> = after.iter().filter(|l| !before.contains(l))
                                     .take(8).collect();
            return Err(format!(
                "the observing capture changed the tree it was describing:  \
                 {} paths before, {} after.\n  {}\n  \
                 Something wrote through an unguarded call;  every mutating \
                 call in an axis has to be behind `observing()`.",
                before.len(), after.len(),
                added.iter().map(|l| l.split('\t').next().unwrap_or(""))
                     .collect::<Vec<_>>().join("\n  ")));
        }
        println!("capture  the tree is unchanged ({} paths)", before.len());
    }

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
        println!("         are complete either way.  {}", env.capture_log_for(label).display());
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
fn write_capture_conf(env: &Env, label: &str) -> Result<PathBuf, String> {
    let p = env.capture_conf_for(label);

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
             email = {what}@adoption.invalid\n\
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
         # Regenerated by every `capture`;  edit adoption.conf instead.\n\
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
         bucket prefix = adoption-{{random}}-\n\
         \n\
         [s3 main]\n\
         # The one account `config` created, which owns the tree.\n\
         display_name = {account}\n\
         user_id = {account}\n\
         email = {account}@adoption.invalid\n\
         access_key = {ak}\n\
         secret_key = {sk}\n\
         # No storage_classes:  NooBaa has no placement to configure,\n\
         # and the storage-class axis falls back to a name that is valid S3\n\
         # but not STANDARD when nothing is named here.\n\
         # The tree's modality reaches the suite here, not only RGW.\n\
         # With MD5 off an object's ETag is a token for the file\n\
         # instance, so the corpus must compare its own body digest\n\
         # against the *kind* of value reported rather than against the\n\
         # value -- which is what `non_md5_etag` selects.\n\
         non_md5_etag = {non_md5}\n\
         \n\
         {alt}\n\
         {tenant}\n\
         {iam}\n\
         {iam_root}\n\
         {iam_alt_root}",
        tree = env.tree().display(),
        /* The side decides the endpoint and nothing else.  The
         * credentials are deliberately the same:  the buckets in the
         * tree are owned by NooBaa's accounts, and adoption imports
         * those accounts *with their keys* -- so the corpus reaching
         * RGW with the key NooBaa used is the import working. */
        port = if label == "noobaa" { env.port } else { env.rgw_port },
        account = env.account,
        non_md5 = if tree_modality(env).as_deref() == Some("md5-on") {
            "False"
        } else {
            "True"
        },
        ak = env.access_key,
        sk = env.secret_key,
        /* For the rgw capture, the imported IAM user and an Admin Ops
         * key:  the corpus's identity check signs as each (see
         * test_corpus/identity.rs).  On NooBaa neither has a use. */
        alt = match (label, iam_user_key(env)?) {
            (l, Some((ak, sk))) if l != "noobaa" => format!(
                "[s3 alt]\n\
                 # The IAM user `iam-user` created through NooBaa's IAM API,\n\
                 # with the key NooBaa minted for it.\n\
                 display_name = {u}\n\
                 user_id = {u}\n\
                 email = {u}@adoption.invalid\n\
                 access_key = {ak}\n\
                 secret_key = {sk}\n",
                u = env.iam_user),
            _ => unused("s3 alt", "alt"),
        },
        tenant = {
            /* `tenant` is the one extra key in that section */
            let mut t = unused("s3 tenant", "tenant");
            t.push_str("tenant = adoption-tenant\n");
            t
        },
        iam = if label != "noobaa" {
            format!(
                "[iam]\n\
                 # rgw_admin_key:  the corpus signs Admin Ops requests as this.\n\
                 display_name = iam\n\
                 user_id = iam\n\
                 email = iam@adoption.invalid\n\
                 access_key = {}\n\
                 secret_key = {}\n",
                env.rgw_admin_key, env.rgw_admin_secret)
        } else {
            unused("iam", "iam")
        },
        iam_root = unused("iam root", "iamroot"),
        iam_alt_root = unused("iam alt root", "iamaltroot"),
    );
    std::fs::write(&p, text).map_err(|e| format!("cannot write {}: {e}", p.display()))?;
    Ok(p)
}

/// Run the nsfs unit tests that read a real NooBaa store, against the
/// config store this stand-up wrote.
///
/// They skip unless `NSFS_NOOBAA_STORE` names a store, and nothing else
/// sets it, so without this step they never ran.  They read the store
/// and import into the unit binary's own sandbox, so the store is left
/// as it was.  A run in which they skip rather than run is a failure:  a
/// step that passes by doing nothing proves nothing.
///
/// The nsfs unit binary purges a fixed data root at set-up, so this must
/// not run beside another run of it.
fn store_tests(env: &Env) -> Result<(), String> {
    let Some(build) = env.ceph_build.as_ref() else {
        return Err("`ceph_build` is not set;  `store-tests` runs the nsfs unit \
                    binary from the ceph build directory".into());
    };
    let bin = build.join("bin/unittest_rgw_nsfs_driver");
    if !bin.exists() {
        return Err(format!("{} is not built", bin.display()));
    }
    const TESTS: usize = 4;
    let out = Command::new(&bin)
        .arg("--gtest_filter=*AStoreOnDisk*")
        .env("NSFS_NOOBAA_STORE", env.config_root())
        .current_dir(build)
        .output()
        .map_err(|e| format!("cannot run {}: {e}", bin.display()))?;
    let text = String::from_utf8_lossy(&out.stdout);
    let ran: Vec<&str> = text.lines()
        .filter(|l| l.starts_with("[       OK ]") || l.starts_with("[  FAILED  ]"))
        .collect();
    for l in &ran {
        println!("store-tests {l}");
    }
    let passed = text.lines().filter(|l| l.starts_with("[       OK ]")).count();
    if !out.status.success() {
        return Err(format!("the store tests failed against {};  rerun {} with \
                            NSFS_NOOBAA_STORE set to it for the detail",
                           env.config_root().display(), bin.display()));
    }
    if passed != TESTS {
        return Err(format!("{passed} of {TESTS} store tests ran;  the rest skipped, \
                            so this run proves nothing about the store"));
    }
    println!("store-tests {passed} passed against {}", env.config_root().display());
    Ok(())
}

/// Evaluate the three captures against the expected-difference list.
///
/// `corpus-compare` (s3-tests-rs, `src/bin/corpus-compare.rs`) over
/// `capture/`, with `expected-differences.tsv` beside this driver and the
/// tree's own modality -- the one it was written with, not the setting,
/// for the reason `tree_modality` gives.  It fails on a difference no row
/// accounts for and on one that breaks its row's class.
fn compare(env: &Env) -> Result<(), String> {
    let root = env.work.join("capture");
    for side in ["noobaa", "rgw-unadopted", "rgw-converted"] {
        if !env.capture_dir(side).is_dir() {
            return Err(format!("no `{side}` capture under {};  run its \
                                `capture` step first", root.display()));
        }
    }
    let Some(modality) = tree_modality(env) else {
        return Err("the tree's modality is not recorded;  was `config` run?".into());
    };
    let list = adoption_home().join("expected-differences.tsv");
    println!("compare  {} against {} ({modality})", root.display(), list.display());
    let st = Command::new("cargo")
        .args(["run", "-q", "--bin", "corpus-compare", "--"])
        .arg(&root)
        .arg(&list)
        .arg(&modality)
        .current_dir(&env.suite)
        .status()
        .map_err(|e| format!("cannot run cargo in {}: {e}", env.suite.display()))?;
    match st.code() {
        Some(0) => {
            println!("compare  every difference is accounted for");
            Ok(())
        }
        Some(1) => Err("the captures differ in ways the list does not account for;  \
                        see the lines above".into()),
        Some(3) => Err("every difference is accounted for, and known defects remain;  \
                        see the KNOWN lines above".into()),
        _ => Err("corpus-compare could not read its inputs;  see above".into()),
    }
}

/// The conversion profile:  every step, in order, stopping at the first
/// failure.
///
/// Stand NooBaa up, write a tree through it and capture it, hand the
/// tree to RGW, capture it after import and again after adoption, and
/// evaluate the three captures.  `store-tests` runs inside it, and fails if
/// the store tests skip, so under this profile they always run.
///
/// **A fresh work directory only.**  The steps are run one at a time
/// elsewhere because chaining them past a failure is how a tree gets
/// destroyed.  This chains them, so it refuses a work directory that
/// already holds a tree:  a failed run can then only have damaged a tree
/// it made itself.
fn conversion(env: &Env) -> Result<(), String> {
    if env.tree().exists() {
        return Err(format!(
            "{} already holds a tree.  The conversion profile chains every step \
             and so runs only on a fresh work directory:  point ADOPTION_WORK at \
             a new one.",
            env.tree().display()
        ));
    }
    let Some(ceph_build) = env.ceph_build.as_ref() else {
        return Err("`ceph_build` is not set;  the profile hands the tree to RGW \
                    and runs the nsfs unit binary".into());
    };
    /* The two things a privileged step has to have done, checked before
     * the NooBaa build rather than at `import` and `adopt`, by when a
     * sudo ticket taken beforehand has expired. */
    preflight_rgw(ceph_build)?;
    let steps: [(&str, &dyn Fn(&Env) -> Result<(), String>); 16] = [
        ("pin", &pin),
        ("worktree", &worktree),
        ("patch", &patch),
        ("build", &build),
        ("config", &config),
        ("serve", &serve),
        ("iam-user", &iam_user),
        ("store-tests", &store_tests),
        ("capture noobaa", &|e| capture(e, "noobaa")),
        ("stop", &stop),
        ("damage", &damage),
        ("import", &|e| adopt(e, "import")),
        ("capture rgw-unadopted", &|e| capture(e, "rgw-unadopted")),
        ("adopt", &|e| adopt(e, "adopt")),
        ("capture rgw-converted", &|e| capture(e, "rgw-converted")),
        ("compare", &compare),
    ];
    for (i, (name, step)) in steps.iter().enumerate() {
        println!("\n==> conversion {}/{}:  {name}", i + 1, steps.len());
        if let Err(e) = step(env) {
            /* A NooBaa left running holds the port the next one binds,
             * and answers in its place. */
            if running(env).is_some() {
                let _ = stop(env);
            }
            return Err(format!("the conversion profile stopped at `{name}`:  {e}"));
        }
    }
    println!("\n==> conversion:  every step passed");
    Ok(())
}

/// The capabilities `setcap` gives the gateway.  Conversion raises
/// `CAP_DAC_OVERRIDE` and `CAP_CHOWN` whether or not it impersonates,
/// so `adopt` needs them on every tree.
const RGW_CAPS: [&str; 6] = ["cap_chown", "cap_dac_override", "cap_dac_read_search",
                             "cap_fowner", "cap_setgid", "cap_setuid"];

/// What `import` and `adopt` need of the ceph build, checked by
/// `conversion` before it starts.
///
/// `rgw-vstart.sh`'s clean fails on a data root holding entries another
/// uid owns -- what an impersonated run leaves -- and `adopt` fails on
/// a `radosgw` relinked since its `setcap`.  Each fails here with the
/// command that fixes it.
fn preflight_rgw(build: &Path) -> Result<(), String> {
    let root = build.join("dev/rgw/nsfs/root");
    let me = std::fs::metadata("/proc/self")
        .map_err(|e| format!("cannot stat /proc/self: {e}"))?
        .uid();
    if let Some(p) = foreign_entry(&root, me) {
        return Err(format!(
            "{} holds {}, which another uid owns or this one cannot read -- \
             left by a --with-impersonation run.  `import`'s rgw-vstart.sh \
             cannot clean it.  Remove it, then run again:\n  sudo rm -rf {}",
            root.display(), p.display(), root.display()));
    }

    let rgw = build.join("bin/radosgw");
    let out = Command::new("getcap").arg(&rgw).output()
        .map_err(|e| format!("cannot run getcap: {e}"))?;
    let caps = String::from_utf8_lossy(&out.stdout);
    let missing: Vec<&str> = RGW_CAPS.iter().copied()
        .filter(|c| !caps.contains(c)).collect();
    if !missing.is_empty() {
        return Err(format!(
            "{} lacks {} -- relinked since its setcap.  `adopt` raises them to \
             convert.  Run, then run again:\n  sudo setcap {}+p {}",
            rgw.display(), missing.join(", "), RGW_CAPS.join(","), rgw.display()));
    }
    Ok(())
}

/// The first entry under `dir` that `uid` does not own or cannot read;
/// none for a directory that is absent.
fn foreign_entry(dir: &Path, uid: u32) -> Option<PathBuf> {
    let entries = match std::fs::read_dir(dir) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return Some(dir.to_path_buf()),
    };
    for e in entries.flatten() {
        let p = e.path();
        let Ok(md) = std::fs::symlink_metadata(&p) else {
            return Some(p);
        };
        if md.uid() != uid {
            return Some(p);
        }
        if md.is_dir() {
            if let Some(f) = foreign_entry(&p, uid) {
                return Some(f);
            }
        }
    }
    None
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
    let _ = privileged(env, "kill").arg(pid.to_string()).status();
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
///   * **Adoption by command.**  `command` is the driver's default:
///     nothing is adopted until asked.  Said explicitly, and then asked,
///     through Admin Ops with `rgw_admin_key`:  the store's identities
///     first, then each bucket NooBaa holds a record of -- the same
///     commands an operator issues.
///   * **`--with-lifecycle`**, because conversion carries bucket
///     configuration and lifecycle is part of it.  NooBaa keeps a
///     bucket's rules in `lifecycle_configuration_rules` in its
///     bucket record;  the nsfs driver keeps them in an xattr.  A run
///     that omits the flag cannot exercise the translation.
///
/// `rgw-vstart.sh` must be run from the build directory, which is why
/// `ceph_build` is a setting rather than a guess.
/// The ETag options RGW is started with, matched to the tree.
///
/// On an md5-off tree no object carries `user.content_md5`, so every
/// one of them looks sideloaded to `NSFSReadOp::prepare()` and the
/// first GET reads it end to end to compute a digest -- RGW's default
/// is to compute MD5.  `rgw_non_md5_etag=true` makes that the cheap
/// file-instance token instead, which is the regime the tree is in.
///
/// On an md5-on tree the default is right:  both gateways digest, and
/// the suite has something to validate.
fn rgw_etag_opts(env: &Env) -> Vec<String> {
    match tree_modality(env).as_deref() {
        Some("md5-on") => vec![],
        _ => vec!["-o".into(), "rgw_non_md5_etag=true".into()],
    }
}

/// `import` and `adopt`, which share everything but the command sent for
/// each bucket.
///
/// `import` imports the identities and leaves every bucket in NooBaa's
/// format, served with its configuration from NooBaa's configuration
/// store.  `adopt` adopts each bucket.  Run after `import`, `adopt` reuses that gateway, so the
/// identities it imported -- and their ids -- are the ones both captures
/// see;  a restart would re-import them, because rgw-vstart.sh empties
/// the userdb.
fn adopt(env: &Env, step: &str) -> Result<(), String> {
    let tag = format!("{step:8}");
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
             ADOPTION_CEPH_BUILD=/path/to/ceph/build standup.rs adopt\n\n  \
             or put `ceph_build = /path/to/ceph/build` in {}.",
            adoption_home().join("adoption.conf").display()
        ));
    };
    /* `import` starts the gateway `adopt` reuses, and both run alone as
     * well as under `conversion`, so both check what conversion does */
    preflight_rgw(build)?;
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
    let buckets = tree_entries(env, &tree);
    println!("{tag} tree {} ({buckets} entries)", tree.display());
    println!("         modality {}",
             tree_modality(env).unwrap_or_else(|| "unwritten".into()));

    let conf = build.join("ceph.conf");
    let want = format!("rgw nsfs base path = {}", tree.display());
    /* Whether a gateway already serves this tree, asked of the gateway:
     * the `adoption` hint reports the base path it serves.  No answer,
     * or another tree, and `converted` starts one. */
    let admin = format!("{}:{}", env.rgw_admin_key, env.rgw_admin_secret);
    let hint = format!("http://127.0.0.1:{}/admin/driver/hint?hint=adoption",
                       env.rgw_port);
    let serving = match rgw_call(&admin, "DELETE", &hint) {
        Ok((200, body)) => json_str(&body, "base_path")
            .map(|p| PathBuf::from(p.trim_end_matches('/')) == tree)
            .unwrap_or(false),
        _ => false,
    };
    if serving && step == "adopt" {
        println!("{tag} RGW already serves this tree;  adopting in place");
    } else {
    println!("         rgw-vstart.sh --store nsfs --data-root {} \
              --noobaa-config-root {} --adoption-mode command --with-lifecycle {} {}",
             env.data.display(), env.config_root().display(),
             rgw_etag_opts(env).join(" "), rgw_identity_opts(env).join(" "));
    let st = Command::new("bash")
        .arg(script.display().to_string())
        .args([
            "--store",
            "nsfs",
            "--data-root",
            &env.data.display().to_string(),
            /* Their store, so RGW can read what NooBaa wrote. */
            "--noobaa-config-root",
            &env.config_root().display().to_string(),
            /* The driver's default, said explicitly:  nothing is
             * adopted until the commands below ask. */
            "--adoption-mode",
            "command",
            "--with-lifecycle",
        ])
        .args(rgw_etag_opts(env))
        .args(rgw_identity_opts(env))
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
    let text = std::fs::read_to_string(&conf)
        .map_err(|e| format!("cannot read {}: {e}", conf.display()))?;
    if !text.contains(&want) {
        return Err(format!(
            "{} does not say {want:?} -- RGW is not serving the tree",
            conf.display()
        ));
    }
    println!("{tag} ceph.conf confirms the base path");
    }

    /* The commands.  Identities first, because every bucket's owner and
     * every policy principal resolves through them;  then each bucket
     * the store holds a record of. */
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", env.rgw_port)).is_err() {
        if std::time::Instant::now() > deadline {
            return Err(format!("nothing accepts a connection on port {}",
                               env.rgw_port));
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    /* A tree whose adoption is complete has nothing left to import or
     * adopt, and the gateway refuses every command against it. */
    let (_, body) = rgw_call(&admin, "DELETE", &hint)?;
    if json_bool(&body, "complete") == Some(true) {
        println!("{tag} adoption completed at {};  nothing to do",
                 json_str(&body, "completed_at").unwrap_or_default());
        return Ok(());
    }
    let (status, body) = rgw_call(&admin, "DELETE", &format!("{hint}&identities=true"))?;
    if status != 200 || json_bool(&body, "ran") != Some(true) {
        return Err(format!("importing the identities failed ({status}):\n{body}"));
    }
    println!("{tag} identities imported:  {} accounts, {} users",
             json_str(&body, "accounts").unwrap_or_default(),
             json_str(&body, "users").unwrap_or_default());

    /* The corpus asks the gateway, as the imported IAM user, what that
     * user's requests run as -- `GET /admin/nsfs/credentials`, which is
     * capped (`nsfs=read`) and which an imported IAM user does not
     * carry.  So it is granted here, through Admin Ops rather than
     * `radosgw-admin`, which would open the store a second time beside
     * the running gateway. */
    if let Some((ak, _)) = iam_user_key(env)? {
        let base = format!("http://127.0.0.1:{}", env.rgw_port);
        let (status, body) = rgw_call(&admin, "GET",
            &format!("{base}/admin/user?access-key={ak}&format=json"))?;
        let user = json_str(&body, "user_id").filter(|u| !u.is_empty())
            .ok_or_else(|| format!(
                "the gateway does not know the IAM user's key ({status}):  the \
                 import did not bring it across.\n{body}"))?;
        let (status, body) = rgw_call(&admin, "PUT",
            &format!("{base}/admin/user?caps&uid={user}&user-caps=nsfs%3Dread&format=json"))?;
        if status != 200 {
            return Err(format!("granting nsfs=read to {user} failed ({status}):\n{body}"));
        }
        println!("{tag} IAM user {:?} is {user};  granted nsfs=read", env.iam_user);
    }

    if step == "import" {
        println!("{tag} buckets left in NooBaa's format;  served from its configuration store");
        return Ok(());
    }

    /* the store is root's under `impersonate` */
    let records = env.config_root().join("buckets");
    let out = privileged(env, "ls").arg("-1").arg(&records).output()
        .map_err(|e| format!("cannot run ls: {e}"))?;
    if !out.status.success() {
        return Err(format!("cannot read {}:  {}", records.display(),
                           String::from_utf8_lossy(&out.stderr).trim()));
    }
    let mut names: Vec<String> = String::from_utf8_lossy(&out.stdout).lines()
        .filter_map(|n| n.strip_suffix(".json").map(str::to_string))
        .collect();
    names.sort();
    let damaged = damaged_id(env);
    for name in &names {
        if damaged.is_some() && name == DAMAGED_BUCKET {
            println!("{tag} bucket {name}:  damaged by `damage`;  left for the escape hatch");
            continue;
        }
        if name == ENDPOINT_BUCKET {
            adopt_by_profile(env, &admin, name)?;
            continue;
        }
        let (status, body) = rgw_call(&admin, "DELETE", &format!("{hint}&bucket={name}"))?;
        if status != 200 || json_bool(&body, "adopted") != Some(true) {
            return Err(format!("adopting bucket {name} failed ({status}):\n{body}"));
        }
        println!("{tag} bucket {name}:  profile {}, {} objects, {} versions",
                 json_str(&body, "profile").unwrap_or_default(),
                 json_str(&body, "objects").unwrap_or_default(),
                 json_str(&body, "versions").unwrap_or_default());
    }

    /* Every bucket is adopted, so a full pass observes the store
     * consumed and writes the completion record (DECISIONS.md
     * 2026-10-13).  Required here, because a pass that does not write it
     * has found something the per-bucket adoption above missed. */
    if step == "adopt" {
        if let Some(id) = &damaged {
            escape_hatch(env, &admin, &hint, id)?;
        }
        let (status, body) = rgw_call(&admin, "DELETE", &format!("{hint}&run=true"))?;
        if status != 200 || json_bool(&body, "complete") != Some(true) {
            return Err(format!(
                "the full pass did not complete adoption ({status}):  {}\n{body}",
                json_str(&body, "why_not_complete").unwrap_or_default()));
        }
        println!("{tag} adoption complete at {}",
                 json_str(&body, "completed_at").unwrap_or_default());
        if damaged.is_some() {
            after_the_escape_hatch(env, &admin)?;
        }
    }
    Ok(())
}

/// The bucket `damage` damages.  Made by `config` like any other.
const DAMAGED_BUCKET: &str = "corpus-damaged";

/// An account `_id` no identity in the store has.
const NO_SUCH_OWNER: &str = "000000000000000000000000";

/// Where `damage` records what it did, for the steps after it.
fn damaged_marker(env: &Env) -> PathBuf {
    env.work.join("damaged.json")
}

/// The `_id` of the record `damage` damaged, if it ran.
fn damaged_id(env: &Env) -> Option<String> {
    let text = std::fs::read_to_string(damaged_marker(env)).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    v["id"].as_str().map(str::to_string)
}

/// Damage one bucket record, discretely:  its `owner_account` becomes an
/// `_id` no identity has.  The record still parses, so it can be found
/// and abandoned by `_id`;  RGW's import refuses it, because its owner
/// is not an account the import wrote.  So adoption meets a record it
/// cannot carry, and the escape hatch has something real to do.
///
/// The harness writes NooBaa's store here, with NooBaa stopped, as a
/// fixture.  RGW never does.
fn damage(env: &Env) -> Result<(), String> {
    if let Some(pid) = running(env) {
        return Err(format!("NooBaa is still running (pid {pid});  run `stop` first"));
    }
    let path = env.config_root().join("buckets").join(format!("{DAMAGED_BUCKET}.json"));
    let text = read_store(env, &path)?;
    let mut record: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("{} is not JSON: {e}", path.display()))?;
    let id = record["_id"].as_str()
        .ok_or_else(|| format!("{} has no _id", path.display()))?
        .to_string();
    let was = record["owner_account"].as_str().unwrap_or_default().to_string();
    if was == NO_SUCH_OWNER {
        println!("damage   {DAMAGED_BUCKET}:  already damaged");
    } else {
        record["owner_account"] = serde_json::Value::from(NO_SUCH_OWNER);
        write_store(env, &path, &record.to_string())?;
        println!("damage   {DAMAGED_BUCKET} (_id {id}):  owner_account {was} -> {NO_SUCH_OWNER}");
    }
    let marker = serde_json::json!({
        "bucket": DAMAGED_BUCKET, "id": id, "field": "owner_account",
        "was": was, "now": NO_SUCH_OWNER,
    });
    std::fs::write(damaged_marker(env), marker.to_string())
        .map_err(|e| format!("cannot write {}: {e}", damaged_marker(env).display()))?;
    Ok(())
}

/// Parse a reply, naming the request on failure.
fn reply_json(what: &str, body: &str) -> Result<serde_json::Value, String> {
    serde_json::from_str(body).map_err(|e| format!("{what}:  not JSON ({e}):\n{body}"))
}

/// The escape hatch, end to end against the live gateway (DECISIONS.md
/// 2026-10-14):  the damaged record blocks completion, it is abandoned
/// with a reason, and the listing shows it.  `adopt` then requires the
/// full pass to complete.
fn escape_hatch(env: &Env, admin: &str, hint: &str, id: &str) -> Result<(), String> {
    let base = format!("http://127.0.0.1:{}", env.rgw_port);

    /* the control:  the damage is real, so the pass cannot complete */
    let (status, _) = rgw_call(admin, "DELETE", &format!("{hint}&run=true"))?;
    if status == 200 {
        return Err("the full pass succeeded with the damaged record in place;  \
                    the damage did not take".into());
    }
    let (_, body) = rgw_call(admin, "DELETE", hint)?;
    let state = reply_json("the adoption hint", &body)?;
    if state["results"]["complete"].as_bool() != Some(false) {
        return Err(format!("adoption reports complete with the damaged record unadopted:\n{body}"));
    }
    println!("adopt    {DAMAGED_BUCKET}:  blocks completion, as damaged");

    /* the cap, which the gateway's bootstrap does not grant */
    let ak = admin.split(':').next().unwrap_or_default();
    let (status, body) = rgw_call(admin, "GET",
        &format!("{base}/admin/user?access-key={ak}&format=json"))?;
    let me = reply_json("the admin user", &body)?;
    let uid = me["user_id"].as_str().filter(|u| !u.is_empty())
        .ok_or_else(|| format!("cannot find the admin user ({status}):\n{body}"))?
        .to_string();
    let (status, body) = rgw_call(admin, "PUT",
        &format!("{base}/admin/user?caps&uid={uid}&user-caps=nsfs-adoption%3D%2A&format=json"))?;
    if status != 200 {
        return Err(format!("granting nsfs-adoption failed ({status}):\n{body}"));
    }

    /* abandon it, with a reason */
    let (status, body) = rgw_call(admin, "PUT", &format!(
        "{base}/admin/nsfs/abandoned?id={id}&reason=damaged%20by%20the%20stand-up%27s%20damage%20step"))?;
    if status != 200 {
        return Err(format!("abandoning {DAMAGED_BUCKET} failed ({status}):\n{body}"));
    }
    let entry = reply_json("the abandonment", &body)?;
    if entry["stage"].as_str() != Some("not imported") || entry["kind"].as_str() != Some("bucket") {
        return Err(format!("the abandonment recorded the wrong kind or stage:\n{body}"));
    }

    let (status, body) = rgw_call(admin, "GET", &format!("{base}/admin/nsfs/abandoned"))?;
    let listed = reply_json("the abandoned listing", &body)?;
    let found = listed.as_array().map_or(false, |a| a.iter().any(|e| e["id"].as_str() == Some(id)));
    if status != 200 || !found {
        return Err(format!("the abandoned listing does not show {DAMAGED_BUCKET} ({status}):\n{body}"));
    }
    println!("adopt    {DAMAGED_BUCKET}:  abandoned (not imported), and listed");
    Ok(())
}

/// After completion:  the abandoned bucket is not served, its tree is
/// untouched, and the abandonment is fixed.
fn after_the_escape_hatch(env: &Env, admin: &str) -> Result<(), String> {
    let base = format!("http://127.0.0.1:{}", env.rgw_port);
    /* a listing, not HEAD:  curl -X HEAD waits for a body that is not
     * coming */
    let (status, _) = rgw_call(admin, "GET", &format!("{base}/{DAMAGED_BUCKET}"))?;
    if status != 404 {
        return Err(format!("{DAMAGED_BUCKET} is served after completion ({status});  \
                            an abandoned bucket never imported must not be"));
    }
    if !tree_is_dir(env, &env.tree().join(DAMAGED_BUCKET)) {
        return Err(format!("{DAMAGED_BUCKET}'s directory is gone;  abandoning must not touch data"));
    }
    let id = damaged_id(env).unwrap_or_default();
    let (status, body) = rgw_call(admin, "DELETE",
        &format!("{base}/admin/nsfs/abandoned?id={id}"))?;
    if status != 409 {
        return Err(format!("taking the abandonment back after completion was not refused ({status}):\n{body}"));
    }
    println!("adopt    {DAMAGED_BUCKET}:  not served, its tree untouched, the abandonment fixed");
    Ok(())
}

/// The bucket `adopt` takes through the profile endpoint rather than the
/// hint.  Leaving base by the endpoint is adoption too:  it imports the
/// bucket's NooBaa record before it rewrites the tree.  This bucket holds
/// the copy axis's objects, so the rgw capture checks their owners
/// against what this path produced.
const ENDPOINT_BUCKET: &str = "corpus-coverage-copy";

/// Adopt one bucket the way an operator changing its profile would:
/// `PUT /admin/nsfs/profile`, base to shared.
///
/// What the bucket looks like afterwards -- owned by the imported
/// account, BucketOwnerEnforced -- is the corpus's to check, in observe
/// mode (`bucket_ownership()` in `test_corpus/generate.rs`);  this step
/// only requires that the endpoint did the change it was asked for.
fn adopt_by_profile(env: &Env, admin: &str, name: &str) -> Result<(), String> {
    let base = format!("http://127.0.0.1:{}", env.rgw_port);
    let (status, body) = rgw_call(admin, "PUT",
        &format!("{base}/admin/nsfs/profile?bucket={name}&profile=shared"))?;
    if status != 200 {
        return Err(format!("setting bucket {name} to shared failed ({status}):\n{body}"));
    }
    let (had, now) = (json_str(&body, "had_profile"), json_str(&body, "profile"));
    if let Some(why) = json_str(&body, "stopped") {
        return Err(format!("converting bucket {name} stopped:  {why}\n{body}"));
    }
    if json_bool(&body, "converting") != Some(false) {
        return Err(format!("bucket {name} is still converting:\n{body}"));
    }
    /* base on the first run;  shared on a run that resumes a conversion
     * an earlier one stopped, whose marker the endpoint finishes */
    if !matches!(had.as_deref(), Some("base") | Some("shared"))
        || now.as_deref() != Some("shared") {
        return Err(format!(
            "bucket {name} went from {had:?} to {now:?}, not to shared:\n{body}"));
    }
    println!("adopt    bucket {name}:  profile shared by the profile endpoint");
    Ok(())
}

/// One request to the adopted gateway, signed by `who` (`key:secret`).
fn rgw_call(who: &str, method: &str, url: &str) -> Result<(u16, String), String> {
    curl(&["-X", method, "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", who, url])
}

/// curl, returning the status and the body.
fn curl(args: &[&str]) -> Result<(u16, String), String> {
    let out = Command::new("curl")
        .args(["-s", "-w", "\n%{http_code}"])
        .args(args)
        .output()
        .map_err(|e| format!("cannot run curl: {e}"))?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", &text));
    let status = code.trim().parse().map_err(|_| {
        format!("curl {} failed:  {}", args.last().unwrap_or(&""),
                String::from_utf8_lossy(&out.stderr).trim())
    })?;
    Ok((status, body.to_string()))
}

/// The text of the first `<tag>` at any depth, for the IAM responses.
fn xml_tag(body: &str, tag: &str) -> Option<String> {
    fn find(el: &mut ScopedDecoder<'_, '_>, tag: &str) -> Option<String> {
        while let Some(mut child) = el.next_tag() {
            if child.start_el().local() == tag {
                return try_data(&mut child).ok().map(|t| t.into_owned());
            }
            if let Some(found) = find(&mut child, tag) {
                return Some(found);
            }
        }
        None
    }
    let mut doc = Document::new(body);
    let mut root = doc.root_element().ok()?;
    find(&mut root, tag)
}

/// A field of an Admin Ops or driver-hint reply.  The hint wraps its
/// fields in `results`;  the other replies are flat.
fn json_field(body: &str, key: &str) -> Option<serde_json::Value> {
    let v = serde_json::from_str::<serde_json::Value>(body).ok()?;
    v.get(key).or_else(|| v.get("results")?.get(key)).cloned()
}

/// A boolean field;  the driver hint renders `true` and
/// `false` unquoted.
fn json_bool(body: &str, key: &str) -> Option<bool> {
    json_field(body, key)?.as_bool()
}

/// A string field.
fn json_str(body: &str, key: &str) -> Option<String> {
    json_field(body, key)?.as_str().map(str::to_string)
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
/// A process that has exited and not been reaped -- a zombie -- is not
/// alive, though `/proc/<pid>` still exists.  In the conversion profile
/// `serve` and `stop` run in one process, so NooBaa is this process's
/// child and stays a zombie until it exits;  run step by step, `serve`
/// has exited by then and init has reaped it.
fn alive(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/status")) {
        Ok(status) => !status
            .lines()
            .any(|l| l.starts_with("State:") && l.contains('Z')),
        Err(_) => false,
    }
}

/// NooBaa's CLI.  Quiet unless it fails, because `status` is used as a
/// probe and a missing record is an expected answer.
fn nsfs_cli(env: &Env, args: &[&str]) -> Result<String, String> {
    let wt = env.worktree();
    let mut a: Vec<String> = vec!["src/cmd/manage_nsfs.js".into()];
    a.extend(args.iter().map(|s| s.to_string()));
    a.push("--config_root".into());
    a.push(env.config_root().display().to_string());

    let out = privileged(env, &node_path(env))
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

/// Under `impersonate`:  serve impersonating, under the access model,
/// with vstart's users and capabilities but not its fixture, which
/// would write into the tree NooBaa wrote.
fn rgw_identity_opts(env: &Env) -> Vec<String> {
    if !env.impersonate {
        return vec![];
    }
    vec!["--with-impersonation".into(), "--no-fixture".into(),
         "--access-model".into(), env.access_model.clone()]
}

/// `program` as root under `impersonate`, where NooBaa is root as an
/// installation's is;  as this process otherwise.  `sudo -n`:  the
/// password was given to whoever runs this, once.
fn privileged(env: &Env, program: &str) -> Command {
    if env.impersonate {
        let mut c = Command::new("sudo");
        c.arg("-n").arg(program);
        c
    } else {
        Command::new(program)
    }
}

/// `node` by its path:  sudo's `secure_path` would not find it.
fn node_path(env: &Env) -> String {
    let n = env.node();
    if n.contains('/') {
        return n;
    }
    Command::new("sh").args(["-c", "command -v \"$1\"", "sh", &n]).output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|p| !p.is_empty())
        .unwrap_or(n)
}

/// The account's uid and gid:  vstart's fixture account under
/// `impersonate`, this process's otherwise.
fn account_ids(env: &Env) -> Result<(u32, u32), String> {
    if env.impersonate { fixture_ids(env) } else { running_ids() }
}

/// vstart's fixture account, `IMP_UID_A` and `IMP_GRP_GID`, read from
/// rgw-vstart.sh, which stays the single authority on them.  It makes
/// them on its first impersonated start, so they must already exist.
fn fixture_ids(env: &Env) -> Result<(u32, u32), String> {
    let Some(build) = env.ceph_build.as_ref() else {
        return Err("`impersonate` takes vstart's fixture ids, so `ceph_build` \
                    must be set".into());
    };
    let p = build.join("../src/script/rgw/rgw-vstart.sh");
    let text = std::fs::read_to_string(&p)
        .map_err(|e| format!("cannot read {}: {e}", p.display()))?;
    let get = |k: &str| text.lines()
        .find_map(|l| l.strip_prefix(&format!("{k}=")))
        .and_then(|v| v.trim().parse::<u32>().ok());
    let (Some(uid), Some(gid)) = (get("IMP_UID_A"), get("IMP_GRP_GID")) else {
        return Err(format!("{} sets no IMP_UID_A or IMP_GRP_GID", p.display()));
    };
    let known = Command::new("getent").args(["passwd", &uid.to_string()])
        .stdout(Stdio::null()).status().map(|s| s.success()).unwrap_or(false);
    if !known {
        return Err(format!("uid {uid} has no passwd entry;  run rgw-vstart.sh \
                            --with-impersonation once, which makes the fixture's users"));
    }
    Ok((uid, gid))
}

/// A directory the account owns:  under `impersonate` made by root for
/// it, at NooBaa's mode, as an installation's are;  this process's
/// otherwise.
fn account_dir(env: &Env, dir: &Path, uid: u32, gid: u32) -> Result<(), String> {
    if !env.impersonate {
        return std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()));
    }
    let st = privileged(env, "install")
        .args(["-d", "-o", &uid.to_string(), "-g", &gid.to_string(), "-m", "0770"])
        .arg(dir).status()
        .map_err(|e| format!("cannot run install: {e}"))?;
    if !st.success() {
        return Err(format!("cannot make {} for the account", dir.display()));
    }
    Ok(())
}

/// Whether `path` in the tree is a directory:  asked as the account under
/// `impersonate`, which owns the tree, and so sees what this process cannot.
fn tree_is_dir(env: &Env, path: &Path) -> bool {
    if !env.impersonate {
        return path.is_dir();
    }
    let Ok((uid, gid)) = account_ids(env) else {
        return false;
    };
    Command::new("sudo")
        .args(["-n", "-u", &format!("#{uid}"), "-g", &format!("#{gid}"), "test", "-d"])
        .arg(path).status().map(|s| s.success()).unwrap_or(false)
}

/// The entries directly under `dir` in the tree, counted as `tree_is_dir` asks.
fn tree_entries(env: &Env, dir: &Path) -> usize {
    if !env.impersonate {
        return std::fs::read_dir(dir).map(|d| d.filter_map(|e| e.ok()).count()).unwrap_or(0);
    }
    let Ok((uid, gid)) = account_ids(env) else {
        return 0;
    };
    Command::new("sudo")
        .args(["-n", "-u", &format!("#{uid}"), "-g", &format!("#{gid}"), "ls", "-A"])
        .arg(dir).output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().count()).unwrap_or(0)
}

/// A file in NooBaa's store, which is root's at `0600` under
/// `impersonate`.
fn read_store(env: &Env, path: &Path) -> Result<String, String> {
    let out = privileged(env, "cat").arg(path).output()
        .map_err(|e| format!("cannot run cat: {e}"))?;
    if !out.status.success() {
        return Err(format!("cannot read {}:  {}", path.display(),
                           String::from_utf8_lossy(&out.stderr).trim()));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn write_store(env: &Env, path: &Path, text: &str) -> Result<(), String> {
    use std::io::Write;
    let mut child = privileged(env, "tee").arg(path)
        .stdin(Stdio::piped()).stdout(Stdio::null()).spawn()
        .map_err(|e| format!("cannot run tee: {e}"))?;
    child.stdin.take().unwrap().write_all(text.as_bytes())
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    if !child.wait().map(|s| s.success()).unwrap_or(false) {
        return Err(format!("cannot write {}", path.display()));
    }
    Ok(())
}

/// The corpus test binary, built as this process:  under `impersonate`
/// it runs as the account, which cannot run cargo in this tree.
fn corpus_executable(env: &Env) -> Result<String, String> {
    let out = Command::new("cargo")
        .args(["test", "--features", "fails_on_noobaa", "--test", "test_corpus",
               "--no-run", "--message-format=json"])
        .current_dir(&env.suite)
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("cannot run cargo: {e}"))?;
    if !out.status.success() {
        return Err("the corpus did not build".into());
    }
    String::from_utf8_lossy(&out.stdout).lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["target"]["name"] == "test_corpus" && v["executable"].is_string())
        .and_then(|v| v["executable"].as_str().map(str::to_string))
        .ok_or_else(|| "cargo built no test_corpus executable".into())
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
    adoption_home().join("locks").join(format!("{}.package-lock.json", env.pin))
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
