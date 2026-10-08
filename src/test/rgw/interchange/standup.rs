#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2021"

[dependencies]
---
//! Stand up NooBaa's NSFS gateway over a data root, so that the
//! conversion corpus can be run against *their* implementation and the
//! result compared with ours.
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
//!     developer's own clone.  Reading their source at some other
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
    /// scratch:  the worktree, the config, the logs
    work: PathBuf,
    /// the tree both gateways will serve
    data: PathBuf,
    /// NooBaa's endpoint port
    port: u16,
    /// the account their gateway serves as
    account: String,
    /// the one bucket, which both gateways will serve
    bucket: String,
    /// fixed, so the corpus can be pointed at it without discovery
    access_key: String,
    secret_key: String,
}

impl Env {
    fn from_environment() -> Env {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".into());
        let work = env_or("IX_WORK", &format!("{home}/.cache/nsfs-interchange"));
        Env {
            src: env_or("IX_NOOBAA_SRC", &format!("{home}/dev/noobaa-core")).into(),
            pin: env_or("IX_PIN", "08d063f86"),
            data: env_or("IX_DATA", &format!("{work}/data")).into(),
            work: work.into(),
            port: env_or("IX_PORT", "6443").parse().unwrap_or(6443),
            account: env_or("IX_ACCOUNT", "interchange"),
            bucket: env_or("IX_BUCKET", "interchange"),
            /* Fixed rather than generated:  the corpus has to be
             * pointed at this endpoint, and a credential that changes
             * per run means a config file that changes per run. */
            access_key: env_or("IX_ACCESS_KEY", "IXAAAAAAAAAAAAAAAAAA"),
            secret_key: env_or(
                "IX_SECRET_KEY",
                "IXsecretIXsecretIXsecretIXsecretIXsecret",
            ),
        }
    }

    fn worktree(&self) -> PathBuf {
        self.work.join(format!("noobaa-{}", self.pin))
    }

    /// Their NC configuration directory -- the config store the
    /// account and bucket records live in.  Separate from the data
    /// root:  the records describe the tree, they are not in it.
    fn config_root(&self) -> PathBuf {
        self.work.join("config")
    }

    fn node(&self) -> String {
        env_or("IX_NODE", "node")
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
    ("build",    "install the dependency tree, and build their native addon"),
    ("config",   "write their account and bucket records over the data root"),
    ("serve",    "start their NSFS endpoint"),
    ("capture",  "run the corpus against their endpoint"),
    ("stop",     "stop the endpoint, leaving the tree"),
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let env = Env::from_environment();
    let cmd = args.first().map(String::as_str).unwrap_or("list");

    let r = match cmd {
        "list" => { list(&env); Ok(()) }
        "pin" => pin(&env),
        "worktree" => worktree(&env),
        "build" => build(&env),
        "config" => config(&env),
        "serve" | "capture" | "stop" => unbuilt(cmd),
        "up" => ["pin", "worktree", "build", "config"]
            .iter()
            .try_for_each(|s| match *s {
                "pin" => pin(&env),
                "worktree" => worktree(&env),
                "build" => build(&env),
                _ => config(&env),
            }),
        other => Err(format!("unknown step {other:?};  try `list`")),
    };

    if let Err(e) = r {
        eprintln!("\x1b[31mstandup: {e}\x1b[0m");
        std::process::exit(1);
    }
}

fn list(env: &Env) {
    println!("steps:");
    for (name, what) in STEPS {
        let mark =
            if matches!(*name, "pin" | "worktree" | "build" | "config") { "x" } else { " " };
        println!("  [{mark}] {name:10} {what}");
    }
    println!("\nsettings (all overridable):");
    let s: BTreeMap<&str, String> = BTreeMap::from([
        ("IX_NOOBAA_SRC", env.src.display().to_string()),
        ("IX_PIN", env.pin.clone()),
        ("IX_WORK", env.work.display().to_string()),
        ("IX_DATA", env.data.display().to_string()),
        ("IX_PORT", env.port.to_string()),
        ("IX_ACCOUNT", env.account.clone()),
        ("IX_BUCKET", env.bucket.clone()),
    ]);
    for (k, v) in s {
        println!("  {k:16} {v}");
    }
    println!("\n[x] is built;  [ ] is not.  `up` runs the built ones.");
}

fn unbuilt(step: &str) -> Result<(), String> {
    Err(format!(
        "step {step:?} is not built yet.  The step model is agreed and \
         the earlier steps work;  this one is where the discussion with \
         Matt had got to.  See README.md."
    ))
}

/* ---------------------------------------------------------------- */

/// Resolve the pin against the developer's clone, and say what it is.
///
/// Read only.  The clone's HEAD and index are not touched, because
/// reading their source at some other commit is a thing we do
/// constantly and this must not disturb it.
fn pin(env: &Env) -> Result<(), String> {
    if !env.src.join(".git").exists() {
        return Err(format!(
            "{} is not a git clone of noobaa-core;  set IX_NOOBAA_SRC",
            env.src.display()
        ));
    }
    let full = git(&env.src, &["rev-parse", &format!("{}^{{commit}}", env.pin)])
        .map_err(|_| {
            format!(
                "commit {} is not in {};  fetch it, or set IX_PIN",
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

/// Install the dependency tree, from a lock we control.
///
/// **Their lock at this pin does not `npm ci`.**  `npm ci` is the
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
/// reproducible from their commit alone.
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

    let ours = ours_lock(env);
    let theirs = wt.join("package-lock.json");

    let verb = if ours.exists() {
        println!("build    npm ci against {}", ours.display());
        std::fs::copy(&ours, &theirs)
            .map_err(|e| format!("cannot place our lock: {e}"))?;
        "ci"
    } else {
        println!("build    no lock of ours for this pin yet;  resolving with");
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
        std::fs::create_dir_all(ours.parent().unwrap())
            .map_err(|e| format!("cannot create locks/: {e}"))?;
        std::fs::copy(&theirs, &ours)
            .map_err(|e| format!("cannot record our lock: {e}"))?;
        println!("build    recorded {}", ours.display());
        println!("         commit it:  that is what makes this pin reproducible");
    }

    git(&wt, &["checkout", "--", "package-lock.json"])?;
    lock_is_pristine(&wt, "after")?;
    native(env)
}

/// Their native addon, which `npm ci` does not build.
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
    /* Their code does not compile under a current GCC with their own
     * -Werror.
     *
     * `fs_napi.cpp:317` memcpy()s into `reqP->payload.buffer[0]`,
     * where `buffer` is declared `char[0]` inside a GPFS
     * `gpfs_fcntl` request struct that is deliberately
     * over-allocated.  The code is correct by construction;  the
     * declaration is the deprecated `char[0]` idiom rather than a C99
     * flexible array member, and GCC 15 reports
     * -Warray-bounds on it.  With `cc1plus: all warnings being
     * treated as errors`, the build stops.
     *
     * One warning is demoted, not -Werror as a whole:  every other
     * warning stays fatal, which is their intent.  Overridable,
     * because which warnings a toolchain raises is a property of the
     * host and this will not be the last one.
     *
     * This is the part no lockfile can pin, and the only honest
     * argument for a container -- about the *toolchain*, not about
     * dependency resolution. */
    let cxxflags = env_or("IX_CXXFLAGS", "-Wno-error=array-bounds");
    println!("native   node-gyp configure && build (CXXFLAGS={cxxflags})");
    let st = Command::new("npm")
        .args(["run", "build:native"])
        .env("CXXFLAGS", &cxxflags)
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

/// Write their account and bucket records over the data root.
///
/// Through `manage_nsfs.js`, their own CLI, rather than by composing
/// JSON:  the whole point of standing their gateway up is to get
/// records *they* wrote.  Hand-written records would be our reading of
/// their schema again, which is the thing this stage exists to stop
/// relying on.
///
/// **uid and gid are the running user's.**  The handoff is sequential
/// against one tree -- NooBaa writes, stops, RGW opens the same
/// directory -- so the files have to be owned by the identity RGW will
/// run as, or the second half of the exercise fails on permissions
/// rather than on anything interesting.
///
/// Idempotent:  a second run finds the records present and says so.
/// Their CLI fails a duplicate `add`, which is correct of it and not
/// what this driver wants.
fn config(env: &Env) -> Result<(), String> {
    let wt = env.worktree();
    if !wt.join("node_modules").exists() {
        return Err("no dependency tree;  run `build` first".into());
    }
    let (uid, gid) = running_ids()?;
    std::fs::create_dir_all(env.config_root())
        .map_err(|e| format!("cannot create {}: {e}", env.config_root().display()))?;
    std::fs::create_dir_all(&env.data)
        .map_err(|e| format!("cannot create {}: {e}", env.data.display()))?;

    println!("config   config_root {}", env.config_root().display());
    println!("         data        {} (uid {uid}, gid {gid})", env.data.display());

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
                "--new_buckets_path", &env.data.display().to_string(),
                "--access_key", &env.access_key,
                "--secret_key", &env.secret_key,
            ],
        )?;
        println!("         account {:?} created", env.account);
    }

    let bucket_path = env.data.join(&env.bucket);
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

    /* What `capture` will need, written where it can find it rather
     * than rediscovered. */
    let creds = env.work.join("credentials");
    std::fs::write(
        &creds,
        format!(
            "# written by standup.rs config\nendpoint=http://127.0.0.1:{}\n\
             access_key={}\nsecret_key={}\nbucket={}\ndata={}\n",
            env.port,
            env.access_key,
            env.secret_key,
            env.bucket,
            env.data.display()
        ),
    )
    .map_err(|e| format!("cannot write {}: {e}", creds.display()))?;
    println!("         wrote {}", creds.display());
    Ok(())
}

/// Their CLI.  Quiet unless it fails, because `status` is used as a
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
fn ours_lock(env: &Env) -> PathBuf {
    let here = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));
    /* -Zscript puts the binary in a cache, so prefer the source
     * directory when it can be found;  fall back to the cwd, which is
     * where the driver is normally run from. */
    let src_dir = std::env::var("IX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or(here));
    src_dir.join("locks").join(format!("{}.package-lock.json", env.pin))
}

/// Their lockfile must still match the pin.
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
