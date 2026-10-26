#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2021"
---
//! Run one validation of the nsfs driver:  build, the unit suite, and
//! the s3-tests-rs profiles under each gateway configuration, with one
//! sudo password for the whole run.
//!
//! This is a meta driver above the suite, not part of it.  It is a
//! `-Zscript`, outside the crate's build graph, and the suite never
//! knows it exists:  every step is a command you could run by hand,
//! and `--dry-run` prints them.  Root is never taken by the suite.
//! The privileged steps stay in `rgw-vstart.sh`, which is the single
//! authority on the fixture;  the one this program issues itself is
//! the unit binary's `setcap`, with the capability string read from
//! that script.
//!
//! Privilege is one `sudo -v` at the start, kept alive while the run
//! lasts, so the sudo calls `rgw-vstart.sh` makes go through on the
//! same tty's ticket.  Run it from your own terminal, as yourself.
//!
//!   scripts/validate.rs [--dry-run] [--no-build] [--no-unit] [--no-librgw]
//!                       [--lifecycle] [--cells a,b] [--test EXPR]
//!                       [--build DIR] [--conf FILE] [--adoption-cache DIR]
//!
//! `--test` runs only the tests a nextest filter expression selects, in
//! each cell, in place of its profiles;  `--cells` picks the cells.  The
//! conversion cells -- the adoption corpus, `standup.rs conversion` --
//! run only when named:  each takes a fresh work directory, with the
//! NooBaa worktree already built in `--adoption-cache` linked into it.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const TARGETS: &[&str] = &["radosgw", "radosgw-admin", "ceph-conf",
    "unittest_rgw_nsfs_driver", "ceph_test_librgw_file_write2",
    "ceph_test_librgw_file_nfsns"];
const FEATURES: &str = "fails_on_nsfs,rgw_admin";
/* vstart's fixed test user, which the librgw suites mount as */
const AWS_KEY: &str = "0555b35654ad1656d804";
const AWS_SECRET: &str = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
const PARALLEL_LOG: &str = "/tmp/s3test-parallel.log";

enum Suite {
    Profile(&'static str),
    Librgw,
    /* `standup.rs conversion`, which starts its own gateway */
    Conversion { impersonate: bool, access_model: &'static str },
}

struct Cell {
    name: &'static str,
    /* empty:  the suite stands its own gateway up */
    vstart: &'static [&'static str],
    suites: &'static [Suite],
    /* run when no --cells names any */
    by_default: bool,
}

/* Adding a configuration is an edit here. */
const CELLS: &[Cell] = &[
    Cell { name: "posix-strict",
           vstart: &["--store", "nsfs", "--with-impersonation", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch")],
           by_default: true },
    Cell { name: "s3-native",
           vstart: &["--store", "nsfs", "--with-impersonation",
                     "--access-model", "s3-native", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch")],
           by_default: true },
    Cell { name: "plain",
           vstart: &["--store", "nsfs", "--bucket-profile", "strong", "--clean"],
           suites: &[Suite::Profile("baseline"), Suite::Librgw],
           by_default: true },
    /* the adoption corpus:  NooBaa writes a tree, RGW captures it before
     * and after conversion, and the captures are compared -- as the
     * gateway (C2b), and under (n) in each model (Phase G) */
    Cell { name: "conversion", vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict" }],
           by_default: false },
    Cell { name: "conversion-posix-strict", vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "posix-strict" }],
           by_default: false },
    Cell { name: "conversion-s3-native", vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "s3-native" }],
           by_default: false },
];

struct Opts {
    dry_run: bool,
    build: bool,
    unit: bool,
    librgw: bool,
    lifecycle: bool,
    cells: Option<Vec<String>>,
    test: Option<String>,
    build_dir: PathBuf,
    conf: PathBuf,
    adoption_cache: PathBuf,
}

/// One command, as it is run and as it is printed.
struct Step {
    cell: String,
    name: String,
    cwd: PathBuf,
    argv: Vec<String>,
    env: Vec<(String, String)>,
    /* the suite's own log convention:  also appended there */
    parallel_log: bool,
}

impl Step {
    fn show(&self) -> String {
        let env: String = self.env.iter()
            .map(|(k, v)| format!("{k}={} ", if k.contains("SECRET") { "…" } else { v }))
            .collect();
        let argv: Vec<String> = self.argv.iter()
            .map(|a| if a.contains(|c: char| " ()|&;<>$*?".contains(c)) {
                format!("'{a}'")
            } else {
                a.clone()
            })
            .collect();
        format!("(cd {} && {env}{})", self.cwd.display(), argv.join(" "))
    }
}

fn main() {
    let opts = parse_args();
    let suite_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().expect("the script lives in scripts/").to_path_buf();
    let stamp = Command::new("date").arg("+%Y%m%d-%H%M%S").output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "run".into());
    let run_dir = suite_dir.join("target/validate").join(&stamp);

    let steps = plan(&opts, &suite_dir, &run_dir);
    println!("validate:  {} steps, logs under {}", steps.len(), run_dir.display());
    for s in &steps {
        println!("  [{}] {}:  {}", s.cell, s.name, s.show());
    }
    if opts.dry_run {
        return;
    }

    preflight(&opts).unwrap_or_else(|e| die(&e));
    take_sudo().unwrap_or_else(|e| die(&e));
    fs::create_dir_all(&run_dir).unwrap_or_else(|e| die(&format!("{}: {e}", run_dir.display())));

    let mut results: Vec<(String, String, Res, String)> = Vec::new();
    let mut skip_cell: Option<String> = None;
    for s in &steps {
        if skip_cell.as_deref() == Some(&s.cell) {
            results.push((s.cell.clone(), s.name.clone(), Res::Fail, "not run:  the cell did not stand up".into()));
            continue;
        }
        println!("\n==> [{}] {}", s.cell, s.name);
        let (res, summary) = run(s, &run_dir);
        let ok = res != Res::Fail;
        println!("    {} {summary}", res.label());
        if !ok && s.name == "vstart" {
            skip_cell = Some(s.cell.clone());
        }
        if !ok && s.name == "build" {
            results.push((s.cell.clone(), s.name.clone(), res, summary));
            break;
        }
        if s.name == "vstart" && ok {
            if let Err(e) = wait_for_gateway() {
                results.push((s.cell.clone(), "gateway".into(), Res::Fail, e));
                skip_cell = Some(s.cell.clone());
                continue;
            }
        }
        results.push((s.cell.clone(), s.name.clone(), res, summary));
    }

    let mut table = String::from("\ncell                  step                    result\n");
    for (cell, name, res, summary) in &results {
        table += &format!("{cell:<21} {name:<23} {} {summary}\n", res.label());
    }
    print!("{table}");
    let _ = fs::write(run_dir.join("summary.txt"), &table);
    if results.iter().any(|r| r.2 == Res::Fail) {
        std::process::exit(1);
    }
    /* as corpus-compare does:  nothing failed, and known defects remain */
    if results.iter().any(|r| r.2 == Res::Known) {
        std::process::exit(3);
    }
}

fn plan(o: &Opts, suite_dir: &Path, run_dir: &Path) -> Vec<Step> {
    let b = &o.build_dir;
    let step = |cell: &str, name: &str, cwd: &Path, argv: Vec<String>| Step {
        cell: cell.into(), name: name.into(), cwd: cwd.to_path_buf(), argv,
        env: Vec::new(), parallel_log: false,
    };
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let mut steps = Vec::new();

    if o.build {
        let mut argv = s(&["ninja", &format!("-j{}", nproc())]);
        argv.extend(s(TARGETS));
        steps.push(step("build", "build", b, argv));
    }
    let caps = vstart_caps(b);
    if o.unit {
        let unit = b.join("bin/unittest_rgw_nsfs_driver");
        let caps = caps.as_ref().unwrap_or_else(|e| die(e));
        steps.push(step("unit", "setcap", b,
            s(&["sudo", "-n", "setcap", &format!("{caps}+p"), &unit.to_string_lossy()])));
        steps.push(step("unit", "unittest", b, vec![unit.to_string_lossy().into()]));
    }

    let vstart = b.join("../src/script/rgw/rgw-vstart.sh");
    for c in CELLS {
        match &o.cells {
            Some(want) if !want.iter().any(|w| w == c.name) => continue,
            None if !c.by_default => continue,
            _ => {}
        }
        /* a conversion's gateway is started with --with-lifecycle already */
        let variants = if o.lifecycle && !c.vstart.is_empty() { &[false, true][..] }
                       else { &[false][..] };
        for lc in variants {
            let cell = if *lc { format!("{}+lifecycle", c.name) } else { c.name.into() };
            let mut argv = vec![vstart.to_string_lossy().into_owned()];
            argv.extend(s(c.vstart));
            if *lc {
                argv.push("--with-lifecycle".into());
            }
            if !c.vstart.is_empty() {
                steps.push(step(&cell, "vstart", b, argv));
            }
            if let Some(expr) = &o.test {
                let mut st = step(&cell, "test", suite_dir, s(&["cargo", "nextest", "run",
                    "--no-fail-fast", "--features", FEATURES, "-E", expr]));
                st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                st.parallel_log = true;
                steps.push(st);
            }
            for suite in c.suites {
                match suite {
                    Suite::Profile(_) | Suite::Conversion { .. } if o.test.is_some() => {}
                    Suite::Librgw if !o.librgw => {}
                    Suite::Conversion { impersonate, access_model } => {
                        /* conversion raises CAP_DAC_OVERRIDE and CAP_CHOWN
                         * whether or not it impersonates, and an unimpersonated
                         * vstart sets no caps */
                        let caps = caps.as_ref().unwrap_or_else(|e| die(e));
                        steps.push(step(&cell, "setcap", b, s(&["sudo", "-n", "setcap",
                            &format!("{caps}+p"), &b.join("bin/radosgw").to_string_lossy()])));
                        /* and the unit binary, which `store-tests` runs against
                         * NooBaa's store:  root's, under impersonation */
                        steps.push(step(&cell, "setcap-unit", b, s(&["sudo", "-n", "setcap",
                            &format!("{caps}+p"),
                            &b.join("bin/unittest_rgw_nsfs_driver").to_string_lossy()])));
                        let work = run_dir.join(&cell).join("work");
                        steps.push(step(&cell, "work", b, s(&["sh", "-c",
                            "mkdir -p \"$1\" && for w in \"$2\"/noobaa-*; do ln -s \"$w\" \"$1\"/; done",
                            "sh", &work.to_string_lossy(), &o.adoption_cache.to_string_lossy()])));
                        /* from its own directory, which is where it finds its
                         * conf, patches, locks and expected differences */
                        let adoption = suite_dir.join("../adoption");
                        let mut st = step(&cell, "conversion", &adoption,
                            s(&["./standup.rs", "conversion"]));
                        st.env.push(("ADOPTION_WORK".into(), work.to_string_lossy().into()));
                        st.env.push(("ADOPTION_CEPH_BUILD".into(), b.to_string_lossy().into()));
                        st.env.push(("ADOPTION_IMPERSONATE".into(), impersonate.to_string()));
                        st.env.push(("ADOPTION_ACCESS_MODEL".into(), access_model.to_string()));
                        steps.push(st);
                    }
                    Suite::Profile(p) => {
                        let mut st = step(&cell, p, suite_dir, s(&["cargo", "nextest", "run",
                            "-P", p, "--no-fail-fast", "--features", FEATURES]));
                        st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                        st.parallel_log = true;
                        steps.push(st);
                    }
                    Suite::Librgw => {
                        for (bin, extra) in [("ceph_test_librgw_file_write2", &["--create"][..]),
                                             ("ceph_test_librgw_file_nfsns", &["--create", "--hier1"][..])] {
                            let log = run_dir.join(&cell).join(format!("{bin}.rgw.log"));
                            let mut argv = vec![b.join("bin").join(bin).to_string_lossy().into_owned()];
                            argv.extend(s(extra));
                            argv.extend(s(&["--userid", "testid"]));
                            argv.push(format!("-c {} --log-file={} --debug-rgw=10",
                                              b.join("ceph.conf").display(), log.display()));
                            let mut st = step(&cell, bin.trim_start_matches("ceph_test_librgw_file_"), b, argv);
                            st.env.push(("AWS_ACCESS_KEY_ID".into(), AWS_KEY.into()));
                            st.env.push(("AWS_SECRET_ACCESS_KEY".into(), AWS_SECRET.into()));
                            steps.push(st);
                        }
                    }
                }
            }
        }
    }
    steps
}

/// A step's result.  `Known` is the conversion step's exit 3:  every
/// difference is accounted for, and known defects remain.
#[derive(Clone, Copy, PartialEq)]
enum Res {
    Ok,
    Known,
    Fail,
}

impl Res {
    fn label(self) -> &'static str {
        match self {
            Res::Ok => "ok   ",
            Res::Known => "KNOWN",
            Res::Fail => "FAIL ",
        }
    }
}

/// Run a step with its output in its own log, and the summary line the
/// suite prints:  nextest's `Summary`, or gtest's PASSED/FAILED counts.
fn run(s: &Step, run_dir: &Path) -> (Res, String) {
    let dir = run_dir.join(&s.cell);
    let _ = fs::create_dir_all(&dir);
    let mut log = match File::create(dir.join(format!("{}.log", s.name))) {
        Ok(f) => f,
        Err(e) => return (Res::Fail, format!("cannot create its log: {e}")),
    };
    let mut parallel = s.parallel_log.then(|| {
        OpenOptions::new().create(true).append(true).open(PARALLEL_LOG).ok()
    }).flatten();
    let _ = writeln!(log, "# {}", s.show());

    /* stderr into the same pipe, so the log holds both in order */
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("exec \"$@\" 2>&1").arg("sh").args(&s.argv)
        .current_dir(&s.cwd).stdin(Stdio::null()).stdout(Stdio::piped());
    for (k, v) in &s.env {
        cmd.env(k, v);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (Res::Fail, format!("cannot start {}: {e}", s.argv[0])),
    };
    let mut summary = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
        let _ = writeln!(log, "{line}");
        if let Some(p) = parallel.as_mut() {
            let _ = writeln!(p, "{line}");
        }
        let t = line.trim_start();
        let counted = t.contains(" tests") || t.contains(" test.");
        if t.starts_with("Summary [") || t.starts_with("[  PASSED  ] ")
            || ((t.starts_with("[  FAILED  ] ") || t.starts_with("[  SKIPPED ] "))
                && counted && !t.contains(" (")) {
            summary.push(t.to_string());
        }
        /* a conversion's result in counts:  corpus-compare's two pair
         * lines, and how the profile ended */
        if let Some(pair) = t.strip_prefix("== ").filter(|p| p.starts_with("adoption (")
                                                          || p.starts_with("conversion (")) {
            summary.push(pair.to_string());
        }
        if t.starts_with("==> conversion:  every step passed")
            || t.contains("the conversion profile stopped at") {
            summary.push(t.trim_start_matches("==> conversion:  ").to_string());
        }
        if t.starts_with("FAIL [") || t.starts_with("error:") || t.starts_with("ERROR:") {
            println!("    {t}");
        }
    }
    let res = match child.wait().ok().and_then(|st| st.code()) {
        Some(0) => Res::Ok,
        /* standup.rs exits 3 when every difference is accounted for and
         * known defects remain */
        Some(3) if s.name == "conversion" => Res::Known,
        _ => Res::Fail,
    };
    (res, summary.join(";  "))
}

fn preflight(o: &Opts) -> Result<(), String> {
    let status = fs::read_to_string("/proc/self/status").map_err(|e| e.to_string())?;
    let euid = status.lines().find(|l| l.starts_with("Uid:"))
        .and_then(|l| l.split_whitespace().nth(2)).unwrap_or("0");
    if euid == "0" {
        return Err("run as yourself, not root:  rgw-vstart.sh --with-impersonation refuses root, and sudo is taken where it is needed".into());
    }
    if !std::io::stdin().is_terminal() {
        return Err("needs a terminal:  the sudo ticket is per tty, and the password is asked once, here".into());
    }
    if Command::new("pgrep").args(["-x", "ninja"]).stdout(Stdio::null()).status()
        .map(|s| s.success()).unwrap_or(false) {
        return Err("a ninja is already running;  one build at a time".into());
    }
    if !o.build_dir.join("bin").is_dir() {
        return Err(format!("{} has no bin/;  is it the ceph build directory?", o.build_dir.display()));
    }
    if !o.conf.is_file() {
        return Err(format!("{} does not exist", o.conf.display()));
    }
    Ok(())
}

/// One password, then the ticket refreshed every minute for the run.
fn take_sudo() -> Result<(), String> {
    println!("\nsudo is needed for setcap and for rgw-vstart.sh's fixture;  once:");
    if !Command::new("sudo").arg("-v").status().map(|s| s.success()).unwrap_or(false) {
        return Err("sudo -v failed".into());
    }
    std::thread::spawn(|| loop {
        std::thread::sleep(Duration::from_secs(60));
        let _ = Command::new("sudo").args(["-n", "-v"]).status();
    });
    Ok(())
}

/// The capability string, from rgw-vstart.sh's own `sudo setcap` line.
fn vstart_caps(build: &Path) -> Result<String, String> {
    let p = build.join("../src/script/rgw/rgw-vstart.sh");
    let text = fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
    text.lines()
        .find_map(|l| l.trim().strip_prefix("sudo setcap ")
                       .and_then(|r| r.split("+p").next()).map(str::to_string))
        .ok_or_else(|| format!("{} has no `sudo setcap <caps>+p` line", p.display()))
}

/// The gateway answers once vstart has started it, but not at once.
fn wait_for_gateway() -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if let Ok(mut s) = TcpStream::connect("127.0.0.1:8000") {
            if s.write_all(b"GET / HTTP/1.0\r\n\r\n").is_ok() {
                let mut first = String::new();
                if BufReader::new(s).read_line(&mut first).is_ok() && first.starts_with("HTTP/") {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err("the gateway did not answer on :8000 within 60 s".into())
}

fn nproc() -> String {
    Command::new("nproc").output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "1".into())
}

fn parse_args() -> Opts {
    let home = std::env::var("HOME").unwrap_or_default();
    let mut o = Opts {
        dry_run: false, build: true, unit: true, librgw: true,
        lifecycle: false, cells: None, test: None,
        build_dir: PathBuf::from("/lv2tb/ceph-cp/build"),
        conf: PathBuf::from(format!("{home}/dev/rgw/s3tests.conf")),
        adoption_cache: PathBuf::from("/lv2tb/nsfs-interchange"),
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = |flag: &str| args.next().unwrap_or_else(|| die(&format!("{flag} takes a value")));
        match a.as_str() {
            "--dry-run" => o.dry_run = true,
            "--no-build" => o.build = false,
            "--no-unit" => o.unit = false,
            "--no-librgw" => o.librgw = false,
            "--test" => o.test = Some(val("--test")),
            "--lifecycle" => o.lifecycle = true,
            "--cells" => o.cells = Some(val("--cells").split(',').map(str::to_string).collect()),
            "--build" => o.build_dir = PathBuf::from(val("--build")),
            "--conf" => o.conf = PathBuf::from(val("--conf")),
            "--adoption-cache" => o.adoption_cache = PathBuf::from(val("--adoption-cache")),
            _ => die(&format!("unknown argument {a};  see the header of validate.rs")),
        }
    }
    if let Some(want) = &o.cells {
        for w in want {
            if !CELLS.iter().any(|c| c.name == w) {
                die(&format!("no cell {w};  the cells are {}",
                             CELLS.iter().map(|c| c.name).collect::<Vec<_>>().join(", ")));
            }
        }
    }
    o
}

fn die(msg: &str) -> ! {
    eprintln!("validate:  {msg}");
    std::process::exit(2);
}
