//! Runs the *upstream* negentropy test-suite (`fuzz.pl`, `protoversion.pl`) from
//! `third_party/negentropy-ref/test` against our harness binary.
//!
//! The upstream suite is written in Perl and resolves a harness command per language through
//! `Utils::harnessTypeToCmd`. We register our `negentropy-harness` binary as the `airfry`
//! language by generating a *copy* of the upstream scripts into a temporary directory and
//! rewriting them there:
//!
//!   * `use lib "$FindBin::Bin"` is replaced with `use lib "<shim>"`, so the shim `Utils.pm`
//!     (which knows about `airfry`) is the one that gets loaded. This is the only reliable way:
//!     `PERL5LIB` / `PERL5OPT=-I` are processed *before* the script body, so the runtime
//!     `use lib $FindBin::Bin` would otherwise unshift the real test dir and shadow the shim.
//!   * `Session::Token` is not installed in this environment, so the shim dir also carries a
//!     drop-in `Session/Token.pm` (found via `PERL5LIB`, which is consulted before the script
//!     body runs). It reproduces the upstream generator's semantics (deterministic PRNG seeded
//!     from the given seed, uniform draws over the given alphabet) so both harnesses receive
//!     byte-identical item streams.
//!
//! The scripts are run with CWD = the real upstream test dir so relative harness paths such as
//! `./cpp/harness` keep working.
//!
//! These tests are the strongest conformance evidence we have: they are the exact same scripts
//! strfry/negentropy maintainers run for cross-implementation interop, including frame-size
//! limits, clustered timestamps, duplicate handling and protocol-version fallback.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn our_harness() -> PathBuf {
    let mut p = std::env::current_exe().unwrap();
    p.pop(); // deps
    p.pop(); // profile dir
    p.push("negentropy-harness");
    p
}

fn ref_test_dir() -> PathBuf {
    repo_root().join("third_party/negentropy-ref/test")
}

fn perl_available() -> bool {
    Command::new("perl")
        .arg("-e")
        .arg("1")
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// True when the upstream reference checkout is present (it lives in the gitignored
/// `third_party/` tree, so a fresh clone does not have it). Both tests must skip cleanly
/// rather than panic when it is absent.
fn ref_available() -> bool {
    ref_test_dir().join("Utils.pm").exists()
}

/// Pure-Perl `Session::Token` replacement: deterministic xorshift PRNG seeded from the seed
/// string, uniform alphabet draws. Only `new(seed=>..., alphabet=>..., length=>...)` and `get()`
/// are used by the upstream scripts.
const SESSION_TOKEN_PM: &str = r#"package Session::Token;
use strict;
sub new {
    my ($class, %args) = @_;
    my $seed = $args{seed} // '';
    my $s = 0;
    for my $c (split //, $seed) { $s = ($s * 131 + ord($c)) & 0xffffffff; }
    $s = 2463534242 if $s == 0;
    my $self = {
        state => $s,
        alphabet => $args{alphabet} // 'abcdefghijklmnopqrstuvwxyz',
        length => $args{length} // 16,
    };
    return bless $self, $class;
}
sub get {
    my $self = shift;
    my @a = split //, $self->{alphabet};
    my $n = scalar @a;
    my $out = '';
    for (1 .. $self->{length}) {
        my $x = $self->{state};
        $x ^= ($x << 13) & 0xffffffff;
        $x ^= ($x >> 17);
        $x ^= ($x << 5) & 0xffffffff;
        $self->{state} = $x & 0xffffffff;
        $out .= $a[$x % $n];
    }
    return $out;
}
1;
"#;

/// Build the shim directory: a patched `Utils.pm` that maps `airfry` to our harness, the
/// `AirfrySessionToken` drop-in, and rewritten copies of the upstream scripts.
fn make_shim(dir: &Path, harness: &Path, upstream: &Path, scripts: &[&str]) -> Vec<PathBuf> {
    std::fs::create_dir_all(dir.join("Session")).unwrap();

    // Copy the upstream Utils.pm verbatim and append an `airfry` branch to its
    // `harnessTypeToCmd`.
    let upstream_src = std::fs::read_to_string(upstream.join("Utils.pm")).unwrap();
    let patched = upstream_src.replacen(
        "    if ($harnessType eq 'cpp') {",
        &format!(
            "    if ($harnessType eq 'airfry') {{\n        return '{}';\n    }} elsif ($harnessType eq 'cpp') {{",
            harness.display()
        ),
        1,
    );
    assert!(
        patched.contains("airfry"),
        "failed to patch upstream Utils.pm"
    );
    std::fs::write(dir.join("Utils.pm"), patched).unwrap();
    std::fs::write(dir.join("Session/Token.pm"), SESSION_TOKEN_PM).unwrap();

    let mut out = Vec::new();
    for script in scripts {
        let src = std::fs::read_to_string(upstream.join(script)).unwrap();
        let rewritten = src.replace(
            "use lib \"$FindBin::Bin\";",
            &format!("use lib \"{}\";", dir.display()),
        );
        assert!(
            rewritten.contains(&format!("use lib \"{}\"", dir.display())),
            "failed to rewrite `use lib` in {script}"
        );
        let dst = dir.join(script);
        std::fs::write(&dst, rewritten).unwrap();
        out.push(dst);
    }
    out
}

fn run_perl(script: &Path, args: &[&str], envs: &[(&str, &str)]) -> bool {
    let test_dir = ref_test_dir();
    let mut cmd = Command::new("perl");
    // CWD must be the real upstream test dir: harness commands are relative (`./cpp/harness`).
    cmd.current_dir(&test_dir).arg(script).args(args);
    // `Session::Token` is not installed system-wide; the shim dir carries a drop-in copy.
    cmd.env("PERL5LIB", script.parent().unwrap());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("run perl");
    let ok = out.status.success();
    if !ok {
        eprintln!(
            "--- {} {args:?} {envs:?} FAILED ---\nstdout:\n{}\nstderr:\n{}",
            script.display(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    ok
}

#[test]
fn upstream_protoversion_suite() {
    if !perl_available() {
        eprintln!("SKIP: perl not available");
        return;
    }
    if !ref_available() {
        eprintln!(
            "SKIP: upstream negentropy reference not present at {} (run scripts/fetch_negentropy_ref.sh)",
            ref_test_dir().display()
        );
        return;
    }
    let harness = our_harness();
    assert!(harness.exists(), "harness not built: {}", harness.display());
    let shim = std::env::temp_dir().join(format!("airfry-neg-shim-{}", std::process::id()));
    let scripts = make_shim(&shim, &harness, &ref_test_dir(), &["protoversion.pl"]);

    assert!(
        run_perl(&scripts[0], &["airfry"], &[]),
        "upstream protoversion.pl failed"
    );
    eprintln!("upstream protoversion.pl: OK");
}

#[test]
fn upstream_fuzz_suite_airfry_vs_cpp() {
    if !perl_available() {
        eprintln!("SKIP: perl not available");
        return;
    }
    if !ref_available() {
        eprintln!(
            "SKIP: upstream negentropy reference not present at {} (run scripts/fetch_negentropy_ref.sh)",
            ref_test_dir().display()
        );
        return;
    }
    let harness = our_harness();
    assert!(harness.exists(), "harness not built: {}", harness.display());
    let ref_harness = ref_test_dir().join("cpp/harness");
    if !ref_harness.exists() {
        eprintln!("SKIP: reference C++ harness not built");
        return;
    }
    let shim = std::env::temp_dir().join(format!("airfry-neg-shim-{}", std::process::id()));
    let scripts = make_shim(&shim, &harness, &ref_test_dir(), &["fuzz.pl"]);

    // The exact option matrix from upstream `test.pl`, run in both directions
    // (airfry initiator <-> C++ responder and vice versa).
    let cases: &[&[(&str, &str)]] = &[
        &[
            ("RECS", "50000"),
            ("P1", "1"),
            ("P2", "0"),
            ("P3", "0"),
            ("NODUPS", "1"),
        ],
        &[
            ("RECS", "50000"),
            ("P1", "0"),
            ("P2", "1"),
            ("P3", "0"),
            ("NODUPS", "1"),
        ],
        &[
            ("RECS", "50000"),
            ("P1", "0"),
            ("P2", "0"),
            ("P3", "1"),
            ("NODUPS", "1"),
        ],
        &[
            ("RECS", "50000"),
            ("P1", "2"),
            ("P2", "2"),
            ("P3", "7"),
            ("NODUPS", "1"),
        ],
        &[
            ("RECS", "100000"),
            ("FRAMESIZELIMIT1", "60000"),
            ("FRAMESIZELIMIT2", "500000"),
            ("P1", "1"),
            ("P2", "0"),
            ("P3", "0"),
        ],
        &[
            ("RECS", "100000"),
            ("FRAMESIZELIMIT1", "60000"),
            ("FRAMESIZELIMIT2", "500000"),
            ("P1", "0"),
            ("P2", "1"),
            ("P3", "0"),
        ],
        &[
            ("RECS", "100000"),
            ("FRAMESIZELIMIT1", "60000"),
            ("FRAMESIZELIMIT2", "500000"),
            ("P1", "0"),
            ("P2", "0"),
            ("P3", "1"),
        ],
        &[
            ("RECS", "100000"),
            ("FRAMESIZELIMIT1", "60000"),
            ("FRAMESIZELIMIT2", "500000"),
            ("P1", "1"),
            ("P2", "1"),
            ("P3", "5"),
        ],
        &[
            ("CLUSTERED", "1"),
            ("FRAMESIZELIMIT1", "50000"),
            ("FRAMESIZELIMIT2", "50000"),
        ],
        &[
            ("CLUSTERED", "1"),
            ("FRAMESIZELIMIT1", "50000"),
            ("FRAMESIZELIMIT2", "50000"),
            ("P1", "1"),
            ("P2", "1"),
            ("P3", "1"),
        ],
        &[("CLUSTERED", "1"), ("NODUPS", "1")],
    ];

    for (i, opts) in cases.iter().enumerate() {
        for (a, b) in [("airfry", "cpp"), ("cpp", "airfry")] {
            assert!(
                run_perl(&scripts[0], &[a, b], opts),
                "upstream fuzz.pl case {i} ({a} vs {b}) failed: {opts:?}"
            );
            eprintln!("upstream fuzz.pl case {i} ({a} vs {b}): OK");
        }
    }
}
