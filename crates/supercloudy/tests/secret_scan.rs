use std::{fs, path::PathBuf, process::Command};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn public_tree_and_history_contain_no_detected_secrets() {
    let status = Command::new("bash")
        .arg(root().join("scripts/secret-scan.sh"))
        .status()
        .expect("run secret scan");
    assert!(status.success(), "repository secret scan failed");
}

#[test]
fn scanner_rejects_synthetic_secret_and_ignored_credential_file() {
    let fixture =
        std::env::temp_dir().join(format!("supercloudy-secret-scan-{}", std::process::id()));
    fs::create_dir(&fixture).expect("create isolated synthetic fixture");
    let scan = || {
        Command::new("bash")
            .arg(root().join("scripts/secret-scan.sh"))
            .arg(&fixture)
            .output()
            .expect("run scanner on fixture")
    };
    // Deliberately generated, invalid credentials; never real account material.
    let synthetic = format!("{}{}", "AKIA", "Z".repeat(16));
    fs::write(fixture.join("sample.txt"), &synthetic).unwrap();
    let rejected = scan();
    assert!(!rejected.status.success(), "missed a synthetic access key");
    assert!(
        !String::from_utf8_lossy(&rejected.stderr).contains(&synthetic),
        "scanner leaked matching content"
    );
    for arguments in [vec!["init", "--quiet"], vec!["add", "sample.txt"]] {
        assert!(
            Command::new("git")
                .args(arguments)
                .current_dir(&fixture)
                .status()
                .unwrap()
                .success()
        );
    }
    fs::remove_file(fixture.join("sample.txt")).unwrap();
    assert!(
        !scan().status.success(),
        "missed staged secret removed from working tree"
    );
    fs::remove_dir_all(fixture.join(".git")).unwrap();
    for value in ["test!synthetic", "\"test synthetic\"", "\"TEST\""] {
        fs::write(
            fixture.join("sample.txt"),
            ["password", "=", value, "\n"].concat(),
        )
        .unwrap();
        assert!(
            !scan().status.success(),
            "dummy allowance must match only the complete literal test"
        );
    }
    fs::write(
        fixture.join("sample.txt"),
        ["AWS_SECRET_ACCESS_KEY", "=test\n"].concat(),
    )
    .unwrap();
    assert!(scan().status.success(), "dummy credentials must be allowed");
    fs::write(fixture.join(".env.local"), "synthetic fixture\n").unwrap();
    assert!(
        !scan().status.success(),
        "missed an ignored credential file"
    );
    fs::remove_dir_all(&fixture).unwrap();
}
