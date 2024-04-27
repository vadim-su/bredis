pub struct Info {
    pub version: String,
    pub rustc: String,
}

impl Default for Info {
    fn default() -> Self {
        let version = format!(
            "{}-{}",
            env!("CARGO_PKG_VERSION"),
            short_sha(env!("VERGEN_GIT_SHA")),
        );
        let rustc = format!(
            "{}-{}",
            env!("VERGEN_RUSTC_SEMVER"),
            short_sha(env!("VERGEN_RUSTC_COMMIT_HASH"))
        );

        return Self { version, rustc };
    }
}

fn short_sha(sha: &str) -> String {
    sha.chars().take(7).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_sha() {
        assert_eq!(short_sha("1234567890"), "1234567");
    }
}
