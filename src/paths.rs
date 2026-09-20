//! Where the DuckDB database lives when `--db` is not given.
//!
//! The database originally sat at `./health.duckdb`, relative to the working
//! directory. New installs keep it under `~/.config/apple-health-mcp/` instead,
//! but an existing database in the working directory still wins so that setups
//! predating the move keep working untouched.

use anyhow::{Context, Result};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Directory created under the user's config home.
pub const APP_DIR: &str = "apple-health-mcp";

/// File name of the DuckDB database.
pub const DB_FILE: &str = "health.duckdb";

/// Environment variable that overrides database discovery entirely.
pub const DB_ENV_VAR: &str = "APPLE_HEALTH_MCP_DB";

/// The pieces of the environment path resolution depends on.
///
/// Captured up front rather than read ad hoc so the resolution rules can be
/// tested without mutating process-wide state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathEnv {
    /// `$APPLE_HEALTH_MCP_DB`.
    pub db_override: Option<PathBuf>,
    /// `$XDG_CONFIG_HOME`.
    pub xdg_config_home: Option<PathBuf>,
    /// `$HOME`.
    pub home: Option<PathBuf>,
    /// Directory the legacy database is looked for in.
    pub working_dir: PathBuf,
}

impl PathEnv {
    /// Read the environment of the current process.
    pub fn from_env() -> Self {
        Self {
            db_override: env_path(DB_ENV_VAR),
            xdg_config_home: env_path("XDG_CONFIG_HOME"),
            home: env_path("HOME"),
            working_dir: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        }
    }

    /// `~/.config/apple-health-mcp`, or `None` when neither `$XDG_CONFIG_HOME`
    /// nor `$HOME` is set.
    pub fn config_dir(&self) -> Option<PathBuf> {
        self.xdg_config_home
            .clone()
            .or_else(|| self.home.as_ref().map(|home| home.join(".config")))
            .map(|config_home| config_home.join(APP_DIR))
    }

    /// The pre-`~/.config` location: `health.duckdb` in the working directory.
    pub fn legacy_db_path(&self) -> PathBuf {
        self.working_dir.join(DB_FILE)
    }

    /// Database path to use when `--db` is not given: the `$APPLE_HEALTH_MCP_DB`
    /// override, then an existing database in the working directory, then
    /// `~/.config/apple-health-mcp/health.duckdb`.
    pub fn default_db_path(&self) -> PathBuf {
        if let Some(path) = &self.db_override {
            return path.clone();
        }

        let legacy = self.legacy_db_path();
        if legacy.exists() {
            return legacy;
        }

        self.config_dir()
            .map(|dir| dir.join(DB_FILE))
            .unwrap_or(legacy)
    }
}

/// Database path to use when `--db` is not given. See
/// [`PathEnv::default_db_path`] for the resolution order.
pub fn default_db_path() -> PathBuf {
    PathEnv::from_env().default_db_path()
}

/// Create the directory `path` lives in, if it does not exist yet.
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display())),
        _ => Ok(()),
    }
}

/// Read an environment variable, treating an empty value as unset.
fn env_path(key: &str) -> Option<PathBuf> {
    non_empty(std::env::var_os(key))
}

/// An unset variable and one set to the empty string mean the same thing here.
fn non_empty(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn env_with_working_dir(dir: &Path) -> PathEnv {
        PathEnv {
            working_dir: dir.to_path_buf(),
            ..PathEnv::default()
        }
    }

    #[test]
    fn override_wins_over_everything() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(DB_FILE), b"").unwrap();

        let env = PathEnv {
            db_override: Some(PathBuf::from("/elsewhere/custom.duckdb")),
            home: Some(PathBuf::from("/home/tester")),
            ..env_with_working_dir(dir.path())
        };

        assert_eq!(
            env.default_db_path(),
            PathBuf::from("/elsewhere/custom.duckdb")
        );
    }

    #[test]
    fn existing_database_in_working_dir_wins_over_config_dir() {
        let dir = TempDir::new().unwrap();
        let legacy = dir.path().join(DB_FILE);
        std::fs::write(&legacy, b"").unwrap();

        let env = PathEnv {
            home: Some(PathBuf::from("/home/tester")),
            ..env_with_working_dir(dir.path())
        };

        assert_eq!(env.default_db_path(), legacy);
    }

    #[test]
    fn falls_back_to_config_dir_when_working_dir_is_empty() {
        let dir = TempDir::new().unwrap();
        let env = PathEnv {
            home: Some(PathBuf::from("/home/tester")),
            ..env_with_working_dir(dir.path())
        };

        assert_eq!(
            env.default_db_path(),
            PathBuf::from("/home/tester/.config/apple-health-mcp/health.duckdb")
        );
    }

    #[test]
    fn xdg_config_home_takes_precedence_over_home() {
        let dir = TempDir::new().unwrap();
        let env = PathEnv {
            xdg_config_home: Some(PathBuf::from("/xdg")),
            home: Some(PathBuf::from("/home/tester")),
            ..env_with_working_dir(dir.path())
        };

        assert_eq!(
            env.config_dir(),
            Some(PathBuf::from("/xdg/apple-health-mcp"))
        );
        assert_eq!(
            env.default_db_path(),
            PathBuf::from("/xdg/apple-health-mcp/health.duckdb")
        );
    }

    #[test]
    fn falls_back_to_working_dir_without_a_config_home() {
        let dir = TempDir::new().unwrap();
        let env = env_with_working_dir(dir.path());

        assert_eq!(env.config_dir(), None);
        assert_eq!(env.default_db_path(), dir.path().join(DB_FILE));
    }

    #[test]
    fn ensure_parent_dir_creates_missing_directories() {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("a").join("b").join(DB_FILE);

        ensure_parent_dir(&db).unwrap();

        assert!(db.parent().unwrap().is_dir());
    }

    #[test]
    fn ensure_parent_dir_accepts_a_bare_file_name() {
        ensure_parent_dir(Path::new(DB_FILE)).unwrap();
    }

    #[test]
    fn ensure_parent_dir_reports_the_offending_path() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("not-a-directory");
        std::fs::write(&file, b"").unwrap();

        let err = ensure_parent_dir(&file.join(DB_FILE)).unwrap_err();

        assert!(err.to_string().contains(&file.display().to_string()));
    }

    #[test]
    fn empty_environment_variables_are_treated_as_unset() {
        assert_eq!(non_empty(None), None);
        assert_eq!(non_empty(Some(OsString::from(""))), None);
        assert_eq!(
            non_empty(Some(OsString::from("/db.duckdb"))),
            Some(PathBuf::from("/db.duckdb"))
        );
    }

    #[test]
    fn from_env_resolves_a_usable_path() {
        let path = PathEnv::from_env().default_db_path();
        assert!(path.ends_with(DB_FILE));
        assert_eq!(default_db_path(), path);
    }
}
