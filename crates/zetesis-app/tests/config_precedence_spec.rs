use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    path::Path,
    sync::{Mutex, OnceLock},
};

use tempfile::TempDir;
use zetesis_app::config;

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("config env mutex poisoned")
}

fn snapshot_env(vars: &[&'static str]) -> Vec<(&'static str, Option<OsString>)> {
    vars.iter().map(|&name| (name, env::var_os(name))).collect()
}

fn restore_env(vars: Vec<(&'static str, Option<OsString>)>) {
    for (name, value) in vars {
        match value {
            Some(val) => set_var(name, val),
            None => remove_var(name),
        }
    }
}

fn write_listen_addr(path: &Path, addr: &str) {
    debug_assert!(!addr.is_empty());
    fs::write(path, format!("[server]\nlisten_addr = \"{addr}\"\n")).expect("write config file");
}

fn set_var(name: &str, value: impl AsRef<OsStr>) {
    unsafe { env::set_var(name, value) }
}

fn remove_var(name: &str) {
    unsafe { env::remove_var(name) }
}

#[test]
fn config_precedence_follows_documented_order() {
    let _guard = env_guard();

    let tracked = [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "ZETESIS_ETC_CONFIG_DIR",
        "ZETESIS_CONFIG_FILE",
        "ZETESIS__SERVER__LISTEN_ADDR",
    ];
    let env_snapshot = snapshot_env(&tracked);
    let original_dir = env::current_dir().expect("capture current dir");

    let workspace = TempDir::new().expect("temp workspace");
    let workspace_path = workspace.path();
    let etc_root = workspace_path.join("etc");
    let xdg_config_root = workspace_path.join("xdg_config");
    let xdg_data_root = workspace_path.join("xdg_data");
    let local_config_dir = workspace_path.join("config");
    let override_path = workspace_path.join("override.toml");

    fs::create_dir_all(etc_root.join("zetesis")).expect("create etc dir");
    fs::create_dir_all(xdg_config_root.join("zetesis")).expect("create xdg dir");
    fs::create_dir_all(&local_config_dir).expect("create local config dir");

    env::set_current_dir(workspace_path).expect("change to workspace");
    set_var("HOME", workspace_path);
    set_var("XDG_CONFIG_HOME", &xdg_config_root);
    set_var("XDG_DATA_HOME", &xdg_data_root);
    set_var("ZETESIS_ETC_CONFIG_DIR", &etc_root);
    set_var("ZETESIS_CONFIG_FILE", &override_path);

    let etc_path = etc_root.join("zetesis").join("settings.toml");
    let xdg_path = xdg_config_root.join("zetesis").join("settings.toml");
    let local_path = local_config_dir.join("settings.toml");

    write_listen_addr(&etc_path, "127.0.0.1:9001");
    write_listen_addr(&xdg_path, "127.0.0.1:9002");
    write_listen_addr(&local_path, "127.0.0.1:9003");
    write_listen_addr(&override_path, "127.0.0.1:9004");

    set_var("ZETESIS__SERVER__LISTEN_ADDR", "127.0.0.1:9005");
    let config_env = config::load().expect("load config with env override");
    assert_eq!(config_env.server.listen_addr, "127.0.0.1:9005");

    remove_var("ZETESIS__SERVER__LISTEN_ADDR");
    let config_override = config::load().expect("load config with override file");
    assert_eq!(config_override.server.listen_addr, "127.0.0.1:9004");

    remove_var("ZETESIS_CONFIG_FILE");
    let config_local = config::load().expect("load config from local config");
    assert_eq!(config_local.server.listen_addr, "127.0.0.1:9003");

    fs::remove_file(&local_path).expect("remove local config");
    let config_xdg = config::load().expect("load config from xdg config");
    assert_eq!(config_xdg.server.listen_addr, "127.0.0.1:9002");

    fs::remove_file(&xdg_path).expect("remove xdg config");
    let config_etc = config::load().expect("load config from etc");
    assert_eq!(config_etc.server.listen_addr, "127.0.0.1:9001");

    env::set_current_dir(&original_dir).expect("restore current dir");
    restore_env(env_snapshot);
}
