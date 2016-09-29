// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::ffi::OsString;
use std::process::{Command, Stdio};
use std::path::Path;

use common::ui::UI;
use hab::VERSION;
use hab_core::crypto::default_cache_key_path;
use hab_core::env as henv;
use hab_core::fs::{CACHE_KEY_PATH, find_command};
use hab_core::package::archive::PackageArchive;
use protocol::jobsrv as proto;

use error::Result;

const DOCKER_CMD: &'static str = "docker";
const DOCKER_CMD_ENVVAR: &'static str = "HAB_DOCKER_BINARY";

const DOCKER_IMAGE: &'static str = "habitat-docker-registry.bintray.io/studio";
const DOCKER_IMAGE_ENVVAR: &'static str = "HAB_DOCKER_STUDIO_IMAGE";

pub fn build(job: &proto::Job, root: &Path) -> Result<PackageArchive> {
    let mut ui = UI::default();
    let commands = vec![OsString::from("build"),
                        OsString::from(&root.join(job.get_project().get_plan_path()))];
    match start(&mut ui, commands) {
        Ok(()) => println!("WE FUCKING DID IT"),
        Err(e) => println!("WE FUCKING FAILED: {:?}", e),
    }
    // actually figure out where the thing that was built came from by reading the results of
    // last_build.env
    let archive = PackageArchive::new(root.join("results"));
    Ok(archive)
}

fn start(ui: &mut UI, args: Vec<OsString>) -> Result<()> {
    let docker = henv::var(DOCKER_CMD_ENVVAR).unwrap_or(DOCKER_CMD.to_string());
    let image = henv::var(DOCKER_IMAGE_ENVVAR).unwrap_or(format!("{}:{}", DOCKER_IMAGE, VERSION));

    let cmd = match find_command(&docker) {
        Some(cmd) => cmd,
        None => panic!("DOCKER NOT FOUND MAN"),
    };

    let mut cmd_args: Vec<OsString> = vec!["run".into(), "--rm".into(), "--privileged".into()];
    if ui.is_a_tty() {
        cmd_args.push("--tty".into());
        cmd_args.push("--interactive".into());
    }
    let env_vars = vec!["HAB_DEPOT_URL", "HAB_ORIGIN", "http_proxy", "https_proxy"];
    for var in env_vars {
        if let Ok(val) = henv::var(var) {
            debug!("Propagating environment variable into container: {}={}",
                   var,
                   val);
            cmd_args.push("--env".into());
            cmd_args.push(format!("{}={}", var, val).into());
        }
    }
    cmd_args.push("--volume".into());
    cmd_args.push("/var/run/docker.sock:/var/run/docker.sock".into());
    cmd_args.push("--volume".into());
    cmd_args.push(format!("{}:/{}",
                          default_cache_key_path(None).to_string_lossy(),
                          CACHE_KEY_PATH)
        .into());
    cmd_args.push("--volume".into());
    cmd_args.push(format!("{}:/src", env::current_dir().unwrap().to_string_lossy()).into());
    cmd_args.push(image.into());
    cmd_args.extend_from_slice(args.as_slice());

    for var in vec!["http_proxy", "https_proxy"] {
        if let Ok(_) = henv::var(var) {
            debug!("Unsetting proxy environment variable '{}' before calling `{}'",
                   var,
                   docker);
            env::remove_var(var);
        }
    }

    let mut child = Command::new(cmd)
        .args(&cmd_args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("failed to spawn child");
    let code = child.wait().expect("failed to wait on child");
    println!("DONE: {:?}", code);
    Ok(())
}
