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

use std::fs;
use std::path::{Path, PathBuf};

use protocol::jobsrv as proto;

use error::{Error, Result};

pub struct Workspace(PathBuf);

impl Workspace {
    pub fn new(data_path: String, job: &proto::Job) -> Self {
        Workspace(PathBuf::from(data_path).join(job.get_id().to_string()))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn setup(&self) -> Result<()> {
        if let Some(err) = fs::create_dir_all(self.path()).err() {
            return Err(Error::WorkspaceSetup(format!("{}", self.0.display()), err));
        }
        Ok(())
    }

    pub fn teardown(&self) -> Result<()> {
        if let Some(err) = fs::remove_dir_all(self.path()).err() {
            return Err(Error::WorkspaceTeardown(format!("{}", self.0.display()), err));
        }
        Ok(())
    }
}
