// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// Enable serving of unstable APIs
    ///
    /// Defaults to `false`, with unstable APIs being disabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_unstable_apis: Option<bool>,

    // Only include this till we have another field that isn't set with a non-default value for
    // testing
    #[doc(hidden)]
    #[serde(skip)]
    pub _hidden: (),
}

impl Config {
    pub fn enable_unstable_apis(&self) -> bool {
        // TODO
        // Until the rest service as a whole is "stabalized" with a sane set of default stable
        // apis, have the default be to enable all apis
        self.enable_unstable_apis.unwrap_or(true)
    }
}

