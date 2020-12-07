#![allow(clippy::type_complexity)]
//! This is an mighty card game server.

pub mod actor;
pub mod app_state;
pub mod db;
#[cfg(feature = "https")]
pub mod https;
pub mod service;
pub mod util;
pub mod mail;
