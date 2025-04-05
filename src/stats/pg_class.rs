use pgrx::pg_sys::SysCacheIdentifier::RELOID;
use pgrx::pg_sys::{AsPgCStr, Name, Oid};
use pgrx::{pg_catalog, pg_extern, pg_sys, IntoDatum, PgOid, PostgresType, Spi};
use serde::{Deserialize, Serialize};
use std::ptr::NonNull;

#[derive(PostgresType, Serialize, Deserialize, Debug)]
pub(crate) struct PostgresClassStat {
    pub relname: String,
    pub relnamespace: Oid,
    pub reltuples: f32,
    pub relpages: i32,
    pub relallvisible: i32,
}
