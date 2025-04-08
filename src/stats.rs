use pgrx::pg_sys::Oid;
use pgrx::PostgresType;
use serde::{Deserialize, Serialize};

#[derive(PostgresType, Serialize, Deserialize, Debug)]
pub(crate) struct PostgresClassStat {
    pub relname: String,
    pub relnamespace: Oid,
    pub reltuples: f32,
    pub relpages: i32,
    pub relallvisible: i32,
}
