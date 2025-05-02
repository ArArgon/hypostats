use core::ffi;
use pgrx::pg_sys::{int16, int32, Oid};
use pgrx::PostgresType;
use serde::{Deserialize, Serialize};

#[derive(PostgresType, Serialize, Deserialize, Debug)]
pub struct MyInt2vector {
    pub vl_len_: int32,
    pub ndim: ffi::c_int,
    pub dataoffset: int32,
    pub elemtype: Oid,
    pub dim1: ffi::c_int,
    pub lbound1: ffi::c_int,
    pub values: Vec<int16>,
}

#[derive(PostgresType, Serialize, Deserialize, Debug)]
pub struct PgStatisticExt {
    pub oid: Oid,
    pub stxrelid: Oid,
    pub stxname: String,
    pub stxnamespace: Oid,
    pub stxowner: Oid,
    pub stxkeys: MyInt2vector,

    // Variable length fields
    pub stxstattarget: Option<i16>,
    pub stxkind: Option<String>,
    pub stxexprs: Option<String>,
}
