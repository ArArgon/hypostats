mod catalog;
pub mod stats;

// use pg_sys::FormData_pg_statistic;
// use pg_sys::SysCacheIdentifier::STATRELATTINH;
use crate::catalog::heap_tuple::{HeapTuple, ModifyContext};
// use pgrx::{direct_function_call, AnyArray, Array, Json};
use crate::catalog::relation::Relation;
use crate::stats::pg_class::PostgresClassStat;
use pgrx::pg_sys;
use pgrx::pg_sys::{AsPgCStr, FunctionCallInfo, Name, NameData, Oid};
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::ffi::{CStr, CString};

::pgrx::pg_module_magic!();

// Safe to ignore pg_class because of its lazy flags

#[derive(Debug, Serialize, Deserialize)]
struct JsonF32arr {
    data: String,
}

impl JsonF32arr {
    fn from(opt_arrf32: Option<Array<f32>>) -> Self {
        let data = match opt_arrf32 {
            None => "null".to_string(),
            Some(arrf32) => {
                let j = unsafe {
                    pgrx::direct_function_call::<pgrx::Json>(
                        pg_sys::array_to_json,
                        &[arrf32.into_datum()],
                    )
                    .unwrap()
                };
                serde_json::to_string(&j).unwrap()
            }
        };
        JsonF32arr { data }
    }

    unsafe fn set_pg_statistic_tuple(
        &self,
        values: &mut Vec<pg_sys::Datum>,
        nulls: &mut Vec<bool>,
        index: usize,
    ) {
        match self.data.as_str() {
            "null" => {
                nulls[index] = true;
                values[index] = pg_sys::Datum::from(0);
            }
            s => {
                let v_f32: Vec<f32> = serde_json::from_str(s).unwrap();
                // let numdatums = pg_sys::palloc(v_f32.len() * size_of::<pg_sys::Datum>()) as *mut pg_sys::Datum;
                // for (i, x) in v_f32.iter().enumerate() {
                //     *numdatums.add(i) = f32::into_datum(*x).unwrap();
                // }
                // let arry = pg_sys::construct_array(numdatums, v_f32.len() as i32, pg_sys::FLOAT4OID, size_of::<pg_sys::float4>() as i32, true, pg_sys::TYPALIGN_INT as i8);
                // let datum = pg_sys::Datum::from(arry);

                nulls[index] = false;
                values[index] = Vec::<f32>::into_datum(v_f32).unwrap();
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonAarr {
    typ: pg_sys::Oid,
    data: String,
}

impl JsonAarr {
    fn from(opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>)) -> Self {
        match opt_aarr {
            (None, _) => JsonAarr {
                typ: pg_sys::ANYARRAYOID,
                data: "null".to_string(),
            },
            (Some(aarr), opt_oid) => {
                let j = unsafe {
                    pgrx::direct_function_call::<pgrx::Json>(
                        pg_sys::array_to_json,
                        &[aarr.into_datum()],
                    )
                    .unwrap()
                };
                JsonAarr {
                    typ: opt_oid.unwrap(),
                    data: serde_json::to_string(&j).unwrap(),
                }
            }
        }
    }

    unsafe fn set_pg_statistic_tuple(
        &self,
        values: &mut Vec<pg_sys::Datum>,
        nulls: &mut Vec<bool>,
        index: usize,
    ) {
        match self.data.as_str() {
            "null" => {
                nulls[index] = true;
                values[index] = pg_sys::Datum::from(0);
            }
            s => {
                let datum = match self.typ {
                    pg_sys::FLOAT4OID => {
                        let v_f32: Vec<f32> = serde_json::from_str(s).unwrap();
                        Vec::<f32>::into_datum(v_f32).unwrap()
                    }
                    pg_sys::FLOAT8OID => {
                        let v_f64: Vec<f64> = serde_json::from_str(s).unwrap();
                        Vec::<f64>::into_datum(v_f64).unwrap()
                    }
                    pg_sys::INT4OID => {
                        let v_i32: Vec<i32> = serde_json::from_str(s).unwrap();
                        Vec::<i32>::into_datum(v_i32).unwrap()
                    }
                    pg_sys::INT8OID => {
                        let v_i64: Vec<i64> = serde_json::from_str(s).unwrap();
                        Vec::<i64>::into_datum(v_i64).unwrap()
                    }
                    _ => panic!("Unsupported type"),
                };
                nulls[index] = false;
                values[index] = datum;
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PgStatisticRow {
    starelid: pg_sys::Oid,
    staattnum: i16,
    stainherit: bool,
    stanullfrac: f32,
    stawidth: i32,
    stadistinct: f32,
    stakind1: i16,
    stakind2: i16,
    stakind3: i16,
    stakind4: i16,
    stakind5: i16,
    staop1: pg_sys::Oid,
    staop2: pg_sys::Oid,
    staop3: pg_sys::Oid,
    staop4: pg_sys::Oid,
    staop5: pg_sys::Oid,
    stacoll1: pg_sys::Oid,
    stacoll2: pg_sys::Oid,
    stacoll3: pg_sys::Oid,
    stacoll4: pg_sys::Oid,
    stacoll5: pg_sys::Oid,
    stanumbers1: JsonF32arr,
    stanumbers2: JsonF32arr,
    stanumbers3: JsonF32arr,
    stanumbers4: JsonF32arr,
    stanumbers5: JsonF32arr,
    stavalues1: JsonAarr,
    stavalues2: JsonAarr,
    stavalues3: JsonAarr,
    stavalues4: JsonAarr,
    stavalues5: JsonAarr,
}

unsafe fn aarr_elemtype(aarr: *mut pg_sys::AnyArrayType) -> Option<pg_sys::Oid> {
    if aarr.is_null() {
        None
    } else {
        // TODO(WAN): is there no pgrx wrapper for VARATT_IS_EXPANDED_HEADER?
        let varatt_is_expanded_header = (*aarr.cast::<pg_sys::varattrib_4b>()).va_4byte.va_header
            == (pg_sys::EOH_HEADER_MAGIC as u32);
        let oid = if varatt_is_expanded_header {
            (*aarr).xpn.element_type
        } else {
            (*aarr).flt.elemtype
        };
        Some(oid)
    }
}

unsafe fn pg_statistic_stavalues(
    tuple: *mut pg_sys::HeapTupleData,
    attnum: pg_sys::AttrNumber,
) -> (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) {
    let mut is_null = false;
    let datum = pg_sys::SysCacheGetAttr(
        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
        tuple,
        attnum,
        &mut is_null,
    );

    if is_null {
        (None, None)
    } else {
        let aarr: *mut pg_sys::AnyArrayType = pg_sys::DatumGetAnyArrayP(datum);
        let elemtype = aarr_elemtype(aarr).unwrap();
        (
            pgrx::AnyArray::from_polymorphic_datum(datum, is_null, elemtype),
            Some(elemtype),
        )
    }
}

#[pg_extern]
fn pg_class_stat_load(
    rel_name: String,
    namespace: Oid,
    rel_tuples: f32,
    rel_pages: i32,
) -> Option<String> {
    let pg_class = Relation::new(pg_sys::RelationRelationId, pg_sys::RowExclusiveLock as i32);
    let oid = unsafe { pg_sys::get_relname_relid(rel_name.as_pg_cstr(), namespace) };
    let _table_lock = Relation::new(oid, pg_sys::RowExclusiveLock as i32);

    let tuple = HeapTuple::from_sys_cache(&pg_class, unsafe {
        pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier::RELOID as i32, oid.into())
    })?;

    let att_size = pg_sys::Natts_pg_class as usize;
    let mut ctx = ModifyContext::new(att_size);

    ctx.replace(pg_sys::Anum_pg_class_reltuples, rel_tuples.into_datum()?);
    ctx.replace(pg_sys::Anum_pg_class_relpages, rel_pages.into_datum()?);

    // TODO: missing lock for the table (??)
    pg_class.update_tuple_with_info(tuple.modify_from(ctx));
    Some(format!("Ok for {:?}", oid))
}

#[pg_extern]
fn pg_class_stat_dump(rel_name: String, namespace: Oid) -> Option<PostgresClassStat> {
    let pg_class = Relation::new(pg_sys::RelationRelationId, pg_sys::RowShareLock as i32);

    // TODO: I don't know how to convert string into `name` datum. C code suggests it is a simple
    //       cast, but I don't know if such strategy works here.
    let oid = unsafe { pg_sys::get_relname_relid(rel_name.as_pg_cstr(), namespace) };

    let mut tuple = HeapTuple::from_sys_cache(&pg_class, unsafe {
        pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier::RELOID as i32, oid.into())
    })?;

    let pg_sys::FormData_pg_class {
        relname,
        reltuples,
        relpages,
        relnamespace,
        relallvisible,
        ..
    } = unsafe { tuple.inner_as() };

    let result = PostgresClassStat {
        relname: pg_sys::name_data_to_str(relname).into(),
        reltuples: *reltuples,
        relpages: *relpages,
        relnamespace: *relnamespace,
        relallvisible: *relallvisible,
    };

    Some(result)
}

#[pg_extern]
fn pg_statistic_dump(starelid: i32, staattnum: i16) -> Option<String> {
    let result = unsafe {
        // https://github.com/postgres/postgres/blob/683df3f4de00bf50b20eae92369e006badf7cd57/src/include/catalog/pg_statistic.h#L135
        let pg_statistic =
            pg_sys::table_open(pg_sys::StatisticRelationId, pg_sys::RowExclusiveLock as i32);
        let tuple = pg_sys::SearchSysCache3(
            pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
            starelid.into(),
            staattnum.into(),
            false.into(),
        );
        let tuple_json = if tuple.is_null() {
            None
        } else {
            let stat_struct = pg_sys::GETSTRUCT(tuple) as *mut pg_sys::FormData_pg_statistic;

            let starelid: pg_sys::Oid = (*stat_struct).starelid;
            let staattnum: i16 = (*stat_struct).staattnum;
            let stainherit: bool = (*stat_struct).stainherit;
            let stanullfrac: f32 = (*stat_struct).stanullfrac;
            let stawidth: i32 = (*stat_struct).stawidth;
            let stadistinct: f32 = (*stat_struct).stadistinct;
            let stakind1: i16 = (*stat_struct).stakind1;
            let stakind2: i16 = (*stat_struct).stakind2;
            let stakind3: i16 = (*stat_struct).stakind3;
            let stakind4: i16 = (*stat_struct).stakind4;
            let stakind5: i16 = (*stat_struct).stakind5;
            let staop1: pg_sys::Oid = (*stat_struct).staop1;
            let staop2: pg_sys::Oid = (*stat_struct).staop2;
            let staop3: pg_sys::Oid = (*stat_struct).staop3;
            let staop4: pg_sys::Oid = (*stat_struct).staop4;
            let staop5: pg_sys::Oid = (*stat_struct).staop5;
            let stacoll1: pg_sys::Oid = (*stat_struct).stacoll1;
            let stacoll2: pg_sys::Oid = (*stat_struct).stacoll2;
            let stacoll3: pg_sys::Oid = (*stat_struct).stacoll3;
            let stacoll4: pg_sys::Oid = (*stat_struct).stacoll4;
            let stacoll5: pg_sys::Oid = (*stat_struct).stacoll5;
            let mut is_null = false;
            let stanumbers1_opt_arrf32: Option<pgrx::Array<f32>> =
                pgrx::Array::from_polymorphic_datum(
                    pg_sys::SysCacheGetAttr(
                        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
                        tuple,
                        pg_sys::Anum_pg_statistic_stanumbers1 as pg_sys::AttrNumber,
                        &mut is_null,
                    ),
                    is_null,
                    pg_sys::FLOAT4OID,
                );
            let stanumbers2_opt_arrf32: Option<pgrx::Array<f32>> =
                pgrx::Array::from_polymorphic_datum(
                    pg_sys::SysCacheGetAttr(
                        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
                        tuple,
                        pg_sys::Anum_pg_statistic_stanumbers2 as pg_sys::AttrNumber,
                        &mut is_null,
                    ),
                    is_null,
                    pg_sys::FLOAT4OID,
                );
            let stanumbers3_opt_arrf32: Option<pgrx::Array<f32>> =
                pgrx::Array::from_polymorphic_datum(
                    pg_sys::SysCacheGetAttr(
                        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
                        tuple,
                        pg_sys::Anum_pg_statistic_stanumbers3 as pg_sys::AttrNumber,
                        &mut is_null,
                    ),
                    is_null,
                    pg_sys::FLOAT4OID,
                );
            let stanumbers4_opt_arrf32: Option<pgrx::Array<f32>> =
                pgrx::Array::from_polymorphic_datum(
                    pg_sys::SysCacheGetAttr(
                        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
                        tuple,
                        pg_sys::Anum_pg_statistic_stanumbers4 as pg_sys::AttrNumber,
                        &mut is_null,
                    ),
                    is_null,
                    pg_sys::FLOAT4OID,
                );
            let stanumbers5_opt_arrf32: Option<pgrx::Array<f32>> =
                pgrx::Array::from_polymorphic_datum(
                    pg_sys::SysCacheGetAttr(
                        pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
                        tuple,
                        pg_sys::Anum_pg_statistic_stanumbers5 as pg_sys::AttrNumber,
                        &mut is_null,
                    ),
                    is_null,
                    pg_sys::FLOAT4OID,
                );
            let stavalues1_opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) =
                pg_statistic_stavalues(
                    tuple,
                    pg_sys::Anum_pg_statistic_stavalues1 as pg_sys::AttrNumber,
                );
            let stavalues2_opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) =
                pg_statistic_stavalues(
                    tuple,
                    pg_sys::Anum_pg_statistic_stavalues2 as pg_sys::AttrNumber,
                );
            let stavalues3_opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) =
                pg_statistic_stavalues(
                    tuple,
                    pg_sys::Anum_pg_statistic_stavalues3 as pg_sys::AttrNumber,
                );
            let stavalues4_opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) =
                pg_statistic_stavalues(
                    tuple,
                    pg_sys::Anum_pg_statistic_stavalues4 as pg_sys::AttrNumber,
                );
            let stavalues5_opt_aarr: (Option<pgrx::AnyArray>, Option<pg_sys::Oid>) =
                pg_statistic_stavalues(
                    tuple,
                    pg_sys::Anum_pg_statistic_stavalues5 as pg_sys::AttrNumber,
                );

            let stanumbers1 = JsonF32arr::from(stanumbers1_opt_arrf32);
            let stanumbers2 = JsonF32arr::from(stanumbers2_opt_arrf32);
            let stanumbers3 = JsonF32arr::from(stanumbers3_opt_arrf32);
            let stanumbers4 = JsonF32arr::from(stanumbers4_opt_arrf32);
            let stanumbers5 = JsonF32arr::from(stanumbers5_opt_arrf32);
            let stavalues1 = JsonAarr::from(stavalues1_opt_aarr);
            let stavalues2 = JsonAarr::from(stavalues2_opt_aarr);
            let stavalues3 = JsonAarr::from(stavalues3_opt_aarr);
            let stavalues4 = JsonAarr::from(stavalues4_opt_aarr);
            let stavalues5 = JsonAarr::from(stavalues5_opt_aarr);

            let pg_row = PgStatisticRow {
                starelid,
                staattnum,
                stainherit,
                stanullfrac,
                stawidth,
                stadistinct,
                stakind1,
                stakind2,
                stakind3,
                stakind4,
                stakind5,
                staop1,
                staop2,
                staop3,
                staop4,
                staop5,
                stacoll1,
                stacoll2,
                stacoll3,
                stacoll4,
                stacoll5,
                stanumbers1,
                stanumbers2,
                stanumbers3,
                stanumbers4,
                stanumbers5,
                stavalues1,
                stavalues2,
                stavalues3,
                stavalues4,
                stavalues5,
            };

            let json_str = serde_json::to_string(&pg_row).unwrap();
            pg_sys::ReleaseSysCache(tuple);
            Some(json_str)
        };
        pg_sys::table_close(pg_statistic, pg_sys::RowExclusiveLock as i32);
        tuple_json
    };
    result
}

#[pg_extern]
fn echo(input: i32) -> i32 {
    input
}

#[pg_extern]
fn pg_statistic_load(data: String) -> bool {
    let pg_row: PgStatisticRow = serde_json::from_str(&data).unwrap();
    let starelid = pg_row.starelid;
    let staattnum = pg_row.staattnum;

    let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(pg_sys::Natts_pg_statistic as usize);
    let mut nulls: Vec<bool> = Vec::with_capacity(pg_sys::Natts_pg_statistic as usize);
    let mut replaces: Vec<bool> = Vec::with_capacity(pg_sys::Natts_pg_statistic as usize);

    for _ in 0..pg_sys::Natts_pg_statistic {
        values.push(pg_sys::Datum::from(0));
        nulls.push(false);
        replaces.push(true);
    }

    // analyze.c performs a slightly cursed blend of Anum and i++ based accessing.
    values[pg_sys::Anum_pg_statistic_starelid as usize - 1] = pg_row.starelid.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_staattnum as usize - 1] =
        pg_row.staattnum.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stainherit as usize - 1] =
        pg_row.stainherit.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stanullfrac as usize - 1] =
        pg_row.stanullfrac.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stawidth as usize - 1] = pg_row.stawidth.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stadistinct as usize - 1] =
        pg_row.stadistinct.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stakind1 as usize - 1] = pg_row.stakind1.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stakind2 as usize - 1] = pg_row.stakind2.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stakind3 as usize - 1] = pg_row.stakind3.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stakind4 as usize - 1] = pg_row.stakind4.into_datum().unwrap();
    values[pg_sys::Anum_pg_statistic_stakind5 as usize - 1] = pg_row.stakind5.into_datum().unwrap();
    // pg_sys::Datum::from(0) is a hack around Oid(0) deserializing to None.
    values[pg_sys::Anum_pg_statistic_staop1 as usize - 1] =
        pg_row.staop1.into_datum().unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_staop2 as usize - 1] =
        pg_row.staop2.into_datum().unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_staop3 as usize - 1] =
        pg_row.staop3.into_datum().unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_staop4 as usize - 1] =
        pg_row.staop4.into_datum().unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_staop5 as usize - 1] =
        pg_row.staop5.into_datum().unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_stacoll1 as usize - 1] = pg_row
        .stacoll1
        .into_datum()
        .unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_stacoll2 as usize - 1] = pg_row
        .stacoll2
        .into_datum()
        .unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_stacoll3 as usize - 1] = pg_row
        .stacoll3
        .into_datum()
        .unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_stacoll4 as usize - 1] = pg_row
        .stacoll4
        .into_datum()
        .unwrap_or(pg_sys::Datum::from(0));
    values[pg_sys::Anum_pg_statistic_stacoll5 as usize - 1] = pg_row
        .stacoll5
        .into_datum()
        .unwrap_or(pg_sys::Datum::from(0));

    unsafe {
        pg_row.stanumbers1.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers1 as usize - 1,
        );
        pg_row.stanumbers2.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers2 as usize - 1,
        );
        pg_row.stanumbers3.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers3 as usize - 1,
        );
        pg_row.stanumbers4.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers4 as usize - 1,
        );
        pg_row.stanumbers5.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stanumbers5 as usize - 1,
        );
        pg_row.stavalues1.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues1 as usize - 1,
        );
        pg_row.stavalues2.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues2 as usize - 1,
        );
        pg_row.stavalues3.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues3 as usize - 1,
        );
        pg_row.stavalues4.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues4 as usize - 1,
        );
        pg_row.stavalues5.set_pg_statistic_tuple(
            &mut values,
            &mut nulls,
            pg_sys::Anum_pg_statistic_stavalues5 as usize - 1,
        );
    }

    unsafe {
        let pg_statistic =
            pg_sys::table_open(pg_sys::StatisticRelationId, pg_sys::RowExclusiveLock as i32);
        let indstate = pg_sys::CatalogOpenIndexes(pg_statistic);
        let pg_statistic_tuple_desc = (*pg_statistic).rd_att;

        let oldtup = pg_sys::SearchSysCache3(
            pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
            starelid.into(),
            staattnum.into(),
            false.into(),
        );

        if !oldtup.is_null() {
            let stup = pg_sys::heap_modify_tuple(
                oldtup,
                pg_statistic_tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
                replaces.as_mut_ptr(),
            );
            pg_sys::ReleaseSysCache(oldtup);
            pg_sys::CatalogTupleUpdateWithInfo(pg_statistic, &mut (*stup).t_self, stup, indstate);
            pg_sys::heap_freetuple(stup);
        } else {
            let stup = pg_sys::heap_form_tuple(
                pg_statistic_tuple_desc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
            pg_sys::CatalogTupleInsertWithInfo(pg_statistic, stup, indstate);
            pg_sys::heap_freetuple(stup);
        }

        if !indstate.is_null() {
            pg_sys::CatalogCloseIndexes(indstate);
        }

        pg_sys::table_close(pg_statistic, pg_sys::RowExclusiveLock as i32);
    }

    true
}

#[pg_extern]
fn anyarray_elemtype(x: pgrx::AnyArray) -> Option<pg_sys::Oid> {
    // Get the type of the elements of the array using pg_sys.
    unsafe { aarr_elemtype(x.datum().cast_mut_ptr()) }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_hypostats() {}
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
