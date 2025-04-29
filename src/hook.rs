use lazy_static::lazy_static;
use pgrx::pg_sys::Oid;
use pgrx::{log, pg_extern, pg_sys};
use std::collections::HashMap;
use std::sync::Mutex;

lazy_static! {
    pub static ref rel_info: Mutex<HashMap<Oid, (pg_sys::BlockNumber, f64)>> =
        Mutex::new(HashMap::new());
}

extern "C" fn relation_hook(
    _root: *mut pg_sys::PlannerInfo,
    oid: Oid,
    inhparent: bool,
    rel: *mut pg_sys::RelOptInfo,
) {
    let rel = unsafe { rel.as_mut().unwrap() };
    log!(
        "oid: {:?}, pages: {}, tuples: {}, inherent: {}",
        oid,
        rel.pages,
        rel.tuples,
        inhparent
    );
    if let Some((pages, tuples)) = rel_info.lock().unwrap().get(&oid) {
        log!(
            "intercept {:?} => pages: {}, tuples: {}",
            oid,
            pages,
            tuples,
        );
        rel.pages = *pages;
        rel.tuples = *tuples;
    }
}

#[pg_extern]
fn install_size_hook() {
    unsafe { pg_sys::get_relation_info_hook = Some(relation_hook) };
}

#[pg_extern]
fn remove_size_hook() {
    unsafe { pg_sys::get_relation_info_hook = None };
}
