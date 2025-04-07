use crate::catalog::heap_tuple::HeapTuple;
use crate::catalog::index_state::IndexState;
use pgrx::pg_sys;
use std::ops::Deref;

pub(crate) struct Relation {
    rel: pg_sys::Relation,
    lock_mode: pg_sys::LOCKMODE,
}

impl Relation {
    pub fn new(oid: pg_sys::Oid, lock_mode: pg_sys::LOCKMODE) -> Self {
        Self {
            rel: unsafe { pg_sys::table_open(oid, lock_mode) },
            lock_mode,
        }
    }

    pub(crate) fn inner(&self) -> Option<&pg_sys::RelationData> {
        unsafe { self.rel.as_ref() }
    }

    pub(crate) fn raw(&self) -> pg_sys::Relation {
        self.rel
    }

    pub fn get_desc(&self) -> pg_sys::TupleDesc {
        self.rd_att
    }

    pub fn insert_tuple_with_info<'rel>(
        &'rel mut self,
        tuple: HeapTuple,
        index_state: IndexState<'rel>,
    ) {
        unsafe { pg_sys::CatalogTupleInsertWithInfo(self.raw(), *tuple, *index_state) };
    }

    pub fn update_tuple_with_info<'rel>(
        &'rel mut self,
        tuple: HeapTuple,
        index_state: IndexState<'rel>,
    ) {
        unsafe {
            pg_sys::CatalogTupleUpdateWithInfo(
                self.raw(),
                &mut (**tuple).t_self,
                *tuple,
                *index_state,
            )
        };
    }
}

impl Deref for Relation {
    type Target = pg_sys::RelationData;

    fn deref(&self) -> &Self::Target {
        self.inner().unwrap()
    }
}

impl Drop for Relation {
    fn drop(&mut self) {
        unsafe { pg_sys::table_close(self.rel, self.lock_mode) }
    }
}
