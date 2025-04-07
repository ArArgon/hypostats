use crate::catalog::relation::Relation;
use pgrx::pg_sys;
use std::marker::PhantomData;
use std::ops::Deref;

pub(crate) struct IndexState<'rel> {
    state: pg_sys::CatalogIndexState,
    phantom_data: PhantomData<&'rel Relation>,
}

impl<'rel> IndexState<'rel> {
    fn new(rel: &'rel Relation) -> Self {
        Self {
            state: unsafe { pg_sys::CatalogOpenIndexes(rel.raw()) },
            phantom_data: Default::default(),
        }
    }
}

impl<'rel> Deref for IndexState<'rel> {
    type Target = pg_sys::CatalogIndexState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<'rel> Drop for IndexState<'rel> {
    fn drop(&mut self) {
        if !self.state.is_null() {
            unsafe { pg_sys::CatalogCloseIndexes(self.state) }
        }
    }
}
