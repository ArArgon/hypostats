use crate::catalog::relation::Relation;
use pgrx::pg_sys;
use std::ops::Deref;

#[derive(Debug, Eq, PartialEq)]
enum ManageType {
    SysCache,
    User,
}

#[derive(Debug)]
pub(crate) struct HeapTuple {
    manage_type: ManageType,
    tuple: pg_sys::HeapTuple,
    descriptor: pg_sys::TupleDesc,
}

impl HeapTuple {
    pub fn assemble_from(
        descriptor: pg_sys::TupleDesc,
        mut data: Vec<pg_sys::Datum>,
        mut nulls: Vec<bool>,
    ) -> Self {
        Self {
            manage_type: ManageType::User,
            tuple: unsafe {
                pg_sys::heap_form_tuple(descriptor, data.as_mut_ptr(), nulls.as_mut_ptr())
            },
            descriptor,
        }
    }

    pub fn from_sys_cache(rel: &Relation, tuple: pg_sys::HeapTuple) -> Option<Self> {
        if tuple.is_null() {
            return None;
        }

        Some(Self {
            manage_type: ManageType::SysCache,
            tuple,
            descriptor: rel.get_desc(),
        })
    }

    pub fn modify_from(
        old_tup: &HeapTuple,
        mut data: Vec<pg_sys::Datum>,
        mut nulls: Vec<bool>,
        mut do_replaces: Vec<bool>,
    ) -> Self {
        Self {
            manage_type: ManageType::User,
            tuple: unsafe {
                pg_sys::heap_modify_tuple(
                    old_tup.tuple,
                    old_tup.descriptor,
                    data.as_mut_ptr(),
                    nulls.as_mut_ptr(),
                    do_replaces.as_mut_ptr(),
                )
            },
            descriptor: old_tup.descriptor,
        }
    }

    pub fn is_null(&self) -> bool {
        self.tuple.is_null()
    }

    pub unsafe fn inner_as<T>(&mut self) -> &mut T {
        (pg_sys::GETSTRUCT(self.tuple) as *mut T).as_mut().unwrap()
    }
}

impl Deref for HeapTuple {
    type Target = pg_sys::HeapTuple;

    fn deref(&self) -> &Self::Target {
        &self.tuple
    }
}

impl Drop for HeapTuple {
    fn drop(&mut self) {
        match self.manage_type {
            ManageType::SysCache => unsafe { pg_sys::ReleaseSysCache(self.tuple) },
            ManageType::User => unsafe { pg_sys::heap_freetuple(self.tuple) },
        }
    }
}
