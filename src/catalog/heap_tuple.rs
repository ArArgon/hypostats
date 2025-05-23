use crate::catalog::relation::Relation;
use pgrx::pg_sys::Datum;
use pgrx::{pg_sys, FromDatum};
use std::ops::Deref;

#[derive(Debug, Eq, PartialEq)]
enum ManageType {
    SysCache,
    User,
}

pub(crate) struct ModifyContext {
    data: Vec<pg_sys::Datum>,
    null: Vec<bool>,
    replace: Vec<bool>,
}

impl ModifyContext {
    pub fn new(size: usize) -> Self {
        let mut data = Vec::with_capacity(size);
        let mut null = Vec::with_capacity(size);
        let mut replace = Vec::with_capacity(size);

        for _ in 0..size {
            data.push(pg_sys::Datum::from(0));
            null.push(false);
            replace.push(false);
        }

        Self {
            data,
            null,
            replace,
        }
    }

    pub fn replace(&mut self, anum: u32, value: pg_sys::Datum) {
        let idx = (anum - 1) as usize;
        assert!(
            idx < self.data.len(),
            "access number {} - 1 out of bound, size: {}",
            anum,
            self.data.len()
        );

        self.replace[idx] = true;
        self.data[idx] = value;
        self.null[idx] = value.is_null();
    }
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
        &self,
        ModifyContext {
            mut data,
            mut null,
            mut replace,
        }: ModifyContext,
    ) -> Self {
        Self {
            manage_type: ManageType::User,
            tuple: unsafe {
                pg_sys::heap_modify_tuple(
                    self.tuple,
                    self.descriptor,
                    data.as_mut_ptr(),
                    null.as_mut_ptr(),
                    replace.as_mut_ptr(),
                )
            },
            descriptor: self.descriptor,
        }
    }

    pub fn is_null(&self) -> bool {
        self.tuple.is_null()
    }

    pub unsafe fn inner_as<T>(&mut self) -> &mut T {
        (pg_sys::GETSTRUCT(self.tuple) as *mut T).as_mut().unwrap()
    }

    pub unsafe fn read_dynamic_field_datum(
        &self,
        cache_id: i32,
        attr_number: u32,
    ) -> Option<Datum> {
        let mut is_null = false;
        let data = pg_sys::SysCacheGetAttr(
            cache_id,
            self.tuple,
            attr_number as pg_sys::AttrNumber,
            &mut is_null,
        );
        if is_null {
            None
        } else {
            Some(data)
        }
    }

    pub unsafe fn read_dynamic_field<T: FromDatum>(
        &self,
        cache_id: i32,
        attr_number: u32,
        type_oid: pg_sys::Oid,
    ) -> Option<T> {
        let attr = self.read_dynamic_field_datum(cache_id, attr_number);
        attr.and_then(|attr| T::from_polymorphic_datum(attr, false, type_oid))
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
