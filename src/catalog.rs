pub mod heap_tuple;
pub mod index_state;
pub mod relation;

trait Catalog {
    type Value;
}
