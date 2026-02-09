use crate::error::DbError;
use crate::page_directory::PageDirectory;
use crate::page_range::PhysicalAddress;

fn addr(offset: usize, collection: usize) -> PhysicalAddress {
    PhysicalAddress {
        offset,
        collection_num: collection,
    }
}

#[test]
fn add_and_get() {
    let mut pd = PageDirectory::default();
    pd.add(0, addr(0, 0));
    pd.add(1, addr(1, 0));
    pd.add(2, addr(2, 0));
    assert_eq!(pd.get(0).unwrap(), addr(0, 0));
    assert_eq!(pd.get(1).unwrap(), addr(1, 0));
    assert_eq!(pd.get(2).unwrap(), addr(2, 0));
}

#[test]
fn add_grows_dynamically() {
    let mut pd = PageDirectory::default();
    pd.add(5000, addr(10, 3));
    assert_eq!(pd.get(5000).unwrap(), addr(10, 3));
}

#[test]
fn get_nonexistent_returns_error() {
    let pd = PageDirectory::default();
    assert_eq!(pd.get(0), Err(DbError::RecordNotFound(0)));
    assert_eq!(pd.get(99999), Err(DbError::RecordNotFound(99999)));
}

#[test]
fn delete_then_get_fails() {
    let mut pd = PageDirectory::default();
    pd.add(5, addr(5, 0));
    assert_eq!(pd.get(5).unwrap(), addr(5, 0));

    pd.delete(5).unwrap();
    assert_eq!(pd.get(5), Err(DbError::RecordNotFound(5)));
}
