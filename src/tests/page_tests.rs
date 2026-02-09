use crate::page::*;

#[test]
fn write_and_read() {
    let mut page = Page::default();
    page.write(Some(42)).unwrap();
    assert_eq!(page.read(0).unwrap(), Some(42));
    assert_eq!(page.len(), 1);
}

#[test]
fn write_none() {
    let mut page = Page::default();
    page.write(None).unwrap();
    assert_eq!(page.read(0).unwrap(), None);
    assert_eq!(page.len(), 1);
}

#[test]
fn write_multiple_values() {
    let mut page = Page::default();
    page.write(Some(10)).unwrap();
    page.write(Some(20)).unwrap();
    page.write(None).unwrap();
    page.write(Some(40)).unwrap();
    assert_eq!(page.len(), 4);
    assert_eq!(page.read(0).unwrap(), Some(10));
    assert_eq!(page.read(1).unwrap(), Some(20));
    assert_eq!(page.read(2).unwrap(), None);
    assert_eq!(page.read(3).unwrap(), Some(40));
}

#[test]
fn write_until_full() {
    let mut page = Page::default();
    for i in 0..Page::PAGE_SIZE {
        assert!(page.has_capacity());
        page.write(Some(i as i64)).unwrap();
    }
    assert_eq!(page.len(), Page::PAGE_SIZE);
    assert!(!page.has_capacity());
}

#[test]
fn write_beyond_capacity_fails() {
    let mut page = Page::default();
    for i in 0..Page::PAGE_SIZE {
        page.write(Some(i as i64)).unwrap();
    }
    assert_eq!(page.write(Some(999)), Err(PageError::Full));
}

#[test]
fn read_out_of_bounds() {
    let page = Page::default();
    assert_eq!(page.read(0), Err(PageError::IndexOutOfBounds(0)));

    let mut page2 = Page::default();
    page2.write(Some(1)).unwrap();
    assert_eq!(page2.read(1), Err(PageError::IndexOutOfBounds(1)));
    assert_eq!(page2.read(100), Err(PageError::IndexOutOfBounds(100)));
}

#[test]
fn update_existing() {
    let mut page = Page::default();
    page.write(Some(10)).unwrap();
    page.update(0, Some(99)).unwrap();
    assert_eq!(page.read(0).unwrap(), Some(99));
}

#[test]
fn update_to_none() {
    let mut page = Page::default();
    page.write(Some(10)).unwrap();
    page.update(0, None).unwrap();
    assert_eq!(page.read(0).unwrap(), None);
}

#[test]
fn update_out_of_bounds() {
    let mut page = Page::default();
    assert_eq!(page.update(0, Some(1)), Err(PageError::IndexOutOfBounds(0)));

    page.write(Some(10)).unwrap();
    assert_eq!(page.update(1, Some(1)), Err(PageError::IndexOutOfBounds(1)));
}

#[test]
fn boundary_values() {
    let mut page = Page::default();
    page.write(Some(i64::MAX)).unwrap();
    page.write(Some(i64::MIN)).unwrap();
    page.write(Some(0)).unwrap();
    assert_eq!(page.read(0).unwrap(), Some(i64::MAX));
    assert_eq!(page.read(1).unwrap(), Some(i64::MIN));
    assert_eq!(page.read(2).unwrap(), Some(0));
}

#[test]
fn empty_page_defaults() {
    let page = Page::default();
    assert_eq!(page.len(), 0);
    assert!(page.has_capacity());
}
