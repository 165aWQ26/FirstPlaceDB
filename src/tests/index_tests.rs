use crate::index::Index;

#[test]
fn insert_one() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);
    assert_eq!(my_index.locate(5), Some(1));
}

#[test]
fn insert_two() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);
    my_index.insert(4, 2);
    assert_eq!(my_index.locate(5), Some(1));
    assert_eq!(my_index.locate(4), Some(2));
}

#[test]
fn insert_overwrites() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);
    my_index.insert(5, 2);
    // BTreeMap overwrites: last insert wins
    assert_eq!(my_index.locate(5), Some(2));
}

#[test]
fn range_query() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);
    my_index.insert(6, 3);
    my_index.insert(7, 4);
    my_index.insert(8, 5);
    assert_eq!(my_index.locate_range(5, 8), Some(vec![1, 3, 4, 5]));
}

#[test]
fn delete_multiple_keys() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);
    my_index.insert(6, 3);
    my_index.insert(7, 4);
    my_index.insert(8, 5);

    my_index.remove(5, 1);
    my_index.remove(6, 3);
    assert_eq!(my_index.locate(5), None);
    assert_eq!(my_index.locate(6), None);
    assert_eq!(
        my_index.locate_range(7, 8),
        Some(vec![4, 5])
    );
}

#[test]
fn delete_one() {
    let mut my_index = Index::new();
    my_index.insert(5, 1);

    my_index.remove(5, 1);

    assert_eq!(my_index.locate(5), None);
}

#[test]
fn insert_unique_prevents_duplicates() {
    let mut my_index = Index::new();
    assert!(my_index.insert_unique(5, 1));
    assert!(!my_index.insert_unique(5, 2));
    assert_eq!(my_index.locate(5), Some(1));
}
