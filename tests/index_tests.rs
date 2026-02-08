#[cfg(test)]
mod tests {
    use lstore::index::Index;

    #[test]
    fn insert_one() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        assert_eq!(my_index.locate(5), Some(&vec![1]));
    }

    #[test]
    fn insert_two() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(4, 2);
        assert_eq!(my_index.locate(5), Some(&vec![1]));
        assert_eq!(my_index.locate(4), Some(&vec![2]));
    }

    #[test]
    fn insert_duplicates() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);
        assert_eq!(my_index.locate(5), Some(&vec![1, 2]));
    }

    #[test]
    fn range_query() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);
        my_index.insert(6, 3);
        my_index.insert(7, 4);
        my_index.insert(8, 5);
        my_index.insert(8, 6);
        assert_eq!(my_index.locate_range(5, 8), Some(vec![1, 2, 3, 4, 5, 6]));
    }

    #[test]
    fn delete_lots() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);
        my_index.insert(6, 3);
        my_index.insert(7, 4);
        my_index.insert(8, 5);
        my_index.insert(8, 6);
        my_index.insert(5, 7);
        my_index.insert(5, 8);
        my_index.insert(6, 9);
        my_index.insert(7, 10);
        my_index.insert(8, 11);
        my_index.insert(8, 12);

        my_index.remove(5, 1);
        my_index.remove(5, 2);
        my_index.remove(6, 3);
        my_index.remove(5, 7);
        my_index.remove(5, 8);
        assert_eq!(my_index.locate(5), None);
        assert_eq!(
            my_index.locate_range(6, 8),
            Some(vec![9, 4, 10, 5, 6, 11, 12])
        );
    }
    #[test]
    fn delete_one() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);

        my_index.remove(5, 1);

        assert_eq!(my_index.locate(5), Some(&vec![2]));
    }

    #[test]
    fn delete_all() {
        let mut my_index = Index::new();
        my_index.insert(5, 1);
        my_index.insert(5, 2);

        my_index.remove(5, 1);
        my_index.remove(5, 2);

        assert_eq!(my_index.locate(5), None);
    }
}
