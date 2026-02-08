// #[cfg(test)]
// mod tests {
//     use lstore::index::*;

//     #[test]
//     fn insert_one() {
//         let mut my_index = Index::new();
//         my_index.insert(5, 1);
//         assert_eq!(my_index.locate(5), Some(&vec![1]));
//     }

//     #[test]
//     fn insert_two() {
//         let mut my_index = Index::new();
//         my_index.insert(5, 1);
//         my_index.insert(4,2);
//         assert_eq!(my_index.locate(5), Some(&vec![1]));
//         assert_eq!(my_index.locate(4), Some(&vec![2]));
//     }

//     #[test]
//     fn insert_dups() {
//         let mut my_index = Index::new();
//         my_index.insert(5, 1);
//         my_index.insert(5,2);
//         assert_eq!(my_index.locate(5), Some(&vec![1, 2]));
//     }

//     #[test]
//     fn range_query() {
//         let mut my_index = Index::new();
//         my_index.insert(5, 1);
//         my_index.insert(5,2);
//         my_index.insert(6, 3);
//         my_index.insert(7,4);
//         my_index.insert(8, 5);
//         my_index.insert(8,6);
//         assert_eq!(my_index.locate_range(5, 8), Some(vec![1, 2, 3, 4, 5, 6]));
//     }
// }