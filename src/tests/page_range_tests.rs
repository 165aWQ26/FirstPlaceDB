// use crate::page_range::PageRanges;
//
// fn new_ranges(num_cols: usize) -> PageRanges {
//     PageRanges::new(num_cols)
// }
//
// #[test]
// fn append_base_and_read() {
//     let mut pr = new_ranges(3);
//     let data = vec![Some(10), Some(20), Some(30)];
//     let addr = pr.append_base(data, 0).unwrap();
//
//     let result = pr.read(&addr).unwrap();
//     assert_eq!(result, vec![Some(10), Some(20), Some(30)]);
// }
//
// #[test]
// fn read_single_column() {
//     let mut pr = new_ranges(3);
//     let data = vec![Some(10), Some(20), Some(30)];
//     let addr = pr.append_base(data, 0).unwrap();
//
//     assert_eq!(pr.read_single(0, &addr).unwrap(), Some(10));
//     assert_eq!(pr.read_single(1, &addr).unwrap(), Some(20));
//     assert_eq!(pr.read_single(2, &addr).unwrap(), Some(30));
// }
//
// #[test]
// fn read_projected() {
//     let mut pr = new_ranges(4);
//     let data = vec![Some(10), Some(20), Some(30), Some(40)];
//     let addr = pr.append_base(data, 0).unwrap();
//
//     let projected = vec![1, 0, 1, 0]; // columns 0 and 2 only
//     let result = pr.read_projected(&projected, &addr).unwrap();
//     assert_eq!(result, vec![Some(10), None, Some(30), None]);
// }
//
// #[test]
// // fn append_base_metadata() {
// //     let mut pr = new_ranges(3);
// //     let data = vec![Some(10), Some(20), Some(30)];
// //     let addr = pr.append_base(data, 42).unwrap();
// //
// //     // append_base sets: RID=42, indirection=42 (self), schema=0, start_time=None
// //     assert_eq!(pr.get_rid(&addr).unwrap(), Some(42));
// //     assert_eq!(pr.get_indirection(&addr).unwrap(), Some(42));
// //     assert_eq!(pr.get_schema_encoding(&addr).unwrap(), Some(0));
// //     assert_eq!(pr.get_start_time(&addr).unwrap(), None);
// // }
//
// #[test]
// fn append_tail_and_read() {
//     let mut pr = new_ranges(3);
//     let tail_data = vec![Some(99), None, Some(77)];
//     let schema: i64 = 0b101; // columns 0 and 2 updated
//     let addr = pr.append_tail(tail_data, 1, 0, Some(schema)).unwrap();
//
//     assert_eq!(pr.read_tail_single(0, &addr).unwrap(), Some(99));
//     assert_eq!(pr.read_tail_single(1, &addr).unwrap(), None);
//     assert_eq!(pr.read_tail_single(2, &addr).unwrap(), Some(77));
//
// //     assert_eq!(pr.get_tail_indirection(&addr).unwrap(), Some(0));
// //     assert_eq!(pr.get_tail_schema_encoding(&addr).unwrap(), Some(schema));
// }
//
// #[test]
// fn write_single_updates_base() {
//     let mut pr = new_ranges(3);
//     let data = vec![Some(10), Some(20), Some(30)];
//     let addr = pr.append_base(data, 0).unwrap();
//
//     pr.write_single(1, &addr, Some(99)).unwrap();
//     assert_eq!(pr.read_single(1, &addr).unwrap(), Some(99));
//     // Other columns unchanged
//     assert_eq!(pr.read_single(0, &addr).unwrap(), Some(10));
//     assert_eq!(pr.read_single(2, &addr).unwrap(), Some(30));
// }
//
// #[test]
// fn multiple_records_same_range() {
//     let mut pr = new_ranges(2);
//     let addr0 = pr.append_base(vec![Some(1), Some(2)], 0).unwrap();
//     let addr1 = pr.append_base(vec![Some(3), Some(4)], 1).unwrap();
//
//     assert_eq!(pr.read(&addr0).unwrap(), vec![Some(1), Some(2)]);
//     assert_eq!(pr.read(&addr1).unwrap(), vec![Some(3), Some(4)]);
//     assert_ne!(addr0, addr1);
// }
