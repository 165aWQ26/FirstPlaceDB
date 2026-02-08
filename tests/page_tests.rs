// #[cfg(test)]
// mod tests {
//
//     // Basic
//     use lstore::page::*;
//
//     #[test]
//     fn test_new_page_is_empty() {
//         let page = Page::default();
//         assert_eq!(page.len(), 0);
//     }
//
//     #[test]
//     fn test_default() {
//         let page = Page::default();
//         assert_eq!(page.len(), 0);
//         assert!(!page.nullable());
//         assert_eq!(page.expected_type(), ValueType::I64);
//     }
//
//
//     #[test]
//     fn test_nullable_and_expected_type() {
//         let page = Page::new_nullable_f64();
//         assert!(page.nullable());
//         assert_eq!(page.expected_type(), ValueType::F64);
//     }
//
//     #[test]
//     fn test_nullable_write_until_full() {
//         let mut page = Page::new_nullable();
//         for i in 0..page.capacity() {
//             let val = if i % 2 == 0 {
//                 Value::I64(i as i64)
//             } else {
//                 Value::None
//             };
//             assert!(page.write(val).is_ok());
//         }
//         assert_eq!(page.len(), page.capacity());
//         assert!(!page.has_capacity());
//     }
//
//     #[test]
//     fn test_write_f64() {
//         let mut page = Page::new_f64();
//         page.write(Value::F64(3.14)).unwrap();
//         assert_eq!(page.read(0).unwrap(), Value::F64(3.14));
//     }
//
//     // #[test]
//     // fn test_nullable_clone_is_independent() {
//     //     let mut page = Page::new_nullable();
//     //     page.write(Value::I64(10)).unwrap();
//     //     page.write(Value::None).unwrap();
//     //
//     //     println!("{}", page.capacity());
//     //
//     //
//     //         let mut cloned = page.clone();
//     //     cloned.write(Value::I64(30)).unwrap();
//     //
//     //     // Original unchanged
//     //     assert_eq!(page.len(), 2);
//     //     assert_eq!(page.read(0).unwrap(), Value::I64(10));
//     //     assert_eq!(page.read(1).unwrap(), Value::None);
//     //
//     //     // Clone has the new write
//     //     assert_eq!(cloned.len(), 3);
//     //     assert_eq!(cloned.read(2).unwrap(), Value::I64(30));
//     // }
//
//     // Capacity
//
//     #[test]
//     fn test_has_capacity_when_empty() {
//         let page = Page::default();
//         assert!(page.has_capacity());
//     }
//
//     // Write
//
//     #[test]
//     fn test_write_single_value() {
//         let mut page = Page::default();
//         let result = page.write(Value::I64(42));
//         assert!(result.is_ok());
//         assert_eq!(page.len(), 1);
//         assert_eq!(page.read(0).unwrap(), Value::I64(42));
//     }
//
//     #[test]
//     fn test_write_multiple_values() {
//         let mut page = Page::default();
//         page.write(Value::I64(10)).unwrap();
//         page.write(Value::I64(20)).unwrap();
//         page.write(Value::I64(30)).unwrap();
//         assert_eq!(page.len(), 3);
//         assert_eq!(page.read(0).unwrap(), Value::I64(10));
//         assert_eq!(page.read(1).unwrap(), Value::I64(20));
//         assert_eq!(page.read(2).unwrap(), Value::I64(30));
//     }
//
//     #[test]
//     fn test_write_until_full() {
//         let mut page = Page::default();
//         for i in 0..page.capacity() {
//             assert!(page.has_capacity());
//             assert!(page.write(Value::I64(i as i64)).is_ok());
//         }
//         assert_eq!(page.len(), page.capacity());
//         assert!(!page.has_capacity());
//     }
//
//     #[test]
//     fn test_write_beyond_capacity_fails() {
//         let mut page = Page::default();
//         for i in 0..page.capacity() {
//             page.write(Value::I64(i as i64)).unwrap();
//         }
//         assert_eq!(
//             page.write(Value::I64(999)).unwrap_err(),
//             PageError::Full
//         );
//     }
//
//     #[test]
//     fn test_write_negative_values() {
//         let mut page = Page::default();
//         page.write(Value::I64(-1)).unwrap();
//         page.write(Value::I64(-100)).unwrap();
//         assert_eq!(page.read(0).unwrap(), Value::I64(-1));
//         assert_eq!(page.read(1).unwrap(), Value::I64(-100));
//     }
//
//     #[test]
//     fn test_write_boundary_values() {
//         let mut page = Page::default();
//         page.write(Value::I64(i64::MAX)).unwrap();
//         page.write(Value::I64(i64::MIN)).unwrap();
//         page.write(Value::I64(0)).unwrap();
//         assert_eq!(page.read(0).unwrap(), Value::I64(i64::MAX));
//         assert_eq!(page.read(1).unwrap(), Value::I64(i64::MIN));
//         assert_eq!(page.read(2).unwrap(), Value::I64(0));
//     }
//
//
//     // Write Errors
//
//     #[test]
//     fn test_write_null_to_non_nullable_fails() {
//         let mut page = Page::default();
//         assert_eq!(
//             page.write(Value::None).unwrap_err(),
//             PageError::NullViolation
//         );
//     }
//
//     #[test]
//     fn test_write_type_mismatch_i64_to_f64_fails() {
//         let mut page = Page::new_f64();
//         assert_eq!(
//             page.write(Value::I64(42)).unwrap_err(),
//             PageError::TypeMismatch {
//                 expected: ValueType::F64,
//                 got: ValueType::I64,
//             }
//         );
//     }
//
//     #[test]
//     fn test_write_type_mismatch_f64_to_i64_fails() {
//         let mut page = Page::default();
//         assert_eq!(
//             page.write(Value::F64(3.14)).unwrap_err(),
//             PageError::TypeMismatch {
//                 expected: ValueType::I64,
//                 got: ValueType::F64,
//             }
//         );
//     }
//
//     #[test]
//     fn test_write_beyond_capacity_fails_for_null() {
//         let mut page = Page::new_nullable();
//         for i in 0..page.capacity() {
//             page.write(Value::I64(i as i64)).unwrap();
//         }
//         assert_eq!(
//             page.write(Value::None).unwrap_err(),
//             PageError::Full
//         );
//     }
//
//     // Nullable Write
//
//     #[test]
//     fn test_write_null_to_nullable() {
//         let mut page = Page::new_nullable();
//         page.write(Value::None).unwrap();
//         assert_eq!(page.len(), 1);
//         assert_eq!(page.read(0).unwrap(), Value::None);
//     }
//
//     #[test]
//     fn test_write_mixed_nullable() {
//         let mut page = Page::new_nullable();
//         page.write(Value::I64(10)).unwrap();
//         page.write(Value::None).unwrap();
//         page.write(Value::I64(30)).unwrap();
//         page.write(Value::None).unwrap();
//         assert_eq!(page.read(0).unwrap(), Value::I64(10));
//         assert_eq!(page.read(1).unwrap(), Value::None);
//         assert_eq!(page.read(2).unwrap(), Value::I64(30));
//         assert_eq!(page.read(3).unwrap(), Value::None);
//     }
//
//
//
//
//
//     // Read
//
//     #[test]
//     fn test_read_empty_page() {
//         let page = Page::default();
//         assert_eq!(
//             page.read(0).unwrap_err(),
//             PageError::IndexOutOfBounds(0)
//         );
//     }
//
//     #[test]
//     fn test_read_invalid_index() {
//         let mut page = Page::default();
//         page.write(Value::I64(42)).unwrap();
//         assert_eq!(
//             page.read(1).unwrap_err(),
//             PageError::IndexOutOfBounds(1)
//         );
//         assert_eq!(
//             page.read(100).unwrap_err(),
//             PageError::IndexOutOfBounds(100)
//         );
//     }
//
//     #[test]
//     fn test_read_last_valid_index() {
//         let mut page = Page::default();
//         for i in 0..10 {
//             page.write(Value::I64(i)).unwrap();
//         }
//         assert_eq!(page.read(9).unwrap(), Value::I64(9));
//         assert_eq!(
//             page.read(10).unwrap_err(),
//             PageError::IndexOutOfBounds(10)
//         );
//     }
//
//     // Len
//
//     #[test]
//     fn test_len_updates_correctly() {
//         let mut page = Page::default();
//         assert_eq!(page.len(), 0);
//         page.write(Value::I64(1)).unwrap();
//         assert_eq!(page.len(), 1);
//         page.write(Value::I64(2)).unwrap();
//         assert_eq!(page.len(), 2);
//     }
//
//     // Clone
//
//     // #[test]
//     // fn test_clone_is_independent() {
//     //     let mut page = Page::default();
//     //     page.write(Value::I64(10)).unwrap();
//     //     page.write(Value::I64(20)).unwrap();
//     //
//     //     let mut cloned = page.clone();
//     //     cloned.write(Value::I64(30)).unwrap();
//     //
//     //     // Original unchanged
//     //     assert_eq!(page.len(), 2);
//     //     assert_eq!(page.read(0).unwrap(), Value::I64(10));
//     //     assert_eq!(page.read(1).unwrap(), Value::I64(20));
//     //
//     //     // Clone has the new write
//     //     assert_eq!(cloned.len(), 3);
//     //     assert_eq!(cloned.read(2).unwrap(), Value::I64(30));
//     // }
//
//
// }
//
//
