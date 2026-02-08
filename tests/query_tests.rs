#[cfg(test)]
mod tests {
    // use ::index::*;src
    // use ::Table::*;src
    // use ::Query::*;
    use lstore::index::Index;
    use lstore::query::Query;
    use lstore::table::Table;

    #[test]
    fn quick_test_all() {
        let table: Table = Table::new(String::from("test"), 4, 5, 0);
        let mut query: Query = Query::new(table);

        let rec_one: Vec<Option<i64>> = vec![Some(1); 5];
        let rec_two: Vec<Option<i64>> = vec![Some(2); 5];
        let rec_three: Vec<Option<i64>> = vec![Some(3), Some(4), Some(5), Some(6), Some(7)];
        let rec_four: Vec<Option<i64>> = vec![Some(4), Some(5), Some(6), Some(7), Some(8)];

        query.insert(rec_one);
        query.insert(rec_two);

        let rid1 = query.table.indices[0].locate(1).unwrap();

        println!("{:?}", query.table.read(rid1[0]));

        //assert!(query.table.read(rid1[0]) == vec![Some(1); 5]);

        let rid2 = query.table.indices[0].locate(2);
        //assert!(query.table.read(rid2.unwrap()[0]) == vec![Some(2); 5]);
        println!("{:?}", query.table.read(rid2.unwrap()[0]));

        query.insert(rec_three);

        let ans: i64 = query.sum(1, 3, 3).unwrap();

        //assert!(ans == 8);
        println!("{:?}", ans);

        query.insert(rec_four);

        query.delete(4);

        let mut mask: [i64; 5] = [1, 0, 1, 0, 1];
        let ans_list: Vec<Vec<Option<i64>>> = query.select(1, 0, &mut mask).unwrap();

        for answer in ans_list {
            println!("{:?}", answer);
        }

        //assert!(query.table.indices[0].locate(4).is_none());
        println!("{:?}", query.table.indices[0].locate(4).is_none());

        query.increment(2, 0);
        query.increment(1, 0);

        let ans_list_two: Vec<Vec<Option<i64>>> = query.select(2, 0, &mut mask).unwrap();

        println!("{:?}", ans_list_two[0])
    }
}
