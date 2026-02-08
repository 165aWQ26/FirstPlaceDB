use crate::table::Record;
use crate::table::Table;

pub struct Query {
    table: Table
}

impl Query {

    //Need to get the values
    pub fn insert(&mut self, record: Vec<Option<i64>>) -> bool {
        //need to know how to set a i64 to none
        let indirection_pointer:Option<i64> = None;
        let schema_encoding: Option<i64> = Some(0);
        let key: Option<i64> = record[self.table.key_index];
        
        if self.table.index[self.table.key_index].locate(key).is_none(){
            //Update all possible indexes into index
            for i in 0..record.len(){  
                self.table.index[i].insert(record[i], self.table.rid);   
            }
            //Appending rid, schema, then the indirection pointer to the end of
            //  it
            let mut metadata : Vec<Option<i64>> = vec![self.table.rid, schema_encoding, indirection_pointer]
            record.append(&mut metadata);
        
            //Generic looking method call
            self.table.pagerange.base_append(record);  
            return true;
        }
        return false
    }

    pub fn select(&self, key: i64, search_key_index:usize,
                  projected_columns_index: &mut [i64]) -> Result<Vec<Record>, bool> { 

        if let Some(rids) = self.table.index[search_key_index].locate(key){
            
            let mut records:Vec<Record> = Vec::new();
        
            for rid in rids{
                records.push(self.table.pagerange.read(projected_columns_index, rid));
            }

            Ok(records)
        }

        else{
            Err(false)
        }

    }

    pub fn select_version(&self, key: i64, search_key_index:usize,
                  projected_columns_index: &mut [i64], relative_version:i64) -> Result<Vec<Record>, bool> {
        
        
    }

    pub fn update(&self, key: i64, record: Vec<Option<i64>>) -> bool {
        let rid: Option<Vec<i64>> = self.table.index[self.table.key_index].locate(key);
        let indirection_pointer: Option<i64> = self.table.index[self.table.key_index].locate(record[self.table.key]);
        let schema_encoding: Option<i64> = self.table.pagerange.readsingle(record.len() + 1,rid);
        let key: Option<i64> = record[self.table.key_index];
        
        if rid.is_some(){

            //Updating index for alll value that have been changed
            for i in 0..record.len(){
                if record[i].is_some(){
                    self.table.index[i].remove(self.table.pagerange.read_single(i,rid),rid);
                    self.table.index[i].insert(record[i],rid);
                    //Updates schema encoding
                    schema_encoding |= 1 << i;
                }
            }
            //Appending rid, schema, then the indirection pointer to the end of
            //  it
            record.push(Some(self.table.rid));
            record.push(schema_encoding);
            record.push(indirection_pointer);
            
            //Generic looking method call
            self.table.pagerange.tail_append(record); 
            return true;
        }
        return false;
    }




    pub fn delete(&self, key: i64) -> bool {
        //update() with only null values
        
        let schema_encoding: i64 = 0;
        let v: Vec<Option<i64> = vec![None; self.table.num_columns];


        //make v all None values
        if self.table.index.locate(key).is_some(){
            self.table.index[i].remove(key,rid);
            self.table.pagerange.tail_append(v); 
        }
        return false
    }


    pub fn sum(&self, start_range:i64, end_range:i64, col: i64) -> Result<i64, bool>{
        if let Some(rids) = self.table.index[self.table.key_index].locate_range(start_range, end_range){
            let sum:i64 = 0
            
            for rid in rids{
                sum += self.table.pagerange.read_single(col, rid)
            }
            OK(sum)
        }
        else{
            Err(false)
        }

    }

    pub fn sum_version(&self, search_key:i64, search_key_index:i64, 
                            projected_columns_index:i64, relative_version:i64){
        
    }

    pub fn increment(&self, key: i64, column: usize) -> bool{

        let mut record:Vec<Option<i64>> = vec![None; self.table.num_columns];

        if let Some(rid) = self.table.index[self.table.key_index].locate(key){
            record[column] = self.table.read(column, rid) + 1;
            
            if self.update(key, record){
                return true;
            }
            else{
                return false;
            }
                
        }

        else{
            return false;
        }
    }

}
