pub struct Query<T> {

}

impl Query<T> {
    
    pub fn select(&self, key: T) -> Result<Vec<Record>, bool>{
        

    }

    pub fn insert(&self, record: Record) -> bool{
        
    }

    pub fn delete(&self) -> bool{

    }

    pub fn sum(&self, key: T){

    }

    pub fn update(&self, ) -> bool{

    }
}
