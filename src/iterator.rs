


#[derive(Clone, Debug)]
pub struct BasicIterator {
    val: std::ops::RangeFrom<usize>,
}


impl BasicIterator {
    fn new() -> Self {
        BasicIterator { val: 0.. }
    }

    pub fn next(&mut self) -> usize {
        self.val.next().unwrap()
    }
}

impl Default for BasicIterator {
    fn default() -> Self {
        BasicIterator::new()
    }
}