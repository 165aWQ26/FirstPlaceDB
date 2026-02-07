


#[derive(Clone, Debug)]
pub struct BasicIterator {
    val: std::ops::RangeFrom<usize>,
}

//All this does it create an iterator from 0 - inf & unwrap the option
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