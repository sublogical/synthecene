use std::mem::replace;
use async_trait::async_trait;

use crate::task::Task;


#[async_trait]
pub trait Controller {
    async fn next_task(& mut self) -> Option<Box<dyn Task + Send>>;
}

pub struct OneTimeUse<T>(Option<T>);

impl <T> OneTimeUse<T> {
    pub fn init(x:T) -> OneTimeUse<T> {
        OneTimeUse(Some(x))
    }
}

impl <T> Iterator for OneTimeUse<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_none() {
            None
        } else {
            replace(&mut self.0, None)
        }
    }
}

#[async_trait]
impl Controller for OneTimeUse<Box<dyn Task + Send>> {
    async fn next_task(& mut self) -> Option<Box<dyn Task + Send>> {
        self.next()
    }
}



#[cfg(test)]
mod tests {
    use super::OneTimeUse;

    #[test]
    fn test_one_time_use() {
        let mut o = OneTimeUse::init(1);
        assert_eq!(o.next(), Some(1));
        assert_eq!(o.next(), None);

        let o = OneTimeUse::init(1);
        let v:Vec<u32> = o.collect();
        assert_eq!(v, vec![1]);
    }
}
