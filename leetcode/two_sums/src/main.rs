use std::collections::HashMap;

pub fn two_sum(nums: Vec<i32>, target: i32) -> Vec<i32> {
    let mut vals:HashMap<i32, i32> = HashMap::new();
    for ii in 0..nums.len() {
        vals.insert(nums[ii],ii as i32);
    }
    
    for left in 0..nums.len() {
        match vals.get(&(target-nums[left])) {
            Some(right) => if left != *right as usize {
                return vec![left as i32, right.clone() as i32]
            },
            None => {}
        }
    }

    panic!("on the streets of london"); 
}
fn main() {
    println!("{:?}",two_sum(vec![3,2,4], 6));


    println!("Hello, world!");
}
