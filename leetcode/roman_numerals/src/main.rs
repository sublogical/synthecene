
use std::collections::HashMap;

fn convert(input: &str) -> u32 {
    let values: HashMap<u8, u32> = HashMap::from([
        (b'M', 1000), 
        (b'D', 500),
        (b'C', 100),
        (b'L', 50),
        (b'X', 10),
        (b'V', 5),
        (b'I', 1)
    ]);
    
    let bytes = input.as_bytes();
    let mut acc = 0;
    let mut last:u32 = 1000;

    for byte in bytes {
        let curr: u32 = values[&byte].try_into().unwrap();
        acc += curr;
        if last < curr {
            acc -= last * 2;
        }
        last = curr;
    }

    return acc;
}

fn main() {
    let tests = vec![
        (1, "I"),
        (3, "III"),
        (4, "IV"),
        (90, "XC"),
        (97, "VCII"),
        (89, "LXXXIX")
    ];

    for test in tests.iter() {
        let result = convert(test.1);
        println!("{} == {} ({})", test.1, result, result == test.0);
    }
}
