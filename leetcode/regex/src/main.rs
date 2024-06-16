impl Solution {
    pub fn is_match_recursive(s: &[u8], p: &[u8]) -> bool {
        println!("COMPARE {:?} {:?}", s, p);

        if s.len()==0 && p.len() == 0 {
            println!("found EOS+EOP");
            return true;
        }

        if p.len()==0 {
            println!("found EOP BEFORE EOS");
            return false;
        }

        fn char_match(c: u8, p:u8) -> bool {
            if p == b'.' || c == p {
                return true;
            } else {
                return false;
            }
        }

        if p[0] == b'*' {
            return is_match_recursive(s, &p[1..]);
        }

        if p.len() > 1 && p[1] == b'*' {
            if s.len() == 0 {
                return is_match_recursive(s, &p[2..]);
            }
            println!("found char match {}", char_match(s[0], p[0]));

            let greedy = char_match(s[0], p[0]) && is_match_recursive(&s[1..], p);
            let lazy = is_match_recursive(s, &p[2..]);
            println!("found * G: {}, L: {}", greedy, lazy);

            greedy || lazy
        } else {
            if s.len() > 0 {
                char_match(s[0], p[0]) && is_match_recursive(&s[1..], &p[1..])
            } else {
                println!("found EOS BEFORE EOP");
                return false;
            }
        }

    }

    pub fn is_match(s: String, p: String) -> bool {
        is_match_recursive(s.as_bytes(), p.as_bytes())
    }

}

fn main() {
    let tests = vec![
        ("ab".to_string(), ".*".to_string(), true),
        ("aa".to_string(), "a".to_string(), false),
        ("ab".to_string(), ".*c".to_string(), false),
        ("abc".to_string(), "a***abc".to_string(), false),
    ];

    for test in tests.iter() {
        let result = is_match(test.0.clone(), test.1.clone());
        println!("PASS: {} (output {}))", result == test.2, result);
    }
}
