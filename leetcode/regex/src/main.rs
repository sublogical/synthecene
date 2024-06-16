struct Solution {}


#[derive(Copy,Clone)]
enum State {
    Unknown,
    Match,
    Fail
}

impl Solution {
    pub fn is_match_recursive(si: usize, pi: usize, state:&mut Vec<State>, s: &[u8], p: &[u8]) -> bool {
        println!("{}{} check", si, pi);
        fn char_match(c: u8, p:u8) -> bool {
            if p == b'.' || c == p {
                return true;
            } else {
                return false;
            }
        }

        if si == s.len() && pi == p.len() {
            return true
        } else if pi == p.len() {
            return false
        } else {
            let me_idx = si * p.len() + pi;
            match state[me_idx] {
                State::Match => return true,
                State::Fail => return false,
                _ => {}
            }
        
            let result = if p[pi] == b'*' {
                Self::is_match_recursive(si, pi+1, state, s, p)
            } else {
                if pi+1 < p.len() && p[pi+1] == b'*' {
                    if si >= s.len() {
                        Self::is_match_recursive(si, pi+2, state, s, p)
                    } else {
                        let greedy = char_match(s[si], p[pi]) && Self::is_match_recursive(si+1, pi, state, s, p);
                        let lazy = Self::is_match_recursive(si, pi+2, state, s, p);

                        greedy || lazy
                    }
                } else {
                    if si < s.len() {
                        char_match(s[si], p[pi]) && Self::is_match_recursive(si+1, pi+1, state, s, p)
                    } else {
                        false
                    }
                }
            };
            state[me_idx] = if result {
                State::Match
            } else {
                State::Fail
            };
            return result;
                
        };
    }

    pub fn is_match(s: String, p: String) -> bool {
        let mut state = vec![State::Unknown; (s.len()+1) * p.len()];

        Self::is_match_recursive(0, 0, &mut state, s.as_bytes(), p.as_bytes())
     }

}

fn main() {
    let tests = vec![
        ("ab".to_string(), ".*".to_string(), true),
        ("aa".to_string(), "a".to_string(), false),
        ("ab".to_string(), ".*c".to_string(), false),
        ("abc".to_string(), "a***abc".to_string(), true),
    ];

    for test in tests.iter() {
        let soln = Solution {};
        let result = Solution::is_match(test.0.clone(), test.1.clone());
        println!("PASS: {} (output {}))", result == test.2, result);
    }
}
