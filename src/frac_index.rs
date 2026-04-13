// Fractional indexing for step ordering
//
// Uses base-62 strings (0-9, A-Z, a-z) to generate sort keys that allow
// O(1) insertions between existing steps without reordering.
#![allow(dead_code)]

/// The base-62 alphabet used for sort keys: 0-9, A-Z, a-z.
const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Returns the index of a character in the base-62 alphabet.
fn char_index(c: u8) -> usize {
    match c {
        b'0'..=b'9' => (c - b'0') as usize,
        b'A'..=b'Z' => (c - b'A') as usize + 10,
        b'a'..=b'z' => (c - b'a') as usize + 36,
        _ => panic!("Invalid fractional index character: {}", c as char),
    }
}

/// Returns the initial sort key for the first step.
pub fn initial_key() -> String {
    "a0".to_string()
}

/// Returns a key that sorts after the given key.
///
/// Increments the last character. If it overflows, appends a '0'.
pub fn key_after(key: &str) -> String {
    let bytes = key.as_bytes();
    let last = bytes[bytes.len() - 1];
    let idx = char_index(last);

    if idx + 1 < ALPHABET.len() {
        // Increment last character
        let mut result = key.to_string();
        let len = result.len();
        // SAFETY: replacing a valid ASCII byte with another valid ASCII byte
        unsafe {
            result.as_bytes_mut()[len - 1] = ALPHABET[idx + 1];
        }
        result
    } else {
        // Last char is 'z', append '0' to go after
        format!("{key}0")
    }
}

/// Returns a key that is lexicographically between `a` and `b`.
///
/// If `b` is `None`, returns `key_after(a)`.
///
/// # Panics
/// Panics if `a >= b`.
pub fn key_between(a: &str, b: &str) -> String {
    assert!(a < b, "key_between requires a < b, got a={a:?} b={b:?}");

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    // Find the first position where they differ
    let min_len = a_bytes.len().min(b_bytes.len());

    for i in 0..min_len {
        let ai = char_index(a_bytes[i]);
        let bi = char_index(b_bytes[i]);

        if ai == bi {
            continue;
        }

        if bi - ai > 1 {
            // There's room between a[i] and b[i]; pick the midpoint
            let mid = ai + (bi - ai) / 2;
            let mut result = a[..i].to_string();
            result.push(ALPHABET[mid] as char);
            return result;
        }

        // Difference is exactly 1. We need to go deeper.
        // Take a's prefix up to and including position i, then find a key
        // between a's suffix and the top of the range.
        let mut result = a[..=i].to_string();
        let a_suffix = if i + 1 < a_bytes.len() {
            &a[i + 1..]
        } else {
            ""
        };
        result.push_str(&suffix_between(a_suffix, None));
        return result;
    }

    // One is a prefix of the other. Since a < b, a must be the shorter one.
    // We need a key between a and b where a is a prefix of b.
    // Take a as prefix, then find between "" and b's remaining suffix.
    let b_suffix = &b[min_len..];
    let mut result = a.to_string();
    result.push_str(&suffix_between("", Some(b_suffix)));
    result
}

/// Finds a suffix string that is lexicographically between `a_suffix` and `b_suffix`.
/// If `b_suffix` is None, it means "no upper bound" — just go higher.
fn suffix_between(a: &str, b: Option<&str>) -> String {
    match b {
        None => {
            // No upper bound — just pick the midpoint of the alphabet after a
            if a.is_empty() {
                // Midpoint of the full alphabet
                return String::from(ALPHABET[31] as char); // 'V'
            }
            let a_bytes = a.as_bytes();
            let first_idx = char_index(a_bytes[0]);
            if first_idx + 1 < ALPHABET.len() {
                let mid = first_idx + (ALPHABET.len() - first_idx) / 2;
                return String::from(ALPHABET[mid] as char);
            }
            // first char is 'z'; recurse on the rest
            let mut result = String::from('z');
            result.push_str(&suffix_between(&a[1..], None));
            result
        }
        Some(b_str) => {
            if b_str.is_empty() {
                panic!("suffix_between: b_suffix is empty but should be > a_suffix");
            }
            let b_bytes = b_str.as_bytes();
            let bi = char_index(b_bytes[0]);

            if a.is_empty() {
                if bi > 1 {
                    // Pick midpoint between 0 and b[0]
                    let mid = bi / 2;
                    return String::from(ALPHABET[mid] as char);
                }
                // b[0] is 0 or 1, go deeper
                let mut result = String::from(ALPHABET[0] as char);
                result.push_str(&suffix_between("", Some(&b_str[1..])));
                return result;
            }

            let a_bytes = a.as_bytes();
            let ai = char_index(a_bytes[0]);

            if ai == bi {
                let mut result = String::from(ALPHABET[ai] as char);
                let a_rest = if a.len() > 1 { &a[1..] } else { "" };
                let b_rest = if b_str.len() > 1 {
                    Some(&b_str[1..])
                } else {
                    None
                };
                result.push_str(&suffix_between(a_rest, b_rest));
                return result;
            }

            // ai < bi since a < b
            if bi - ai > 1 {
                let mid = ai + (bi - ai) / 2;
                return String::from(ALPHABET[mid] as char);
            }

            // Difference is 1, go deeper after a[0]
            let mut result = String::from(ALPHABET[ai] as char);
            let a_rest = if a.len() > 1 { &a[1..] } else { "" };
            result.push_str(&suffix_between(a_rest, None));
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_key() {
        assert_eq!(initial_key(), "a0");
    }

    #[test]
    fn test_key_after() {
        assert_eq!(key_after("a0"), "a1");
        assert_eq!(key_after("a9"), "aA");
        assert_eq!(key_after("aZ"), "aa");
        assert_eq!(key_after("ay"), "az");
        // Overflow: 'z' wraps by appending
        assert_eq!(key_after("az"), "az0");
    }

    #[test]
    fn test_key_between_simple() {
        let mid = key_between("a0", "a1");
        assert!(mid.as_str() > "a0", "mid={mid} should be > a0");
        assert!(mid.as_str() < "a1", "mid={mid} should be < a1");
    }

    #[test]
    fn test_key_between_wide_gap() {
        let mid = key_between("a0", "a9");
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a9");
    }

    #[test]
    fn test_key_between_different_lengths() {
        let mid = key_between("a0", "a10");
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a10");
    }

    #[test]
    fn test_key_between_adjacent_letters() {
        // a0 and a1 differ by 1, so midpoint requires going deeper
        let mid = key_between("a0", "a1");
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a1");
        // Should be something like "a0V"
        assert!(mid.len() > 2);
    }

    #[test]
    fn test_multiple_insertions_maintain_order() {
        let mut keys = vec![initial_key()];
        // Insert 10 keys after each previous one
        for _ in 0..10 {
            let last = keys.last().unwrap();
            keys.push(key_after(last));
        }

        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] < keys[i + 1],
                "keys[{i}]={} should be < keys[{}]={}",
                keys[i],
                i + 1,
                keys[i + 1]
            );
        }
    }

    #[test]
    fn test_key_between_repeated_midpoints() {
        // Insert several keys between a0 and a1
        let mut lo = "a0".to_string();
        let hi = "a1";
        let mut keys = vec![lo.clone()];

        for _ in 0..5 {
            let mid = key_between(&lo, hi);
            assert!(mid.as_str() > lo.as_str(), "mid={mid} should be > lo={lo}");
            assert!(mid.as_str() < hi, "mid={mid} should be < hi={hi}");
            keys.push(mid.clone());
            lo = mid;
        }
        keys.push(hi.to_string());

        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] < keys[i + 1],
                "keys[{i}]={} should be < keys[{}]={}",
                keys[i],
                i + 1,
                keys[i + 1]
            );
        }
    }

    #[test]
    fn test_char_index_roundtrip() {
        for (i, &ch) in ALPHABET.iter().enumerate() {
            assert_eq!(char_index(ch), i, "char_index failed for {}", ch as char);
        }
    }

    #[test]
    #[should_panic(expected = "key_between requires a < b")]
    fn test_key_between_panics_when_a_ge_b() {
        key_between("a1", "a0");
    }

    #[test]
    #[should_panic(expected = "key_between requires a < b")]
    fn test_key_between_panics_when_equal() {
        key_between("a0", "a0");
    }
}
