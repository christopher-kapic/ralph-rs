// Fractional indexing for step ordering
//
// Uses base-62 strings (0-9, A-Z, a-z) to generate sort keys that allow
// O(1) insertions between existing steps without reordering.

use std::fmt;

/// The base-62 alphabet used for sort keys: 0-9, A-Z, a-z.
const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Errors returned by fractional indexing operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FracIndexError {
    /// No fractional index key exists between `a` and `b` under this scheme.
    ///
    /// Triggered when `b` is `a` followed by one or more `'0'` characters
    /// (or other inputs that leave no room for a midpoint to be synthesized).
    NoKeyBetween { a: String, b: String },
    /// A sort key contained a character outside the base-62 alphabet.
    InvalidChar(char),
    /// A fractional index key was empty where a non-empty key is required.
    EmptyKey,
    /// `key_between` was called without `a < b`.
    InvalidOrder { a: String, b: String },
}

impl fmt::Display for FracIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FracIndexError::NoKeyBetween { a, b } => {
                write!(
                    f,
                    "cannot find fractional index key between {a:?} and {b:?}"
                )
            }
            FracIndexError::InvalidChar(c) => {
                write!(f, "invalid fractional index character: {c:?}")
            }
            FracIndexError::EmptyKey => {
                write!(f, "fractional index key must not be empty")
            }
            FracIndexError::InvalidOrder { a, b } => {
                write!(f, "key_between requires a < b, got a={a:?} b={b:?}")
            }
        }
    }
}

impl std::error::Error for FracIndexError {}

/// Returns the index of a character in the base-62 alphabet.
fn char_index(c: u8) -> Result<usize, FracIndexError> {
    match c {
        b'0'..=b'9' => Ok((c - b'0') as usize),
        b'A'..=b'Z' => Ok((c - b'A') as usize + 10),
        b'a'..=b'z' => Ok((c - b'a') as usize + 36),
        _ => Err(FracIndexError::InvalidChar(c as char)),
    }
}

/// Returns the initial sort key for the first step.
pub fn initial_key() -> String {
    "a0".to_string()
}

/// Returns a key that sorts after the given key.
///
/// Increments the last character. If it overflows, appends a midpoint suffix
/// so there remains room to insert between the original key and the new key.
///
/// # Errors
/// Returns [`FracIndexError::EmptyKey`] if `key` is empty.
/// Returns [`FracIndexError::InvalidChar`] if the last character of `key` is
/// outside the base-62 alphabet.
pub fn key_after(key: &str) -> Result<String, FracIndexError> {
    let bytes = key.as_bytes();
    if bytes.is_empty() {
        return Err(FracIndexError::EmptyKey);
    }
    let last = bytes[bytes.len() - 1];
    let idx = char_index(last)?;

    if idx + 1 < ALPHABET.len() {
        // Increment last character
        let mut result = key.to_string();
        let len = result.len();
        // SAFETY: replacing a valid ASCII byte with another valid ASCII byte
        unsafe {
            result.as_bytes_mut()[len - 1] = ALPHABET[idx + 1];
        }
        Ok(result)
    } else {
        Ok(format!("{key}{}", ALPHABET[ALPHABET.len() / 2] as char))
    }
}

/// Returns a key that sorts before the given key.
///
/// # Errors
/// Returns [`FracIndexError::NoKeyBetween`] when the key is already at the
/// lower edge of the alphabet and no non-empty key can sort before it.
pub fn key_before(key: &str) -> Result<String, FracIndexError> {
    key_between("", key)
}

/// Returns a key that is lexicographically between `a` and `b`.
///
/// # Errors
/// Returns [`FracIndexError::NoKeyBetween`] when the inputs admit no key
/// between them under this scheme (e.g. `a = "0"`, `b = "00"`).
/// Returns [`FracIndexError::InvalidOrder`] if `a >= b` (the ordering
/// precondition is violated; this also covers the `a == b == ""` case).
/// Returns [`FracIndexError::EmptyKey`] if an empty `b` would otherwise be
/// indexed (only reachable when the `a < b` guard does not already reject it).
pub fn key_between(a: &str, b: &str) -> Result<String, FracIndexError> {
    if a >= b {
        return Err(FracIndexError::InvalidOrder {
            a: a.to_string(),
            b: b.to_string(),
        });
    }
    // Past the `a < b` guard, an empty `b` is impossible (only "" sorts <= "",
    // and "" >= any non-empty b). An empty `a` here is the legitimate
    // "a is a prefix of b" case, which `suffix_between` handles. The empty `b`
    // guard below is therefore unreachable defense-in-depth: if a future
    // refactor weakened the ordering check, an empty `b` would make the
    // `b[min_len..]` / suffix recursion misbehave, so reject it explicitly.
    if b.is_empty() {
        return Err(FracIndexError::EmptyKey);
    }

    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    // Find the first position where they differ
    let min_len = a_bytes.len().min(b_bytes.len());

    let make_err = || FracIndexError::NoKeyBetween {
        a: a.to_string(),
        b: b.to_string(),
    };

    for i in 0..min_len {
        let ai = char_index(a_bytes[i])?;
        let bi = char_index(b_bytes[i])?;

        if ai == bi {
            continue;
        }

        if bi - ai > 1 {
            // There's room between a[i] and b[i]; pick the midpoint
            let mid = ai + (bi - ai) / 2;
            let mut result = a[..i].to_string();
            result.push(ALPHABET[mid] as char);
            return Ok(result);
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
        result.push_str(&suffix_between(a_suffix, None).map_err(|_| make_err())?);
        return Ok(result);
    }

    // One is a prefix of the other. Since a < b, a must be the shorter one.
    // We need a key between a and b where a is a prefix of b.
    // Take a as prefix, then find between "" and b's remaining suffix.
    let b_suffix = &b[min_len..];
    let mut result = a.to_string();
    result.push_str(&suffix_between("", Some(b_suffix)).map_err(|_| make_err())?);
    Ok(result)
}

/// Finds a suffix string that is lexicographically between `a_suffix` and `b_suffix`.
/// If `b_suffix` is None, it means "no upper bound" — just go higher.
fn suffix_between(a: &str, b: Option<&str>) -> Result<String, FracIndexError> {
    match b {
        None => {
            // No upper bound — just pick the midpoint of the alphabet after a
            if a.is_empty() {
                // Midpoint of the full alphabet
                return Ok(String::from(ALPHABET[31] as char)); // 'V'
            }
            let a_bytes = a.as_bytes();
            let first_idx = char_index(a_bytes[0])?;
            if first_idx + 1 < ALPHABET.len() {
                let mid = first_idx + (ALPHABET.len() - first_idx) / 2;
                return Ok(String::from(ALPHABET[mid] as char));
            }
            // first char is 'z'; recurse on the rest
            let mut result = String::from('z');
            result.push_str(&suffix_between(&a[1..], None)?);
            Ok(result)
        }
        Some(b_str) => {
            if b_str.is_empty() {
                // No room to synthesize a key — bubble up; key_between will
                // rewrite this with the original (full) a/b for the user.
                return Err(FracIndexError::NoKeyBetween {
                    a: a.to_string(),
                    b: String::new(),
                });
            }
            let b_bytes = b_str.as_bytes();
            let bi = char_index(b_bytes[0])?;

            if a.is_empty() {
                if bi > 1 {
                    // Pick midpoint between 0 and b[0]
                    let mid = bi / 2;
                    return Ok(String::from(ALPHABET[mid] as char));
                }
                if bi == 1 {
                    // Anything starting with '0' sorts before a key starting
                    // with '1'; choose a midpoint inside the '0...' range so
                    // repeated move-to-front operations do not bottom out at
                    // "1".
                    let mut result = String::from(ALPHABET[0] as char);
                    result.push_str(&suffix_between("", None)?);
                    return Ok(result);
                }
                if b_str.len() > 1 {
                    // Stay inside the "0..." range without returning the
                    // exact alphabet-floor key "0"; exact floor-prefix gaps
                    // such as ("0", "000") remain unsplittable by design.
                    let mut result = String::from(ALPHABET[0] as char);
                    result.push_str(&suffix_between("", Some(&b_str[1..]))?);
                    return Ok(result);
                }
                // No non-empty key sorts between "" and the alphabet floor.
                return Err(FracIndexError::NoKeyBetween {
                    a: String::new(),
                    b: b_str.to_string(),
                });
            }

            let a_bytes = a.as_bytes();
            let ai = char_index(a_bytes[0])?;

            if ai == bi {
                let mut result = String::from(ALPHABET[ai] as char);
                let a_rest = if a.len() > 1 { &a[1..] } else { "" };
                let b_rest = if b_str.len() > 1 {
                    Some(&b_str[1..])
                } else {
                    None
                };
                result.push_str(&suffix_between(a_rest, b_rest)?);
                return Ok(result);
            }

            // ai < bi since a < b
            if bi - ai > 1 {
                let mid = ai + (bi - ai) / 2;
                return Ok(String::from(ALPHABET[mid] as char));
            }

            // Difference is 1, go deeper after a[0]
            let mut result = String::from(ALPHABET[ai] as char);
            let a_rest = if a.len() > 1 { &a[1..] } else { "" };
            result.push_str(&suffix_between(a_rest, None)?);
            Ok(result)
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
        assert_eq!(key_after("a0").unwrap(), "a1");
        assert_eq!(key_after("a9").unwrap(), "aA");
        assert_eq!(key_after("aZ").unwrap(), "aa");
        assert_eq!(key_after("ay").unwrap(), "az");
        // Overflow appends a midpoint suffix, leaving room between old and new.
        assert_eq!(key_after("az").unwrap(), "azV");
        assert!(key_between("az", "azV").is_ok());
    }

    #[test]
    fn test_key_between_simple() {
        let mid = key_between("a0", "a1").unwrap();
        assert!(mid.as_str() > "a0", "mid={mid} should be > a0");
        assert!(mid.as_str() < "a1", "mid={mid} should be < a1");
    }

    #[test]
    fn test_key_between_wide_gap() {
        let mid = key_between("a0", "a9").unwrap();
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a9");
    }

    #[test]
    fn test_key_between_different_lengths() {
        let mid = key_between("a0", "a10").unwrap();
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a10");
    }

    #[test]
    fn test_key_between_adjacent_letters() {
        // a0 and a1 differ by 1, so midpoint requires going deeper
        let mid = key_between("a0", "a1").unwrap();
        assert!(mid.as_str() > "a0");
        assert!(mid.as_str() < "a1");
        // Should be something like "a0V"
        assert!(mid.len() > 2);
    }

    #[test]
    fn test_multiple_insertions_maintain_order() {
        let mut keys = vec![initial_key()];
        // Insert enough keys to cross the base-62 suffix boundary that used
        // to produce adjacent unsplittable keys (`az` / `az0`).
        for _ in 0..70 {
            let last = keys.last().unwrap();
            keys.push(key_after(last).unwrap());
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

        for pair in keys.windows(2) {
            let between = key_between(&pair[0], &pair[1]).unwrap();
            assert!(
                between > pair[0] && between < pair[1],
                "expected a splittable gap between {:?} and {:?}, got {between:?}",
                pair[0],
                pair[1],
            );
        }
    }

    #[test]
    fn test_key_before_first_normal_key() {
        let before = key_before("a0").unwrap();
        assert!(before.as_str() > "");
        assert!(before.as_str() < "a0");
        let before_one = key_before("1").unwrap();
        assert!(before_one.as_str() > "");
        assert!(before_one.as_str() < "1");
        assert_eq!(before_one, "0V");
        assert_eq!(
            key_before("0").unwrap_err(),
            FracIndexError::NoKeyBetween {
                a: String::new(),
                b: "0".to_string(),
            }
        );
    }

    #[test]
    fn test_repeated_key_before_stays_ordered_and_splittable() {
        let mut keys = vec![initial_key()];
        for _ in 0..20 {
            let first = keys.first().unwrap().clone();
            keys.insert(0, key_before(&first).unwrap());
        }

        for pair in keys.windows(2) {
            assert!(
                pair[0] < pair[1],
                "front-inserted keys must remain ordered: {pair:?}"
            );
            let between = key_between(&pair[0], &pair[1]).unwrap();
            assert!(
                between > pair[0] && between < pair[1],
                "front-inserted gap must remain splittable between {:?} and {:?}, got {between:?}",
                pair[0],
                pair[1],
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
            let mid = key_between(&lo, hi).unwrap();
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
            assert_eq!(
                char_index(ch).unwrap(),
                i,
                "char_index failed for {}",
                ch as char
            );
        }
    }

    #[test]
    fn test_char_index_invalid_returns_err() {
        for &c in &[b'!', b'-', b'.', b'/', b'@', b'[', b'`', b'{', 0u8, 0xFFu8] {
            let result = char_index(c);
            assert_eq!(
                result,
                Err(FracIndexError::InvalidChar(c as char)),
                "expected Err for byte {c:#x}, got {result:?}",
            );
        }
    }

    #[test]
    fn test_key_after_invalid_char_returns_err() {
        // Last byte '!' is outside the base-62 alphabet.
        let result = key_after("a!");
        assert_eq!(result, Err(FracIndexError::InvalidChar('!')));
    }

    #[test]
    fn test_key_between_invalid_char_returns_err() {
        // First byte '!' is outside the alphabet; should surface as InvalidChar.
        let result = key_between("!0", "!1");
        assert_eq!(result, Err(FracIndexError::InvalidChar('!')));
    }

    #[test]
    fn test_key_between_a_gt_b_returns_invalid_order() {
        let result = key_between("a1", "a0");
        assert_eq!(
            result,
            Err(FracIndexError::InvalidOrder {
                a: "a1".to_string(),
                b: "a0".to_string(),
            })
        );
    }

    #[test]
    fn test_key_between_equal_returns_invalid_order() {
        let result = key_between("a0", "a0");
        assert_eq!(
            result,
            Err(FracIndexError::InvalidOrder {
                a: "a0".to_string(),
                b: "a0".to_string(),
            })
        );
    }

    #[test]
    fn test_key_after_empty_returns_empty_key() {
        assert_eq!(key_after(""), Err(FracIndexError::EmptyKey));
    }

    #[test]
    fn test_key_between_both_empty_returns_invalid_order() {
        // "" >= "" so the ordering guard rejects this before any indexing.
        assert_eq!(
            key_between("", ""),
            Err(FracIndexError::InvalidOrder {
                a: String::new(),
                b: String::new(),
            })
        );
    }

    #[test]
    fn test_key_between_empty_a_prefix_path_still_works() {
        // Empty `a` with non-empty `b` is the legitimate prefix case and must
        // NOT regress to an error — it should still synthesize a valid key.
        let mid = key_between("", "a1").unwrap();
        assert!(mid.as_str() > "", "mid={mid} should be > \"\"");
        assert!(mid.as_str() < "a1", "mid={mid} should be < a1");
    }

    #[test]
    fn test_key_between_regression_no_panic_on_bad_inputs() {
        // Previously-panicking inputs now return a clean Err instead of
        // aborting the process.
        for (a, b) in [("a1", "a0"), ("a0", "a0"), ("zzz", "a0"), ("", "")] {
            let result = key_between(a, b);
            assert!(
                matches!(result, Err(FracIndexError::InvalidOrder { .. })),
                "expected InvalidOrder for key_between({a:?}, {b:?}), got {result:?}",
            );
        }
        assert_eq!(key_after(""), Err(FracIndexError::EmptyKey));
    }

    #[test]
    fn test_new_error_variants_display() {
        assert_eq!(
            FracIndexError::EmptyKey.to_string(),
            "fractional index key must not be empty",
        );
        let order = FracIndexError::InvalidOrder {
            a: "a1".to_string(),
            b: "a0".to_string(),
        };
        let msg = order.to_string();
        assert!(
            msg.contains("requires a < b"),
            "InvalidOrder message should explain the precondition: {msg}",
        );
        assert!(
            msg.contains("\"a1\"") && msg.contains("\"a0\""),
            "InvalidOrder message should mention both inputs: {msg}",
        );
    }

    #[test]
    fn test_key_between_zero_prefix_returns_error() {
        // b is a followed by only '0' characters — no key can be synthesized.
        let err = key_between("0", "00").expect_err("expected error for '0' vs '00'");
        assert_eq!(
            err,
            FracIndexError::NoKeyBetween {
                a: "0".to_string(),
                b: "00".to_string(),
            }
        );
    }

    #[test]
    fn test_key_between_zero_suffix_edge_pairs_return_error() {
        // Each of these pairs has b = a + some "0" characters, so the recursion
        // hits an empty upper-bound suffix and must surface an error.
        for (a, b) in [
            ("0", "00"),
            ("0", "000"),
            ("00", "000"),
            ("a0", "a00"),
            ("a0", "a000"),
        ] {
            let result = key_between(a, b);
            assert!(
                result.is_err(),
                "expected Err for key_between({a:?}, {b:?}), got {result:?}",
            );
        }
    }

    #[test]
    fn test_frac_index_error_display_mentions_inputs() {
        let err = key_between("0", "00").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("\"0\""), "message should mention a: {msg}");
        assert!(msg.contains("\"00\""), "message should mention b: {msg}");
    }
}
