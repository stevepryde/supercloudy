//! Local BM25 inference for Qdrant's `qdrant/bm25` wire model.
//!
//! Compatibility reference: Qdrant v1.18.3 `lib/bm25/src/lib.rs` and
//! `src/common/inference/bm25_inference.rs`. Query terms have unit weights;
//! document terms use BM25 TF weights. IDF is applied by the collection scorer.

use std::collections::{BTreeMap, BTreeSet};

use rust_stemmers::{Algorithm, Stemmer};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Options {
    k: f64,
    b: f64,
    avg_len: f64,
    tokenizer: String,
    language: Option<String>,
    lowercase: Option<bool>,
    ascii_folding: Option<bool>,
    stopwords: Option<Stopwords>,
    stemmer: Option<StemmerOptions>,
    min_token_len: Option<usize>,
    max_token_len: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            k: 1.2,
            b: 0.75,
            avg_len: 256.0,
            tokenizer: "word".into(),
            language: None,
            lowercase: None,
            ascii_folding: None,
            stopwords: None,
            stemmer: None,
            min_token_len: None,
            max_token_len: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Stopwords {
    Language(String),
    Custom(StopwordsSet),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StopwordsSet {
    languages: Option<Vec<String>>,
    custom: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StemmerOptions {
    r#type: String,
    language: String,
}

/// Embed document text (`query == false`) or query text (`query == true`).
/// Unsupported preprocessing options are rejected, never approximated silently.
pub(super) fn embed(
    text: &str,
    options: Option<&Value>,
    query: bool,
) -> Result<(Vec<u32>, Vec<f32>), String> {
    let options: Options = match options.filter(|v| !v.is_null()) {
        Some(value) => serde_json::from_value(value.clone()).map_err(|e| e.to_string())?,
        None => Options::default(),
    };
    if !options.k.is_finite() || options.k < 0.0 {
        return Err("BM25 k must be finite and non-negative".into());
    }
    if !options.b.is_finite() || !(0.0..=1.0).contains(&options.b) {
        return Err("BM25 b must be between 0 and 1".into());
    }
    if !options.avg_len.is_finite() || options.avg_len <= 0.0 {
        return Err("BM25 avg_len must be positive".into());
    }
    if options.ascii_folding == Some(true) {
        return Err("BM25 ascii_folding is not supported".into());
    }
    if !matches!(options.tokenizer.as_str(), "word" | "whitespace" | "prefix") {
        return Err("BM25 supports word, whitespace and prefix tokenizers".into());
    }
    let language = options.language.as_deref().unwrap_or("english");
    if !matches!(language, "english" | "none") {
        return Err("BM25 supports english and none preprocessing languages".into());
    }
    let lowercase = options.lowercase.unwrap_or(true);
    let mut stopwords = BTreeSet::new();
    match &options.stopwords {
        None if language != "none" => add_language_stopwords("english", &mut stopwords)?,
        None => (),
        Some(Stopwords::Language(language)) => {
            add_language_stopwords(language, &mut stopwords)?;
        }
        Some(Stopwords::Custom(set)) => {
            for language in set.languages.iter().flatten() {
                add_language_stopwords(language, &mut stopwords)?;
            }
            stopwords.extend(set.custom.iter().flatten().cloned());
        }
    }
    if lowercase {
        stopwords = stopwords.into_iter().map(|s| s.to_lowercase()).collect();
    }
    let stemmer = match &options.stemmer {
        Some(config) => {
            if config.r#type != "snowball" {
                return Err("BM25 stemmer type must be snowball".into());
            }
            Some(Stemmer::create(algorithm(&config.language)?))
        }
        None if language == "none" => None,
        None => Some(Stemmer::create(Algorithm::English)),
    };
    let raw: Vec<&str> = if options.tokenizer == "whitespace" {
        text.split_whitespace().collect()
    } else {
        text.split(|c: char| !c.is_alphanumeric()).collect()
    };
    let prefix = options.tokenizer == "prefix";
    let mut tokens = Vec::new();
    for token in raw.into_iter().filter(|t| !t.is_empty()) {
        let token = if lowercase {
            token.to_lowercase()
        } else {
            token.to_owned()
        };
        // Prefix query matching deliberately retains stopwords, matching Qdrant.
        if !(prefix && query) && stopwords.contains(&token) {
            continue;
        }
        let token = match &stemmer {
            Some(stemmer) => stemmer.stem(&token).into_owned(),
            None => token,
        };
        let length = token.chars().count();
        if options.min_token_len.is_some_and(|min| length < min) {
            continue;
        }
        if prefix {
            let maximum = options.max_token_len.unwrap_or(usize::MAX).min(length);
            if query {
                tokens.push(token.chars().take(maximum).collect::<String>());
            } else {
                let minimum = options.min_token_len.unwrap_or(1);
                for n in minimum..=maximum {
                    tokens.push(token.chars().take(n).collect::<String>());
                }
            }
        } else if !options.max_token_len.is_some_and(|max| length > max) {
            tokens.push(token);
        }
    }
    let length = tokens.len() as f64;
    let mut counts = BTreeMap::<String, u32>::new();
    for token in tokens {
        *counts.entry(token).or_default() += 1;
    }
    let mut weights = BTreeMap::new();
    for (token, count) in counts {
        let weight = if query {
            1.0
        } else {
            let count = f64::from(count);
            (count * (options.k + 1.0)
                / options.k.mul_add(
                    1.0 - options.b + options.b * length / options.avg_len,
                    count,
                )) as f32
        };
        weights.insert(token_id(&token), weight);
    }
    Ok(weights.into_iter().unzip())
}

fn algorithm(language: &str) -> Result<Algorithm, String> {
    Ok(match language {
        "arabic" => Algorithm::Arabic,
        "armenian" => Algorithm::Armenian,
        "danish" => Algorithm::Danish,
        "dutch" => Algorithm::Dutch,
        "english" => Algorithm::English,
        "finnish" => Algorithm::Finnish,
        "french" => Algorithm::French,
        "german" => Algorithm::German,
        "greek" => Algorithm::Greek,
        "hungarian" => Algorithm::Hungarian,
        "italian" => Algorithm::Italian,
        "norwegian" => Algorithm::Norwegian,
        "portuguese" => Algorithm::Portuguese,
        "romanian" => Algorithm::Romanian,
        "russian" => Algorithm::Russian,
        "spanish" => Algorithm::Spanish,
        "swedish" => Algorithm::Swedish,
        "tamil" => Algorithm::Tamil,
        "turkish" => Algorithm::Turkish,
        _ => return Err("Unsupported BM25 snowball language".into()),
    })
}

fn add_language_stopwords(language: &str, out: &mut BTreeSet<String>) -> Result<(), String> {
    if !matches!(language, "english") {
        return Err("Only english BM25 stopwords are supported".into());
    }
    // Qdrant v1.18.3's English stopword vocabulary, written as lexical protocol
    // data. Removal precedes stemming, including fragments split at apostrophes.
    const WORDS: &str = "a about above after again against ain all am an and any are aren aren't as at be because been before being below between both but by can couldn couldn't d did didn didn't do does doesn doesn't doing don don't down during each few for from further had hadn hadn't has hasn hasn't have haven haven't having he her here hers herself him himself his how i if in into is isn isn't it it's its itself just ll m ma me mightn mightn't more most mustn mustn't my myself needn needn't no nor not now o of off on once only or other our ours ourselves out over own re s same shan shan't she she's should should've shouldn shouldn't so some such t than that that'll the their theirs them themselves then there these they this those through to too under until up ve very was wasn wasn't we were weren weren't what when where which while who whom why will with won won't wouldn wouldn't y you you'd you'll you're you've your yours yourself yourselves";
    out.extend(WORDS.split_whitespace().map(str::to_owned));
    Ok(())
}

/// MurmurHash3 x86 32-bit with seed zero, converted with signed absolute value.
fn token_id(token: &str) -> u32 {
    let bytes = token.as_bytes();
    let mut hash = 0_u32;
    let (chunks, remainder) = bytes.as_chunks::<4>();
    for chunk in chunks {
        let mut word = u32::from_le_bytes(*chunk);
        word = word
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        hash ^= word;
        hash = hash
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64);
    }
    let mut tail = 0_u32;
    for (index, byte) in remainder.iter().enumerate() {
        tail |= u32::from(*byte) << (index * 8);
    }
    if !remainder.is_empty() {
        hash ^= tail
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    (hash as i32).unsigned_abs()
}

#[cfg(test)]
mod tests {
    use super::embed;
    use serde_json::json;

    #[test]
    fn real_qdrant_document_embeddings_cover_stemming_stopwords_unicode_and_tf() {
        // Retrieved over HTTP from qdrant/qdrant:v1.18.3-unprivileged.
        let fixtures = [
            (
                "The QUICK, brown foxes jumping jumps over the lazy dog.",
                vec![
                    226376294, 741580288, 771291085, 1312749093, 1621867415, 1913189942,
                ],
                vec![1.660867, 1.660867, 1.660867, 1.660867, 1.660867, 1.8927912],
            ),
            (
                "alpha beta alpha gamma alpha",
                vec![977130622, 1447029955, 2022730153],
                vec![1.6697302, 1.989403, 1.6697302],
            ),
            (
                "café naïve façade",
                vec![605818632, 756638508, 1681038935],
                vec![1.6786885, 1.6786885, 1.6786885],
            ),
        ];
        for (text, expected_indices, expected_values) in fixtures {
            let (indices, values) = embed(text, None, false).unwrap();
            assert_eq!(indices, expected_indices);
            for (got, expected) in values.into_iter().zip(expected_values) {
                assert!((got - expected).abs() < 1e-6, "{got} != {expected}");
            }
        }
    }

    #[test]
    fn query_uses_unique_unit_weights_and_default_preprocessing() {
        // The reference server scores "alpha alpha" against the second fixture
        // above as 1.989403, proving repeated query terms do not multiply TF.
        assert_eq!(
            embed("alpha alpha", None, true).unwrap(),
            (vec![1447029955], vec![1.0])
        );
        assert!(embed("The and or", None, true).unwrap().0.is_empty());
        assert_eq!(
            embed("foxes jumping", None, true).unwrap().0,
            vec![1621867415, 1913189942]
        );
    }

    #[test]
    fn options_change_length_normalization_and_token_selection() {
        // At avg_len=5, 5-token document with alpha repeated 3 times has
        // TF = 3*2.2/(3+1.2), independently calculable as 11/7.
        let (_, values) = embed(
            "alpha beta alpha gamma alpha",
            Some(&json!({"avg_len":5})),
            false,
        )
        .unwrap();
        assert!((values[1] - 11.0 / 7.0).abs() < 1e-6);
        assert!(
            embed(
                "alpha",
                Some(&json!({"stopwords":{"custom":["alpha"]}})),
                true
            )
            .unwrap()
            .0
            .is_empty()
        );
        assert!(
            embed("alpha", Some(&json!({"min_token_len":6})), true)
                .unwrap()
                .0
                .is_empty()
        );
        assert!(embed("alpha", Some(&json!({"avg_len":0})), true).is_err());
        assert!(embed("alpha", Some(&json!({"unknown":true})), true).is_err());
        assert!(embed("alpha", Some(&json!({"tokenizer":"multilingual"})), true).is_err());
    }
}
