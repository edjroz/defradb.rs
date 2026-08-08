//! Deterministic two-node divergence fixtures.
//!
//! A scenario is `n` documents seeded on both nodes, then `d` of them updated
//! on the writer only. Everything is derived from a seed so a measurement can
//! be reproduced exactly.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// One document as first written to both nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SeedDoc {
    pub name: String,
    pub value: i64,
}

/// An update applied on the writer only, leaving the reader behind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DocUpdate {
    /// Index into [`DivergenceFixture::docs`].
    pub doc_index: usize,
    pub value: i64,
}

/// `n` seeded documents plus `d` writer-only updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DivergenceFixture {
    pub seed: u64,
    pub docs: Vec<SeedDoc>,
    pub updates: Vec<DocUpdate>,
}

impl DivergenceFixture {
    /// Build the fixture for `docs` documents of which `diverged` differ.
    ///
    /// # Panics
    /// If `diverged` exceeds `docs`.
    pub(crate) fn new(seed: u64, docs: usize, diverged: usize) -> Self {
        assert!(
            diverged <= docs,
            "cannot diverge {diverged} of {docs} documents"
        );

        let mut rng = StdRng::seed_from_u64(seed);
        let docs: Vec<SeedDoc> = (0..docs)
            .map(|index| SeedDoc {
                name: format!("doc-{index:06}"),
                value: rng.gen_range(0..1_000_000),
            })
            .collect();

        let mut indices: Vec<usize> = (0..docs.len()).collect();
        indices.shuffle(&mut rng);
        indices.truncate(diverged);
        indices.sort_unstable();

        let updates = indices
            .into_iter()
            .map(|doc_index| DocUpdate {
                doc_index,
                value: rng.gen_range(1_000_000..2_000_000),
            })
            .collect();

        Self {
            seed,
            docs,
            updates,
        }
    }

    /// The document set the reader is expected to converge on: every seeded
    /// document with the writer-only updates applied.
    pub(crate) fn converged_values(&self) -> Vec<i64> {
        let mut values: Vec<i64> = self.docs.iter().map(|doc| doc.value).collect();
        for update in &self.updates {
            values[update.doc_index] = update.value;
        }
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_yields_an_identical_fixture() {
        for seed in [0, 1, 7, 42, u64::MAX] {
            let left = DivergenceFixture::new(seed, 50, 10);
            let right = DivergenceFixture::new(seed, 50, 10);
            assert_eq!(left, right, "seed {seed} was not reproducible");
        }
    }

    #[test]
    fn different_seeds_yield_different_fixtures() {
        let left = DivergenceFixture::new(1, 50, 10);
        let right = DivergenceFixture::new(2, 50, 10);
        assert_ne!(left, right);
    }

    #[test]
    fn shapes_match_the_requested_parameters() {
        let fixture = DivergenceFixture::new(9, 500, 37);
        assert_eq!(fixture.docs.len(), 500);
        assert_eq!(fixture.updates.len(), 37);

        let names: std::collections::HashSet<&str> =
            fixture.docs.iter().map(|doc| doc.name.as_str()).collect();
        assert_eq!(names.len(), 500, "document names must be unique");
    }

    #[test]
    fn every_update_targets_a_distinct_document() {
        let fixture = DivergenceFixture::new(3, 100, 100);
        let targets: std::collections::HashSet<usize> =
            fixture.updates.iter().map(|u| u.doc_index).collect();
        assert_eq!(targets.len(), 100);
    }

    #[test]
    fn updates_change_the_value_they_target() {
        let fixture = DivergenceFixture::new(5, 200, 20);
        let converged = fixture.converged_values();
        for update in &fixture.updates {
            assert_eq!(converged[update.doc_index], update.value);
            assert_ne!(fixture.docs[update.doc_index].value, update.value);
        }
    }

    #[test]
    fn zero_divergence_leaves_the_seeded_state_untouched() {
        let fixture = DivergenceFixture::new(11, 10, 0);
        assert!(fixture.updates.is_empty());
        assert_eq!(
            fixture.converged_values(),
            fixture.docs.iter().map(|d| d.value).collect::<Vec<_>>()
        );
    }

    #[test]
    #[should_panic(expected = "cannot diverge")]
    fn diverging_more_documents_than_exist_is_rejected() {
        DivergenceFixture::new(1, 10, 11);
    }
}
