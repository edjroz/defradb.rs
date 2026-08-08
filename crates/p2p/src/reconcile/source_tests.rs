use super::*;
use crate::reconcile::error::ReconcileError;

fn id(n: u64) -> ItemId {
    ItemId::new(n.to_be_bytes().to_vec())
}

fn source(heights_and_ids: &[(u64, u64)]) -> MemorySource {
    MemorySource::new(
        heights_and_ids
            .iter()
            .map(|(h, n)| Item::new(*h, id(*n)))
            .collect::<Vec<_>>(),
    )
    .expect("distinct items")
}

#[test]
fn sort_key_is_big_endian_height_then_id() {
    let key = SortKey::new(0x0102_0304_0506_0708, &ItemId::new(vec![0xaa, 0xbb]));
    assert_eq!(
        key.as_bytes(),
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xaa, 0xbb]
    );
}

#[test]
fn sort_keys_order_by_height_then_id() {
    let low = SortKey::new(1, &id(9));
    let high = SortKey::new(2, &id(0));
    assert!(low < high, "height dominates the ordering");

    let a = SortKey::new(1, &id(1));
    let b = SortKey::new(1, &id(2));
    assert!(a < b, "id breaks height ties");
}

#[test]
fn bounds_order_min_below_keys_below_max() {
    let key = Bound::Key(SortKey::new(0, &id(1)));
    assert!(Bound::Min < key);
    assert!(key < Bound::Max);
    assert!(Bound::Min < Bound::Max);
}

#[test]
fn seek_maps_sentinels_to_the_ends() {
    let src = source(&[(0, 1), (0, 2), (0, 3)]);
    assert_eq!(src.seek(&Bound::Min), 0);
    assert_eq!(src.seek(&Bound::Max), 3);
}

#[test]
fn seek_returns_the_lower_bound_insertion_point() {
    let src = source(&[(0, 1), (0, 3), (0, 5)]);
    assert_eq!(src.seek(&Bound::Key(SortKey::new(0, &id(1)))), 0);
    assert_eq!(src.seek(&Bound::Key(SortKey::new(0, &id(2)))), 1);
    assert_eq!(src.seek(&Bound::Key(SortKey::new(0, &id(3)))), 1);
    assert_eq!(src.seek(&Bound::Key(SortKey::new(0, &id(6)))), 3);
}

#[test]
fn seek_on_an_empty_source_is_zero() {
    let src = source(&[]);
    assert!(src.is_empty());
    assert_eq!(src.seek(&Bound::Min), 0);
    assert_eq!(src.seek(&Bound::Max), 0);
    assert_eq!(src.seek(&Bound::Key(SortKey::new(0, &id(1)))), 0);
}

#[test]
fn construction_sorts_regardless_of_insertion_order() {
    let src = source(&[(2, 7), (0, 9), (1, 1)]);
    assert_eq!(src.key(0), &SortKey::new(0, &id(9)));
    assert_eq!(src.key(1), &SortKey::new(1, &id(1)));
    assert_eq!(src.key(2), &SortKey::new(2, &id(7)));
    assert_eq!(src.id(0), &id(9));
}

#[test]
fn duplicate_sort_keys_are_rejected() {
    let items = vec![Item::new(1, id(4)), Item::new(1, id(4))];
    assert_eq!(MemorySource::new(items), Err(ReconcileError::DuplicateItem));
}
