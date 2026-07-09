#[test]
fn legacy_ltx_object_layout_is_owned_by_core() {
    use walrust::walrust_core::legacy_manifest::{
        build_ltx_key, format_ltx_filename, is_snapshot, parse_ltx_filename, GENERATION_LIVE,
    };

    assert_eq!(
        build_ltx_key("base", "db", GENERATION_LIVE, 2, 3),
        "base/db/0000/0000000000000002-0000000000000003.ltx"
    );
    assert_eq!(
        build_ltx_key("base/", "db", 1, 1, 42),
        "base/db/0001/0000000000000001-000000000000002a.ltx"
    );
    assert_eq!(
        format_ltx_filename(7, 9),
        "0000000000000007-0000000000000009.ltx"
    );
    assert_eq!(
        parse_ltx_filename("0000000000000007-0000000000000009.ltx"),
        Some((7, 9))
    );
    assert!(is_snapshot(1, 1, 42));
    assert!(is_snapshot(GENERATION_LIVE, 1, 1));
    assert!(!is_snapshot(GENERATION_LIVE, 2, 3));
}
