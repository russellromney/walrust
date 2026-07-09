use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn legacy_ltx_codec_is_owned_by_core_and_round_trips_real_sqlite() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("legacy-source.db");
    let restored_path = dir.path().join("legacy-restored.db");

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "
        PRAGMA page_size=4096;
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO items (id, value) VALUES (1, 'base');
        INSERT INTO items (id, value) VALUES (2, 'wal-resident');
        ",
    )
    .unwrap();

    let (encoded, encoded_checksum) =
        walrust::ltx::encode_sqlite_snapshot_to_vec(&db_path, 4096, 1).unwrap();
    let decoded = walrust::walrust_core::legacy_ltx::decode_to_db(
        std::io::Cursor::new(encoded),
        &restored_path,
    )
    .unwrap();

    let restored = Connection::open(&restored_path).unwrap();
    let integrity: String = restored
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    let count: i64 = restored
        .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
        .unwrap();

    assert_eq!(integrity, "ok");
    assert_eq!(count, 2);
    assert_eq!(encoded_checksum, decoded.post_apply_checksum);
}
