//! Create one native HADBP snapshot from a SQLite database.

use std::env;
use std::io::Write;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <db_path> <hadbp_path>", args[0]);
        std::process::exit(1);
    }

    let db_path = Path::new(&args[1]);
    let hadbp_path = Path::new(&args[2]);

    let payload = walrust::ltx::encode_snapshot(db_path, 4096, 1, 0)?;
    let mut output = std::fs::File::create(hadbp_path)?;
    output.write_all(&payload)?;
    output.sync_all()?;

    println!("Created HADBP snapshot: {}", hadbp_path.display());
    Ok(())
}
