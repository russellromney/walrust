// Simple tool to create an LTX file from a database
use std::env;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <db_path> <ltx_path>", args[0]);
        std::process::exit(1);
    }

    let db_path = Path::new(&args[1]);
    let ltx_path = Path::new(&args[2]);

    let output = std::fs::File::create(ltx_path)?;
    walrust::ltx::encode_snapshot(output, db_path, 4096, 1)?;

    println!("Created LTX file: {}", ltx_path.display());
    Ok(())
}
