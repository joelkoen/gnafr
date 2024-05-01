use std::{
    collections::HashMap,
    fs::{self, remove_file, File},
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{bail, Result};
use rusqlite::{params, Connection};

fn main() -> Result<()> {
    let reader = BufReader::new(zstd::Decoder::new(File::open(
        "../postcodes/gnaf-core.psv.zst",
    )?)?);

    let path = Path::new("gnafr.db");
    if path.exists() {
        remove_file(path)?;
    }
    let mut db = Connection::open(path)?;
    db.execute_batch(include_str!("../db.sql"))?;

    let tx = db.transaction()?;
    {
        let mut stmt = tx.prepare("insert into address (id, display, site_name, building_name, flat_type, flat_number, level_type, level_number, number_first, number_last, lot_number, street_name, street_type, street_suffix, locality, state, postcode, alias_of, child_of, latitude, longitude) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        for (i, result) in reader.lines().enumerate().skip(1) {
            let line = result?;
            if (i % 1_000_000) == 0 {
                eprintln!("{i}");
            }

            let fields: Vec<_> = line.split('|').collect();
            assert!(fields.len() == 27);

            let id = e(fields[0]).unwrap();
            let display = e(fields[2]).unwrap();
            let site_name = e(fields[3]);
            let building_name = e(fields[4]);
            let flat_type = e(fields[5]);
            let flat_number = e(fields[6]);
            let level_type = e(fields[7]);
            let level_number = e(fields[8]);
            let number_first = e(fields[9]);
            let number_last = e(fields[10]);
            let lot_number = e(fields[11]);
            let street_name = e(fields[12]).unwrap();
            let street_type = e(fields[13]);
            let street_suffix = e(fields[14]);
            let locality = e(fields[15]).unwrap();
            let state = e(fields[16]).unwrap();
            let postcode = e(fields[17]).unwrap();
            let alias_of = e(fields[21]);
            let child_of = e(fields[23]);
            let latitude: f64 = fields[26].parse()?;
            let longitude: f64 = fields[25].parse()?;
            stmt.execute(params!(
                id,
                display,
                site_name,
                building_name,
                flat_type,
                flat_number,
                level_type,
                level_number,
                number_first,
                number_last,
                lot_number,
                street_name,
                street_type,
                street_suffix,
                locality,
                state,
                postcode,
                alias_of,
                child_of,
                latitude,
                longitude,
            ))?;
        }
    }
    eprintln!("committing");
    tx.commit()?;

    eprintln!("indexing ids");
    db.execute("create unique index address_ids on address (id)", ())?;
    eprintln!("indexing coords");
    db.execute(
        "create index address_coords on address (latitude, longitude)",
        (),
    )?;
    eprintln!("indexing aliases");
    db.execute("create index address_aliases on address (alias_of)", ())?;
    eprintln!("indexing children");
    db.execute("create index address_children on address (child_of)", ())?;

    Ok(())
}

fn e(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}
