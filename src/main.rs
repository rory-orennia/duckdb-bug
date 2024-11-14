// Basic example copy from README
extern crate skia_safe;

mod canvas;
use canvas::Canvas;

use std::fs::File;
use std::io::Write;

extern crate duckdb;

use duckdb::{
    arrow::{record_batch::RecordBatch, util::pretty::print_batches},
    params, Connection, Result,
};

#[derive(Debug)]
struct Person {
    _id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn main() -> Result<()> {
    // Copied from https://github.com/rust-skia/rust-skia/tree/master/skia-safe/examples/hello
    let mut canvas = Canvas::new(2560, 1280);
    canvas.scale(1.2, 1.2);
    canvas.move_to(36.0, 48.0);
    canvas.quad_to(660.0, 880.0, 1200.0, 360.0);
    canvas.translate(10.0, 10.0);
    canvas.set_line_width(20.0);
    canvas.stroke();
    canvas.save();
    canvas.move_to(30.0, 90.0);
    canvas.line_to(110.0, 20.0);
    canvas.line_to(240.0, 130.0);
    canvas.line_to(60.0, 130.0);
    canvas.line_to(190.0, 20.0);
    canvas.line_to(270.0, 90.0);
    canvas.fill();
    let d = canvas.data();
    let mut file = File::create("test.png").unwrap();
    let bytes = d.as_bytes();
    file.write_all(bytes).unwrap();

    // Copied from https://github.com/duckdb/duckdb-rs/blob/main/crates/duckdb/examples/basic.rs
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
        ",
    )?;

    let me = Person {
        _id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    // query table by rows
    // HERE IS THE ERROR - changed name to named to make it miss.  Should return a nice error from duckdb but instead we panic
    let mut stmt = conn.prepare("SELECT id, named, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            _id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }

    // query table by arrow
    let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    print_batches(&rbs).unwrap();
    Ok(())
}
