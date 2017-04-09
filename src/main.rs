use std::io;
use std::mem;
use std::ptr;
use std::ffi::{CString, CStr};

#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[derive(Debug)]
enum SQLite3Error {
    OpenError,
    StatementError,
    ExecuteError,
}

struct RawConnection {
    db: *mut ffi::sqlite3,
}

struct Statement {
    stmt: *mut ffi::sqlite3_stmt,
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_finalize(self.stmt);
        }
    }
}

impl Drop for RawConnection {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_close(self.db);
        }
    }
}

fn create_statement(conn: &RawConnection, query: String) -> Result<Statement, SQLite3Error> {
    let raw_query = CString::new(query).unwrap();

    let mut stmt: *mut ffi::sqlite3_stmt = unsafe { mem::uninitialized() };
    unsafe {
        let r =
            ffi::sqlite3_prepare_v2(conn.db, raw_query.as_ptr(), -1, &mut stmt, ptr::null_mut());
        if stmt.is_null() {
            println!("The statement is null!");
        }
        match r {
            ffi::SQLITE_OK => Ok(Statement { stmt: stmt }),
            x => {
                println!("Create error: {}", x);
                Err(SQLite3Error::StatementError)
            }
        }
    }
}

fn open_connection(path: String) -> Result<RawConnection, SQLite3Error> {
    let mut db: *mut ffi::sqlite3 = unsafe { mem::uninitialized() };
    let c_path = CString::new(path).unwrap();
    let r = unsafe {
        let ptr_path = c_path.as_ptr();
        ffi::sqlite3_open_v2(ptr_path,
                             &mut db,
                             ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_READWRITE,
                             ptr::null())
    };
    match r {
        ffi::SQLITE_OK => Ok(RawConnection { db: db }),
        x => {
            println!("Open error: {}", x);
            return Err(SQLite3Error::OpenError);
        }
    }
}

enum Cursor {
    OKCursor,
    DONECursor,
    RowsCursor {
        stmt: Statement,
        num_columns: i32,
        types: Vec<EntityType>,
        previous_status: i32,
    },
}

fn execute_statement(stmt: Statement) -> Result<Cursor, SQLite3Error> {

    match unsafe { ffi::sqlite3_step(stmt.stmt) } {
        ffi::SQLITE_OK => Ok(Cursor::OKCursor),
        ffi::SQLITE_DONE => Ok(Cursor::DONECursor),
        ffi::SQLITE_ROW => {
            let n_columns = unsafe { ffi::sqlite3_column_count(stmt.stmt) } as i32;
            let mut types: Vec<EntityType> = Vec::new();
            for i in 0..n_columns {
                types.push(match unsafe { ffi::sqlite3_column_type(stmt.stmt, i) } {
                    ffi::SQLITE_INTEGER => EntityType::Integer,
                    ffi::SQLITE_FLOAT => EntityType::Float,
                    ffi::SQLITE_TEXT => EntityType::Text,
                    ffi::SQLITE_BLOB => EntityType::Blob,
                    ffi::SQLITE_NULL => EntityType::Null,
                    _ => EntityType::Null,
                })
            }
            Ok(Cursor::RowsCursor {
                stmt: stmt,
                num_columns: n_columns,
                types: types,
                previous_status: ffi::SQLITE_ROW,
            })
        }
        x => {
            println!("Exec error: {}", x);
            return Err(SQLite3Error::ExecuteError);
        }
    }

}

enum EntityType {
    Integer,
    Float,
    Text,
    Blob,
    Null,
}

#[derive(Debug)]
enum Entity {
    Integer { int: i32 },
    Float { float: f64 },
    Text { text: String },
    Blob { blob: String },
    Null,
    OK,
    DONE,
}


type Row = Vec<Entity>;

impl Iterator for Cursor {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            Cursor::OKCursor => Some(vec![Entity::OK]),
            Cursor::DONECursor => Some(vec![Entity::DONE]),

            Cursor::RowsCursor { ref stmt, num_columns, ref types, ref mut previous_status } => {
                match *previous_status {
                    ffi::SQLITE_ROW => {
                        let mut result = vec![];
                        for i in 0..num_columns {
                            let entity_value = match types[i as usize] {
                                EntityType::Integer => {
                                    let value = unsafe { ffi::sqlite3_column_int(stmt.stmt, i) };
                                    Entity::Integer { int: value }
                                }
                                EntityType::Float => {
                                    let value = unsafe { ffi::sqlite3_column_double(stmt.stmt, i) };
                                    Entity::Float { float: value }
                                }
                                EntityType::Text => {
                                    let value =
                                unsafe {
                                    CStr::from_ptr(ffi::sqlite3_column_text(stmt.stmt, i) as *const i8).to_string_lossy().into_owned()
                                };
                                    Entity::Text { text: value }
                                }
                                EntityType::Blob => {
                                    let value = 
                                unsafe { 
                                    CStr::from_ptr(ffi::sqlite3_column_blob(stmt.stmt, i) as *const i8).to_string_lossy().into_owned() 
                                };
                                    Entity::Blob { blob: value }
                                }
                                EntityType::Null => Entity::Null {},
                            };
                            result.push(entity_value);
                        }
                        unsafe {
                            *previous_status = ffi::sqlite3_step(stmt.stmt);
                        };
                        Some(result)
                    }
                    _ => None,
                }
            }
        }
    }
}

fn main() {
    println!("Main!");
    let mut buff = String::new();
    println!("Open a new database: ");
    io::stdin().read_line(&mut buff).unwrap();
    println!("String read: {}", buff);
    buff = buff.trim().to_string();
    let to_print = CString::new(buff.clone()).unwrap();
    let conn = &open_connection(buff).unwrap();

    loop {
        let mut query = String::new();
        println!("Insert your query: ");
        io::stdin().read_line(&mut query).expect("Could not read line!");

        let stmt_option = create_statement(conn, query.clone());
        match stmt_option {
            Ok(stmt) => {
                match execute_statement(stmt) {
                    Err(_) => println!("Error"),
                    Ok(cursor) => {
                        match cursor {
                            Cursor::OKCursor => println!("OK"),
                            Cursor::DONECursor => println!("DONE"),
                            Cursor::RowsCursor { stmt: _,
                                                 num_columns: _,
                                                 types: _,
                                                 previous_status: _ } => {
                                let rows = cursor.collect::<Vec<_>>();
                                println!("{:?}", rows);
                            }
                        }
                    }
                }
            }
            Err(err) => println!("{:?}", err),
        }
    }
}
