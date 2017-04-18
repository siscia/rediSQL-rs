
use std::mem;
use std::ptr;
use std::ffi::{CString, CStr};
use std::clone::Clone;

use std::sync;

#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/bindings_sqlite.rs"));
}

pub enum SQLite3Error {
    OpenError,
    StatementError,
    ExecuteError,
    SharedConnection,
}

#[derive(Clone)]
pub struct RawConnection {
    db: ffi::sqlite3,
}

pub struct Statement {
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
            ffi::sqlite3_close(&mut self.db);
        }
    }
}


pub fn create_statement(conn: &mut RawConnection,
                        query: String)
                        -> Result<Statement, SQLite3Error> {

    println!("Query from create_statement: {}", query);

    let raw_query = CString::new(query).unwrap();

    let mut stmt: *mut ffi::sqlite3_stmt = unsafe { mem::uninitialized() };
    let mut db = conn.db;
    let r = unsafe {
        ffi::sqlite3_prepare_v2(&mut db,
                                raw_query.as_ptr(),
                                -1,
                                &mut stmt,
                                ptr::null_mut())
    };
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

pub fn open_connection(path: String) -> Result<RawConnection, SQLite3Error> {
    let mut db: *mut ffi::sqlite3 = unsafe { mem::uninitialized() };
    let c_path = CString::new(path).unwrap();
    let r = unsafe {
        let ptr_path = c_path.as_ptr();
        ffi::sqlite3_open_v2(ptr_path,
                             &mut db,
                             ffi::SQLITE_OPEN_CREATE |
                             ffi::SQLITE_OPEN_READWRITE,
                             ptr::null())
    };
    match r {
        ffi::SQLITE_OK => Ok(RawConnection { db: unsafe { *db } }),
        x => {
            println!("Open error: {}", x);
            return Err(SQLite3Error::OpenError);
        }
    }
}

pub enum Cursor {
    OKCursor,
    DONECursor,
    RowsCursor {
        stmt: Statement,
        num_columns: i32,
        types: Vec<EntityType>,
        previous_status: i32,
    },
}

pub fn execute_statement(stmt: Statement) -> Result<Cursor, SQLite3Error> {

    match unsafe { ffi::sqlite3_step(stmt.stmt) } {
        ffi::SQLITE_OK => Ok(Cursor::OKCursor),
        ffi::SQLITE_DONE => Ok(Cursor::DONECursor),
        ffi::SQLITE_ROW => {
            let n_columns =
                unsafe { ffi::sqlite3_column_count(stmt.stmt) } as i32;
            let mut types: Vec<EntityType> = Vec::new();
            for i in 0..n_columns {
                types.push(match unsafe {
                    ffi::sqlite3_column_type(stmt.stmt, i)
                } {
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

pub enum EntityType {
    Integer,
    Float,
    Text,
    Blob,
    Null,
}

pub enum Entity {
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

            Cursor::RowsCursor { ref stmt,
                                 num_columns,
                                 ref types,
                                 ref mut previous_status } => {
                match *previous_status {
                    ffi::SQLITE_ROW => {
                        let mut result = vec![];
                        for i in 0..num_columns {
                            let entity_value =
                                match types[i as usize] {
                                    EntityType::Integer => {
                                        let value =
                                            unsafe {
                                                ffi::sqlite3_column_int(stmt.stmt, i)
                                            };
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


#[repr(C)]
pub struct db_connection {
    pub connection: RawConnection,
}
