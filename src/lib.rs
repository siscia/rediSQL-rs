extern crate libc;

use std::ffi::{CString, CStr};
use std::string;

use std::thread;
use std::sync::mpsc::{Receiver, RecvError, channel, Sender};
use std::sync::Arc;

#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(improper_ctypes)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/bindings_redis.rs"));
}

mod sqlite;
use sqlite as sql;


trait RedisReply {
    fn reply(&self, ctx: *mut ffi::RedisModuleCtx);
}

impl RedisReply for sql::Entity {
    fn reply(&self, ctx: *mut ffi::RedisModuleCtx) {
        unsafe {
            match *self {
                sql::Entity::Integer { int } => {
                    ffi::RedisModule_ReplyWithLongLong.unwrap()(ctx,
                                                                int as i64);
                }
                sql::Entity::Float { float } => {
                    ffi::RedisModule_ReplyWithDouble.unwrap()(ctx, float);
                }
                sql::Entity::Text { ref text } => {
                    let text_c = CString::new(text.clone()).unwrap();
                    ffi::RedisModule_ReplyWithStringBuffer.unwrap()(ctx, text_c.as_ptr(), text.len());
                }
                sql::Entity::Blob { ref blob } => {
                    let blob_c = CString::new(blob.clone()).unwrap();
                    ffi::RedisModule_ReplyWithStringBuffer.unwrap()(ctx, blob_c.as_ptr(), blob.len());
                }
                sql::Entity::Null => {
                    ffi::RedisModule_ReplyWithNull.unwrap()(ctx);
                }
                sql::Entity::OK => {
                    let ok = String::from("OK");
                    let ok_c = CString::new(ok.clone()).unwrap();
                    ffi::RedisModule_ReplyWithStringBuffer.unwrap()(ctx, ok_c.as_ptr(), ok.len());
                }                
                sql::Entity::DONE => {
                    let done = String::from("DONE");
                    let done_c = CString::new(done.clone()).unwrap();
                    ffi::RedisModule_ReplyWithStringBuffer.unwrap()(ctx,
                                                                    done_c.as_ptr(),
                                                                    done.len());
                }
            }
        }
    }
}

#[allow(dead_code)]
struct Context {
    ctx: *mut ffi::RedisModuleCtx,
}


fn create_argument(ctx: *mut ffi::RedisModuleCtx,
                   argv: *mut *mut ffi::RedisModuleString,
                   argc: i32)
                   -> (Context, Vec<String>) {
    let context = Context { ctx: ctx };
    let argvector = parse_args(argv, argc).unwrap();
    (context, argvector)
}

struct RedisModuleString {
    rm_string: *mut ffi::RedisModuleString,
}

fn create_rm_string(ctx: *mut ffi::RedisModuleCtx,
                    s: String)
                    -> RedisModuleString {
    let l = s.len();
    let cs = CString::new(s).unwrap();


    RedisModuleString {
        rm_string: unsafe {
            ffi::RedisModule_CreateString.unwrap()(ctx, cs.as_ptr(), l)
        },
    }
}

#[repr(C)]
struct RedisKey {
    key: *mut ffi::RedisModuleKey,
}

impl Drop for RedisKey {
    fn drop(&mut self) {
        println!("Key closed");
        unsafe {
            ffi::RedisModule_CloseKey.unwrap()(self.key);
        }
    }
}

#[allow(non_snake_case)]
extern "C" fn DeleteDB(ctx: *mut ffi::RedisModuleCtx,
                       argv: *mut *mut ffi::RedisModuleString,
                       argc: ::std::os::raw::c_int)
                       -> i32 {
    let (_context, argvector) = create_argument(ctx, argv, argc);
    match argvector.len() {
        2 => {
            let key_name = create_rm_string(ctx, argvector[1].clone());
            let key = unsafe {
                ffi::Export_RedisModule_OpenKey(ctx,
                                                key_name.rm_string,
                                                ffi::REDISMODULE_WRITE)
            };
            let safe_key = RedisKey { key: key };
            let key_type = unsafe { ffi::RedisModule_KeyType.unwrap()(key) };
            if unsafe {
                ffi::DBType ==
                ffi::RedisModule_ModuleTypeGetType.unwrap()(safe_key.key) &&
                key_type != ffi::REDISMODULE_KEYTYPE_EMPTY
            } {

                let db_ptr = unsafe {
                    ffi::RedisModule_ModuleTypeGetValue.unwrap()(safe_key.key) as *mut DBKey
                };


                let _db: Box<DBKey> = unsafe { Box::from_raw(db_ptr) };

                unsafe {
                    ffi::RedisModule_DeleteKey.unwrap()(safe_key.key);
                }

                let ok = CString::new("OK").unwrap();
                unsafe {
                    ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, ok.as_ptr())
                }
            } else {
                match key_type {
                    ffi::REDISMODULE_KEYTYPE_EMPTY => {
                        let error = CString::new("ERR - Error the key is \
                                                  empty")
                            .unwrap();
                        unsafe {
                        ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                    }
                    }
                    _ => {
                        let error = CStr::from_bytes_with_nul(ffi::REDISMODULE_ERRORMSG_WRONGTYPE).unwrap();
                        unsafe {
                        ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                    }
                    }

                }

            }

        }
        _ => unsafe { ffi::RedisModule_WrongArity.unwrap()(ctx) },
    }
}


enum Command {
    Stop,
    Exec {
        query: String, /* client: ffi::RedisModuleBlockedClient,
                        * db: Arc<sql::RawConnection>, */
    },
}

fn listen_and_execute(db: sql::RawConnection, rx: Receiver<Command>) {
    loop {
        match rx.recv() {
            Ok(Command::Exec { query }) => {
                println!("Inside long running function receive query: {}",
                         query)
            }
            Ok(Command::Stop) => return,
            Err(RecvError) => return,
        }
    }
}

#[allow(non_snake_case)]
extern "C" fn Exec(ctx: *mut ffi::RedisModuleCtx,
                   argv: *mut *mut ffi::RedisModuleString,
                   argc: ::std::os::raw::c_int)
                   -> i32 {
    let (_context, argvector) = create_argument(ctx, argv, argc);

    match argvector.len() {
        3 => {
            let key_name = create_rm_string(ctx, argvector[1].clone());
            let key = unsafe {
                ffi::Export_RedisModule_OpenKey(ctx,
                                                key_name.rm_string,
                                                ffi::REDISMODULE_WRITE)
            };
            let safe_key = RedisKey { key: key };
            let key_type =
                unsafe { ffi::RedisModule_KeyType.unwrap()(safe_key.key) };
            if unsafe {
                ffi::DBType ==
                ffi::RedisModule_ModuleTypeGetType.unwrap()(safe_key.key)
            } {
                println!("Get the type ok!");

                let db_ptr = unsafe {
                    ffi::RedisModule_ModuleTypeGetValue.unwrap()(safe_key.key) as *mut DBKey
                };

                let mut db: Box<DBKey> = unsafe { Box::from_raw(db_ptr) };

                let ch = db.tx.clone();

                let cmd = Command::Exec { query: argvector[2].clone() };

                //               ch.send(cmd);

                // return ffi::REDISMODULE_OK;
/*
                if db.db.is_null() {
                    println!("Empty db!");
                }
*/




                match sql::create_statement(&mut db.db, argvector[2].clone()) {
                    Ok(stmt) => {


                        // mem::forget(db);

                        Box::into_raw(db);

                        match sql::execute_statement(stmt) {
                            Ok(cursor) => {
                                match cursor {
                                    sql::Cursor::OKCursor => {
                                        let ok = CString::new("OK").unwrap();
                                        unsafe {
                                            ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, ok.as_ptr())
                                        }
                                    }
                                    sql::Cursor::DONECursor => {
                                        let done = CString::new("DONE")
                                            .unwrap();
                                        unsafe {
                                            ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, done.as_ptr())
                                        }
                                    }
                                    sql::Cursor::RowsCursor { .. } => {
                                        let result =
                                            cursor.collect::<Vec<Vec<sql::Entity>>>();
                                        unsafe {
                                            ffi::RedisModule_ReplyWithArray.unwrap()(ctx, result.len() as i64);
                                        }
                                        for row in result {
                                            unsafe {
                                                ffi::RedisModule_ReplyWithArray.unwrap()(ctx, row.len() as i64);
                                            }
                                            for entity in row {
                                                entity.reply(ctx);
                                            }
                                        }

                                        ffi::REDISMODULE_OK
                                    }
                                }
                            }
                            Err(_) => {
                                let error = CString::new("ERR - Error, the \
                                                          statement to \
                                                          executed gave \
                                                          some problem")
                                    .unwrap();
                                unsafe {
                            ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                        }
                            }
                        }
                    }
                    Err(_) => {


                        Box::into_raw(db);

                        let error = CString::new("ERR - Error, was \
                                                  impossible to create the \
                                                  statement")
                            .unwrap();
                        unsafe {
                            ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                        }
                    }
                }
            } else {
                match key_type {
                    ffi::REDISMODULE_KEYTYPE_EMPTY => {
                        let error = CString::new("ERR - Error the key is \
                                                  empty")
                            .unwrap();
                        unsafe {
                        ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                    }
                    }
                    _ => {
                        let error = CStr::from_bytes_with_nul(ffi::REDISMODULE_ERRORMSG_WRONGTYPE).unwrap();
                        unsafe {
                        ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                    }
                    }
                }

            }
        }
        _ => {
            let error = CString::new("Wrong number of arguments, it accepts \
                                      3")
                .unwrap();
            unsafe {
                ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
            }
        }
    }
}

struct DBKey {
    tx: Sender<Command>,
    db: sql::RawConnection,
}

#[allow(non_snake_case)]
extern "C" fn CreateDB(ctx: *mut ffi::RedisModuleCtx,
                       argv: *mut *mut ffi::RedisModuleString,
                       argc: ::std::os::raw::c_int)
                       -> i32 {

    let (_context, argvector) = create_argument(ctx, argv, argc);

    match argvector.len() {
        2 => {
            let key_name = create_rm_string(ctx, argvector[1].clone());
            let key = unsafe {
                ffi::Export_RedisModule_OpenKey(ctx,
                                                key_name.rm_string,
                                                ffi::REDISMODULE_WRITE)
            };
            let safe_key = RedisKey { key: key };
            match unsafe { ffi::RedisModule_KeyType.unwrap()(safe_key.key) } {

                ffi::REDISMODULE_KEYTYPE_EMPTY => {

                    println!("Open the empty key!");

                    match sql::open_connection(String::from(":memory:")) {
                        Ok(rc) => {

                            let (tx, rx) = channel();
                            let db = DBKey {
                                tx: tx,
                                db: rc.clone(),
                            };
                            /*
                            thread::spawn(|| {
                                listen_and_execute(rc, rx);
                            });
*/

                            println!("Open the database");
                            let ptr = Box::into_raw(Box::new(db));
                            let type_set = unsafe {
                                ffi::RedisModule_ModuleTypeSetValue.unwrap()(safe_key.key, ffi::DBType, ptr as *mut std::os::raw::c_void)
                            };
                            match type_set {
                                ffi::REDISMODULE_OK => {
                                    let ok = CString::new("OK").unwrap();
                                    unsafe {
                                        ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, ok.as_ptr())
                                    }
                                }
                                ffi::REDISMODULE_ERR => {
                                    let err = CString::new("ERR - Error in \
                                                            saving the \
                                                            database inside \
                                                            Redis")
                                        .unwrap();

                                    unsafe {
                                        ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, err.as_ptr())
                                    }
                                }
                                _ => {
                                    let err = CString::new("ERR - Error \
                                                            unknow")
                                        .unwrap();

                                    unsafe {
                                        ffi::RedisModule_ReplyWithSimpleString.unwrap()(ctx, err.as_ptr())
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            let error = CString::new("Err - Error \
                                                      opening the in \
                                                      memory databade")
                                .unwrap();
                            unsafe { ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr()) }
                        }
                    }
                }

                _ => {
                    let error = CStr::from_bytes_with_nul(ffi::REDISMODULE_ERRORMSG_WRONGTYPE)
                        .unwrap();
                    unsafe {
                        ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
                    }
                }
            }

        }
        _ => {
            println!("Wrong number of arguments");
            let error = CString::new("Wrong number of arguments, it accepts \
                                      2")
                .unwrap();
            unsafe {
                ffi::RedisModule_ReplyWithError.unwrap()(ctx, error.as_ptr())
            }
        }
    }

}

fn parse_args(argv: *mut *mut ffi::RedisModuleString,
              argc: i32)
              -> Result<Vec<String>, string::FromUtf8Error> {
    let mut args: Vec<String> = Vec::with_capacity(argc as usize);
    for i in 0..argc {
        let redis_str = unsafe { *argv.offset(i as isize) };
        args.push(string_ptr_len(redis_str));
    }
    Ok(args)
}

pub fn string_ptr_len(str: *mut ffi::RedisModuleString) -> String {
    unsafe {
        CStr::from_ptr(ffi::RedisModule_StringPtrLen.unwrap()(str, std::ptr::null_mut()))
            .to_string_lossy()
            .into_owned()
    }
}

unsafe extern "C" fn free_db(_: *mut ::std::os::raw::c_void) {
    println!("Call free");
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn RedisModule_OnLoad(ctx: *mut ffi::RedisModuleCtx,
                                     _argv: *mut *mut ffi::RedisModuleString,
                                     _argc: i32)
                                     -> i32 {

    println!("Starting!");


    let c_data_type_name = CString::new("rediSQLDB").unwrap();
    let ptr_data_type_name = c_data_type_name.as_ptr();

    let mut types = ffi::RedisModuleTypeMethods {
        version: 1,
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,
        mem_usage: None,
        digest: None,
        free: Some(free_db),
    };

    let module_c_name = CString::new("helloworld").unwrap();
    let module_ptr_name = module_c_name.as_ptr();
    if unsafe {
        ffi::Export_RedisModule_Init(ctx,
                                     module_ptr_name,
                                     1,
                                     ffi::REDISMODULE_APIVER_1)
    } == ffi::REDISMODULE_ERR {
        println!("Error in Init");
        return ffi::REDISMODULE_ERR;
    }

    println!("About to register the type!");

    unsafe {
        ffi::DBType =
            ffi::RedisModule_CreateDataType.unwrap()(ctx,
                                                     ptr_data_type_name,
                                                     1,
                                                     &mut types);
    }

    println!("Just created the type!");

    if unsafe { ffi::DBType } == std::ptr::null_mut() {
        println!("Error in Creating the types");
        return ffi::REDISMODULE_ERR;
    }

    let create_db: ffi::RedisModuleCmdFunc = Some(CreateDB);

    let command_c_name = CString::new("REDISQL.CREATE_DB").unwrap();
    let command_ptr_name = command_c_name.as_ptr();

    let flag_c_name = CString::new("write").unwrap();
    let flag_ptr_name = flag_c_name.as_ptr();

    if unsafe {
        ffi::RedisModule_CreateCommand.unwrap()(ctx,
                                                command_ptr_name,
                                                create_db,
                                                flag_ptr_name,
                                                0,
                                                0,
                                                0)
    } == ffi::REDISMODULE_ERR {
        println!("Error in CreateCommand");
        return ffi::REDISMODULE_ERR;
    }


    let remove_db: ffi::RedisModuleCmdFunc = Some(DeleteDB);

    let command_c_name = CString::new("REDISQL.Delete_DB").unwrap();
    let command_ptr_name = command_c_name.as_ptr();

    let flag_c_name = CString::new("write").unwrap();
    let flag_ptr_name = flag_c_name.as_ptr();

    if unsafe {
        ffi::RedisModule_CreateCommand.unwrap()(ctx,
                                                command_ptr_name,
                                                remove_db,
                                                flag_ptr_name,
                                                0,
                                                0,
                                                0)
    } == ffi::REDISMODULE_ERR {
        println!("Error in CreateCommand");
        return ffi::REDISMODULE_ERR;
    }





    let exec: ffi::RedisModuleCmdFunc = Some(Exec);

    let command_c_name = CString::new("REDISQL.EXEC").unwrap();
    let command_ptr_name = command_c_name.as_ptr();

    let flag_c_name = CString::new("write").unwrap();
    let flag_ptr_name = flag_c_name.as_ptr();

    if unsafe {
        ffi::RedisModule_CreateCommand.unwrap()(ctx,
                                                command_ptr_name,
                                                exec,
                                                flag_ptr_name,
                                                0,
                                                0,
                                                0)
    } == ffi::REDISMODULE_ERR {
        println!("Error in CreateCommand");
        return ffi::REDISMODULE_ERR;
    }
    ffi::REDISMODULE_OK
}
