use rusqlite::OptionalExtension;
use serde::{Serialize, Deserialize};
use std::fmt::Debug;

pub trait Field: Serialize + for<'a> Deserialize <'a> + Default + Debug {
    fn key() -> String;
}

pub trait KeyValueStore {
    type Error;
    fn set<
        K: Serialize + for<'a> Deserialize <'a>,
        V: Serialize + for<'a> Deserialize <'a>,
    >(&mut self, key: &K, value: &V) -> Result<(), Self::Error>;
        
    fn get<
        V: Serialize + for<'a> Deserialize <'a>,
        K: Serialize + for<'a> Deserialize <'a>,
    >(&self, key: &K) -> Result<Option<V>, Self::Error>;

    fn set_field<F: Field + 'static>(&mut self, item: &F) -> Result<(), Self::Error>;
    fn get_field<F: Field + 'static>(&self) -> Result<F, Self::Error>;
}

impl KeyValueStore for rusqlite::Connection {
    type Error = Error;

    fn set<
        K: Serialize + for<'a> Deserialize <'a>,
        V: Serialize + for<'a> Deserialize <'a>,
    >(&mut self, key: &K, value: &V) -> Result<(), Self::Error> {
        self.execute("CREATE TABLE if not exists kvs(key TEXT NOT NULL UNIQUE, value TEXT);", [])?;
        self.execute(
            "INSERT INTO kvs(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
            [hex::encode(serde_json::to_vec(&key)?), hex::encode(&serde_json::to_vec(&value)?)]
        )?;
        Ok(())
    }
        
    fn get<
        V: Serialize + for<'a> Deserialize <'a>,
        K: Serialize + for<'a> Deserialize <'a>,
    >(&self, key: &K) -> Result<Option<V>, Self::Error> {
        self.execute("CREATE TABLE if not exists kvs(key TEXT NOT NULL UNIQUE, value TEXT);", [])?;
        self.query_row_and_then(
            &format!("SELECT value FROM kvs where key = \'{}\'", hex::encode(serde_json::to_vec(&key)?)), [],
            |row| row.get(0).optional()?.map(|row: String| Ok::<V, Self::Error>(serde_json::from_slice(&hex::decode(row)?)?)).transpose()
        )
    }

    fn set_field<F: Field + 'static>(&mut self, item: &F) -> Result<(), Self::Error> {self.set(&F::key(), item)}
    fn get_field<F: Field + 'static>(&self) -> Result<F, Self::Error> {Ok(self.get(&F::key())?.unwrap_or_default())}
}

#[derive(Debug)]
pub enum Error{
    Hex(hex::FromHexError),
    Rusqlite(rusqlite::Error),
    SerdeJson(serde_json::Error),
}

impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<hex::FromHexError> for Error {fn from(error: hex::FromHexError) -> Error {Error::Hex(error)}}
impl From<rusqlite::Error> for Error {fn from(error: rusqlite::Error) -> Error {Error::Rusqlite(error)}}
impl From<serde_json::Error> for Error {fn from(error: serde_json::Error) -> Error {Error::SerdeJson(error)}}
