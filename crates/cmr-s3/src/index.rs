use std::{path::Path, sync::Mutex};

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Result,
    metadata::{Bucket, Object, Upload},
};

pub(crate) struct Index(Mutex<Connection>);

impl Index {
    pub fn open(path: &Path, identity: &str) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS identity (value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS objects (
                bucket TEXT NOT NULL, key TEXT NOT NULL COLLATE BINARY, json TEXT NOT NULL,
                PRIMARY KEY(bucket, key));
             CREATE TABLE IF NOT EXISTS uploads (id TEXT PRIMARY KEY, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS layouts (id TEXT PRIMARY KEY, json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS catalog_state (ready INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS pending (path TEXT PRIMARY KEY, bucket TEXT NOT NULL, key TEXT);",
        )?;
        let old: Option<String> = connection
            .query_row("SELECT value FROM identity", [], |r| r.get(0))
            .optional()?;
        if old.as_deref().is_some_and(|old| old != identity) {
            return Err(crate::Error::Configuration(
                "the state directory belongs to another storage root or key".into(),
            ));
        }
        if old.is_none() {
            connection.execute("INSERT INTO identity VALUES (?1)", [identity])?;
        }
        Ok(Self(Mutex::new(connection)))
    }

    pub fn clear_catalog(&self) -> Result<()> {
        self.0.lock().unwrap().execute_batch("BEGIN IMMEDIATE; DELETE FROM objects; DELETE FROM buckets; DELETE FROM layouts; COMMIT;")?;
        Ok(())
    }

    pub fn bucket(&self, name: &str) -> Result<Option<Bucket>> {
        self.get("SELECT json FROM buckets WHERE name=?1", [name])
    }

    pub fn ready(&self) -> Result<bool> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .query_row("SELECT ready FROM catalog_state", [], |r| {
                r.get::<_, bool>(0)
            })
            .optional()?
            .unwrap_or(false))
    }

    pub fn set_ready(&self, ready: bool) -> Result<()> {
        let mut connection = self.0.lock().unwrap();
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM catalog_state", [])?;
        transaction.execute("INSERT INTO catalog_state VALUES (?1)", [ready])?;
        if ready {
            transaction.execute("DELETE FROM pending", [])?;
        }
        transaction.commit()?;
        Ok(())
    }

    // Persist intent before touching a mutable cloud record. Null key identifies a bucket.
    pub fn begin(&self, path: &str, bucket: &str, key: Option<&str>) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT OR REPLACE INTO pending VALUES (?1,?2,?3)",
            params![path, bucket, key],
        )?;
        Ok(())
    }

    pub fn pending(&self) -> Result<Vec<(String, String, Option<String>)>> {
        let connection = self.0.lock().unwrap();
        let mut statement =
            connection.prepare("SELECT path,bucket,key FROM pending ORDER BY path")?;
        Ok(statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<std::result::Result<_, _>>()?)
    }

    pub fn finish(&self, path: &str) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .execute("DELETE FROM pending WHERE path=?1", [path])?;
        Ok(())
    }

    pub fn buckets(&self) -> Result<Vec<Bucket>> {
        self.query("SELECT json FROM buckets ORDER BY name", [])
    }

    pub fn save_bucket(&self, bucket: &Bucket) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT OR REPLACE INTO buckets VALUES (?1, ?2)",
            params![bucket.name, serde_json::to_string(bucket)?],
        )?;
        Ok(())
    }

    pub fn remove_bucket(&self, name: &str) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .execute("DELETE FROM buckets WHERE name=?1", [name])?;
        Ok(())
    }

    pub fn object(&self, bucket: &str, key: &str) -> Result<Option<Object>> {
        self.get(
            "SELECT json FROM objects WHERE bucket=?1 AND key=?2",
            [bucket, key],
        )
    }

    pub fn save_object(&self, object: &Object) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT OR REPLACE INTO objects VALUES (?1, ?2, ?3)",
            params![object.bucket, object.key, serde_json::to_string(object)?],
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_object_writes(&self) -> Result<()> {
        // A real SQLite failure, scoped to this connection so restart needs no repair.
        self.0.lock().unwrap().execute_batch(
            "CREATE TEMP TRIGGER fail_object_writes BEFORE INSERT ON objects
             BEGIN SELECT RAISE(FAIL, 'injected index failure'); END;",
        )?;
        Ok(())
    }

    pub fn remove_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.0.lock().unwrap().execute(
            "DELETE FROM objects WHERE bucket=?1 AND key=?2",
            [bucket, key],
        )?;
        Ok(())
    }

    // Keyset pages keep both memory and database work bounded for large listings.
    pub fn object_page(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Object>> {
        self.query(
            "SELECT json FROM objects WHERE bucket=?1 AND key>?2 AND key>=?3 AND substr(CAST(key AS BLOB),1,length(CAST(?3 AS BLOB)))=CAST(?3 AS BLOB) ORDER BY key LIMIT ?4",
            params![bucket, after, prefix, limit as i64],
        )
    }

    pub fn roots(&self) -> Result<Vec<String>> {
        let connection = self.0.lock().unwrap();
        let mut statement =
            connection.prepare("SELECT json_extract(json,'$.content') FROM objects")?;
        Ok(statement
            .query_map([], |row| row.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }

    pub fn upload(&self, id: &str) -> Result<Option<Upload>> {
        self.get("SELECT json FROM uploads WHERE id=?1", [id])
    }

    pub fn uploads(&self) -> Result<Vec<Upload>> {
        self.query(
            "SELECT json FROM uploads ORDER BY json_extract(json,'$.key'),id",
            [],
        )
    }

    pub fn save_upload(&self, upload: &Upload) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT OR REPLACE INTO uploads VALUES (?1, ?2)",
            params![upload.id, serde_json::to_string(upload)?],
        )?;
        Ok(())
    }

    pub fn remove_upload(&self, id: &str) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .execute("DELETE FROM uploads WHERE id=?1", [id])?;
        Ok(())
    }

    pub fn layout<T: DeserializeOwned>(&self, id: &str) -> Result<Option<T>> {
        self.get("SELECT json FROM layouts WHERE id=?1", [id])
    }

    pub fn save_layout<T: Serialize>(&self, id: &str, value: &T) -> Result<()> {
        self.0.lock().unwrap().execute(
            "INSERT OR REPLACE INTO layouts VALUES (?1, ?2)",
            params![id, serde_json::to_string(value)?],
        )?;
        Ok(())
    }

    pub fn remove_layout(&self, id: &str) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .execute("DELETE FROM layouts WHERE id=?1", [id])?;
        Ok(())
    }

    fn get<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Option<T>> {
        let json: Option<String> = self
            .0
            .lock()
            .unwrap()
            .query_row(sql, params, |r| r.get(0))
            .optional()?;
        Ok(json.map(|json| serde_json::from_str(&json)).transpose()?)
    }

    fn query<T: DeserializeOwned>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<T>> {
        let connection = self.0.lock().unwrap();
        let mut statement = connection.prepare(sql)?;
        statement
            .query_map(params, |r| r.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str(&row?)?))
            .collect()
    }
}
