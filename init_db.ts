import Database from "better-sqlite3";

const db = new Database("sefin_audit.db");

console.log("Creating users table...");
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    openId TEXT NOT NULL UNIQUE,
    name TEXT,
    email TEXT,
    loginMethod TEXT,
    role TEXT NOT NULL DEFAULT 'user',
    createdAt INTEGER,
    updatedAt INTEGER,
    lastSignedIn INTEGER
  );
`);

console.log("Database initialized successfully.");
db.close();
