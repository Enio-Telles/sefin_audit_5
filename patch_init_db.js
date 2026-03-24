const fs = require('fs');

let content = fs.readFileSync('init_db.ts', 'utf8');
const auditSql = fs.readFileSync('drizzle/0002_audit_execution.sql', 'utf8');

const newContent = `import fs from "fs";
import Database from "better-sqlite3";

const db = new Database("sefin_audit.db");

console.log("Creating users table...");
db.exec(\`
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
\`);

console.log("Creating audit tables...");
const auditSql = fs.readFileSync("drizzle/0002_audit_execution.sql", "utf8");
db.exec(auditSql);

console.log("Database initialized successfully.");
db.close();
`;

fs.writeFileSync('init_db.ts', newContent);
