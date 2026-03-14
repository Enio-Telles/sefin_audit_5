import * as db from "./server/db";

async function diag() {
  console.log("Checking database...");
  try {
    const user = await db.upsertUser({
      openId: "diag-user",
      name: "Diag User",
      email: "diag@example.com",
      loginMethod: "diag",
      lastSignedIn: new Date(),
    });
    console.log("Upsert success:", user);
  } catch (error) {
    console.error("Upsert failed:", error);
  }
}

diag().catch(console.error);
