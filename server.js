const express = require("express");
const cors = require("cors");
const { MongoClient } = require("mongodb");

const app = express();
app.use(express.json());
app.use(cors());

// =====================
// CONFIG
// =====================
const TOTAL_CONDITIONS = 4;
const PROLIFIC_API_TOKEN = process.env.PROLIFIC_API_TOKEN;
const STUDY_ID = process.env.STUDY_ID;
const MONGO_URI = process.env.MONGO_URI;

// =====================
// DB SETUP
// =====================
const client = new MongoClient(MONGO_URI);
let db, conditions, assignments;

async function initDB() {
  await client.connect();
  db = client.db("experiment");

  conditions = db.collection("conditions");
  assignments = db.collection("assignments");

  // initialize conditions once
  const count = await conditions.countDocuments();
  if (count === 0) {
    const docs = [];
    for (let i = 1; i <= TOTAL_CONDITIONS; i++) {
      docs.push({ condition: i, assigned: false });
    }
    await conditions.insertMany(docs);
    console.log("Initialized conditions");
  }
}

initDB();

// =====================
// ASSIGNMENT ENDPOINT
// =====================
app.post("/assign_with_refresh", async (req, res) => {
  const { participantId } = req.body;

  try {
    // 1. already assigned?
    const existing = await assignments.findOne({ participantId });
    if (existing) {
      return res.json({ condition: existing.condition });
    }

    // 2. atomic assignment
    const result = await conditions.findOneAndUpdate(
      { assigned: false },
      { $set: { assigned: true, assigned_to: participantId } },
      { returnDocument: "after" }
    );
    console.log("result:", result);

    if (!result) {
      // try cleanup once
      await cleanupReturnedParticipants();

      const retry = await conditions.findOneAndUpdate(
        { assigned: false },
        { $set: { assigned: true, assigned_to: participantId } },
        { returnDocument: "after" }
      );
      console.log("retry:", retry);

      if (!retry) {
        return res.json({ condition: null });
      }

      await assignments.insertOne({
        participantId,
        condition: retry.value.condition,
        timestamp: new Date()
      });

      return res.json({ condition: retry.value.condition });
    }

    // 3. save assignment
    await assignments.insertOne({
      participantId,
      condition: result.value.condition,
      timestamp: new Date()
    });

    res.json({ condition: result.value.condition });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "server error" });
  }
});

// =====================
// PROLIFIC CLEANUP
// =====================
async function fetchReturnedParticipants() {
  const res = await fetch(
    `https://api.prolific.co/api/v1/submissions/?study=${STUDY_ID}`,
    {
      headers: {
        Authorization: `Token ${PROLIFIC_API_TOKEN}`
      }
    }
  );

  const data = await res.json();

  return data.results
    .filter(sub =>
      ["RETURNED", "TIMED_OUT", "REJECTED"].includes(sub.status)
    )
    .map(sub => sub.participant_id);
}

async function cleanupReturnedParticipants() {
  try {
    const returnedIds = await fetchReturnedParticipants();

    if (returnedIds.length === 0) return;

    // free conditions
    await conditions.updateMany(
      { assigned_to: { $in: returnedIds } },
      { $set: { assigned: false }, $unset: { assigned_to: "" } }
    );

    // remove assignments
    await assignments.deleteMany({
      participantId: { $in: returnedIds }
    });

    console.log("Cleanup:", returnedIds.length);

  } catch (err) {
    console.error("Cleanup error:", err);
  }
}

// run every minute
setInterval(cleanupReturnedParticipants, 60000);

// =====================
// DEBUG ENDPOINT
// =====================
app.get("/status", async (req, res) => {
  const remaining = await conditions.countDocuments({ assigned: false });
  res.json({ remaining });
});

// =====================
app.listen(3000, () => {
  console.log("Server running on port 3000");
});