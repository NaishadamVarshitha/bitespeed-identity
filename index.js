const express = require("express");
const sqlite3 = require("sqlite3").verbose();

const app = express();
app.use(express.json());

// Create database
const db = new sqlite3.Database("./db.sqlite");

// Create table if not exists
db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS Contact(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      phoneNumber TEXT,
      email TEXT,
      linkedId INTEGER,
      linkPrecedence TEXT,
      createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP,
      deletedAt DATETIME
    )
  `);
});

// Helper functions
function query(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

function run(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function(err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}


// MAIN ENDPOINT
app.post("/identify", async (req, res) => {

  const { email, phoneNumber } = req.body;

  if (!email && !phoneNumber) {
    return res.status(400).send("Provide email or phoneNumber");
  }

  // Find matches
  let matches = await query(
    `SELECT * FROM Contact 
     WHERE email = ? OR phoneNumber = ?`,
    [email, phoneNumber]
  );


  // CASE 1 → No matches → create primary
  if (matches.length === 0) {

    const result = await run(
      `INSERT INTO Contact
       (email, phoneNumber, linkPrecedence)
       VALUES (?,?, 'primary')`,
      [email, phoneNumber]
    );

    return res.json({
      contact: {
        primaryContactId: result.lastID,
        emails: email ? [email] : [],
        phoneNumbers: phoneNumber ? [phoneNumber] : [],
        secondaryContactIds: []
      }
    });
  }


  // Extract primary IDs
  let primaryIds = new Set();

  matches.forEach(c => {
    if (c.linkPrecedence === "primary")
      primaryIds.add(c.id);
    else
      primaryIds.add(c.linkedId);
  });

  primaryIds = [...primaryIds];


  // Fetch full cluster
  let cluster = await query(
    `SELECT * FROM Contact
     WHERE id IN (${primaryIds.join(",")})
     OR linkedId IN (${primaryIds.join(",")})`
  );


  // Find oldest primary
  let primary = cluster
    .filter(c => c.linkPrecedence === "primary")
    .sort((a,b)=> new Date(a.createdAt)-new Date(b.createdAt))[0];


  // Convert newer primaries → secondary
  for (let c of cluster) {

    if(c.linkPrecedence==="primary" && c.id!==primary.id){

      await run(`
        UPDATE Contact
        SET linkedId=?, linkPrecedence='secondary'
        WHERE id=?`,
        [primary.id,c.id]
      );
    }
  }


  // Unique emails & phones
  let emails = new Set(cluster.map(c=>c.email).filter(Boolean));
  let phones = new Set(cluster.map(c=>c.phoneNumber).filter(Boolean));


  // Add secondary if new info
  if(
    (email && !emails.has(email)) ||
    (phoneNumber && !phones.has(phoneNumber))
  ){

    await run(`
      INSERT INTO Contact
      (email,phoneNumber,linkedId,linkPrecedence)
      VALUES (?,?,?,'secondary')`,
      [email,phoneNumber,primary.id]
    );

    cluster = await query(
      `SELECT * FROM Contact
       WHERE id=${primary.id}
       OR linkedId=${primary.id}`
    );

  }


  emails = [...new Set(cluster.map(c=>c.email).filter(Boolean))];
  phones = [...new Set(cluster.map(c=>c.phoneNumber).filter(Boolean))];

  const secondaryIds = cluster
    .filter(c=>c.id!==primary.id)
    .map(c=>c.id);


  res.json({

    contact:{

      primaryContactId: primary.id,

      emails,

      phoneNumbers: phones,

      secondaryContactIds: secondaryIds

    }

  });

});


app.listen(3000,()=>{
 console.log("Server running on port 3000");
});
