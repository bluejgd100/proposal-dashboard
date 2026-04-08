const express = require("express");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { Connection, Request } = require("tedious");
const { ClientSecretCredential } = require("@azure/identity");

const app = express();
app.use(express.json());

// ── Config ──
const PORT = process.env.PORT || 8080;
const FABRIC_SQL_SERVER = process.env.FABRIC_SQL_SERVER || "";
const FABRIC_DATABASE = process.env.FABRIC_DATABASE || "";
const AZURE_TENANT_ID = process.env.AZURE_TENANT_ID || "";
const AZURE_CLIENT_ID = process.env.AZURE_CLIENT_ID || "";
const AZURE_CLIENT_SECRET = process.env.AZURE_CLIENT_SECRET || "";

// Data directory for change log persistence
const DATA_DIR = process.env.DATA_DIR || (process.env.WEBSITE_SITE_NAME ? "/home/data" : path.join(__dirname, "data"));
const CHANGELOG_FILE = path.join(DATA_DIR, "changelog.json");
const SNAPSHOT_FILE = path.join(DATA_DIR, "snapshot.json");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ── Auth (same pattern as pine-fabric-connector) ──
const sessions = new Map();

function getUsers() {
  const raw = process.env.USERS || "";
  if (!raw) return {};
  const users = {};
  for (const pair of raw.split(",")) {
    const parts = pair.trim().split(":");
    if (parts.length >= 2) {
      users[parts[0].trim()] = {
        password: parts[1].trim(),
        role: parts[2]?.trim() || "admin",
      };
    }
  }
  return users;
}

app.post("/auth/login", (req, res) => {
  const users = getUsers();
  if (!Object.keys(users).length)
    return res.status(500).json({ error: "No users configured" });
  const { username, password } = req.body;
  const user = users[username];
  if (!user || user.password !== password)
    return res.status(401).json({ error: "Invalid credentials" });
  const token = crypto.randomBytes(32).toString("hex");
  sessions.set(token, {
    user: username,
    role: user.role,
    expires: Date.now() + 86400000,
  });
  res.json({ token, user: username, role: user.role });
});

function verifyAuth(req, res, next) {
  const auth = req.headers.authorization || "";
  if (auth.startsWith("Bearer ")) {
    const token = auth.slice(7);
    const session = sessions.get(token);
    if (session && session.expires > Date.now()) {
      req.user = session;
      return next();
    }
  }
  res.status(401).json({ error: "Unauthorized" });
}

// ── Fabric SQL Connection ──
async function getAzureToken() {
  const credential = new ClientSecretCredential(AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET);
  const tokenResponse = await credential.getToken("https://database.windows.net/.default");
  console.log("[Fabric] Azure AD token acquired");
  return tokenResponse.token;
}

function createConnection(token) {
  return new Promise((resolve, reject) => {
    const config = {
      server: FABRIC_SQL_SERVER,
      authentication: {
        type: "azure-active-directory-access-token",
        options: { token },
      },
      options: {
        database: FABRIC_DATABASE,
        encrypt: true,
        port: 1433,
        connectTimeout: 60000,
        requestTimeout: 60000,
        rowCollectionOnDone: true,
        rowCollectionOnRequestCompletion: true,
      },
    };
    const connection = new Connection(config);
    connection.on("connect", (err) => {
      if (err) {
        console.error("[Fabric] Connection error:", err.message);
        reject(err);
      } else {
        console.log("[Fabric] Connected to SQL endpoint");
        resolve(connection);
      }
    });
    connection.connect();
  });
}

async function queryFabric(query) {
  const token = await getAzureToken();
  const connection = await createConnection(token);

  return new Promise((resolve, reject) => {
    const rows = [];
    const columns = [];
    const request = new Request(query, (err, rowCount) => {
      connection.close();
      if (err) reject(err);
      else resolve(rows);
    });
    request.on("columnMetadata", (cols) => {
      for (const col of cols) columns.push(col.colName);
    });
    request.on("row", (row) => {
      const obj = {};
      row.forEach((col) => { obj[col.metadata.colName] = col.value; });
      rows.push(obj);
    });
    connection.execSql(request);
  });
}

// ── Pipeline Stage Config ──
const STAGE_ORDER = [
  "Intro Call",
  "Proposal Build",
  "Proposal Sent",
  "Proposal Follow Up #1",
  "Proposal Follow Up #2",
  "Proposal Follow Up #3",
  "Service Agreement",
];

function getStageOrder(stageName) {
  const idx = STAGE_ORDER.findIndex(
    (s) => s.toLowerCase() === (stageName || "").toLowerCase()
  );
  return idx >= 0 ? idx : 99;
}

// ── Deal Fetching from Fabric ──
let cachedDeals = [];
let lastRefresh = null;

async function fetchAllDeals() {
  const rows = await queryFabric("SELECT * FROM dbo.hubspot_deals");

  return rows.map((row) => {
    const stageName = row.dealstage_name || row.dealstage || "Unknown";
    return {
      id: row.deal_id,
      name: row.dealname || "Unnamed Deal",
      amount: parseFloat(row.amount) || 0,
      stageId: row.dealstage,
      stage: stageName,
      stageOrder: getStageOrder(stageName),
      closeDate: row.closedate,
      createDate: row.createdate,
      lastModified: row.hs_lastmodifieddate,
      ownerId: row.hubspot_owner_id,
      ownerName: row.owner_name || "Unassigned",
      clientCompany: row.client_company_name || "",
      linesOfBusiness: row.lines_of_business || "",
      serviceLine: row.service_line || "",
      dealType: row.dealtype || "",
      loiFee: row.loi_fee || "",
      term: row.term || "",
      proposalDate: row.proposal_date,
      serviceAgreementSigned: row.service_agreement_signed,
      referralCompany: row.referral_company || "",
      serviceProvider: row.service_provider_company_name || "",
      daysToClose: row.days_to_close,
      description: row.description || "",
    };
  });
}

// ── Change Log ──
function loadChangelog() {
  try {
    if (fs.existsSync(CHANGELOG_FILE)) {
      return JSON.parse(fs.readFileSync(CHANGELOG_FILE, "utf-8"));
    }
  } catch (e) {
    console.error("[Changelog] Load error:", e.message);
  }
  return [];
}

function saveChangelog(log) {
  try {
    fs.writeFileSync(CHANGELOG_FILE, JSON.stringify(log, null, 2));
  } catch (e) {
    console.error("[Changelog] Save error:", e.message);
  }
}

function loadSnapshot() {
  try {
    if (fs.existsSync(SNAPSHOT_FILE)) {
      return JSON.parse(fs.readFileSync(SNAPSHOT_FILE, "utf-8"));
    }
  } catch (e) {
    console.error("[Snapshot] Load error:", e.message);
  }
  return {};
}

function saveSnapshot(deals) {
  const snap = {};
  for (const d of deals) {
    snap[d.id] = { stage: d.stage, stageId: d.stageId, amount: d.amount, name: d.name };
  }
  try {
    fs.writeFileSync(SNAPSHOT_FILE, JSON.stringify(snap, null, 2));
  } catch (e) {
    console.error("[Snapshot] Save error:", e.message);
  }
  return snap;
}

function detectChanges(newDeals) {
  const prev = loadSnapshot();
  const changelog = loadChangelog();
  const now = new Date().toISOString();

  for (const deal of newDeals) {
    const old = prev[deal.id];
    if (!old) {
      changelog.push({
        timestamp: now,
        dealId: deal.id,
        dealName: deal.name,
        type: "new_deal",
        fromStage: null,
        toStage: deal.stage,
        amount: deal.amount,
      });
    } else if (old.stageId !== deal.stageId) {
      changelog.push({
        timestamp: now,
        dealId: deal.id,
        dealName: deal.name,
        type: "stage_change",
        fromStage: old.stage,
        toStage: deal.stage,
        amount: deal.amount,
      });
    }
  }

  const currentIds = new Set(newDeals.map((d) => d.id));
  for (const [id, old] of Object.entries(prev)) {
    if (!currentIds.has(id)) {
      changelog.push({
        timestamp: now,
        dealId: id,
        dealName: old.name,
        type: "deal_removed",
        fromStage: old.stage,
        toStage: null,
        amount: old.amount,
      });
    }
  }

  const trimmed = changelog.slice(-1000);
  saveChangelog(trimmed);
  saveSnapshot(newDeals);
  return trimmed;
}

// ── Data Refresh ──
async function refreshData() {
  const deals = await fetchAllDeals();
  detectChanges(deals);
  cachedDeals = deals;
  lastRefresh = new Date().toISOString();
  console.log(`[Refresh] ${deals.length} deals loaded at ${lastRefresh}`);
  return deals;
}

// ── API Routes ──
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    lastRefresh,
    dealCount: cachedDeals.length,
    configured: !!(FABRIC_SQL_SERVER && AZURE_CLIENT_ID),
  });
});

app.get("/api/deals", verifyAuth, (req, res) => {
  const stageFilter = req.query.stage;
  let deals = cachedDeals;

  if (stageFilter) {
    deals = deals.filter(
      (d) => d.stage.toLowerCase() === stageFilter.toLowerCase()
    );
  }

  res.json({
    count: deals.length,
    lastRefresh,
    stages: STAGE_ORDER,
    deals,
  });
});

app.get("/api/pipeline", verifyAuth, (req, res) => {
  const deals = cachedDeals;

  const stages = STAGE_ORDER.map((name) => {
    const stageDeals = deals.filter(
      (d) => d.stage.toLowerCase() === name.toLowerCase()
    );
    return {
      name,
      count: stageDeals.length,
      totalValue: stageDeals.reduce((s, d) => s + d.amount, 0),
      deals: stageDeals.sort((a, b) => b.amount - a.amount),
    };
  });

  const trackedNames = new Set(STAGE_ORDER.map((s) => s.toLowerCase()));
  const other = deals.filter(
    (d) => !trackedNames.has(d.stage.toLowerCase())
  );

  res.json({
    lastRefresh,
    totalDeals: deals.length,
    totalValue: deals.reduce((s, d) => s + d.amount, 0),
    stages,
    other: {
      count: other.length,
      totalValue: other.reduce((s, d) => s + d.amount, 0),
      deals: other,
    },
  });
});

app.get("/api/changelog", verifyAuth, (req, res) => {
  const log = loadChangelog();
  const limit = parseInt(req.query.limit) || 200;
  res.json({
    count: log.length,
    entries: log.slice(-limit).reverse(),
  });
});

app.post("/api/refresh", verifyAuth, async (req, res) => {
  try {
    const deals = await refreshData();
    res.json({ success: true, count: deals.length, lastRefresh });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Serve Frontend ──
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "frontend", "index.html"));
});
app.use("/frontend", express.static(path.join(__dirname, "frontend")));

// ── Startup ──
app.listen(PORT, async () => {
  console.log(`[Server] Running on port ${PORT}`);
  if (!FABRIC_SQL_SERVER || !AZURE_CLIENT_ID) {
    console.error("[Startup] Missing env vars: FABRIC_SQL_SERVER, FABRIC_DATABASE, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET");
  } else {
    try {
      await refreshData();
    } catch (e) {
      console.error("[Startup] Initial refresh failed:", e.message);
    }
  }
  // Auto-refresh every 15 minutes
  setInterval(async () => {
    try {
      await refreshData();
    } catch (e) {
      console.error("[Auto-refresh] Failed:", e.message);
    }
  }, 15 * 60 * 1000);
});
