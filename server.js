const express = require("express");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json());

// ── Config ──
const PORT = process.env.PORT || 8080;
const HUBSPOT_ACCESS_TOKEN = process.env.HUBSPOT_ACCESS_TOKEN || "";
const HUBSPOT_PERSONAL_KEY = process.env.HUBSPOT_PERSONAL_KEY || "";

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

// ── HubSpot Token Management ──
let hubspotToken = HUBSPOT_ACCESS_TOKEN;
let tokenExpiry = 0;

async function getHubSpotToken() {
  // If we have a direct access token and it hasn't expired, use it
  if (hubspotToken && Date.now() < tokenExpiry) return hubspotToken;

  // Try to exchange personal access key for access token
  if (HUBSPOT_PERSONAL_KEY) {
    try {
      const r = await fetch(
        "https://api.hubapi.com/oauth/personal-access-keys/v1/tokens",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ personalAccessKey: HUBSPOT_PERSONAL_KEY }),
        }
      );
      if (r.ok) {
        const data = await r.json();
        hubspotToken = data.accessToken || data.token;
        tokenExpiry = Date.now() + 25 * 60 * 1000; // 25 min
        console.log("[HubSpot] Token refreshed via personal access key");
        return hubspotToken;
      }
    } catch (e) {
      console.error("[HubSpot] Token exchange failed:", e.message);
    }
  }

  // Fallback to direct token
  if (HUBSPOT_ACCESS_TOKEN) {
    hubspotToken = HUBSPOT_ACCESS_TOKEN;
    return hubspotToken;
  }

  throw new Error("No HubSpot token available");
}

async function hubspotGet(endpoint) {
  const token = await getHubSpotToken();
  const r = await fetch(`https://api.hubapi.com${endpoint}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });
  if (r.status === 401) {
    // Token expired, reset and retry once
    tokenExpiry = 0;
    const newToken = await getHubSpotToken();
    const r2 = await fetch(`https://api.hubapi.com${endpoint}`, {
      headers: {
        Authorization: `Bearer ${newToken}`,
        "Content-Type": "application/json",
      },
    });
    if (!r2.ok) throw new Error(`HubSpot API ${r2.status}: ${await r2.text()}`);
    return r2.json();
  }
  if (!r.ok) throw new Error(`HubSpot API ${r.status}: ${await r.text()}`);
  return r.json();
}

// ── Pipeline Stage Mapping ──
let stageMap = {}; // stageId -> { label, displayOrder }
let pipelineId = null;

const STAGE_ORDER = [
  "Intro Call",
  "Proposal Build",
  "Proposal Sent",
  "Proposal Follow Up #1",
  "Proposal Follow Up #2",
  "Proposal Follow Up #3",
  "Service Agreement",
];

async function loadPipelineStages() {
  try {
    const data = await hubspotGet("/crm/v3/pipelines/deals");
    for (const pipeline of data.results || []) {
      for (const stage of pipeline.stages || []) {
        stageMap[stage.id] = {
          label: stage.label,
          displayOrder: stage.displayOrder,
          pipelineId: pipeline.id,
          pipelineLabel: pipeline.label,
        };
      }
      // Detect the proposal pipeline
      const hasProposalStages = (pipeline.stages || []).some(
        (s) =>
          s.label.toLowerCase().includes("proposal") ||
          s.label.toLowerCase().includes("intro call")
      );
      if (hasProposalStages) pipelineId = pipeline.id;
    }
    console.log(
      `[HubSpot] Loaded ${Object.keys(stageMap).length} stages across ${data.results.length} pipelines`
    );
  } catch (e) {
    console.error("[HubSpot] Failed to load pipelines:", e.message);
  }
}

function getStageName(stageId) {
  return stageMap[stageId]?.label || `Stage ${stageId}`;
}

function getStageOrder(stageName) {
  const idx = STAGE_ORDER.findIndex(
    (s) => s.toLowerCase() === stageName.toLowerCase()
  );
  return idx >= 0 ? idx : 99;
}

// ── Owner Mapping ──
let ownerMap = {}; // ownerId -> name

async function loadOwners() {
  try {
    const data = await hubspotGet("/crm/v3/owners?limit=100");
    for (const owner of data.results || []) {
      ownerMap[owner.id] = `${owner.firstName || ""} ${owner.lastName || ""}`.trim() || owner.email;
    }
    console.log(`[HubSpot] Loaded ${Object.keys(ownerMap).length} owners`);
  } catch (e) {
    console.error("[HubSpot] Failed to load owners:", e.message);
  }
}

// ── Deal Fetching ──
let cachedDeals = [];
let lastRefresh = null;

async function fetchAllDeals() {
  const properties = [
    "dealname", "amount", "dealstage", "pipeline", "closedate",
    "createdate", "hs_lastmodifieddate", "hubspot_owner_id",
    "hs_deal_stage_probability",
  ].join(",");

  const allDeals = [];
  let after = null;

  while (true) {
    let url = `/crm/v3/objects/deals?limit=100&properties=${properties}`;
    if (after) url += `&after=${after}`;
    const data = await hubspotGet(url);

    for (const deal of data.results || []) {
      const stageName = getStageName(deal.properties.dealstage);
      allDeals.push({
        id: deal.id,
        name: deal.properties.dealname,
        amount: parseFloat(deal.properties.amount) || 0,
        stageId: deal.properties.dealstage,
        stage: stageName,
        stageOrder: getStageOrder(stageName),
        pipelineId: deal.properties.pipeline,
        closeDate: deal.properties.closedate,
        createDate: deal.properties.createdate,
        lastModified: deal.properties.hs_lastmodifieddate,
        ownerId: deal.properties.hubspot_owner_id,
        ownerName: ownerMap[deal.properties.hubspot_owner_id] || "Unassigned",
        probability: deal.properties.hs_deal_stage_probability,
        url: deal.url || `https://app.hubspot.com/contacts/${deal.properties.hs_object_id || deal.id}/record/0-3/${deal.id}`,
      });
    }

    if (data.paging?.next?.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }

  return allDeals;
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
      // New deal
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
      // Stage changed
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

  // Detect removed deals
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

  // Keep last 1000 entries
  const trimmed = changelog.slice(-1000);
  saveChangelog(trimmed);
  saveSnapshot(newDeals);
  return trimmed;
}

// ── Data Refresh ──
async function refreshData() {
  await loadPipelineStages();
  await loadOwners();
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
    hasToken: !!(HUBSPOT_ACCESS_TOKEN || HUBSPOT_PERSONAL_KEY),
  });
});

app.get("/api/deals", verifyAuth, (req, res) => {
  // Filter to only our pipeline stages if requested
  const stageFilter = req.query.stage;
  const pipelineOnly = req.query.pipeline !== "all";
  let deals = cachedDeals;

  if (pipelineOnly && pipelineId) {
    deals = deals.filter((d) => d.pipelineId === pipelineId);
  }
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
  const deals = pipelineId
    ? cachedDeals.filter((d) => d.pipelineId === pipelineId)
    : cachedDeals;

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

  // Deals not in our tracked stages
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
  try {
    await refreshData();
  } catch (e) {
    console.error("[Startup] Initial refresh failed:", e.message);
    console.error("Set HUBSPOT_ACCESS_TOKEN or HUBSPOT_PERSONAL_KEY env var");
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
