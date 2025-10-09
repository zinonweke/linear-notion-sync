// sync-linear-to-notion.js
import fetch from "node-fetch";
import { writeFile } from "fs/promises";

/* ===================== ENV ===================== */
const {
  LINEAR_API_KEY,
  NOTION_TOKEN,
  NOTION_DATABASE_ID,
  LOOKBACK_MINUTES = "25", // a bit bigger than schedule to avoid gaps
  REQUIRED_LABEL = "Customer - Hapag Lloyd"
} = process.env;

// === ADD: normalization helpers ===
function normalizeOptionName(value) {
  if (value == null) return null;                 // null/undefined
  if (typeof value === "string") {
    const s = value.trim();
    return s.length ? s : null;
  }
  if (typeof value === "number") return String(value);
  if (typeof value === "object") {
    // accept shapes like { name: "..." }
    if (typeof value.name === "string" && value.name.trim()) return value.name.trim();
  }
  return null; // unknown/empty → skip
}

// Optional: map numeric Linear priority to your text labels
function mapPriorityToText(p) {
  if (p == null) return null;
  if (typeof p === "string") return p.trim() || null;
  if (typeof p === "number") {
    const map = { 5: "Very Low", 4: "Low", 3: "Medium", 2: "High", 1: "Urgent", 0: "None" };
    return map[p] ?? String(p);
  }
  return null;
}

// Split long strings into Notion-safe rich_text chunks
function toRichTextArray(str) {
  if (!str) return [];
  const s = String(str);
  const MAX = 1900; // conservative chunk size
  const chunks = [];
  for (let i = 0; i < s.length; i += MAX) {
    chunks.push({ type: "text", text: { content: s.slice(i, i + MAX) } });
  }
  return chunks;
}

// Always use current time in ISO (UTC); Notion will display using a time_zone
function nowISO() {
  return new Date().toISOString();
}

if (!LINEAR_API_KEY || !NOTION_TOKEN || !NOTION_DATABASE_ID) {
  console.error("Missing env vars: LINEAR_API_KEY, NOTION_TOKEN, NOTION_DATABASE_ID are required.");
  process.exit(1);
}

/* ===================== CONSTANTS ===================== */
const NOTION_HEADERS = {
  "Authorization": `Bearer ${NOTION_TOKEN}`,
  "Notion-Version": "2022-06-28",
  "Content-Type": "application/json"
};
const LINEAR_HEADERS = {
  "Content-Type": "application/json",
  "Authorization": LINEAR_API_KEY // NOTE: Linear expects the key directly (no "Bearer")
};
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/* ===================== SUMMARY ===================== */
const summary = {
  processed: 0,
  created: 0,
  updated: 0,
  skippedNoLabel: 0,
  errors: 0,
  items: []
};

/* ===================== LINEAR QUERY ===================== */
async function* fetchUpdatedIssuesSince(isoSince) {
  let after = null;
  while (true) {
    const query = `
      query UpdatedIssues($first:Int!,$after:String,$since:DateTimeOrDuration!){
        issues(
          first:$first
          after:$after
          orderBy:updatedAt
          filter:{
            updatedAt:{ gte:$since }
            labels:{ some:{ name:{ eq:"${REQUIRED_LABEL}" } } }
          }
        ){
          pageInfo{ hasNextPage endCursor }
          edges{
            node{
              id
              identifier
              title
              url
              priority
              dueDate
              updatedAt
              state{ name }
              labels{ nodes{ name } }
              cycle{ name }
              description
            }
          }
        }
      }`;

    const res = await fetch("https://api.linear.app/graphql", {
      method: "POST",
      headers: LINEAR_HEADERS,
      body: JSON.stringify({ query, variables: { first: 50, after, since: isoSince } })
    });

    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`Linear GraphQL failed ${res.status}: ${t}`);
    }

    const json = await res.json();
    const edges = json?.data?.issues?.edges || [];
    for (const e of edges) yield e.node;

    const pi = json?.data?.issues?.pageInfo;
    if (pi?.hasNextPage) after = pi.endCursor; else break;
  }
}

/* ===================== NOTION HELPERS ===================== */
let dbSchemaCache = null;

async function getDatabase() {
  if (dbSchemaCache) return dbSchemaCache;
  const r = await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}`, {
    headers: NOTION_HEADERS
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`Get database failed ${r.status}: ${err}`);
  }
  dbSchemaCache = await r.json();
  return dbSchemaCache;
}

async function getTitlePropertyName() {
  const db = await getDatabase();
  const entries = Object.entries(db.properties || {});
  for (const [name, def] of entries) {
    if (def?.type === "title") return name; // Most workspaces use "Name", some use "Title"
  }
  throw new Error("No title property found on the Notion database.");
}

async function ensureSelectOption(propertyName, rawOption) {
  const optionName = normalizeOptionName(rawOption);
  if (!optionName) return; // nothing to add

  const db = await getDatabase();
  const prop = db?.properties?.[propertyName];
  if (!prop) return; // property missing → silently skip or console.warn

  const typeKey = prop?.type;
  if (typeKey !== "select" && typeKey !== "multi_select") return;

  const existing = prop[typeKey]?.options || [];
  const exists = existing.some(o => {
    const n = (o?.name ?? "").toString().toLowerCase();
    return n === optionName.toLowerCase();
  });
  if (exists) return;

  const nextOptions = [...existing, { name: optionName }];
  const r = await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}`, {
    method: "PATCH",
    headers: NOTION_HEADERS,
    body: JSON.stringify({
      properties: {
        [propertyName]: {
          [typeKey]: { options: nextOptions }
        }
      }
    })
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`Update DB schema (add select option "${optionName}" to "${propertyName}") failed ${r.status}: ${err}`);
  }
  dbSchemaCache = null; // refresh cache after schema change
  await getDatabase();
}

async function findPageByIssueId(identifier) {
  const r = await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}/query`, {
    method: "POST",
    headers: NOTION_HEADERS,
    body: JSON.stringify({
      filter: { property: "Linear Issue ID", rich_text: { equals: identifier } },
      page_size: 1
    })
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`Query DB by Linear Issue ID failed ${r.status}: ${err}`);
  }
  const j = await r.json();
  return j?.results?.[0];
}

async function notionWrite(endpoint, method, payload) {
  const doFetch = async () => {
    const r = await fetch(endpoint, {
      method,
      headers: NOTION_HEADERS,
      body: JSON.stringify(payload)
    });
    if (r.status === 429) {
      await sleep(500);
      return doFetch();
    }
    return r;
  };
  const resp = await doFetch();
  if (!resp.ok) {
    const errTxt = await resp.text().catch(() => "");
    throw new Error(`${method} ${endpoint} failed ${resp.status}: ${errTxt}`);
  }
  return resp.json();
}

/* Comment block (pretty) */
async function appendSyncComment(pageId, issue, cycleVal) {
  const ts = new Date().toISOString().replace("T"," ").split(".")[0] + " UTC";

  const makeLine = (label, value, bold = false) => ({
    object: "block",
    type: "paragraph",
    paragraph: {
      rich_text: [
        { type: "text", text: { content: label }, annotations: { bold } },
        { type: "text", text: { content: value } }
      ]
    }
  });

  const endpoint = `https://api.notion.com/v1/blocks/${pageId}/children`;
  await notionWrite(endpoint, "PATCH", {
    children: [
      makeLine("Last synced: ", `${ts}\n`, true),
      makeLine("Linear State: ", `${issue.state?.name || "N/A"}\n`),
      makeLine("Priority: ", `${issue.priority ?? "N/A"}\n`),
      makeLine("Cycle: ", `${cycleVal || "N/A"}`)
    ]
  });
}

/* ===================== MAPPING ===================== */
function mapExtras(labels, cycleName) {
  const MODULE_LABELS = ["Planning", "Procurement", "Post-bunkering"];
  const SUBAREA_LABELS = ["Planning", "Lab", "Approval"];
  const TYPE_LABELS = ["Features", "Bug", "Chore"];
  const pickFirst = (cands) =>
    labels.find(l => cands.some(c => c.toLowerCase() === String(l).toLowerCase()));
  return {
    moduleVal: pickFirst(MODULE_LABELS) || "",
    subareaVal: pickFirst(SUBAREA_LABELS) || "",
    typeVal: pickFirst(TYPE_LABELS) || (labels.includes("Features") ? "Features" : ""),
    cycleVal: cycleName || ""
  };
}

/* ===================== UPSERT ===================== */
async function upsert(issue) {
  const id = issue.identifier;
  const labels = (issue.labels?.nodes || []).map(n => n?.name || "").filter(Boolean);

  // Our Linear query already filters by REQUIRED_LABEL, but keep a guard:
  const hasRequired = labels.some(n => n.toLowerCase() === REQUIRED_LABEL.toLowerCase());
  if (!hasRequired) {
    summary.skippedNoLabel++;
    summary.items.push(`Skipped (no label): ${id} — ${issue.title}`);
    return { action: "skipped" };
  }

  // Map extra selects
  const { moduleVal, subareaVal, typeVal, cycleVal } = mapExtras(labels, issue.cycle?.name || "");


  // === ADD: normalize everything going into Selects
  const statusName  = normalizeOptionName(issue.state?.name);
  const priorityVal = normalizeOptionName(mapPriorityToText(issue.priority)); // handles number or text
  const moduleNorm  = normalizeOptionName(moduleVal);
  const subareaNorm = normalizeOptionName(subareaVal);
  const typeNorm    = normalizeOptionName(typeVal);
  const cycleNorm   = normalizeOptionName(cycleVal);

  // Ensure select options exist (Status, Priority text, Module, etc.)
  if (statusName)  await ensureSelectOption("Status",   statusName);
  if (priorityVal) await ensureSelectOption("Priority", priorityVal);
  if (moduleNorm)  await ensureSelectOption("Module",   moduleNorm);
  if (subareaNorm) await ensureSelectOption("Sub-Area", subareaNorm);
  if (typeNorm)    await ensureSelectOption("Type",     typeNorm);
  if (cycleNorm)   await ensureSelectOption("Cycle",    cycleNorm);

  // Build properties dynamically, including the real title property name
  const titlePropName = await getTitlePropertyName();
  
  const props = {
  [titlePropName]: { title: [{ type: "text", text: { content: issue.title || "" } }] },
  "Linear Issue ID": { rich_text: [{ type: "text", text: { content: id } }] },
  "Linear URL": { url: issue.url },
  ...(statusName  ? { "Status":   { select: { name: statusName } } } : {}),
  ...(priorityVal ? { "Priority": { select: { name: priorityVal } } } : {}),
  ...(issue.dueDate ? { "Due Date": { date: { start: issue.dueDate } } } : {}),
  ...(moduleNorm  ? { "Module":   { select: { name: moduleNorm } } } : {}),
  ...(subareaNorm ? { "Sub-Area": { select: { name: subareaNorm } } } : {}),
  ...(typeNorm    ? { "Type":     { select: { name: typeNorm } } } : {}),
  ...(cycleNorm   ? { "Cycle":    { select: { name: cycleNorm } } } : {}),
  "Last Sync": {
    date: { start: nowISO(), time_zone: "Europe/Berlin" }
  },
    "Description": {
      rich_text: toRichTextArray(issue.description || "")
    }
  };

  // If you also keep a separate Text property named "Title", populate it too (optional, safe if property exists & is rich_text)
  const db = await getDatabase();
  if (db?.properties?.["Title"]?.type === "rich_text") {
    props["Title"] = { rich_text: [{ type: "text", text: { content: issue.title || "" } }] };
  }

  // Upsert by Linear Issue ID
  const existing = await findPageByIssueId(id);
  let pageId, action;

  if (existing) {
    // UPDATE
    const endpoint = `https://api.notion.com/v1/pages/${existing.id}`;
    await notionWrite(endpoint, "PATCH", { properties: props });
    pageId = existing.id;
    action = "updated";
  } else {
    // CREATE
    const endpoint = "https://api.notion.com/v1/pages";
    const result = await notionWrite(endpoint, "POST", {
      parent: { database_id: NOTION_DATABASE_ID },
      properties: props
    });
    pageId = result.id;
    action = "created";
  }

  // Append tiny summary comment
  await appendSyncComment(pageId, issue, cycleVal);

  // Optional: ensure 'Last Sync' always reflects the final write time
  await notionWrite(`https://api.notion.com/v1/pages/${pageId}`, "PATCH", {
    properties: {
      "Last Sync": { date: { start: nowISO(), time_zone: "Europe/Berlin" } }
    }
  });
  
  return { action, pageId };
}

/* ===================== MAIN ===================== */
(async () => {
  try {
    const sinceISO = new Date(Date.now() - Number(LOOKBACK_MINUTES) * 60_000).toISOString();

    for await (const issue of fetchUpdatedIssuesSince(sinceISO)) {
      summary.processed++;
      try {
        const res = await upsert(issue);
        if (res?.action === "created") {
          summary.created++;
          summary.items.push(`Created: ${issue.identifier} — ${issue.title}`);
        } else if (res?.action === "updated") {
          summary.updated++;
          summary.items.push(`Updated: ${issue.identifier} — ${issue.title}`);
        }
      } catch (e) {
        summary.errors++;
        summary.items.push(`Error: ${issue.identifier} — ${e.message}`);
        console.error(e);
      }
      await sleep(200); // polite pacing for Notion
    }

    // Console summary
    const line = `Summary — processed: ${summary.processed}, created: ${summary.created}, updated: ${summary.updated}, skipped(no label): ${summary.skippedNoLabel}, errors: ${summary.errors}`;
    console.log(line);

    // GitHub Step Summary
    const summaryPath = process.env.GITHUB_STEP_SUMMARY;
    if (summaryPath) {
      const md = [
        `# Linear → Notion Sync`,
        ``,
        `**Label filter:** \`${REQUIRED_LABEL}\``,
        ``,
        `| metric | count |`,
        `|---|---:|`,
        `| processed | ${summary.processed} |`,
        `| created | ${summary.created} |`,
        `| updated | ${summary.updated} |`,
        `| skipped (no label) | ${summary.skippedNoLabel} |`,
        `| errors | ${summary.errors} |`,
        ``,
        `<details><summary>Items</summary>`,
        ``,
        summary.items.map(x => `- ${x}`).join("\n") || "_(none)_",
        ``,
        `</details>`
      ].join("\n");
      await writeFile(summaryPath, md + "\n", { encoding: "utf8", flag: "a" });
    }

    // Uncomment this to fail the job when any error occurs:
    // if (summary.errors > 0) process.exit(1);

  } catch (fatal) {
    // Top-level fatal (e.g., Linear auth/schema fatal)
    console.error("FATAL:", fatal);
    process.exit(1);
  }
})();
