// sync-linear-to-notion.js
import fetch from "node-fetch";

const {
  LINEAR_API_KEY,
  NOTION_TOKEN,
  NOTION_DATABASE_ID,
  LOOKBACK_MINUTES = "12",
  REQUIRED_LABEL = "Customer - Hapag Lloyd"
} = process.env;

if (!LINEAR_API_KEY || !NOTION_TOKEN || !NOTION_DATABASE_ID) {
  console.error("Missing env vars"); process.exit(1);
}

const headersLinear = { "Content-Type": "application/json", "Authorization": LINEAR_API_KEY };
const headersNotion = { "Authorization": `Bearer ${NOTION_TOKEN}`, "Notion-Version": "2022-06-28", "Content-Type": "application/json" };
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function* fetchUpdatedIssuesSince(isoSince) {
  let after = null;
  while (true) {
    const query = `
      query UpdatedIssues($first:Int!, $after:String, $since: DateTime!) {
        issues(first: $first, after: $after, orderBy: updatedAt, filter: { updatedAt: { gte: $since } }) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              identifier
              title
              url
              priority
              dueDate
              state { name }
              labels { nodes { name } }
              cycle { name }
            }
          }
        }
      }`;
    const res = await fetch("https://api.linear.app/graphql", {
      method: "POST",
      headers: headersLinear,
      body: JSON.stringify({ query, variables: { first: 50, after, since: isoSince } })
    });
    const json = await res.json();
    const edges = json?.data?.issues?.edges || [];
    for (const e of edges) yield e.node;
    const pi = json?.data?.issues?.pageInfo;
    if (pi?.hasNextPage) after = pi.endCursor; else break;
  }
}

let dbSchema;
const getDb = async () => {
  if (!dbSchema) {
    const r = await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}`, { headers: headersNotion });
    dbSchema = await r.json();
  }
  return dbSchema;
};

async function ensureSelectOption(propertyName, optionName) {
  if (!optionName) return;
  const db = await getDb();
  const prop = db?.properties?.[propertyName];
  const typeKey = prop?.type;
  if (!prop || (typeKey !== "select" && typeKey !== "multi_select")) return;
  const existing = prop[typeKey]?.options || [];
  if (existing.some(o => (o?.name || "").toLowerCase() === optionName.toLowerCase())) return;
  const nextOptions = [...existing, { name: optionName }];
  await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}`, {
    method: "PATCH",
    headers: headersNotion,
    body: JSON.stringify({ properties: { [propertyName]: { [typeKey]: { options: nextOptions } } } })
  });
  dbSchema = null; await getDb();
}

function mapExtras(labels, cycleName) {
  const MODULE_LABELS = ["Planning","Procurement","Post-bunkering"];
  const SUBAREA_LABELS = ["Planning","Lab","Approval"];
  const TYPE_LABELS = ["Features","Bug","Chore"];
  const pickFirst = (cands) => labels.find(l => cands.some(c => c.toLowerCase() === l.toLowerCase()));
  return {
    moduleVal: pickFirst(MODULE_LABELS) || "",
    subareaVal: pickFirst(SUBAREA_LABELS) || "",
    typeVal: pickFirst(TYPE_LABELS) || (labels.includes("Features") ? "Features" : ""),
    cycleVal: cycleName || ""
  };
}

async function findPageByIssueId(id) {
  const resp = await fetch(`https://api.notion.com/v1/databases/${NOTION_DATABASE_ID}/query`, {
    method: "POST", headers: headersNotion,
    body: JSON.stringify({ filter: { property: "Linear Issue ID", rich_text: { equals: id } }, page_size: 1 })
  });
  return (await resp.json())?.results?.[0];
}

async function upsert(issue) {
  const id = issue.identifier;
  const labels = (issue.labels?.nodes || []).map(n => n?.name || "").filter(Boolean);
  const hasRequired = labels.some(n => n.toLowerCase() === REQUIRED_LABEL.toLowerCase());
  if (!hasRequired) return; // skip if "ny issue" not present

  const { moduleVal, subareaVal, typeVal, cycleVal } = mapExtras(labels, issue.cycle?.name || "");

  // Ensure needed select options exist
  await ensureSelectOption("Status", issue.state?.name || "");
  if (issue.priority) {
  await ensureSelectOption("Priority", issue.priority);
  }
  await ensureSelectOption("Module", moduleVal);
  await ensureSelectOption("Sub-Area", subareaVal);
  await ensureSelectOption("Type", typeVal);
  await ensureSelectOption("Cycle", cycleVal);

  const props = {
    "Linear Issue ID": { rich_text: [{ type: "text", text: { content: id } }] },
    "Linear URL": { url: issue.url },
    "Title": { title: [{ type: "text", text: { content: issue.title || "" } }] },
    ...(issue.state?.name ? { "Status": { select: { name: issue.state.name } } } : {}),
    ...(issue.priority !== null && issue.priority !== undefined ? { "Priority": { select: { name: String(issue.priority) } } } : {}),
    ...(issue.dueDate ? { "Due Date": { date: { start: issue.dueDate } } } : {}),
    ...(moduleVal ? { "Module": { select: { name: moduleVal } } } : {}),
    ...(subareaVal ? { "Sub-Area": { select: { name: subareaVal } } } : {}),
    ...(typeVal ? { "Type": { select: { name: typeVal } } } : {}),
    ...(cycleVal ? { "Cycle": { select: { name: cycleVal } } } : {})
  };

  const page = await findPageByIssueId(id);
  if (page) {
    const r = await fetch(`https://api.notion.com/v1/pages/${page.id}`, {
      method: "PATCH", headers: headersNotion, body: JSON.stringify({ properties: props })
    });
    if (r.status === 429) { await sleep(400); return upsert(issue); }
  } else {
    const r = await fetch(`https://api.notion.com/v1/pages`, {
      method: "POST", headers: headersNotion,
      body: JSON.stringify({ parent: { database_id: NOTION_DATABASE_ID }, properties: props })
    });
    if (r.status === 429) { await sleep(400); return upsert(issue); }
  }
  await sleep(300); // polite pacing
}

(async () => {
  const since = new Date(Date.now() - Number(LOOKBACK_MINUTES) * 60_000).toISOString();
  for await (const issue of fetchUpdatedIssuesSince(since)) {
    await upsert(issue);
  }
})();
