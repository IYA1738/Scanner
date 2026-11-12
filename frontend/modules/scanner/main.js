const connection = document.querySelector("#connection");
const connectionText = document.querySelector("#connection-text");
const emptyState = document.querySelector("#empty-state");
const emptyTitle = document.querySelector("#empty-title");
const emptyMessage = document.querySelector("#empty-message");
const eventList = document.querySelector("#event-list");
const eventCount = document.querySelector("#event-count");
const maxVisibleEvents = 200;
let receivedCount = 0;
let lastRecordId = "";

function setConnection(state, text) {
  connection.dataset.state = state;
  connectionText.textContent = text;
}

// 后端会从头重放；仅跳过已经展示过的 Stream 记录，不按业务 event_id 去重。
function isReplay(id) {
  if (!id || !lastRecordId) return false;
  const streamId = /^\d+-\d+$/;
  if (!streamId.test(id) || !streamId.test(lastRecordId)) {
    return id === lastRecordId;
  }
  const [time, sequence] = id.split("-").map((part) => BigInt(part));
  const [lastTime, lastSequence] = lastRecordId.split("-").map((part) => BigInt(part));
  return time < lastTime || (time === lastTime && sequence <= lastSequence);
}

const eventNames = {
  demo: ["测试事件", "消息"],
  "univ3.pool_created": ["V3 新池创建", "V3"],
  "v3.pool_created": ["V3 新池创建", "V3"],
};
const fieldNames = {
  pool: "池子地址",
  token0: "代币 0 地址",
  token1: "代币 1 地址",
  fee: "手续费率",
  tickspacing: "Tick 间距",
  message: "消息内容",
};
const poolTypes = new Set(["univ3.pool_created", "v3.pool_created"]);
const normalizeKey = (key) => key.replace(/[_\s-]/g, "").toLowerCase();

function node(tag, className, text) {
  const item = document.createElement(tag);
  if (className) item.className = className;
  if (text !== undefined) item.textContent = String(text);
  return item;
}

function renderValue(value, key, eventType) {
  if (value === null) return node("span", "muted", "空值");
  if (value === "") return node("span", "muted", "空文本");
  if (Array.isArray(value)) {
    if (!value.length) return node("span", "muted", "空列表");
    const list = node("ul", "value-list");
    for (const entry of value) {
      const item = node("li");
      item.append(renderValue(entry, "", eventType));
      list.append(item);
    }
    return list;
  }
  if (typeof value === "object") return renderFields(value, eventType);
  if (normalizeKey(key) === "fee" && poolTypes.has(eventType) &&
      typeof value === "number" && Number.isFinite(value)) {
    return node("span", "fee-value", `${value / 10000}%`);
  }
  const address = typeof value === "string" && /^0x[\da-fA-F]{40,64}$/.test(value);
  return node(address ? "code" : "span", address ? "address" : "field-value", value);
}

function renderFields(data, eventType) {
  const priority = ["pool", "token0", "token1", "fee", "tickspacing"];
  const entries = Object.entries(data);
  if (!entries.length) return node("p", "muted", "暂无详细信息");
  const grid = node("dl", "field-grid");
  if (poolTypes.has(eventType)) {
    const rank = (key) => {
      const index = priority.indexOf(normalizeKey(key));
      return index === -1 ? priority.length : index;
    };
    entries.sort(([a], [b]) => rank(a) - rank(b));
  }
  for (const [key, value] of entries) {
    const normalized = normalizeKey(key);
    const field = node("div", normalized === "pool" ? "field field-wide" : "field");
    const knownField = poolTypes.has(eventType) && Object.hasOwn(fieldNames, normalized);
    const label = knownField ? fieldNames[normalized] : normalized === "message" ? "消息内容" : key;
    field.append(node("dt", "field-label", label));
    const content = node("dd");
    content.append(renderValue(value, key, eventType));
    field.append(content);
    grid.append(field);
  }
  return grid;
}

const timeFormat = new Intl.DateTimeFormat("zh-CN", {
  month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
});

function recordTime(recordId) {
  if (!/^\d+-\d+$/.test(recordId)) return null;
  const date = new Date(Number(recordId.split("-")[0]));
  return Number.isNaN(date.getTime()) ? null : date;
}

function showEvent(payload, recordId) {
  const envelope = payload !== null && typeof payload === "object" && !Array.isArray(payload);
  const eventType = envelope ? String(payload.event_type ?? "") : "";
  const [title, symbol] = Object.hasOwn(eventNames, eventType)
    ? eventNames[eventType]
    : [eventType || "扫链事件", "事"];
  const card = node("article", "event-card");
  card.dataset.kind = poolTypes.has(eventType) ? "pool" : eventType === "demo" ? "message" : "event";
  const header = node("header", "event-header");
  const heading = node("div", "event-heading");
  const icon = node("span", "event-symbol", symbol);
  icon.setAttribute("aria-hidden", "true");
  const titleNode = node("h3", "event-title", title);
  titleNode.id = `event-title-${receivedCount + 1}`;
  card.setAttribute("aria-labelledby", titleNode.id);
  heading.append(icon, titleNode);
  if (envelope && payload.chain_id != null) {
    heading.append(node("span", "chain", `链 ${payload.chain_id}`));
  }
  header.append(heading);
  const time = recordTime(recordId);
  if (time) {
    const timeNode = node("time", "event-time", `记录于 ${timeFormat.format(time)}`);
    timeNode.dateTime = time.toISOString();
    header.append(timeNode);
  }
  card.append(header);

  const body = node("div", "event-body");
  const data = envelope && Object.hasOwn(payload, "data") ? payload.data : payload;
  if (data !== null && typeof data === "object" && !Array.isArray(data)) {
    const remaining = { ...data };
    if (typeof data.message === "string" && data.message.length > 0) {
      body.append(node("p", "event-message", data.message));
      delete remaining.message;
    }
    if (Object.keys(remaining).length || !Object.hasOwn(data, "message")) {
      body.append(renderFields(remaining, eventType));
    }
  } else {
    body.append(renderValue(data, "", eventType));
  }
  card.append(body);

  const metadata = node("dl", "event-metadata");
  const details = [
    ["事件类型", eventType],
    ["事件 ID", envelope ? payload.event_id : null],
    ["记录 ID", recordId],
  ];
  for (const [label, value] of details) {
    if (value == null || value === "") continue;
    const field = node("div");
    field.append(node("dt", "", label), node("dd", "", value));
    metadata.append(field);
  }
  if (metadata.children.length) {
    const details = node("details", "event-details");
    const summary = node("summary", "", "事件详情");
    summary.setAttribute("aria-label", `事件详情，第 ${receivedCount + 1} 条事件`);
    details.append(summary, metadata);
    card.append(details);
  }
  const item = node("li");
  item.append(card);
  eventList.prepend(item);
  if (eventList.children.length > maxVisibleEvents) eventList.lastElementChild.remove();

  receivedCount += 1;
  emptyState.hidden = true;
  eventCount.textContent = receivedCount > maxVisibleEvents
    ? `已接收 ${receivedCount} 条 · 展示最近 ${maxVisibleEvents} 条`
    : `${receivedCount} 条事件`;
}

const source = new EventSource("/events");

source.onopen = () => {
  setConnection("connected", "已连接");
  emptyTitle.textContent = "等待第一条事件";
  emptyMessage.textContent = "已连接，新的扫链事件会自动显示在这里。";
};

source.onmessage = (message) => {
  if (isReplay(message.lastEventId)) return;
  let payload;
  try {
    payload = JSON.parse(message.data);
  } catch {
    payload = message.data;
  }
  showEvent(payload, message.lastEventId);
  if (message.lastEventId) lastRecordId = message.lastEventId;
};

source.onerror = () => {
  const closed = source.readyState === EventSource.CLOSED;
  setConnection(
    closed ? "closed" : "reconnecting",
    closed ? "连接已关闭，请刷新重试" : "连接中断，正在重连",
  );
  emptyTitle.textContent = closed ? "连接已关闭" : "连接暂时中断";
  emptyMessage.textContent = closed
    ? "事件连接已关闭，请刷新页面后重试。"
    : "暂时无法连接事件接口，正在自动重试…";
};

window.addEventListener("pagehide", () => source.close());
window.addEventListener("pageshow", (event) => {
  if (event.persisted) window.location.reload();
});
