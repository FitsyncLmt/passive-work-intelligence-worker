const endpoints = {
  health: "/api/health",
  events: "/api/events",
  sessions: "/api/sessions",
  devices: "/api/devices",
  deviceHealth: "/api/device-health",
  summary: "/api/summary",
  actions: "/api/actions",
  refreshDeviceHealth: "/api/actions/refresh-device-health",
  backupSqlite: "/api/actions/backup-sqlite",
  generateDailyReport: "/api/actions/generate-daily-report",
  openLogs: "/api/actions/open-logs",
};

function text(value) {
  return value === null || value === undefined || value === "" ? "-" : String(value);
}

function html(value) {
  return text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return response.json();
}

async function postJson(url) {
  const response = await fetch(url, { method: "POST", cache: "no-store" });
  if (!response.ok) {
    throw new Error(`${url} returned ${response.status}`);
  }
  return response.json();
}

function setStatus(ok, message) {
  const el = document.getElementById("refreshStatus");
  el.textContent = message;
  el.classList.toggle("ok", ok);
  el.classList.toggle("bad", !ok);
}

function setCommandStatus(ok, message) {
  const el = document.getElementById("commandStatus");
  el.textContent = message;
  el.classList.toggle("ok-text", ok);
  el.classList.toggle("bad-text", !ok);
}

function renderEvents(events) {
  const body = document.getElementById("eventsBody");
  body.innerHTML = events.map((event) => `
    <tr>
      <td>${html(event.event_id)}</td>
      <td>${html(event.event_time)}</td>
      <td>${html(event.event_type)}</td>
      <td>${html(event.file_name)}</td>
      <td>${html(event.category)}</td>
      <td>${html(event.handled_action)}</td>
    </tr>
  `).join("");
}

function renderSessions(sessions) {
  const body = document.getElementById("sessionsBody");
  body.innerHTML = sessions.map((session) => `
    <tr>
      <td>${html(session.session_key)}</td>
      <td>${html(session.start_time)}</td>
      <td>${html(session.end_time)}</td>
      <td>${html(session.category)}</td>
      <td>${html(session.project)}</td>
      <td>${html(session.event_count)}</td>
    </tr>
  `).join("");
}

function renderDevices(devices) {
  const list = document.getElementById("devicesList");
  list.innerHTML = devices.length ? devices.map((device) => `
    <div class="list-item">
      <strong>${html(device.name || device.device_id)}</strong>
      <div class="muted">${html(device.device_type)} | ${html(device.status)}</div>
      <div>${html(device.drive_letter)} ${html(device.free_space_gb)} GB free</div>
    </div>
  `).join("") : `<div class="muted">No devices recorded.</div>`;
}

function renderDeviceHealth(devices) {
  const list = document.getElementById("deviceHealthList");
  list.innerHTML = devices.length ? devices.map((device) => `
    <div class="device-card ${device.status === "missing" ? "missing" : "available"}">
      <div class="device-title">
        <strong>${html(device.name)}</strong>
        <span>${html(device.status)}</span>
      </div>
      <dl>
        <div><dt>Role</dt><dd>${html(device.role)}</dd></div>
        <div><dt>Path</dt><dd>${html(device.drive_path || device.drive_letter)}</dd></div>
        <div><dt>Free</dt><dd>${device.free_space_gb === null || device.free_space_gb === undefined ? "-" : `${html(device.free_space_gb)} GB`}</dd></div>
        <div><dt>Last Seen</dt><dd>${html(device.last_seen)}</dd></div>
      </dl>
    </div>
  `).join("") : `<div class="muted">No configured devices found.</div>`;
}

function renderKeyValueList(id, values, unit) {
  const list = document.getElementById(id);
  const entries = Object.entries(values || {});
  list.innerHTML = entries.length ? entries.map(([name, value]) => `
    <div class="list-item row-item">
      <strong>${html(name)}</strong>
      <span>${html(value)}${html(unit)}</span>
    </div>
  `).join("") : `<div class="muted">No category data recorded.</div>`;
}

function renderSummary(summary) {
  document.getElementById("latestEventTime").textContent = text(summary.latest_event_time);
  document.getElementById("summaryLast24h").textContent = text(summary.last_24h_event_count);
  document.getElementById("last24hCount").textContent = text(summary.last_24h_event_count);
  renderKeyValueList("eventsByCategory", summary.events_by_category, "");
  renderKeyValueList("sessionMinutesByCategory", summary.session_minutes_by_category, " min");
}

function renderActions(actions) {
  const list = document.getElementById("actionsList");
  list.innerHTML = actions.length ? actions.map((action) => `
    <div class="list-item">
      <strong>${html(action.action_type)}</strong>
      <div class="muted">${html(action.action_time)} | ${html(action.status)}</div>
      <div>${html(action.notes)}</div>
    </div>
  `).join("") : `<div class="muted">No actions recorded.</div>`;
}

async function refresh() {
  try {
    const [health, events, sessions, devices, deviceHealth, summary, actions] = await Promise.all([
      fetchJson(endpoints.health),
      fetchJson(endpoints.events),
      fetchJson(endpoints.sessions),
      fetchJson(endpoints.devices),
      fetchJson(endpoints.deviceHealth),
      fetchJson(endpoints.summary),
      fetchJson(endpoints.actions),
    ]);

    document.getElementById("dbConnected").textContent = health.db_connected ? "Yes" : "No";
    document.getElementById("eventsCount").textContent = events.count;
    document.getElementById("sessionsCount").textContent = sessions.count;
    document.getElementById("devicesCount").textContent = devices.count;
    document.getElementById("serviceStatus").textContent = health.status;
    document.getElementById("databasePath").textContent = health.database_path;
    document.getElementById("lastRefresh").textContent = new Date().toLocaleTimeString();

    renderEvents(events.items);
    renderSessions(sessions.items);
    renderDeviceHealth(deviceHealth.items);
    renderSummary(summary);
    renderDevices(devices.items);
    renderActions(actions.items);
    setStatus(true, "Live");
  } catch (error) {
    setStatus(false, "Offline");
    document.getElementById("serviceStatus").textContent = error.message;
  }
}

async function runCommand(command) {
  const url = endpoints[command];
  if (!url) {
    setCommandStatus(false, "Unknown command");
    return;
  }
  setCommandStatus(true, "Running");
  try {
    const result = await postJson(url);
    if (!result.success) {
      setCommandStatus(false, `${text(result.action_type)}: ${text(result.errors || result.notes)}`);
      return;
    }
    setCommandStatus(true, `${text(result.action_type)} complete`);
    await refresh();
  } catch (error) {
    setCommandStatus(false, error.message);
  }
}

document.querySelectorAll("[data-command]").forEach((button) => {
  button.addEventListener("click", () => {
    runCommand(button.dataset.command);
  });
});

refresh();
setInterval(refresh, 10000);
