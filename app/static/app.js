const endpoints = {
  health: "/api/health",
  events: "/api/events",
  sessions: "/api/sessions",
  devices: "/api/devices",
  actions: "/api/actions",
};

function text(value) {
  return value === null || value === undefined || value === "" ? "-" : String(value);
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
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

function renderEvents(events) {
  const body = document.getElementById("eventsBody");
  body.innerHTML = events.map((event) => `
    <tr>
      <td>${text(event.event_id)}</td>
      <td>${text(event.event_time)}</td>
      <td>${text(event.event_type)}</td>
      <td>${text(event.file_name)}</td>
      <td>${text(event.category)}</td>
      <td>${text(event.handled_action)}</td>
    </tr>
  `).join("");
}

function renderSessions(sessions) {
  const body = document.getElementById("sessionsBody");
  body.innerHTML = sessions.map((session) => `
    <tr>
      <td>${text(session.session_key)}</td>
      <td>${text(session.start_time)}</td>
      <td>${text(session.end_time)}</td>
      <td>${text(session.category)}</td>
      <td>${text(session.project)}</td>
      <td>${text(session.event_count)}</td>
    </tr>
  `).join("");
}

function renderDevices(devices) {
  const list = document.getElementById("devicesList");
  list.innerHTML = devices.length ? devices.map((device) => `
    <div class="list-item">
      <strong>${text(device.name || device.device_id)}</strong>
      <div class="muted">${text(device.device_type)} | ${text(device.status)}</div>
      <div>${text(device.drive_letter)} ${text(device.free_space_gb)} GB free</div>
    </div>
  `).join("") : `<div class="muted">No devices recorded.</div>`;
}

function renderActions(actions) {
  const list = document.getElementById("actionsList");
  list.innerHTML = actions.length ? actions.map((action) => `
    <div class="list-item">
      <strong>${text(action.action_type)}</strong>
      <div class="muted">${text(action.action_time)} | ${text(action.status)}</div>
      <div>${text(action.notes)}</div>
    </div>
  `).join("") : `<div class="muted">No actions recorded.</div>`;
}

async function refresh() {
  try {
    const [health, events, sessions, devices, actions] = await Promise.all([
      fetchJson(endpoints.health),
      fetchJson(endpoints.events),
      fetchJson(endpoints.sessions),
      fetchJson(endpoints.devices),
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
    renderDevices(devices.items);
    renderActions(actions.items);
    setStatus(true, "Live");
  } catch (error) {
    setStatus(false, "Offline");
    document.getElementById("serviceStatus").textContent = error.message;
  }
}

refresh();
setInterval(refresh, 10000);
