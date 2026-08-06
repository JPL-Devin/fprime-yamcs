/**
 * fprime-events.js:
 *
 * Yamcs web extension providing a GDS-style F Prime event display: whole-row
 * severity coloring and filtering on event ID, name, and F Prime severity.
 *
 * Two custom elements are defined:
 *  - <fprime-yamcs>: initializer element (named after the Yamcs plugin id),
 *    instantiated by yamcs-web at startup to register the sidebar item.
 *  - <fprime-events>: the event display page, mounted at /ext/fprime-events.
 */

const SEVERITIES = [
  "FATAL",
  "WARNING_HI",
  "WARNING_LO",
  "ACTIVITY_HI",
  "ACTIVITY_LO",
  "COMMAND",
  "DIAGNOSTIC",
];

// Row colors matching fprime-gds fpstyle.css
const SEVERITY_COLORS = {
  FATAL: "rgba(251, 128, 114, 1)",
  WARNING_HI: "rgba(242, 142, 44, 1)",
  WARNING_LO: "rgba(237, 201, 73, 1)",
  ACTIVITY_HI: "rgba(128, 177, 211, 1)",
  ACTIVITY_LO: "rgba(186, 176, 171, 1)",
  COMMAND: "rgba(127, 201, 127, 1)",
  DIAGNOSTIC: "transparent",
};

// Fallback for events without fprime_severity extra (e.g. pre-existing archive)
const YAMCS_SEVERITY_FALLBACK = {
  CRITICAL: "FATAL",
  SEVERE: "FATAL",
  ERROR: "FATAL",
  DISTRESS: "WARNING_HI",
  WARNING: "WARNING_LO",
  WATCH: "ACTIVITY_HI",
  INFO: "ACTIVITY_LO",
};

const EVENT_SOURCE = "FPrimeEventProcessor";
const MAX_EVENTS = 10000;
const BACKFILL_LIMIT = 1000;

const PAGE_STYLE = `
  :host {
    display: block;
    height: 100%;
    font-family: Roboto, sans-serif;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.87);
  }
  .fp-events {
    display: flex;
    flex-direction: column;
    height: 100%;
    padding: 8px 16px 16px 16px;
    box-sizing: border-box;
  }
  .fp-toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    padding: 8px 0;
  }
  .fp-toolbar input[type="text"] {
    flex: 0 1 320px;
    padding: 5px 8px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 12px;
  }
  .fp-severities {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 2px;
  }
  .fp-severities label {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    border: 1px solid rgba(0, 0, 0, 0.2);
    border-radius: 3px;
    padding: 2px 6px;
    cursor: pointer;
    user-select: none;
  }
  .fp-time-range {
    display: inline-flex;
    align-items: center;
    gap: 4px;
  }
  .fp-time-range input {
    padding: 3px 4px;
    border: 1px solid #ccc;
    border-radius: 3px;
    font-size: 12px;
  }
  .fp-follow {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  .fp-toolbar button {
    padding: 4px 10px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: #fff;
    cursor: pointer;
    font-size: 12px;
  }
  .fp-toolbar button:hover {
    background: #f0f0f0;
  }
  .fp-count {
    margin-left: auto;
    color: rgba(0, 0, 0, 0.54);
  }
  .fp-table-holder {
    flex: 1 1 auto;
    overflow: auto;
    border: 1px solid rgba(0, 0, 0, 0.125);
  }
  table {
    width: 100%;
    border-collapse: collapse;
  }
  th, td {
    border: 1px solid rgba(0, 0, 0, 0.125);
    padding: 3px 8px;
    text-align: left;
    white-space: nowrap;
  }
  td.fp-message {
    white-space: normal;
    word-break: break-word;
  }
  thead th {
    position: sticky;
    top: 0;
    background: #eee;
    z-index: 1;
  }
`;

/** Normalizes a Yamcs event into display fields */
function normalize(event) {
  const extra = event.extra || {};
  const severity =
    extra["fprime_severity"] ||
    YAMCS_SEVERITY_FALLBACK[event.severity] ||
    "ACTIVITY_LO";
  const idNumber = parseInt(extra["fprime_event_id"], 10);
  const id = isNaN(idNumber) ? "" : "0x" + idNumber.toString(16).toUpperCase().padStart(4, "0");
  const name = extra["fprime_event_name"] || event.type || "";
  // Strip the redundant "[EventName] " prefix that the processor prepends
  let message = event.message || "";
  const match = message.match(/^\[[^\]]*\] /);
  if (match && name) {
    message = message.substring(match[0].length);
  }
  return {
    key: `${event.generationTime}-${event.seqNumber}`,
    time: event.generationTime,
    id,
    idNumber: isNaN(idNumber) ? null : idNumber,
    name,
    severity,
    message,
  };
}

/** True if the event was published by the F Prime event processor */
function isFprimeEvent(event) {
  return event.source === EVENT_SOURCE || !!(event.extra || {})["fprime_event_id"];
}

class FprimeEventsElement extends HTMLElement {
  constructor() {
    super();
    this._events = [];
    this._keys = new Set();
    this._filterText = "";
    this._enabledSeverities = new Set(SEVERITIES);
    this._subscription = null;
    this._service = null;
    this._follow = true;
    this._shownCount = 0;
    this._timeStart = null;
    this._timeStop = null;
  }

  set extensionService(service) {
    this._service = service;
    this.render();
    this.connect();
  }

  connectedCallback() {
    if (this._service && !this._subscription) {
      this.connect();
    }
  }

  disconnectedCallback() {
    this.disconnect();
  }

  disconnect() {
    if (this._subscription) {
      this._subscription.cancel();
      this._subscription = null;
    }
  }

  render() {
    const root = this.shadowRoot || this.attachShadow({ mode: "open" });
    root.innerHTML = "";

    const style = document.createElement("style");
    style.textContent = PAGE_STYLE;
    root.appendChild(style);

    const page = document.createElement("div");
    page.className = "fp-events";

    const toolbar = document.createElement("div");
    toolbar.className = "fp-toolbar";

    this._filterInput = document.createElement("input");
    this._filterInput.type = "text";
    this._filterInput.placeholder = "Filter by event ID, name, or message";
    this._filterInput.addEventListener("input", () => {
      this._filterText = this._filterInput.value.trim().toLowerCase();
      this.redraw();
    });
    toolbar.appendChild(this._filterInput);

    const makeTimeInput = (title, onChange) => {
      const input = document.createElement("input");
      input.type = "datetime-local";
      input.step = "1";
      input.title = title;
      input.addEventListener("change", () => {
        onChange(input.value ? new Date(input.value).toISOString() : null);
        this.redraw();
      });
      return input;
    };
    const timeRange = document.createElement("span");
    timeRange.className = "fp-time-range";
    timeRange.appendChild(document.createTextNode("From"));
    timeRange.appendChild(makeTimeInput("Show events at or after this time", (v) => (this._timeStart = v)));
    timeRange.appendChild(document.createTextNode("To"));
    timeRange.appendChild(makeTimeInput("Show events before this time", (v) => (this._timeStop = v)));
    toolbar.appendChild(timeRange);

    const severities = document.createElement("div");
    severities.className = "fp-severities";
    for (const severity of SEVERITIES) {
      const label = document.createElement("label");
      label.style.background = SEVERITY_COLORS[severity];
      const check = document.createElement("input");
      check.type = "checkbox";
      check.checked = true;
      check.addEventListener("change", () => {
        if (check.checked) {
          this._enabledSeverities.add(severity);
        } else {
          this._enabledSeverities.delete(severity);
        }
        this.redraw();
      });
      label.appendChild(check);
      label.appendChild(document.createTextNode(severity));
      severities.appendChild(label);
    }
    toolbar.appendChild(severities);

    const followLabel = document.createElement("label");
    followLabel.className = "fp-follow";
    this._followCheck = document.createElement("input");
    this._followCheck.type = "checkbox";
    this._followCheck.checked = this._follow;
    this._followCheck.addEventListener("change", () => {
      this._follow = this._followCheck.checked;
      this.scrollIfPinned();
    });
    followLabel.appendChild(this._followCheck);
    followLabel.appendChild(document.createTextNode("Follow latest"));
    toolbar.appendChild(followLabel);

    const clearButton = document.createElement("button");
    clearButton.textContent = "Clear";
    clearButton.addEventListener("click", () => {
      this._events = [];
      this._keys.clear();
      this.redraw();
    });
    toolbar.appendChild(clearButton);

    this._countLabel = document.createElement("span");
    this._countLabel.className = "fp-count";
    toolbar.appendChild(this._countLabel);

    page.appendChild(toolbar);

    this._tableHolder = document.createElement("div");
    this._tableHolder.className = "fp-table-holder";
    // Scrolling away from the bottom releases follow; scrolling back engages it
    this._tableHolder.addEventListener("scroll", () => {
      const holder = this._tableHolder;
      this._follow =
        holder.scrollTop + holder.clientHeight >= holder.scrollHeight - 5;
      this._followCheck.checked = this._follow;
    });

    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const column of ["Time", "ID", "Name", "Severity", "Message"]) {
      const th = document.createElement("th");
      th.textContent = column;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);
    this._tbody = document.createElement("tbody");
    table.appendChild(this._tbody);
    this._tableHolder.appendChild(table);
    page.appendChild(this._tableHolder);

    root.appendChild(page);
    this.redraw();
  }

  async connect() {
    this.disconnect();
    const yamcs = this._service.yamcs;
    const instance = yamcs.instance;
    const client = yamcs.yamcsClient;

    this._subscription = client.createEventSubscription(
      { instance },
      (event) => this.addEvents([event]),
    );

    try {
      const backfill = await client.getEvents(instance, {
        source: [EVENT_SOURCE],
        limit: BACKFILL_LIMIT,
        order: "desc",
      });
      this.addEvents(backfill.reverse());
    } catch (err) {
      console.error("fprime-events: backfill failed", err);
    }
  }

  addEvents(events) {
    const appended = [];
    let inOrder = true;
    for (const event of events) {
      if (!isFprimeEvent(event)) {
        continue;
      }
      const normalized = normalize(event);
      if (this._keys.has(normalized.key)) {
        continue;
      }
      const last = this._events[this._events.length - 1];
      if (last && normalized.time.localeCompare(last.time) < 0) {
        inOrder = false;
      }
      this._keys.add(normalized.key);
      this._events.push(normalized);
      appended.push(normalized);
    }
    if (!appended.length) {
      return;
    }
    if (!inOrder) {
      this._events.sort((a, b) => a.time.localeCompare(b.time));
    }
    let trimmed = false;
    if (this._events.length > MAX_EVENTS) {
      const removed = this._events.splice(0, this._events.length - MAX_EVENTS);
      for (const item of removed) {
        this._keys.delete(item.key);
      }
      trimmed = true;
    }
    // Append-only fast path: avoids rebuilding up to MAX_EVENTS DOM rows
    // per received event during event storms
    if (inOrder && !trimmed && this._tbody) {
      let shown = 0;
      for (const item of appended) {
        if (this.matches(item)) {
          this._tbody.appendChild(this.buildRow(item));
          shown++;
        }
      }
      this._shownCount += shown;
      this.updateCount();
      this.scrollIfPinned();
    } else {
      this.redraw();
    }
  }

  matches(item) {
    // Unknown severities are never filtered out (they have no toggle)
    if (SEVERITIES.includes(item.severity) && !this._enabledSeverities.has(item.severity)) {
      return false;
    }
    if (this._timeStart && item.time < this._timeStart) {
      return false;
    }
    if (this._timeStop && item.time >= this._timeStop) {
      return false;
    }
    if (!this._filterText) {
      return true;
    }
    const haystack = [
      item.id,
      item.idNumber !== null ? String(item.idNumber) : "",
      item.name,
      item.message,
      item.severity,
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(this._filterText);
  }

  buildRow(item) {
    const row = document.createElement("tr");
    row.style.background = SEVERITY_COLORS[item.severity] || "transparent";
    for (const value of [item.time, item.id, item.name, item.severity, item.message]) {
      const td = document.createElement("td");
      td.textContent = value;
      row.appendChild(td);
    }
    row.lastChild.className = "fp-message";
    return row;
  }

  updateCount() {
    this._countLabel.textContent = `${this._shownCount} of ${this._events.length} events`;
  }

  scrollIfPinned() {
    if (this._follow) {
      this._tableHolder.scrollTop = this._tableHolder.scrollHeight;
    }
  }

  redraw() {
    if (!this._tbody) {
      return;
    }
    this._tbody.innerHTML = "";
    this._shownCount = 0;
    for (const item of this._events) {
      if (!this.matches(item)) {
        continue;
      }
      this._shownCount++;
      this._tbody.appendChild(this.buildRow(item));
    }
    this.updateCount();
    this.scrollIfPinned();
  }
}

class FprimeYamcsInitializer extends HTMLElement {
  set extensionService(service) {
    service.addNavItem("archive", {
      path: "ext/fprime-events",
      label: "F´ Events",
      icon: "event_note",
    });
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-events")) {
  customElements.define("fprime-events", FprimeEventsElement);
}
if (!customElements.get("fprime-yamcs")) {
  customElements.define("fprime-yamcs", FprimeYamcsInitializer);
}
