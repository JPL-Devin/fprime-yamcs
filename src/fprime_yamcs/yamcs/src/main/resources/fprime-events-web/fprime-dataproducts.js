/**
 * fprime-dataproducts.js:
 *
 * Yamcs web extension providing the F´ Data Products page: the data products detected among the files downlinked
 * into YAMCS, with their container (from the dictionary), header time, decode state, a Decode button per row, an
 * auto-decode toggle and links to the decoded JSON and the raw file.
 *
 * The page talks to the fprime-yamcs-dataproducts processor only through the fprimeDataProducts bucket: it polls
 * catalog.json, writes requests/<name> to ask for a decode and settings/auto_decode to toggle auto-decode.
 *
 * Defines <fprime-dataproducts>, mounted at /ext/fprime-dataproducts; the sidebar item is registered by the
 * <fprime-yamcs> initializer in fprime-events.js.
 */

const DP_BUCKET = "fprimeDataProducts";
const FILES_BUCKET = "fprimeFilesIn";
const CATALOG_OBJECT = "catalog.json";
const POLL_MS = 2000;
const PENDING_MS = 5000;
const TB_WORKSTATION = 2;

// Dark enough for white 11 px text (WCAG AA)
const STATE_COLORS = {
  DETECTED: "#6c757d",
  QUEUED: "#0f6674",
  DECODING: "#0f6674",
  DECODED: "#1e7e34",
  FAILED: "#c82333",
  DUPLICATE: "#5a6268",
};

const PAGE_STYLE = `
  :host {
    display: block;
    height: 100%;
    font-family: Roboto, sans-serif;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.87);
  }
  .fp-dps {
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
    gap: 12px;
    padding: 8px 0;
  }
  .fp-toolbar label {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    cursor: pointer;
    user-select: none;
  }
  .fp-status {
    margin-left: auto;
    color: rgba(0, 0, 0, 0.54);
  }
  .fp-status.fp-warn {
    color: #dc3545;
  }
  .fp-table-holder {
    overflow: auto;
    border: 1px solid rgba(0, 0, 0, 0.125);
    flex: 1 1 auto;
  }
  table {
    border-collapse: collapse;
    width: 100%;
  }
  th, td {
    padding: 4px 8px;
    text-align: left;
    white-space: nowrap;
    border-bottom: 1px solid rgba(0, 0, 0, 0.06);
  }
  th {
    position: sticky;
    top: 0;
    background: #fafafa;
    font-weight: 500;
    z-index: 1;
  }
  td.fp-error {
    max-width: 260px;
    overflow: hidden;
    text-overflow: ellipsis;
    color: #c82333;
  }
  td.fp-file {
    max-width: 170px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  /* Actions stay reachable however wide the other columns get */
  th.fp-actions, td.fp-actions {
    position: sticky;
    right: 0;
    background: #fff;
    box-shadow: -4px 0 4px -4px rgba(0, 0, 0, 0.2);
  }
  th.fp-actions {
    background: #fafafa;
    z-index: 2;
  }
  td.fp-num {
    text-align: right;
  }
  .fp-badge {
    display: inline-block;
    padding: 1px 6px;
    border-radius: 3px;
    color: #fff;
    font-size: 11px;
  }
  button {
    padding: 2px 8px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: #fff;
    cursor: pointer;
    font-size: 12px;
  }
  button:hover:not(:disabled) {
    background: #f0f0f0;
  }
  button:disabled {
    cursor: default;
    opacity: 0.5;
  }
  a {
    color: #1565c0;
  }
  .fp-empty {
    padding: 16px;
    color: rgba(0, 0, 0, 0.54);
  }
`;

/** Raw data product header time as <seconds>.<microseconds> */
export function dpTime(time) {
  return time.seconds + "." + String(time.microseconds).padStart(6, "0");
}

/** Calendar time for workstation time (base 2), the padded raw time for any other time base */
export function displayTime(time) {
  if (time.base !== TB_WORKSTATION) {
    return dpTime(time);
  }
  const date = new Date(time.seconds * 1000 + Math.floor(time.microseconds / 1000));
  return isNaN(date) ? dpTime(time) : date.toISOString();
}

/** Label of the decode button for a product state, or null while a decode is queued or running */
export function decodeLabel(state) {
  return { DETECTED: "Decode", DECODED: "Re-decode", FAILED: "Retry" }[state] || null;
}

/** Base name of a bucket object, without the DpCatalog's leading dots */
export function baseName(name) {
  return name.split("/").pop().replace(/^\.+/, "");
}

/** Error text without a trailing quoted path (the full text stays in the tooltip) */
export function shortError(error) {
  return (error || "").replace(/:\s*'[^']*'\s*$/, "");
}

export function formatHexId(id) {
  return `${id} (0x${Number(id).toString(16).toUpperCase()})`;
}

class FprimeDataProductsElement extends HTMLElement {
  constructor() {
    super();
    this._service = null;
    this._client = null;
    this._timer = null;
    this._catalog = null;
    this._pending = new Map(); // product name -> state when the decode was requested
    this._autoPending = null; // {value, until}: optimistic checkbox value until the server confirms it
    this._rows = new Map(); // product name -> {signature, row}
    this._inFlight = false;
    this._lastError = null;
    this._parseError = false;
  }

  set extensionService(service) {
    this._service = service;
    this._client = service.yamcs.createStorageClient();
    this.render();
    this.startPolling();
  }

  connectedCallback() {
    if (this._client && !this._timer) {
      this.startPolling();
    }
  }

  disconnectedCallback() {
    this.stopPolling();
  }

  startPolling() {
    this.stopPolling();
    this.poll();
    this._timer = setInterval(() => this.poll(), POLL_MS);
  }

  stopPolling() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  async poll() {
    if (this._inFlight) {
      return; // a slow response must not be overtaken by a newer one
    }
    this._inFlight = true;
    try {
      const response = await this._client.getObject(DP_BUCKET, CATALOG_OBJECT);
      const text = await response.text();
      if (!this._timer) {
        return; // stopped while waiting
      }
      try {
        const catalog = JSON.parse(text);
        if (!this._catalog || !(catalog.revision < this._catalog.revision)) {
          this._catalog = catalog;
        }
        this._parseError = false;
      } catch (err) {
        this._parseError = true; // keep showing the last good catalog
      }
      this._lastError = null;
    } catch (err) {
      this._catalog = null;
      this._lastError = err;
    } finally {
      this._inFlight = false;
    }
    this.redraw();
  }

  async decode(product) {
    this._pending.set(product.name, product.state);
    this.redraw();
    try {
      await this._client.uploadObject(DP_BUCKET, "requests/" + product.name, new Blob([]));
    } catch (err) {
      console.error("fprime-dataproducts: decode request failed", err);
      this._pending.delete(product.name);
      this.redraw();
      return;
    }
    setTimeout(() => {
      this._pending.delete(product.name);
      this.redraw();
    }, PENDING_MS);
  }

  async setAutoDecode(checked) {
    this._autoPending = { value: checked, until: Date.now() + PENDING_MS };
    try {
      await this._client.uploadObject(DP_BUCKET, "settings/auto_decode", new Blob([String(checked)]));
    } catch (err) {
      console.error("fprime-dataproducts: auto-decode toggle failed", err);
      this._autoPending = null;
      this.redraw();
    }
  }

  render() {
    const root = this.shadowRoot || this.attachShadow({ mode: "open" });
    root.innerHTML = "";
    const style = document.createElement("style");
    style.textContent = PAGE_STYLE;
    root.appendChild(style);

    const page = document.createElement("div");
    page.className = "fp-dps";

    const toolbar = document.createElement("div");
    toolbar.className = "fp-toolbar";
    const autoLabel = document.createElement("label");
    this._autoBox = document.createElement("input");
    this._autoBox.type = "checkbox";
    this._autoBox.title = "Decode every data product as it arrives (server-side setting, off after a restart)";
    this._autoBox.addEventListener("change", () => this.setAutoDecode(this._autoBox.checked));
    autoLabel.appendChild(this._autoBox);
    autoLabel.appendChild(document.createTextNode("Auto-decode new products"));
    toolbar.appendChild(autoLabel);
    this._status = document.createElement("span");
    this._status.className = "fp-status";
    toolbar.appendChild(this._status);
    page.appendChild(toolbar);

    this._tableHolder = document.createElement("div");
    this._tableHolder.className = "fp-table-holder";
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const title of ["Received", "DP Time", "File", "Container", "ID", "Priority", "Size (B)", "State", "Actions", "Error"]) {
      const th = document.createElement("th");
      th.textContent = title;
      if (title === "Actions") th.className = "fp-actions";
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);
    this._tbody = document.createElement("tbody");
    table.appendChild(this._tbody);
    this._tableHolder.appendChild(table);
    this._empty = document.createElement("div");
    this._empty.className = "fp-empty";
    this._tableHolder.appendChild(this._empty);
    page.appendChild(this._tableHolder);

    root.appendChild(page);
    this.redraw();
  }

  redraw() {
    if (!this._tbody) {
      return;
    }
    const catalog = this._catalog;
    if (!catalog) {
      this._tbody.innerHTML = "";
      this._rows.clear();
      const status = this._lastError && this._lastError.statusCode === 404 ? "not published yet" : "unreachable";
      this._status.textContent = `Catalog ${status}: is the fprime-yamcs-dataproducts processor running?`;
      this._status.className = "fp-status fp-warn";
      this._empty.textContent = "No data product catalog (bucket " + DP_BUCKET + "/" + CATALOG_OBJECT + ").";
      this._empty.style.display = "";
      this._autoBox.disabled = true;
      return;
    }
    this._autoBox.disabled = !catalog.enabled;
    const pending = this._autoPending;
    if (pending && (pending.value === !!catalog.auto_decode || Date.now() > pending.until)) {
      this._autoPending = null;
    }
    this._autoBox.checked = this._autoPending ? this._autoPending.value : !!catalog.auto_decode;
    const products = catalog.products || [];
    const decoded = products.filter((p) => p.state === "DECODED").length;
    const problem = this._parseError ? "catalog.json is not valid JSON" : catalog.processor && catalog.processor.last_error;
    this._status.className = problem ? "fp-status fp-warn" : "fp-status";
    this._status.textContent = (catalog.enabled
      ? `${products.length} products, ${decoded} decoded, queue ${catalog.queue.size}/${catalog.queue.limit}`
      : "Dictionary has no data product containers") + (problem ? ` — ${problem}` : "");
    if (!products.length) {
      this._tbody.innerHTML = "";
      this._rows.clear();
      this._empty.textContent = catalog.enabled
        ? "No data products detected yet. Files downlinked into bucket " + FILES_BUCKET +
          " whose content starts with a valid data product header are listed here."
        : "The loaded dictionary defines no data product containers.";
      this._empty.style.display = "";
      return;
    }
    this._empty.style.display = "none";
    // Newest first; rows are reused when unchanged so focus, selection and tooltips survive the polls
    const live = new Set();
    let position = 0;
    for (let i = products.length - 1; i >= 0; i--) {
      const product = products[i];
      live.add(product.name);
      const signature = JSON.stringify(product) + "|" + (this._pending.get(product.name) || "");
      let entry = this._rows.get(product.name);
      if (!entry || entry.signature !== signature) {
        const row = this.buildRow(product);
        if (entry) {
          entry.row.replaceWith(row);
        }
        entry = { signature, row };
        this._rows.set(product.name, entry);
      }
      if (this._tbody.children[position] !== entry.row) {
        this._tbody.insertBefore(entry.row, this._tbody.children[position] || null);
      }
      position++;
    }
    for (const [name, entry] of this._rows) {
      if (!live.has(name)) {
        entry.row.remove();
        this._rows.delete(name);
      }
    }
  }

  buildRow(product) {
    const row = document.createElement("tr");
    const cell = (text, className, title) => {
      const td = document.createElement("td");
      td.textContent = text;
      if (className) td.className = className;
      if (title) td.title = title;
      row.appendChild(td);
      return td;
    };
    const received = new Date(product.received);
    cell(isNaN(received) ? product.received : received.toISOString());
    cell(displayTime(product.time), null, `raw ${dpTime(product.time)}, time base ${product.time.base}`);
    cell(baseName(product.name), "fp-file", product.name);
    cell(product.short_name || "unknown", null, product.container || `container id ${product.container_id} not in dictionary`);
    cell(formatHexId(product.container_id));
    cell(String(product.priority), "fp-num");
    cell(String(product.data_size), "fp-num", `file ${product.file_size} bytes`);
    const stateCell = cell("");
    const badge = document.createElement("span");
    badge.className = "fp-badge";
    badge.style.background = STATE_COLORS[product.state] || "#adb5bd";
    badge.textContent = product.state;
    stateCell.appendChild(badge);

    const actions = document.createElement("td");
    actions.className = "fp-actions";
    const label = product.incomplete ? null : decodeLabel(product.state);
    if (label) {
      const button = document.createElement("button");
      button.textContent = label;
      button.disabled = this._pending.get(product.name) === product.state;
      button.addEventListener("click", () => this.decode(product));
      actions.appendChild(button);
    }
    if (product.state === "DECODED" || product.state === "DUPLICATE") {
      actions.appendChild(document.createTextNode(" "));
      const link = document.createElement("a");
      link.href = this._client.getObjectURL(DP_BUCKET, product.json_name);
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = "Open JSON";
      link.title = product.json_name;
      actions.appendChild(link);
    }
    actions.appendChild(document.createTextNode(" "));
    const raw = document.createElement("a");
    raw.href = this._client.getObjectURL(FILES_BUCKET, product.name);
    raw.download = baseName(product.name);
    raw.textContent = ".fdp";
    raw.title = product.name;
    actions.appendChild(raw);
    row.appendChild(actions);
    cell(shortError(product.error), "fp-error", product.error || "");
    return row;
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-dataproducts")) {
  customElements.define("fprime-dataproducts", FprimeDataProductsElement);
}
