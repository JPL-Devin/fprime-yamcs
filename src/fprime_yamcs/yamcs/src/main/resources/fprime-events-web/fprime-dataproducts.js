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
const POLL_MS = 1000;
const PENDING_MS = 3000;
const TB_WORKSTATION = 2;

const STATE_COLORS = {
  DETECTED: "#6c757d",
  QUEUED: "#17a2b8",
  DECODING: "#17a2b8",
  DECODED: "#28a745",
  FAILED: "#dc3545",
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
    white-space: normal;
    max-width: 320px;
    color: #dc3545;
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
    this._lastError = null;
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
    try {
      const response = await this._client.getObject(DP_BUCKET, CATALOG_OBJECT);
      this._catalog = await response.json();
      this._lastError = null;
    } catch (err) {
      this._catalog = null;
      this._lastError = err;
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
    try {
      await this._client.uploadObject(DP_BUCKET, "settings/auto_decode", new Blob([String(checked)]));
    } catch (err) {
      console.error("fprime-dataproducts: auto-decode toggle failed", err);
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
    for (const title of ["Received", "DP Time", "Container", "ID", "Priority", "Size (B)", "State", "Error", "Actions"]) {
      const th = document.createElement("th");
      th.textContent = title;
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
    this._tbody.innerHTML = "";
    if (!catalog) {
      const status = this._lastError && this._lastError.statusCode === 404 ? "not published yet" : "unreachable";
      this._status.textContent = `Catalog ${status}: is the fprime-yamcs-dataproducts processor running?`;
      this._status.className = "fp-status fp-warn";
      this._empty.textContent = "No data product catalog (bucket " + DP_BUCKET + "/" + CATALOG_OBJECT + ").";
      this._empty.style.display = "";
      this._autoBox.disabled = true;
      return;
    }
    this._autoBox.disabled = !catalog.enabled;
    if (document.activeElement !== this._autoBox) {
      this._autoBox.checked = !!catalog.auto_decode;
    }
    const products = catalog.products || [];
    const decoded = products.filter((p) => p.state === "DECODED").length;
    this._status.className = "fp-status";
    this._status.textContent = catalog.enabled
      ? `${products.length} products, ${decoded} decoded, queue ${catalog.queue.size}/${catalog.queue.limit}`
      : "Dictionary has no data product containers";
    if (!products.length) {
      this._empty.textContent = catalog.enabled
        ? "No data products detected yet. Files downlinked into bucket " + FILES_BUCKET +
          " whose content starts with a valid data product header are listed here."
        : "The loaded dictionary defines no data product containers.";
      this._empty.style.display = "";
      return;
    }
    this._empty.style.display = "none";
    // Newest first, like the Events page in follow mode reversed
    for (let i = products.length - 1; i >= 0; i--) {
      this._tbody.appendChild(this.buildRow(products[i]));
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
    cell(shortError(product.error), "fp-error", product.error || "");

    const actions = document.createElement("td");
    const label = decodeLabel(product.state);
    if (label) {
      const button = document.createElement("button");
      button.textContent = label;
      button.disabled = this._pending.get(product.name) === product.state;
      button.addEventListener("click", () => this.decode(product));
      actions.appendChild(button);
    }
    if (product.state === "DECODED") {
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
    raw.download = product.name.split("/").pop().replace(/^\.+/, "");
    raw.textContent = ".fdp";
    raw.title = product.name;
    actions.appendChild(raw);
    row.appendChild(actions);
    return row;
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-dataproducts")) {
  customElements.define("fprime-dataproducts", FprimeDataProductsElement);
}
