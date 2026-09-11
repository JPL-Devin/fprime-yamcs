/**
 * fprime-dataflow.js:
 *
 * Yamcs web extension providing the GDS-style data-flow orb: an indicator
 * in the yamcs-web top toolbar (left of STORAGE) that turns into a green
 * orb while telemetry or events are flowing and into a red X once neither
 * has been seen for DATA_TIMEOUT_MS (mirroring the orb in the fprime-gds UI).
 * If the toolbar cannot be located it floats in the bottom-right corner.
 *
 * Telemetry flow is detected from the processor's TM statistics stream
 * (received-packet count deltas); event flow from the event subscription.
 *
 * The <fprime-dataflow-orb> element is mounted by the <fprime-yamcs>
 * initializer defined in fprime-events.js via FprimeDataflowOrbElement.mount.
 */

// Matches the fprime-gds default (config_init.js dataTimeout: 5 seconds)
const DATA_TIMEOUT_MS = 5000;

const ORB_SIZE = 24;

// yamcs-web app bar (app.component.html): the orb goes before the right-hand
// tab nav so it sits left of STORAGE
const TOOLBAR_SELECTOR = "mat-toolbar-row.app-bar";
const TOOLBAR_NAV_SELECTOR = "nav.top-tabs";

const ORB_STYLE = `
  :host {
    position: relative;
    display: inline-flex;
    align-items: center;
    flex: 0 0 auto;
    height: 100%;
    padding: 0 16px;
    box-sizing: border-box;
    font-family: Roboto, sans-serif;
    cursor: default;
  }
  :host([placement="floating"]) {
    position: fixed;
    right: 16px;
    bottom: 16px;
    z-index: 10000;
    height: auto;
    padding: 0;
  }
  .orb {
    width: ${ORB_SIZE}px;
    height: ${ORB_SIZE}px;
    border-radius: 50%;
    box-sizing: border-box;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: bold;
    font-size: ${Math.round(ORB_SIZE * 0.6)}px;
    line-height: 1;
    user-select: none;
    color: #fff;
  }
  /* Green orb: data flowing (fprime-gds success.svg color) */
  .orb.flowing {
    background: #4caf50;
    box-shadow: 0 0 6px 2px rgba(76, 175, 80, 0.6);
    animation: fp-orb-pulse 2s ease-in-out infinite;
  }
  /* Red X: no data flow (fprime-gds error.svg) */
  .orb.stale {
    background: #ff0000;
    border-radius: 4px;
  }
  /* Grey: no instance selected, nothing to monitor */
  .orb.idle {
    background: #9e9e9e;
    opacity: 0.6;
  }
  @keyframes fp-orb-pulse {
    0%, 100% { box-shadow: 0 0 4px 1px rgba(76, 175, 80, 0.4); }
    50% { box-shadow: 0 0 10px 4px rgba(76, 175, 80, 0.8); }
  }
  .detail {
    position: absolute;
    right: 0;
    top: 100%;
    display: none;
    z-index: 10000;
    background: rgba(0, 0, 0, 0.85);
    color: #fff;
    font-size: 12px;
    border-radius: 4px;
    padding: 6px 10px;
    white-space: nowrap;
  }
  :host([placement="floating"]) .detail {
    top: auto;
    bottom: ${ORB_SIZE + 8}px;
  }
  :host(:hover) .detail {
    display: block;
  }
`;

class FprimeDataflowOrbElement extends HTMLElement {
  // Mounts the singleton orb left of the toolbar's tab nav (floating fallback);
  // the nav renders asynchronously, so the toolbar is observed to keep it in place.
  static mount(service) {
    if (document.querySelector("fprime-dataflow-orb")) {
      return;
    }
    const orb = document.createElement("fprime-dataflow-orb");
    const toolbar = document.querySelector(TOOLBAR_SELECTOR);
    if (!toolbar) {
      orb.setAttribute("placement", "floating");
      document.body.appendChild(orb);
      orb.extensionService = service;
      return;
    }
    orb.setAttribute("placement", "toolbar");
    const place = () => {
      const nav = toolbar.querySelector(TOOLBAR_NAV_SELECTOR);
      if (nav && orb.nextElementSibling !== nav) {
        toolbar.insertBefore(orb, nav);
      } else if (!nav && orb.parentElement !== toolbar) {
        toolbar.appendChild(orb);
      }
    };
    place();
    new MutationObserver(place).observe(toolbar, { childList: true });
    orb.extensionService = service;
  }

  constructor() {
    super();
    this._service = null;
    this._connectionSubscription = null;
    this._eventSubscription = null;
    this._statsSubscription = null;
    this._instance = null;
    this._context = null;
    this._flags = { telemetry: false, events: false };
    this._timeouts = { telemetry: null, events: null };
    this._receivedPackets = null;
  }

  set extensionService(service) {
    this._service = service;
    this.render();
    this.subscribe();
  }

  // Subscriptions live only while the element is attached to the document, so
  // moving the node (disconnect + reconnect) transparently re-subscribes.
  connectedCallback() {
    this.subscribe();
  }

  disconnectedCallback() {
    if (this._connectionSubscription) {
      this._connectionSubscription.unsubscribe();
      this._connectionSubscription = null;
    }
    this.disconnect();
    this._context = null;
  }

  subscribe() {
    if (!this._service || !this.isConnected || this._connectionSubscription) {
      return;
    }
    // Re-subscribe whenever the instance/processor context changes
    this._connectionSubscription = this._service.yamcs.connectionInfo$.subscribe(
      (info) => this.connect(info),
    );
  }

  disconnect() {
    for (const subscription of [this._eventSubscription, this._statsSubscription]) {
      if (subscription) {
        subscription.cancel();
      }
    }
    this._eventSubscription = null;
    this._statsSubscription = null;
    for (const key in this._timeouts) {
      clearTimeout(this._timeouts[key]);
      this._timeouts[key] = null;
    }
  }

  connect(connectionInfo) {
    const instance = connectionInfo?.instance?.name || null;
    const processor = connectionInfo?.processor?.name || "realtime";
    const context = instance ? `${instance}/${processor}` : null;
    if (context === this._context) {
      return;
    }
    this.disconnect();
    this._instance = instance;
    this._context = context;
    this._flags = { telemetry: false, events: false };
    this._receivedPackets = null;
    if (!instance) {
      this.update();
      return;
    }
    const client = this._service.yamcs.yamcsClient;
    this._eventSubscription = client.createEventSubscription(
      { instance },
      () => this.bump("events"),
    );
    this._statsSubscription = client.createTMStatisticsSubscription(
      { instance, processor },
      (statistics) => this.processStatistics(statistics),
    );
    this.update();
  }

  /** Marks telemetry active when the total received-packet count grows */
  processStatistics(statistics) {
    let total = 0;
    for (const entry of statistics.tmstats || []) {
      total += Number(entry.receivedPackets || 0);
    }
    const previous = this._receivedPackets;
    this._receivedPackets = total;
    if (previous !== null && total > previous) {
      this.bump("telemetry");
    }
  }

  /** Marks a flow active, and schedules it stale after the data timeout */
  bump(key) {
    this._flags[key] = true;
    clearTimeout(this._timeouts[key]);
    this._timeouts[key] = setTimeout(() => {
      this._flags[key] = false;
      this.update();
    }, DATA_TIMEOUT_MS);
    this.update();
  }

  render() {
    const root = this.shadowRoot || this.attachShadow({ mode: "open" });
    root.innerHTML = "";

    const style = document.createElement("style");
    style.textContent = ORB_STYLE;
    root.appendChild(style);

    this._orb = document.createElement("div");
    this._orb.className = "orb";
    root.appendChild(this._orb);

    this._detail = document.createElement("div");
    this._detail.className = "detail";
    root.appendChild(this._detail);

    this.update();
  }

  update() {
    if (!this._orb) {
      return;
    }
    const flowing = this._flags.telemetry || this._flags.events;
    if (!this._instance) {
      this._orb.className = "orb idle";
      this._orb.textContent = "";
      this._detail.textContent = "F´ data flow: no instance selected";
    } else if (flowing) {
      this._orb.className = "orb flowing";
      this._orb.textContent = "";
      this._detail.textContent = this.detailText();
    } else {
      this._orb.className = "orb stale";
      this._orb.textContent = "\u2715";
      this._detail.textContent = this.detailText();
    }
  }

  detailText() {
    const describe = (active) => (active ? "flowing" : "none");
    return (
      `F´ data flow — telemetry: ${describe(this._flags.telemetry)}, ` +
      `events: ${describe(this._flags.events)}`
    );
  }
}

// Guarded: a stale bundle double-load must not throw on re-registration
if (!customElements.get("fprime-dataflow-orb")) {
  customElements.define("fprime-dataflow-orb", FprimeDataflowOrbElement);
}
