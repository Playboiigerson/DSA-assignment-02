/*
 * Web UI for the Distributed Food Delivery Platform.
 *
 * Every call goes to /api/<service-path>, which nginx (see ../nginx.conf) forwards
 * to the matching Ballerina service inside the Docker network:
 *   /api/customers   -> customer-service:9001
 *   /api/restaurants -> restaurant-service:9002
 *   /api/orders      -> order-service:9003
 *   /api/deliveries  -> delivery-service:9005
 *   /api/admin       -> admin-service:9006
 * Payment and Notification services have no HTTP API; they only consume Kafka
 * events, so their activity is shown through the order/delivery state they change.
 */

const POLL_MS = 4000;
const ORDER_FLOW = ['CREATED', 'CONFIRMED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERED'];
const ALL_STATUSES = [...ORDER_FLOW, 'CANCELLED'];
const STATUS_LABEL = {
  CREATED: 'Created', CONFIRMED: 'Confirmed', PREPARING: 'Preparing', READY: 'Ready',
  OUT_FOR_DELIVERY: 'Out for delivery', DELIVERED: 'Delivered', CANCELLED: 'Cancelled',
};

// ---------------------------------------------------------------- helpers
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const num = (v) => {
  if (v && typeof v === 'object') v = v.$numberDecimal ?? v.$numberDouble ?? v.$numberInt ?? v.$numberLong;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};
const money = (v) => '$' + num(v).toFixed(2);
const fmtTime = (iso) => {
  if (!iso) return '—';
  const d = new Date(iso);
  return isNaN(d) ? esc(iso) : d.toLocaleString();
};
const ago = (iso) => {
  const d = new Date(iso);
  if (!iso || isNaN(d)) return '';
  const s = Math.max(0, Math.round((Date.now() - d.getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
};
const pill = (status) => `<span class="pill st-${esc(status)}">${esc(STATUS_LABEL[status] || status || '—')}</span>`;
const store = {
  get(k) { try { return localStorage.getItem(k); } catch { return null; } },
  set(k, v) { try { v == null ? localStorage.removeItem(k) : localStorage.setItem(k, v); } catch { /* ignore */ } },
};

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch('/api' + path, opts);
  } catch {
    return { ok: false, status: 0, data: null, offline: true };
  }
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  return { ok: res.ok, status: res.status, data, offline: [502, 503, 504].includes(res.status) };
}

function toast(title, msg = '', kind = 'info') {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.innerHTML = `<b>${esc(title)}</b>${esc(msg)}`;
  $('#toasts').appendChild(el);
  setTimeout(() => el.remove(), kind === 'bad' ? 7000 : 4500);
}

function offlineToast(service) {
  toast(`${service} unreachable`, 'The container may still be starting. Check `docker-compose ps`.', 'bad');
}

function windhoekHour() {
  return (new Date().getUTCHours() + 2) % 24;
}
function orderingOpen() {
  const h = windhoekHour();
  return h >= 9 && h < 21;
}

// ---------------------------------------------------------------- state
const state = {
  restaurants: [],
  orders: [],          // all orders across all restaurants
  drivers: [],
  deliveries: [],
  restaurantReports: [],
  deliveryReport: null,
  customerOrders: [],
  customer: null,
  customerId: store.get('fd.customerId') || '',
  selectedRestaurantId: store.get('fd.restaurantId') || '',
  cart: { restaurantId: null, items: [] },   // items: {itemId, name, price, quantity}
  driver: null,        // {id, password}
  driverDeliveries: [],
  health: {},
  feed: [],
  seenOrders: null,    // Map orderId -> status (null until first poll)
  seenDeliveries: null,
  flash: new Set(),
};

// ---------------------------------------------------------------- tabs
$$('.tab').forEach((btn) => btn.addEventListener('click', () => showTab(btn.dataset.tab)));
function showTab(name) {
  $$('.tab').forEach((b) => b.classList.toggle('active', b.dataset.tab === name));
  $$('.panel').forEach((p) => p.classList.toggle('active', p.id === `tab-${name}`));
  store.set('fd.tab', name);
  renderAll();
}

// ---------------------------------------------------------------- clock
function tickClock() {
  const now = new Date();
  const h = String(windhoekHour()).padStart(2, '0');
  const m = String(now.getUTCMinutes()).padStart(2, '0');
  $('#windhoekClock').textContent = `${h}:${m}`;
  const badge = $('#hoursBadge');
  if (orderingOpen()) {
    badge.className = 'badge ok';
    badge.textContent = 'Ordering open';
  } else {
    badge.className = 'badge warn';
    badge.textContent = 'Ordering closed (09:00–21:00)';
  }
}

// ---------------------------------------------------------------- polling
let polling = false;
async function refreshAll() {
  if (polling) return;
  polling = true;
  try {
    const [rests, drivers, deliveries, reports, delReport, custProbe, orderProbe] = await Promise.all([
      api('GET', '/restaurants'),
      api('GET', '/deliveries/drivers'),
      api('GET', '/deliveries'),
      api('GET', '/admin/reports/restaurants'),
      api('GET', '/admin/reports/deliveries'),
      state.customerId ? api('GET', `/customers/${encodeURIComponent(state.customerId)}`) : api('GET', '/customers/__ui_healthcheck__'),
      api('GET', '/orders/__ui_healthcheck__'),
    ]);

    if (rests.ok && Array.isArray(rests.data)) state.restaurants = rests.data;
    if (drivers.ok && Array.isArray(drivers.data)) state.drivers = drivers.data;
    if (deliveries.ok && Array.isArray(deliveries.data)) state.deliveries = deliveries.data;
    if (reports.ok && Array.isArray(reports.data)) state.restaurantReports = reports.data;
    if (delReport.ok) state.deliveryReport = delReport.data;
    else if (delReport.status === 404) state.deliveryReport = null;

    if (state.customerId) {
      if (custProbe.ok) state.customer = custProbe.data;
      else if (custProbe.status === 404) state.customer = null;
    }

    // Orders: the Order Service has no "list all" endpoint, so gather per restaurant.
    let ordersOk = !orderProbe.offline;
    if (rests.ok) {
      const lists = await Promise.all(state.restaurants.map((r) => api('GET', `/orders/restaurant/${encodeURIComponent(r.id)}`)));
      const all = [];
      lists.forEach((l) => { if (l.ok && Array.isArray(l.data)) all.push(...l.data); else if (l.offline) ordersOk = false; });
      all.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
      state.orders = all;
    }

    if (state.customerId) {
      const co = await api('GET', `/orders/customer/${encodeURIComponent(state.customerId)}`);
      if (co.ok && Array.isArray(co.data)) {
        state.customerOrders = co.data.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
      }
    }

    if (state.driver) await loadDriverDeliveries(true);

    state.health = {
      customer: healthFrom(custProbe),
      restaurant: healthFrom(rests),
      order: ordersOk ? healthFrom(orderProbe) : 'bad',
      delivery: healthFrom(drivers),
      admin: healthFrom(reports),
    };

    trackChanges();
    renderAll();
  } finally {
    polling = false;
  }
}

function healthFrom(res) {
  if (res.offline || res.status === 0) return 'bad';
  if (res.status >= 500) return 'warn';   // service up but erroring (e.g. MongoDB unavailable)
  return 'ok';                            // 2xx or 404 both prove the service answered
}

// Turn observed state changes into feed entries that mirror the Kafka topics.
function trackChanges() {
  const first = state.seenOrders === null;
  const seenO = state.seenOrders || new Map();
  const seenD = state.seenDeliveries || new Map();
  const restName = (id) => state.restaurants.find((r) => r.id === id)?.name || id;
  const driverName = (id) => state.drivers.find((d) => d.id === id)?.name || id;

  for (const o of [...state.orders].reverse()) {
    const prev = seenO.get(o.id);
    if (prev === o.status) continue;
    seenO.set(o.id, o.status);
    if (first) continue;
    state.flash.add(o.id);
    const id = o.id;
    switch (o.status) {
      case 'CREATED': pushFeed('orders.created', `Order ${id} placed at ${restName(o.restaurantId)} (${money(o.totalPrice)}). Waiting for payment…`); break;
      case 'CONFIRMED': pushFeed('payments.completed → orders.confirmed', `Payment of ${money(o.totalPrice)} succeeded for ${id}. ${restName(o.restaurantId)} notified to start preparing.`); break;
      case 'PREPARING': pushFeed('orders.preparing', `${restName(o.restaurantId)} is preparing order ${id}.`); break;
      case 'READY': pushFeed('orders.ready', `Order ${id} is ready for pickup.`); break;
      case 'OUT_FOR_DELIVERY': pushFeed('delivery.assigned', `Order ${id} is out for delivery.`); break;
      case 'DELIVERED': pushFeed('delivery.completed', `Order ${id} delivered. Enjoy your meal!`); break;
      case 'CANCELLED': pushFeed(prev === 'CREATED' || !prev ? 'payments.completed (FAILED) / orders.cancelled' : 'orders.cancelled', `Order ${id} was cancelled${prev === 'CREATED' ? ' (payment failed or cancelled before payment)' : ' and refund processed'}.`); break;
      default: pushFeed('order', `Order ${id} → ${o.status}`);
    }
  }

  for (const d of state.deliveries) {
    const key = d.id + '|' + d.orderId;
    const sig = d.status + '|' + d.driverId;
    if (seenD.get(key) === sig) continue;
    seenD.set(key, sig);
    if (first) continue;
    if (d.status === 'PENDING') pushFeed('orders.ready', `Delivery ${d.id} created for order ${d.orderId || '—'}. Waiting for an available driver.`);
    else if (d.status === 'ASSIGNED') pushFeed('delivery.assigned', `Driver ${driverName(d.driverId)} (${d.driverId}) assigned to delivery ${d.id}.`);
    else if (d.status === 'COMPLETED') pushFeed('delivery.completed', `Delivery ${d.id} completed${d.driverId ? ` by ${driverName(d.driverId)}` : ''}. Driver is available again.`);
    else if (d.status === 'CANCELLED') pushFeed('orders.cancelled', `Delivery ${d.id} cancelled; driver released.`);
  }

  state.seenOrders = seenO;
  state.seenDeliveries = seenD;
}

function pushFeed(topic, text) {
  state.feed.unshift({ at: new Date(), topic, text });
  state.feed.length = Math.min(state.feed.length, 150);
}

// ---------------------------------------------------------------- render
function activeTab() {
  return $('.tab.active')?.dataset.tab || 'dashboard';
}

function renderAll() {
  const tab = activeTab();
  if (tab === 'dashboard') renderDashboard();
  if (tab === 'customers') renderCustomers();
  if (tab === 'restaurants') renderRestaurants();
  if (tab === 'delivery') renderDelivery();
  if (tab === 'admin') renderAdmin();
  state.flash.clear();
}

// ---------- dashboard
function renderDashboard() {
  const h = state.health;
  const stuckCreated = state.orders.filter((o) => o.status === 'CREATED' && Date.now() - new Date(o.createdAt).getTime() > 20000).length;
  const paidOrders = state.orders.filter((o) => o.status !== 'CREATED').length;
  const services = [
    { name: 'Customer Service', port: 9001, s: h.customer },
    { name: 'Restaurant Service', port: 9002, s: h.restaurant },
    { name: 'Order Service', port: 9003, s: h.order },
    { name: 'Payment Service', port: null, s: stuckCreated ? 'warn' : paidOrders ? 'ok' : 'info',
      meta: stuckCreated ? `${stuckCreated} order(s) waiting >20s for payment` : 'Kafka consumer · orders.created' },
    { name: 'Delivery Service', port: 9005, s: h.delivery },
    { name: 'Notification Service', port: null, s: 'info', meta: 'Kafka consumer · logs only' },
    { name: 'Admin Service', port: 9006, s: h.admin },
  ];
  const label = { ok: 'Running', warn: 'Degraded', bad: 'Offline', info: 'Event-driven' };
  $('#healthGrid').innerHTML = services.map((x) => `
    <div class="health">
      <div class="name"><span class="sdot ${x.s || ''}"></span>${esc(x.name)}</div>
      <div class="meta">${x.s ? label[x.s] : 'Checking…'}${x.port ? ` · :${x.port}` : ''}</div>
      ${x.meta ? `<div class="meta">${esc(x.meta)}</div>` : ''}
    </div>`).join('');

  const by = Object.fromEntries(ALL_STATUSES.map((s) => [s, 0]));
  state.orders.forEach((o) => { by[o.status] = (by[o.status] || 0) + 1; });
  const active = state.orders.filter((o) => !['DELIVERED', 'CANCELLED'].includes(o.status)).length;
  const avail = state.drivers.filter((d) => d.status === 'AVAILABLE').length;
  const pending = state.deliveries.filter((d) => d.status === 'PENDING').length;
  const revenue = state.orders.filter((o) => o.status !== 'CANCELLED' && o.status !== 'CREATED').reduce((s, o) => s + num(o.totalPrice), 0);
  $('#kpis').innerHTML = [
    kpi('Restaurants', state.restaurants.length, `${state.restaurants.filter((r) => r.isOpen).length} open`),
    kpi('Orders', state.orders.length, `${active} in progress`),
    kpi('Delivered', by.DELIVERED, `${by.CANCELLED} cancelled`),
    kpi('Drivers available', `${avail}/${state.drivers.length}`, `${state.drivers.length - avail} busy`),
    kpi('Deliveries waiting', pending, pending && !avail ? 'No free driver' : 'for a driver'),
    kpi('Paid order value', money(revenue), 'excl. cancelled'),
  ].join('');

  const deliveryFor = (o) => state.deliveries.find((d) => d.orderId === o.id || d.id === o.deliveryId);
  $('#pipeline').innerHTML = ALL_STATUSES.map((s) => {
    const list = state.orders.filter((o) => o.status === s);
    const shown = list.slice(0, 12);
    return `<div class="col">
      <div class="col-head st-${s}"><span>${STATUS_LABEL[s]}</span><span class="count">${list.length}</span></div>
      ${shown.map((o) => {
        const d = deliveryFor(o);
        let sub = '';
        if (d && d.status === 'PENDING' && o.status !== 'CANCELLED') sub = 'Food ready · awaiting driver';
        else if (d && d.status === 'ASSIGNED') sub = 'Driver ' + esc(state.drivers.find((x) => x.id === d.driverId)?.name || d.driverId);
        return `<div class="ocard st-${s} ${state.flash.has(o.id) ? 'flash' : ''}">
          <div class="oid">${esc(o.id)}</div>
          <div class="meta">${esc(state.restaurants.find((r) => r.id === o.restaurantId)?.name || o.restaurantId)} · ${money(o.totalPrice)}</div>
          <div class="meta">${esc(o.customerId)} · ${ago(o.updatedAt)}</div>
          ${sub ? `<div class="meta">${sub}</div>` : ''}
        </div>`;
      }).join('')}
      ${list.length > shown.length ? `<div class="muted small">+${list.length - shown.length} more</div>` : ''}
    </div>`;
  }).join('');

  $('#feed').innerHTML = state.feed.length
    ? state.feed.map((f) => `<li><div class="t">${f.at.toLocaleTimeString()} · <span class="topic">${esc(f.topic)}</span></div>${esc(f.text)}</li>`).join('')
    : '<li class="empty">Waiting for activity… place an order to watch it flow through every service.</li>';
}

function kpi(label, value, hint) {
  return `<div class="kpi"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div><div class="hint">${esc(hint)}</div></div>`;
}

// ---------- customers
function renderCustomers() {
  const c = state.customer;
  const prof = $('#customerProfile');
  if (c) {
    prof.className = 'profile';
    prof.innerHTML = `<dl class="kv">
      <dt>Customer ID</dt><dd class="mono">${esc(c.id)}</dd>
      <dt>Name</dt><dd>${esc(c.name)}</dd>
      <dt>Email</dt><dd>${esc(c.email)}</dd>
      <dt>Address</dt><dd>${esc(c.address)}</dd>
      <dt>Balance</dt><dd class="balance">${money(c.balance)}</dd>
    </dl>
    <div class="row" style="margin-top:8px"><button class="ghost sm" id="forgetCustomer">Switch customer</button></div>`;
    $('#forgetCustomer').onclick = () => setCustomer('');
    $('#depositForm').classList.remove('hidden');
  } else {
    prof.className = 'profile empty';
    prof.textContent = state.customerId
      ? `Customer '${state.customerId}' not found.`
      : 'No customer loaded. Register one or enter an ID.';
    $('#depositForm').classList.add('hidden');
  }

  renderBrowse();
  renderCart();
  renderCustomerOrders();
}

function stockOf(r, itemId) {
  const inv = r.inventory || {};
  return num(inv[itemId]);
}

function renderBrowse() {
  const box = $('#browseRestaurants');
  // Don't yank focus away from a quantity box the user is editing.
  if (box.contains(document.activeElement) && document.activeElement.tagName === 'INPUT') return;
  // Keep typed quantities while re-rendering.
  const typed = {};
  $$('.qty', box).forEach((i) => { typed[i.dataset.key] = i.value; });

  if (!state.restaurants.length) {
    box.innerHTML = '<div class="empty">No registered restaurants found. Add one in the Restaurants tab.</div>';
    return;
  }
  box.innerHTML = state.restaurants.map((r) => `
    <div class="rest">
      <div class="rest-head">
        <div><span class="rname">${esc(r.name)}</span> <span class="muted mono">${esc(r.id)}</span>
          <div class="muted small">${esc(r.address)}</div></div>
        <span class="badge ${r.isOpen ? 'ok' : 'bad'}">${r.isOpen ? 'Open' : 'Closed'}</span>
      </div>
      ${(r.menu || []).length ? r.menu.map((m) => {
        const stock = stockOf(r, m.id);
        const key = `${r.id}::${m.id}`;
        const disabled = !r.isOpen || stock <= 0;
        return `<div class="menu-item">
          <div><div>${esc(m.name)}</div><div class="muted small mono">${esc(m.id)} · ${stock} available</div></div>
          <div class="price">${money(m.price)}</div>
          <input class="qty" type="number" min="1" max="${stock}" step="1" value="${esc(typed[key] || 1)}" data-key="${esc(key)}" ${disabled ? 'disabled' : ''}>
          <button class="sm" data-add="${esc(key)}" ${disabled ? 'disabled' : ''}>${stock <= 0 ? 'Sold out' : 'Add'}</button>
        </div>`;
      }).join('') : '<div class="menu-item"><span class="muted">(No menu items added yet)</span></div>'}
    </div>`).join('');

  $$('[data-add]', box).forEach((btn) => btn.addEventListener('click', () => {
    const [rid, iid] = btn.dataset.add.split('::');
    const qty = parseInt($(`.qty[data-key="${CSS.escape(btn.dataset.add)}"]`, box).value, 10);
    addToCart(rid, iid, qty);
  }));
}

function addToCart(restaurantId, itemId, qty) {
  if (!Number.isInteger(qty) || qty <= 0) return toast('Invalid quantity', 'Enter a positive whole number.', 'warn');
  const r = state.restaurants.find((x) => x.id === restaurantId);
  const m = r?.menu.find((x) => x.id === itemId);
  if (!m) return;
  if (state.cart.restaurantId && state.cart.restaurantId !== restaurantId && state.cart.items.length) {
    if (!confirm('An order can only contain items from one restaurant. Clear your current order and start a new one?')) return;
    state.cart.items = [];
  }
  state.cart.restaurantId = restaurantId;
  const existing = state.cart.items.find((i) => i.itemId === itemId);
  const newQty = (existing?.quantity || 0) + qty;
  if (newQty > stockOf(r, itemId)) return toast('Not enough stock', `Only ${stockOf(r, itemId)} of ${m.name} available.`, 'warn');
  if (existing) existing.quantity = newQty;
  else state.cart.items.push({ itemId, name: m.name, price: num(m.price), quantity: qty });
  renderCart();
}

function renderCart() {
  const box = $('#cart');
  const { items, restaurantId } = state.cart;
  if (!items.length) {
    box.innerHTML = '<div class="empty">Add menu items from a restaurant to build an order.</div>';
    return;
  }
  const r = state.restaurants.find((x) => x.id === restaurantId);
  const total = items.reduce((s, i) => s + i.price * i.quantity, 0);
  const bal = state.customer ? num(state.customer.balance) : null;
  box.innerHTML = `
    <div class="muted small">From <b>${esc(r?.name || restaurantId)}</b></div>
    ${items.map((i, idx) => `<div class="cart-line">
      <span>${i.quantity} × ${esc(i.name)}</span>
      <span>${money(i.price * i.quantity)} <button class="ghost sm" data-rm="${idx}" title="Remove">✕</button></span>
    </div>`).join('')}
    <div class="cart-total"><span>Total</span><span>${money(total)}</span></div>
    ${!state.customer ? '<div class="note warn">Load or register a customer first.</div>' : ''}
    ${bal !== null && bal < total ? `<div class="note warn">Balance ${money(bal)} is below the total.</div>` : ''}
    ${!orderingOpen() ? '<div class="note warn">Orders are only accepted 09:00–21:00 Windhoek time.</div>' : ''}
    <div class="row"><button id="placeOrder" ${state.customer ? '' : 'disabled'}>Place order</button>
      <button class="ghost" id="clearCart">Clear</button></div>`;
  $$('[data-rm]', box).forEach((b) => b.addEventListener('click', () => { items.splice(+b.dataset.rm, 1); renderCart(); }));
  $('#clearCart').onclick = () => { state.cart = { restaurantId: null, items: [] }; renderCart(); };
  $('#placeOrder').onclick = placeOrder;
}

async function placeOrder() {
  const btn = $('#placeOrder');
  btn.disabled = true;
  const payload = {
    customerId: state.customer.id,
    restaurantId: state.cart.restaurantId,
    items: state.cart.items.map((i) => ({ itemId: i.itemId, quantity: i.quantity })),
  };
  const res = await api('POST', '/orders', payload);
  if (res.ok) {
    toast('Order placed', `Order ${res.data.id} · Delivery ${res.data.deliveryId} · ${money(res.data.totalPrice)}`, 'ok');
    state.cart = { restaurantId: null, items: [] };
    refreshAll();
  } else if (res.offline) {
    offlineToast('Order Service');
  } else if (res.status === 404) {
    toast('Order failed', 'Customer ID or Restaurant ID was not found.', 'bad');
  } else if (res.status === 400) {
    toast('Order rejected', 'Orders can only be placed 09:00–21:00 (Windhoek). Otherwise check the restaurant is open, item stock, and customer balance.', 'bad');
  } else {
    toast('Order failed', `Server returned status ${res.status}.`, 'bad');
  }
  renderCart();
}

function progress(status) {
  if (status === 'CANCELLED') return '';
  const idx = ORDER_FLOW.indexOf(status);
  return `<div class="progress st-${esc(status)}">${ORDER_FLOW.map((_, i) => `<span class="${i <= idx ? 'on' : ''}"></span>`).join('')}</div>`;
}

function itemsText(o) {
  return (o.items || []).map((i) => `${num(i.quantity)} × ${esc(i.itemId)} @ ${money(i.price)}`).join('<br>');
}

function renderCustomerOrders() {
  const box = $('#customerOrders');
  if (!state.customer) {
    box.className = 'empty';
    box.textContent = 'Load a customer to see their orders.';
    return;
  }
  if (!state.customerOrders.length) {
    box.className = 'empty';
    box.textContent = `No past order history found for Customer ID '${state.customer.id}'.`;
    return;
  }
  box.className = 'table-wrap';
  box.innerHTML = `<table>
    <thead><tr><th>Order</th><th>Restaurant</th><th>Items</th><th class="num">Total</th><th>Status</th><th>Placed</th><th></th></tr></thead>
    <tbody>${state.customerOrders.map((o) => `<tr>
      <td><div class="mono">${esc(o.id)}</div><div class="muted small mono">${esc(o.deliveryId)}</div></td>
      <td>${esc(state.restaurants.find((r) => r.id === o.restaurantId)?.name || o.restaurantId)}</td>
      <td class="small">${itemsText(o)}</td>
      <td class="num">${money(o.totalPrice)}</td>
      <td>${pill(o.status)}${progress(o.status)}<div class="muted small">${ago(o.updatedAt)}</div></td>
      <td class="small">${fmtTime(o.createdAt)}</td>
      <td>${['DELIVERED', 'CANCELLED'].includes(o.status) ? '' : `<button class="danger sm" data-cancel="${esc(o.id)}">Cancel</button>`}</td>
    </tr>`).join('')}</tbody></table>`;
  $$('[data-cancel]', box).forEach((b) => b.addEventListener('click', () => cancelOrder(b.dataset.cancel)));
}

async function cancelOrder(id) {
  if (!confirm(`Cancel order ${id}? The customer is refunded, stock is restored and any driver is released.`)) return;
  const res = await api('POST', `/orders/${encodeURIComponent(id)}/cancel`);
  if (res.ok) toast('Order cancelled', 'Balance refunded, restaurant stock restored, driver released.', 'ok');
  else if (res.offline) offlineToast('Order Service');
  else if (res.status === 404) toast('Not found', `Order '${id}' not found.`, 'bad');
  else if (res.status === 400) toast('Cannot cancel', 'The order has already been delivered.', 'bad');
  else toast('Cancel failed', `Status ${res.status}`, 'bad');
  refreshAll();
}

async function setCustomer(id) {
  state.customerId = id;
  state.customer = null;
  state.customerOrders = [];
  store.set('fd.customerId', id || null);
  if (id) {
    const res = await api('GET', `/customers/${encodeURIComponent(id)}`);
    if (res.ok) state.customer = res.data;
    else if (res.offline) offlineToast('Customer Service');
    else if (res.status === 404) toast('Not found', `Customer ID '${id}' not found.`, 'bad');
  }
  renderCustomers();
  refreshAll();
}

$('#loadCustomerForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const id = e.target.id.value.trim();
  if (id) setCustomer(id);
});

$('#registerCustomerForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const balance = Number(f.balance.value);
  if (!(balance >= 0)) return toast('Invalid balance', 'Starting balance cannot be negative.', 'warn');
  const res = await api('POST', '/customers', {
    id: '', name: f.name.value.trim(), email: f.email.value.trim(), address: f.address.value.trim(), balance,
  });
  if (res.ok) {
    toast('Customer registered', `ID ${res.data.id} · ${res.data.name}`, 'ok');
    f.reset();
    f.balance.value = 0;
    setCustomer(res.data.id);
  } else if (res.offline) offlineToast('Customer Service');
  else if (res.status === 409) toast('Email already registered', 'Email addresses must be unique.', 'bad');
  else if (res.status === 400) toast('Invalid data', 'Check the email format; balance cannot be negative.', 'bad');
  else toast('Registration failed', `Status ${res.status}`, 'bad');
});

$('#depositForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const amount = Number(e.target.amount.value);
  if (!(amount > 0)) return toast('Invalid amount', 'Enter a positive amount.', 'warn');
  const res = await api('POST', `/customers/${encodeURIComponent(state.customer.id)}/add-balance`, { amount });
  if (res.ok) { toast('Funds deposited', `${money(amount)} added.`, 'ok'); e.target.reset(); setCustomer(state.customer.id); }
  else if (res.offline) offlineToast('Customer Service');
  else toast('Deposit failed', `Status ${res.status}`, 'bad');
});

$('#trackForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const id = e.target.id.value.trim();
  const box = $('#trackResult');
  const res = await api('GET', `/orders/${encodeURIComponent(id)}`);
  if (res.ok) {
    const o = res.data;
    box.innerHTML = `<dl class="kv" style="margin-top:12px">
      <dt>Order ID</dt><dd class="mono">${esc(o.id)}</dd>
      <dt>Delivery ID</dt><dd class="mono">${esc(o.deliveryId)}</dd>
      <dt>Customer</dt><dd class="mono">${esc(o.customerId)}</dd>
      <dt>Restaurant</dt><dd>${esc(state.restaurants.find((r) => r.id === o.restaurantId)?.name || o.restaurantId)}</dd>
      <dt>Status</dt><dd>${pill(o.status)}${progress(o.status)}</dd>
      <dt>Total</dt><dd>${money(o.totalPrice)}</dd>
      <dt>Items</dt><dd>${itemsText(o)}</dd>
      <dt>Placed</dt><dd>${fmtTime(o.createdAt)}</dd>
      <dt>Last updated</dt><dd>${fmtTime(o.updatedAt)}</dd>
    </dl>`;
  } else if (res.offline) { box.innerHTML = ''; offlineToast('Order Service'); }
  else if (res.status === 404) box.innerHTML = `<p class="empty">Order or Delivery ID '${esc(id)}' not found.</p>`;
  else box.innerHTML = `<p class="empty">Error: status ${res.status}</p>`;
});

// ---------- restaurants
function renderRestaurants() {
  const sel = $('#restaurantSelect');
  const ids = state.restaurants.map((r) => r.id).join(',');
  if (sel.dataset.ids !== ids) {
    sel.innerHTML = '<option value="">— select —</option>' +
      state.restaurants.map((r) => `<option value="${esc(r.id)}">${esc(r.name)} (${esc(r.id)})</option>`).join('');
    sel.dataset.ids = ids;
  }
  sel.value = state.restaurants.some((r) => r.id === state.selectedRestaurantId) ? state.selectedRestaurantId : '';

  const r = state.restaurants.find((x) => x.id === sel.value);
  const prof = $('#restaurantProfile');
  $('#restaurantManage').classList.toggle('hidden', !r);
  if (!r) {
    prof.className = 'profile empty';
    prof.textContent = 'Select a restaurant to manage its menu, stock and orders.';
    return;
  }
  prof.className = 'profile';
  prof.innerHTML = `<dl class="kv">
      <dt>ID</dt><dd class="mono">${esc(r.id)}</dd>
      <dt>Address</dt><dd>${esc(r.address)}</dd>
      <dt>Status</dt><dd><span class="badge ${r.isOpen ? 'ok' : 'bad'}">${r.isOpen ? 'Open' : 'Closed'}</span></dd>
    </dl>
    <div class="table-wrap" style="margin-top:12px"><table>
      <thead><tr><th>Menu item</th><th>ID</th><th class="num">Price</th><th class="num">Stock</th></tr></thead>
      <tbody>${(r.menu || []).length ? r.menu.map((m) => `<tr>
        <td>${esc(m.name)}</td><td class="mono">${esc(m.id)}</td>
        <td class="num">${money(m.price)}</td><td class="num">${stockOf(r, m.id)}</td></tr>`).join('')
        : '<tr><td colspan="4" class="muted">No menu items yet.</td></tr>'}</tbody>
    </table></div>`;

  const restockSel = $('#restockItem');
  const menuIds = (r.menu || []).map((m) => m.id).join(',');
  if (restockSel.dataset.ids !== r.id + ':' + menuIds) {
    const prev = restockSel.value;
    restockSel.innerHTML = (r.menu || []).map((m) => `<option value="${esc(m.id)}">${esc(m.name)} (${esc(m.id)})</option>`).join('');
    restockSel.dataset.ids = r.id + ':' + menuIds;
    if ((r.menu || []).some((m) => m.id === prev)) restockSel.value = prev;
  }

  const orders = state.orders.filter((o) => o.restaurantId === r.id);
  const box = $('#restaurantOrders');
  if (!orders.length) {
    box.innerHTML = `<p class="empty">No orders found for ${esc(r.name)}.</p>`;
    return;
  }
  box.innerHTML = `<div class="table-wrap"><table>
    <thead><tr><th>Order</th><th>Customer</th><th>Items</th><th class="num">Total</th><th>Status</th><th>Update status</th></tr></thead>
    <tbody>${orders.map((o) => {
      const done = ['DELIVERED', 'CANCELLED'].includes(o.status);
      return `<tr>
        <td><div class="mono">${esc(o.id)}</div><div class="muted small mono">${esc(o.deliveryId)}</div></td>
        <td class="mono">${esc(o.customerId)}</td>
        <td class="small">${itemsText(o)}</td>
        <td class="num">${money(o.totalPrice)}</td>
        <td>${pill(o.status)}<div class="muted small">${ago(o.updatedAt)}</div></td>
        <td>${done ? '' : `<div class="actions">
          ${['PREPARING', 'READY', 'OUT_FOR_DELIVERY'].map((s) => `<button class="ghost sm" data-status="${s}" data-order="${esc(o.id)}" ${o.status === s ? 'disabled' : ''}>${STATUS_LABEL[s]}</button>`).join('')}
        </div>`}</td>
      </tr>`;
    }).join('')}</tbody></table></div>`;
  $$('[data-status]', box).forEach((b) => b.addEventListener('click', () => updateOrderStatus(r.id, b.dataset.order, b.dataset.status)));
}

async function updateOrderStatus(restId, orderId, status) {
  const res = await api('POST', `/restaurants/${encodeURIComponent(restId)}/orders/${encodeURIComponent(orderId)}/status`, { status });
  if (res.ok) toast('Status updated', `Order ${orderId} → ${STATUS_LABEL[status]}`, 'ok');
  else if (res.offline) offlineToast('Restaurant Service');
  else if (res.status === 404) toast('Not found', `Order ${orderId} not found.`, 'bad');
  else toast('Update failed', `Status ${res.status}`, 'bad');
  refreshAll();
}

$('#restaurantSelect').addEventListener('change', (e) => {
  state.selectedRestaurantId = e.target.value;
  store.set('fd.restaurantId', e.target.value || null);
  renderRestaurants();
});

$('#registerRestaurantForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const res = await api('POST', '/restaurants', {
    id: '', name: f.name.value.trim(), address: f.address.value.trim(), menu: [], inventory: {}, isOpen: f.isOpen.checked,
  });
  if (res.ok) {
    toast('Restaurant registered', `Assigned ID ${res.data.id}`, 'ok');
    f.reset();
    f.isOpen.checked = true;
    state.selectedRestaurantId = res.data.id;
    store.set('fd.restaurantId', res.data.id);
    await refreshAll();
  } else if (res.offline) offlineToast('Restaurant Service');
  else if (res.status === 409) toast('Conflict', 'A restaurant with this ID already exists.', 'bad');
  else toast('Registration failed', `Status ${res.status}`, 'bad');
});

$('#addMenuForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const price = Number(f.price.value);
  if (!(price > 0)) return toast('Invalid price', 'Enter a positive price.', 'warn');
  const id = f.id.value.trim();
  const rid = state.selectedRestaurantId;
  if (state.restaurants.find((r) => r.id === rid)?.menu.some((m) => m.id === id)) {
    return toast('Duplicate item ID', `Menu item '${id}' already exists in this restaurant.`, 'warn');
  }
  const res = await api('POST', `/restaurants/${encodeURIComponent(rid)}/menu`, [{ id, name: f.name.value.trim(), price }]);
  if (res.ok) { toast('Menu item added', `${f.name.value} (${id}). Stock starts at 0 — restock it next.`, 'ok'); f.reset(); refreshAll(); }
  else if (res.offline) offlineToast('Restaurant Service');
  else if (res.status === 404) toast('Not found', 'Restaurant not found.', 'bad');
  else toast('Failed to add item', `Status ${res.status}`, 'bad');
});

$('#restockForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const qty = parseInt(f.qty.value, 10);
  if (!(qty > 0)) return toast('Invalid quantity', 'Enter a positive whole number.', 'warn');
  const itemId = f.itemId.value;
  if (!itemId) return toast('No menu item', 'Add a menu item first.', 'warn');
  const res = await api('POST', `/restaurants/${encodeURIComponent(state.selectedRestaurantId)}/inventory`, { items: { [itemId]: qty } });
  if (res.ok) { toast('Inventory updated', `+${qty} ${itemId}`, 'ok'); f.qty.value = ''; refreshAll(); }
  else if (res.offline) offlineToast('Restaurant Service');
  else toast('Restock failed', `Status ${res.status}`, 'bad');
});

// ---------- delivery
function renderDelivery() {
  const drivers = $('#driversTable');
  drivers.className = state.drivers.length ? 'table-wrap' : 'empty';
  drivers.innerHTML = state.drivers.length ? `<table>
    <thead><tr><th>Driver</th><th>Status</th><th>Set status</th></tr></thead>
    <tbody>${state.drivers.map((d) => `<tr>
      <td>${esc(d.name)}<div class="muted small mono">${esc(d.id)}</div></td>
      <td>${pill(d.status)}</td>
      <td><div class="actions">${['AVAILABLE', 'BUSY'].map((s) => `<button class="ghost sm" data-drv="${esc(d.id)}" data-st="${s}" ${d.status === s ? 'disabled' : ''}>${s === 'AVAILABLE' ? 'Available' : 'Busy'}</button>`).join('')}</div></td>
    </tr>`).join('')}</tbody></table>` : 'No registered delivery drivers found.';
  $$('[data-drv]', drivers).forEach((b) => b.addEventListener('click', () => setDriverStatus(b.dataset.drv, b.dataset.st)));

  const dels = [...state.deliveries].reverse();
  const box = $('#deliveriesTable');
  box.className = dels.length ? 'table-wrap' : 'empty';
  box.innerHTML = dels.length ? `<table>
    <thead><tr><th>Delivery</th><th>Order</th><th>Driver</th><th>Status</th><th>Assigned</th><th>Completed</th></tr></thead>
    <tbody>${dels.map((d) => `<tr>
      <td class="mono">${esc(d.id)}</td>
      <td class="mono">${esc(d.orderId || '—')}</td>
      <td>${d.driverId ? esc(state.drivers.find((x) => x.id === d.driverId)?.name || d.driverId) : '<span class="muted">(Unassigned)</span>'}</td>
      <td>${pill(d.status)}</td>
      <td class="small">${d.assignedAt ? fmtTime(d.assignedAt) : 'N/A'}</td>
      <td class="small">${d.completedAt ? fmtTime(d.completedAt) : 'N/A'}</td>
    </tr>`).join('')}</tbody></table>` : 'No active or pending deliveries found.';

  renderDriverPortal();
}

function renderDriverPortal() {
  const box = $('#driverDeliveries');
  $('#driverLogout').classList.toggle('hidden', !state.driver);
  if (!state.driver) { box.innerHTML = ''; return; }
  const list = state.driverDeliveries;
  const name = state.drivers.find((d) => d.id === state.driver.id)?.name || state.driver.id;
  if (!list.length) {
    box.innerHTML = `<p class="empty">No deliveries assigned to ${esc(name)} yet.</p>`;
    return;
  }
  box.innerHTML = `<p class="muted small" style="margin-top:12px">Signed in as <b>${esc(name)}</b>. Instructions: pick up the order from the restaurant and deliver it to the customer address.</p>
    <div class="table-wrap"><table>
    <thead><tr><th>Delivery</th><th>Order</th><th>Status</th><th>Assigned</th><th></th></tr></thead>
    <tbody>${[...list].reverse().map((d) => `<tr>
      <td class="mono">${esc(d.id)}</td><td class="mono">${esc(d.orderId)}</td>
      <td>${pill(d.status)}</td><td class="small">${fmtTime(d.assignedAt)}</td>
      <td>${d.status === 'ASSIGNED' ? `<button class="sm" data-complete="${esc(d.id)}">Mark delivered</button>` : ''}</td>
    </tr>`).join('')}</tbody></table></div>`;
  $$('[data-complete]', box).forEach((b) => b.addEventListener('click', () => completeDelivery(b.dataset.complete, state.driver.password)));
}

async function loadDriverDeliveries(silent) {
  const { id, password } = state.driver;
  const res = await api('GET', `/deliveries/driver/${encodeURIComponent(id)}?password=${encodeURIComponent(password)}`);
  if (res.ok && Array.isArray(res.data)) { state.driverDeliveries = res.data; return true; }
  if (!silent) {
    if (res.offline) offlineToast('Delivery Service');
    else if (res.status === 401) toast('Sign-in failed', `Incorrect password for driver '${id}'.`, 'bad');
    else toast('Error', `Status ${res.status}`, 'bad');
  }
  return false;
}

async function setDriverStatus(id, status) {
  const res = await api('PUT', `/deliveries/drivers/${encodeURIComponent(id)}/status`, { status });
  if (res.ok) toast('Driver updated', `${id} → ${status}${status === 'AVAILABLE' ? ' (pending deliveries will be auto-assigned)' : ''}`, 'ok');
  else if (res.offline) offlineToast('Delivery Service');
  else toast('Update failed', `Status ${res.status}`, 'bad');
  refreshAll();
}

async function completeDelivery(id, password) {
  const res = await api('POST', `/deliveries/${encodeURIComponent(id)}/complete`, { password });
  if (res.ok) toast('Delivery completed', 'Driver is now available for new orders.', 'ok');
  else if (res.offline) offlineToast('Delivery Service');
  else if (res.status === 401) toast('Incorrect password', 'The driver password does not match.', 'bad');
  else if (res.status === 400) toast('Already completed', `Delivery ${id} was already completed.`, 'warn');
  else if (res.status === 404) toast('Not found', `Delivery '${id}' not found.`, 'bad');
  else toast('Failed', `Status ${res.status}`, 'bad');
  refreshAll();
}

$('#registerDriverForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  if (f.password.value !== f.confirm.value) return toast('Passwords do not match', 'Please try again.', 'warn');
  const res = await api('POST', '/deliveries/drivers', { id: '', name: f.name.value.trim(), password: f.password.value, status: 'AVAILABLE' });
  if (res.ok) {
    toast('Driver registered', `Driver ID ${res.data.id} · ${res.data.status}`, 'ok');
    $('#driverLoginForm').id.value = res.data.id;
    f.reset();
    refreshAll();
  } else if (res.offline) offlineToast('Delivery Service');
  else toast('Registration failed', `Status ${res.status}`, 'bad');
});

$('#driverLoginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  state.driver = { id: f.id.value.trim(), password: f.password.value };
  if (await loadDriverDeliveries(false)) renderDriverPortal();
  else { state.driver = null; renderDriverPortal(); }
});

$('#driverLogout').addEventListener('click', () => {
  state.driver = null;
  state.driverDeliveries = [];
  $('#driverLoginForm').reset();
  renderDriverPortal();
});

$('#completeForm').addEventListener('submit', (e) => {
  e.preventDefault();
  completeDelivery(e.target.id.value.trim(), e.target.password.value);
  e.target.reset();
});

// ---------- admin
function renderAdmin() {
  const d = state.deliveryReport;
  $('#deliveryStats').innerHTML = d
    ? [
      kpi('Completed deliveries', num(d.totalDeliveries), 'all drivers'),
      kpi('Total delivery time', `${num(d.totalDeliveryTimeSeconds)}s`, 'assigned → completed'),
      kpi('Average delivery time', `${num(d.avgDeliveryTimeSeconds).toFixed(1)}s`, 'per delivery'),
    ].join('')
    : kpi('Delivery statistics', '—', 'Updates automatically as drivers complete deliveries');

  const box = $('#restaurantReports');
  const reps = state.restaurantReports;
  if (!reps.length) {
    box.innerHTML = '<p class="empty">No restaurant performance reports compiled yet. Reports update automatically as orders are confirmed and prepared.</p>';
    return;
  }
  box.innerHTML = `<div class="table-wrap"><table>
    <thead><tr><th>Restaurant</th><th class="num">Orders prepared</th><th class="num">Revenue</th><th class="num">Total prep time</th><th class="num">Avg prep time</th></tr></thead>
    <tbody>${reps.map((r) => `<tr>
      <td>${esc(state.restaurants.find((x) => x.id === r.id)?.name || r.id)}<div class="muted small mono">${esc(r.id)}</div></td>
      <td class="num">${num(r.totalOrders)}</td>
      <td class="num">${money(r.totalRevenue)}</td>
      <td class="num">${num(r.totalPrepTimeSeconds)}s</td>
      <td class="num">${num(r.avgPrepTimeSeconds).toFixed(1)}s</td>
    </tr>`).join('')}</tbody></table></div>`;
}

// ---------------------------------------------------------------- boot
let timer = null;
function startPolling() {
  clearInterval(timer);
  timer = setInterval(() => { if (!document.hidden) refreshAll(); }, POLL_MS);
}
document.addEventListener('visibilitychange', () => {
  $('#liveIndicator').classList.toggle('paused', document.hidden);
  if (!document.hidden) refreshAll();
});

tickClock();
setInterval(tickClock, 15000);
const savedTab = store.get('fd.tab');
if (savedTab && $(`.tab[data-tab="${savedTab}"]`)) showTab(savedTab);
renderAll();
refreshAll();
startPolling();
