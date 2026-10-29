/* VainGlory ドラフト: ルームの進行役(サーバー側)。
   Vercel の API 関数 + Upstash Redis(REST)だけで動きます。追加パッケージは不要です。
   環境変数(Vercel の Upstash Redis 連携が自動で入れます):
     UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN  または  KV_REST_API_URL / KV_REST_API_TOKEN
   クライアントは 1秒ちょっと毎に POST /api/room {op:"sync"} を呼び、操作(a)も同じ呼び出しで送ります。 */

var DB_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
var DB_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

var HOST_USER = process.env.HOST_USER || "";
var HOST_PASSWORD = process.env.HOST_PASSWORD || "";
var TTL = 6 * 3600;          /* ルームは最後の更新から6時間で消えます */
var PRESENCE_MS = 10000;     /* これ以上応答がない人は「切断中」 */
var MAX_MEMBERS = 80;

/* ---------- Redis (Upstash REST pipeline) ---------- */
async function redis(cmds) {
  var r = await fetch(DB_URL + "/pipeline", {
    method: "POST",
    headers: { Authorization: "Bearer " + DB_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error("redis http " + r.status);
  var j = await r.json();
  return j.map(function (x) { if (x && x.error) throw new Error(x.error); return x.result; });
}
function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
var kRoom = function (id) { return "vgd:r:" + id; };
var kSeen = function (id) { return "vgd:s:" + id; };
var kLock = function (id) { return "vgd:l:" + id; };

async function withLock(id, fn) {
  var ok = false;
  for (var i = 0; i < 25 && !ok; i++) {
    ok = (await redis([["SET", kLock(id), "1", "NX", "PX", "4000"]]))[0] === "OK";
    if (!ok) await sleep(60 + Math.random() * 80);
  }
  if (!ok) { var e = new Error("busy"); e.busy = true; throw e; }
  try { return await fn(); }
  finally { try { await redis([["DEL", kLock(id)]]); } catch (e) {} }
}
function seenMap(arr) {      /* HGETALL の [k,v,k,v…] → {k:v} */
  var o = {}; arr = arr || [];
  for (var i = 0; i + 1 < arr.length; i += 2) o[arr[i]] = +arr[i + 1];
  return o;
}

/* ---------- ドラフトのモデル(画面側 index.html と同じ規則) ---------- */
function buildTurns(cfg) {
  var N = cfg.size, turns = [], p = 0, b = 0;
  for (var r = 0; r < cfg.rows.length; r++) {
    var type = cfg.rows[r], k = type === "P" ? p++ : b++;
    var slot = type === "P" ? k : (k % N), order = k % 2 === 0 ? ["home", "away"] : ["away", "home"];
    turns.push({ r: r, team: order[0], type: type, slot: slot });
    turns.push({ r: r, team: order[1], type: type, slot: slot });
  }
  return turns;
}
function rankOf(S, team) {
  var mem = S.members || {};
  return Object.keys(mem).filter(function (k) { return mem[k].team === team; })
    .sort(function (a, b) { return (mem[a].t - mem[b].t) || (a < b ? -1 : 1); });
}
function derive(S) {
  var cfg = S.cfg, N = cfg.size, mem = S.members || {}, seats;
  if (S.phase === "lobby") seats = { home: rankOf(S, "home").slice(0, N), away: rankOf(S, "away").slice(0, N) };
  else seats = { home: (S.seats && S.seats.home) || [], away: (S.seats && S.seats.away) || [] };
  var tr = {};
  ["home", "away"].forEach(function (t) {
    tr[t] = seats[t].length === N && seats[t].every(function (u) { return mem[u] && mem[u].ready === true; });
  });
  var turns = buildTurns(cfg);
  var cur = S.phase === "draft" ? turns[S.turn] || null : null;
  var actor = cur ? seats[cur.team][cur.slot] : null;
  return { cfg: cfg, N: N, turns: turns, seats: seats, teamReady: tr, cur: cur, actor: actor, all: seats.home.concat(seats.away) };
}
function taken(S, h) {
  if (S.cfg.fixedBans.indexOf(h) >= 0) return true;
  var lg = S.log || {};
  for (var k in lg) { if (lg[k].hero === h) return true; }
  return false;
}
function cleanName(n) { return String(n == null ? "" : n).replace(/[\u0000-\u001f<>]/g, "").trim().slice(0, 14); }
function autoHero(S) {
  var free = S.cfg.heroes.filter(function (h) { return !taken(S, h); });
  return free.length ? free[Math.floor(Math.random() * free.length)] : "";
}
function advance(S, hero, auto, by) {
  var D = derive(S);
  S.log[S.turn] = { hero: hero, by: by || "", auto: !!auto };
  S.pendingHero = ""; S.pendingTurn = -1; S.turn++;
  S.cands = S.cands || {};
  Object.keys(S.cands).forEach(function (k) { if (hero && S.cands[k] === hero) delete S.cands[k]; });
  if (S.turn >= D.turns.length) { S.phase = "done"; S.deadline = 0; S.cands = {}; }
  else {
    S.deadline = Date.now() + S.cfg.timer * 1000;
    var nt = D.turns[S.turn]; delete S.cands[D.seats[nt.team][nt.slot]];
  }
}
function newState(cfg, host) {
  return { cfg: cfg, host: host, created: Date.now(), v: 0, phase: "lobby", turn: 0, deadline: 0, startsAt: 0,
    pendingHero: "", pendingTurn: -1, resetT: -1, cands: {}, log: {}, seats: { home: [], away: [] }, members: {}, toks: {} };
}

/* 送信者 uid の操作 m を検証して反映する。変化したら changed:true、メッセージは msg */
function handle(S, uid, m, body) {
  var D = derive(S), now = Date.now(), mem = S.members, me = mem[uid], ph = S.phase;
  var seated = D.all.indexOf(uid) >= 0, isHost = uid === S.host, changed = false;
  if (!me || body.hello) {
    if (!me && Object.keys(mem).length >= MAX_MEMBERS) return { msg: "ルームが満員です" };
    if (!me) me = mem[uid] = { name: "", team: "", t: now, ready: false, left: false, spec: false };
    var nm = cleanName(body.name);
    if (nm) me.name = nm; else if (!me.name) me.name = "Player";
    me.left = false; me.spec = seated ? false : !!body.spec;
    changed = true;
  }
  var msg = null;
  if (body.hello && (body.team === "home" || body.team === "away") && ph === "lobby" && !me.spec && !me.team) {
    if (D.seats[body.team].length >= D.N) msg = "そのチームは満席です";
    else { me.team = body.team; me.t = now; me.ready = false; changed = true; }      /* チーム別の参加リンクから入った人は、そのまま着席 */
  }
  if (!m || typeof m.a !== "string") return { changed: changed, msg: msg };
  switch (m.a) {
    case "sit":
      if (ph !== "lobby" || me.spec || (m.team !== "home" && m.team !== "away")) break;
      if (me.team === m.team) break;
      if (D.seats[m.team].length >= D.N) { msg = "満席です"; break; }
      me.team = m.team; me.t = now; me.ready = false; changed = true; break;
    case "stand":
      if (ph !== "lobby") break;
      me.team = ""; me.ready = false; changed = true; break;
    case "ready":
      if ((ph !== "lobby" && ph !== "countdown") || !seated) break;
      me.ready = !me.ready; changed = true; break;
    case "leave":
      me.left = true;
      if (ph === "lobby" || ph === "countdown") me.ready = false;
      if (ph === "lobby") me.team = "";
      changed = true; break;
    case "kick":
      if (!isHost || ph !== "lobby" || typeof m.u !== "string" || !mem[m.u] || m.u === uid) break;
      mem[m.u].team = ""; mem[m.u].ready = false; changed = true; break;
    case "reset":
      if (!isHost || ph !== "done") break;
      S.phase = "lobby"; S.turn = 0; S.deadline = 0; S.startsAt = 0; S.pendingHero = ""; S.pendingTurn = -1; S.resetT = -1;
      S.cands = {}; S.log = {}; S.seats = { home: [], away: [] };
      Object.keys(mem).forEach(function (k) { mem[k].ready = false; });
      changed = true; break;
    case "cand":      /* 手番でない人の候補。同じチームの人にだけ配信される */
      if (ph !== "draft" || !seated || D.actor === uid) break;
      S.cands = S.cands || {};
      if (m.h === "") { if (S.cands[uid]) { delete S.cands[uid]; changed = true; } break; }
      if (S.cfg.heroes.indexOf(m.h) < 0 || taken(S, m.h)) break;
      S.cands[uid] = m.h; changed = true; break;
    case "pick":
      if (ph !== "draft" || D.actor !== uid || S.cfg.heroes.indexOf(m.h) < 0 || taken(S, m.h)) break;
      S.pendingTurn = S.turn; S.pendingHero = m.h; changed = true; break;
    case "confirm":
      if (ph !== "draft" || D.actor !== uid) break;
      if (S.pendingTurn !== S.turn || !S.pendingHero || taken(S, S.pendingHero)) break;
      advance(S, S.pendingHero, false, uid); changed = true; break;
    case "extend":
      if (ph !== "draft" || D.actor !== uid || S.resetT === S.turn || now >= S.deadline) break;
      S.deadline = now + S.cfg.timer * 1000; S.resetT = S.turn; changed = true; break;
  }
  return { changed: changed, msg: msg };
}

/* 時間経過で進む状態遷移 + 切断した人の準備解除。変化したら true */
function step(S, seen, selfUid) {
  var now = Date.now(), ch = false;
  if (S.phase === "lobby" || S.phase === "countdown") {
    Object.keys(S.members).forEach(function (u) {
      var m = S.members[u];
      if (m.ready && u !== selfUid && !(seen[u] && now - seen[u] < PRESENCE_MS)) { m.ready = false; ch = true; }
    });
  }
  var D = derive(S), both = D.teamReady.home && D.teamReady.away;
  if (S.phase === "lobby") {
    if (both) { S.phase = "countdown"; S.startsAt = now + 5000; S.seats = { home: D.seats.home.slice(), away: D.seats.away.slice() }; ch = true; }
  } else if (S.phase === "countdown") {
    if (!both) { S.phase = "lobby"; S.startsAt = 0; ch = true; }
    else if (now >= S.startsAt) {
      S.phase = "draft"; S.turn = 0; S.deadline = now + S.cfg.timer * 1000; S.pendingHero = ""; S.pendingTurn = -1; S.resetT = -1; S.cands = {}; ch = true;
    }
  } else if (S.phase === "draft" && D.cur && now > S.deadline) {
    advance(S, autoHero(S), true, ""); ch = true;      /* 時間切れはランダム */
  }
  return ch;
}

/* 受信者ごとに状態を絞る: 候補(cands)は同じチームの人にしか送らない。トークンは送らない */
function viewFor(S, uid, seen, now) {
  var D = derive(S);
  var teamOf = function (u) { return D.seats.home.indexOf(u) >= 0 ? "home" : (D.seats.away.indexOf(u) >= 0 ? "away" : ""); };
  var t = teamOf(uid), c = {}, src = S.cands || {}, v = {};
  Object.keys(src).forEach(function (k) { if (k === uid || (t && teamOf(k) === t)) c[k] = src[k]; });
  for (var k in S) { if (k !== "toks") v[k] = S[k]; }
  v.cands = c;
  v.members = {};
  Object.keys(S.members).forEach(function (u) {
    var m = S.members[u], o = {};
    for (var f in m) o[f] = m[f];
    o.on = !m.left && (u === uid || !!(seen[u] && now - seen[u] < PRESENCE_MS));
    v.members[u] = o;
  });
  return v;
}

/* ---------- 入力の検証 ---------- */
function validCfg(c) {
  if (!c || typeof c !== "object") return null;
  var size = +c.size, timer = +c.timer, rows = String(c.rows || "");
  if (size !== 3 && size !== 5) return null;
  if ([15, 30, 45, 60].indexOf(timer) < 0) return null;
  if (!/^[BP]+$/.test(rows) || rows.length > 20) return null;
  var bans = rows.split("").filter(function (x) { return x === "B"; }).length;
  if (bans < 1 || bans > 5 || rows.length - bans !== size) return null;
  if (rows[0] !== "B" || rows[rows.length - 1] !== "P") return null;
  var hs = c.heroes;
  if (!Array.isArray(hs) || hs.length < 10 || hs.length > 150) return null;
  var seen = {};
  for (var i = 0; i < hs.length; i++) {
    if (typeof hs[i] !== "string" || !hs[i] || hs[i].length > 30 || seen[hs[i]]) return null;
    seen[hs[i]] = 1;
  }
  var fx = Array.isArray(c.fixedBans) ? c.fixedBans.filter(function (h, i, a) { return hs.indexOf(h) >= 0 && a.indexOf(h) === i; }) : [];
  if (hs.length - fx.length < size * 2 + bans * 2) return null;       /* ヒーローが足りない */
  return { size: size, timer: timer, rows: rows, fixedBans: fx, heroes: hs.slice() };
}
var okUid = function (u) { return typeof u === "string" && /^[A-Za-z0-9]{3,40}$/.test(u); };
var okTok = function (t) { return typeof t === "string" && t.length >= 8 && t.length <= 64; };
var okId = function (i) { return typeof i === "string" && /^[A-Z0-9]{4,10}$/.test(i); };
function genId() { var a = "ABCDEFGHJKMNPQRSTUVWXYZ23456789", s = ""; for (var i = 0; i < 6; i++) s += a[Math.floor(Math.random() * a.length)]; return s; }

/* ---------- ホスト認証(ルームを作れるのはホストだけ) ---------- */
var crypto = require("crypto");
function same(a, b) {
  var x = crypto.createHash("sha256").update(String(a == null ? "" : a)).digest();
  var y = crypto.createHash("sha256").update(String(b == null ? "" : b)).digest();
  return crypto.timingSafeEqual(x, y);
}
/* 失敗は IP ごとに10分で10回まで(総当たり対策)。戻り値: "ok" | "bad" | "limit" | "nopw" */
async function checkHost(req, b) {
  if (!HOST_PASSWORD) return "nopw";
  var ip = String((req.headers && (req.headers["x-forwarded-for"] || req.headers["x-real-ip"])) || "ip").split(",")[0].trim().slice(0, 64);
  var key = "vgd:f:" + ip;
  var cnt = +((await redis([["GET", key]]))[0] || 0);
  if (cnt >= 10) return "limit";
  var ok = same(b.hp, HOST_PASSWORD) & (HOST_USER ? same(b.hu, HOST_USER) : true);
  if (ok) return "ok";
  await redis([["INCR", key], ["EXPIRE", key, "600"]]);
  return "bad";
}
function hostFail(res, st) {
  if (st === "nopw") return send(res, 500, { err: "nopw" });
  if (st === "limit") return send(res, 429, { err: "limit" });
  return send(res, 401, { err: "auth" });
}

/* ---------- ハンドラ ---------- */
function send(res, code, obj) { res.status(code).json(obj); }

async function opCreate(b, res) {
  if (!okUid(b.uid) || !okTok(b.tok)) return send(res, 400, { err: "bad" });
  var cfg = validCfg(b.cfg);
  if (!cfg) return send(res, 400, { err: "cfg" });
  for (var k = 0; k < 8; k++) {
    var id = genId(), S = newState(cfg, b.uid);
    S.toks[b.uid] = b.tok;
    handle(S, b.uid, null, { hello: true, name: b.name, spec: false });
    S.v = 1;
    var r = await redis([["SET", kRoom(id), JSON.stringify(S), "NX", "EX", String(TTL)], ["HSET", kSeen(id), b.uid, String(Date.now())], ["EXPIRE", kSeen(id), String(TTL)]]);
    if (r[0] === "OK") {
      var now = Date.now();
      return send(res, 200, { id: id, s: viewFor(S, b.uid, seenMap(["x", "0"]), now), now: now });
    }
  }
  send(res, 503, { err: "idfull" });
}

async function opSync(b, res) {
  if (!okId(b.id) || !okUid(b.uid) || !okTok(b.tok)) return send(res, 400, { err: "bad" });
  var id = b.id, uid = b.uid, tok = b.tok, now = Date.now();
  var m = b.a && typeof b.a === "object" ? b.a : null;
  var r = await redis([["GET", kRoom(id)], ["HSET", kSeen(id), uid, String(now)], ["HGETALL", kSeen(id)]]);
  if (!r[0]) { try { await redis([["DEL", kSeen(id)]]); } catch (e) {} return send(res, 404, { err: "noroom" }); }
  var S = JSON.parse(r[0]), seen = seenMap(r[2]);
  if (S.toks[uid] && S.toks[uid] !== tok) return send(res, 403, { err: "denied" });

  var need = !!m || !!b.hello || !S.toks[uid] || !S.members[uid];
  if (!need) {                 /* 変更がなさそうなら読み取りだけで返す。時間経過で状態が変わる場合だけ書き込む */
    var probe = JSON.parse(r[0]);
    need = step(probe, seen, uid);
  }
  var msg = null;
  if (need) {
    await withLock(id, async function () {
      var rr = await redis([["GET", kRoom(id)], ["HGETALL", kSeen(id)]]);
      if (!rr[0]) { var e = new Error("gone"); e.gone = true; throw e; }
      S = JSON.parse(rr[0]); seen = seenMap(rr[1]); seen[uid] = now;
      if (S.toks[uid] && S.toks[uid] !== tok) { var e2 = new Error("denied"); e2.denied = true; throw e2; }
      var ch = false;
      if (!S.toks[uid]) { S.toks[uid] = tok; ch = true; }
      var h = handle(S, uid, m, b);
      if (h.changed) ch = true;
      if (h.msg) msg = h.msg;
      if (step(S, seen, uid)) ch = true;
      if (ch) {
        S.v = (S.v || 0) + 1;
        await redis([["SET", kRoom(id), JSON.stringify(S), "EX", String(TTL)], ["EXPIRE", kSeen(id), String(TTL)]]);
      }
    });
  }
  send(res, 200, { s: viewFor(S, uid, seen, Date.now()), now: Date.now(), msg: msg });
}

module.exports = async function (req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (req.method !== "POST") return send(res, 405, { err: "method" });
  if (!DB_URL || !DB_TOKEN) return send(res, 500, { err: "nodb" });
  var b = req.body;
  if (typeof b === "string") { try { b = JSON.parse(b); } catch (e) { b = null; } }
  if (!b || typeof b !== "object") return send(res, 400, { err: "bad" });
  try {
    if (b.op === "auth") { var st = await checkHost(req, b); return st === "ok" ? send(res, 200, { ok: true }) : hostFail(res, st); }
    if (b.op === "create") { var st2 = await checkHost(req, b); if (st2 !== "ok") return hostFail(res, st2); return await opCreate(b, res); }
    if (b.op === "sync") return await opSync(b, res);
    return send(res, 400, { err: "op" });
  } catch (e) {
    if (e && e.busy) return send(res, 503, { err: "busy" });
    if (e && e.gone) return send(res, 404, { err: "noroom" });
    if (e && e.denied) return send(res, 403, { err: "denied" });
    console.error(e && e.stack || e);
    return send(res, 500, { err: "server" });
  }
};
