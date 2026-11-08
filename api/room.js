/* VainGlory ドラフト: ルームの進行役(サーバー側)。
   Vercel の API 関数 + Upstash Redis(REST)だけで動きます。追加パッケージは不要です。
   環境変数(Vercel の Upstash Redis 連携が自動で入れます):
     UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN  または  KV_REST_API_URL / KV_REST_API_TOKEN
   クライアントは 1秒ちょっと毎に POST /api/room {op:"sync"} を呼び、操作(a)も同じ呼び出しで送ります。 */

var DB_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
var DB_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

var ROLES3 = ["ローム", "ジャングル", "レーン"], ROLES5 = ["ローム", "ジャングル", "トップ", "ミッド", "ボット"];
var FREE_LIMIT = 3, FREE_DRAFTS = 3, FREE_WINDOW = 24 * 3600;      /* free.html: 同じIPから作れる部屋の数 / 始められるドラフトの数 / 期間(秒) */
var HOST_USER = process.env.HOST_USER || "";
var HOST_PASSWORD = process.env.HOST_PASSWORD || "";
/* ホストが複数いる場合: HOST_USERS = "ユーザー名:パスワード,ユーザー名:パスワード,..."(パスワードにカンマは使えません) */
var HOST_ACCOUNTS = (process.env.HOST_USERS || "").split(",").map(function (x) {
  var i = x.indexOf(":"); return i > 0 ? { u: x.slice(0, i).trim(), p: x.slice(i + 1).trim() } : null;
}).filter(function (a) { return a && a.u && a.p; });
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
/* 行ごとの先攻(H=ホームが先 / A=アウェイが先)の初期値: BANとPICKを別々に数えて、1行目・3行目…はホーム先、2行目・4行目…はアウェイ先(ジグザグ) */
function defaultFirst(rows) {
  var p = 0, b = 0, out = "";
  for (var r = 0; r < rows.length; r++) { var k = rows[r] === "P" ? p++ : b++; out += k % 2 === 0 ? "H" : "A"; }
  return out;
}
function buildTurns(cfg) {
  var N = cfg.size, turns = [], p = 0, b = 0;
  for (var r = 0; r < cfg.rows.length; r++) {
    var type = cfg.rows[r], k = type === "P" ? p++ : b++;
    var fh = cfg.first && cfg.first[r] ? cfg.first[r] === "H" : k % 2 === 0;
    var slot = type === "P" ? k : (k % N), order = fh ? ["home", "away"] : ["away", "home"];
    turns.push({ r: r, team: order[0], type: type, slot: slot });
    turns.push({ r: r, team: order[1], type: type, slot: slot });
  }
  return turns;
}
function rankOf(S, team) {
  var mem = S.members || {};
  var ld = S.cfg && S.cfg.leader;      /* 代表者モード: 代表者を先頭にして、必ず席に入るようにする */
  return Object.keys(mem).filter(function (k) { return mem[k].team === team; })
    .sort(function (a, b) { return ((ld && mem[b].leader ? 1 : 0) - (ld && mem[a].leader ? 1 : 0)) || (mem[a].t - mem[b].t) || (a < b ? -1 : 1); });
}
function derive(S) {
  var cfg = S.cfg, N = cfg.size, mem = S.members || {}, seats;
  if (S.phase === "lobby") seats = { home: rankOf(S, "home").slice(0, N), away: rankOf(S, "away").slice(0, N) };
  else seats = { home: (S.seats && S.seats.home) || [], away: (S.seats && S.seats.away) || [] };
  var tr = {}, leaders = { home: null, away: null };
  if (cfg.leader) {
    ["home", "away"].forEach(function (t) {
      if (S.phase === "lobby") { seats[t].forEach(function (u) { if (mem[u] && mem[u].leader) leaders[t] = u; }); }
      else leaders[t] = (S.leaders && S.leaders[t]) || null;
      tr[t] = !!(leaders[t] && mem[leaders[t]] && mem[leaders[t]].ready === true);      /* 代表者が準備OKならチーム準備完了(全員揃っていなくてよい) */
    });
  } else {
    ["home", "away"].forEach(function (t) {
      tr[t] = seats[t].length === N && seats[t].every(function (u) { return mem[u] && mem[u].ready === true; });
    });
  }
  var turns = buildTurns(cfg);
  var cur = S.phase === "draft" ? turns[S.turn] || null : null;
  var actorAt = function (tn) { return !tn ? null : (cfg.leader ? leaders[tn.team] : seats[tn.team][tn.slot]); };
  return { cfg: cfg, N: N, turns: turns, seats: seats, teamReady: tr, leaders: leaders, cur: cur, actorAt: actorAt, actor: actorAt(cur), all: seats.home.concat(seats.away) };
}
/* ヒーローが「選べない」状態か。team/type は、これから選ぶ側(省略時は誰でも同じ扱い)。
   ミラーあり: PICKは同じチームが二重に選べないだけ(BANされたヒーローは、全員選べない) */
function taken(S, h, team, type) {
  var cfg = S.cfg;
  if (cfg.fixedBans.indexOf(h) >= 0) return true;
  if ((S.used || []).indexOf(h) >= 0) return true;      /* フィアレス: 前の試合で使用済み */
  if (S.avail && S.avail.indexOf(h) < 0) return true;      /* ランダムプール: 今回の対象外 */
  var lg = S.log || {}, turns = null;
  for (var k in lg) {
    if (lg[k].hero !== h) continue;
    if (!cfg.mirror || type === "B") return true;
    turns = turns || buildTurns(cfg);
    var t = turns[+k];
    if (!t || t.type === "B" || t.team === team) return true;
  }
  return false;
}
function pickedBy(S, team) {
  var turns = buildTurns(S.cfg), lg = S.log || {}, out = [];
  for (var k in lg) { var t = turns[+k]; if (t && t.type === "P" && t.team === team && lg[k].hero) out.push(lg[k].hero); }
  return out;
}
function quotaOf(cfg) { return { C: 1, J: 1, L: cfg.size - 2 }; }
/* BANなら「取られていない」だけ。PICKは、ロール別・予算制の条件も見る */
function pickable(S, h, team, type, extra) {
  if (taken(S, h, team, type)) return false;
  if (type !== "P") {      /* BAN: 取られていないだけでなく、そのBANで、両チームのピックが成り立たなくならないこと */
    if (!(S.cfg.cost || S.cfg.hcls)) return true;
    return ["home", "away"].every(function (t) { return feasibleAfter(S, h, t); });
  }
  var cfg = S.cfg, mine = pickedBy(S, team).concat((extra || []).filter(function (x) { return x !== h; })), left = cfg.size - mine.length - 1;
  if (left < 0) return false;
  var cnt = { C: 0, J: 0, L: 0 }, spent = 0;
  mine.forEach(function (x) { if (cfg.hcls) cnt[cfg.hcls[x]]++; if (cfg.cost) spent += cfg.cost[x]; });
  if (cfg.hcls) { var c = cfg.hcls[h]; if (cnt[c] >= quotaOf(cfg)[c]) return false; cnt[c]++; }
  if (cfg.cost) spent += cfg.cost[h];
  if (!cfg.cost && !cfg.hcls) return true;
  if (!feasible(S, h, team, mine.concat([h]), cnt, spent)) return false;
  return feasibleAfter(S, cfg.mirror ? "" : h, team === "home" ? "away" : "home", mine.concat([h]));      /* 自分のピックで、相手チームが詰まらないこと */
}
/* team の、いまのピックのまま、h が(BANや相手に)取られても、残りの枠を埋められるか */
function feasibleAfter(S, h, team, otherPicks) {
  var cfg = S.cfg, m = pickedBy(S, team), cn = { C: 0, J: 0, L: 0 }, sp = 0;
  m.forEach(function (x) { if (cfg.hcls) cn[cfg.hcls[x]]++; if (cfg.cost) sp += cfg.cost[x]; });
  return feasible(S, h, team, m, cn, sp, true, otherPicks);
}
/* h を選んだあとも、残りの枠を、実際にまだ空いているヒーローで、役割と予算の範囲内に埋められるか。
   相手チームの残りのピックが、いちばん安いヒーローから取っていく最悪の場合でも足りるかを見る */
function feasible(S, h, team, mine, cnt, spent, isBan, otherPicks) {
  var cfg = S.cfg, left = cfg.size - mine.length, q = quotaOf(cfg);
  if (left <= 0) return !cfg.cost || spent <= cfg.budget;
  var opp = team === "home" ? "away" : "home", oppPicked = otherPicks || pickedBy(S, opp);
  var oc = { C: 0, J: 0, L: 0 };
  if (cfg.hcls) oppPicked.forEach(function (x) { oc[cfg.hcls[x]]++; });
  var nb = 0;      /* これから行われるBANは見込まない(BANも「ピックを成り立たなくするもの」は選べないため) */
  var cls = cfg.hcls ? ["C", "J", "L"] : ["*"], lists = { C: [], J: [], L: [], "*": [] };
  cfg.heroes.forEach(function (x) {
    if (x === h || mine.indexOf(x) >= 0 || taken(S, x, team, "P")) return;
    lists[cfg.hcls ? cfg.hcls[x] : "*"].push(cfg.cost ? cfg.cost[x] : 0);
  });
  var total = 0;
  for (var k = 0; k < cls.length; k++) {
    var c = cls[k], r = cfg.hcls ? q[c] - cnt[c] : left;
    if (r <= 0) continue;
    var oppNeed = cfg.mirror ? 0 : Math.max(0, cfg.hcls ? q[c] - oc[c] : cfg.size - oppPicked.length);
    var L = lists[c].sort(function (a, b) { return a - b; }), skip = oppNeed + nb;
    if (L.length < skip + r) return false;
    for (var j = skip; j < skip + r; j++) total += L[j];
  }
  return !cfg.cost || spent + total <= cfg.budget;
}
/* 予算の最低ライン: コスト表で、両チームがチームを組める最小の予算。相手が安いヒーローから取っていく最悪の場合でも足りる点数(ミラーありなら、両チームが同じ安いヒーローを選べる) */
function minBudget(hs, fx, cost, hcls, size, mirror) {
  var groups = hcls ? { C: [], J: [], L: [] } : { "*": [] }, q = hcls ? { C: 1, J: 1, L: size - 2 } : { "*": size }, tot = 0;
  hs.forEach(function (h) { if (fx.indexOf(h) < 0) groups[hcls ? hcls[h] : "*"].push(cost[h]); });
  for (var c in groups) {
    var L = groups[c].sort(function (a, b) { return a - b; }), from = mirror ? 0 : q[c];
    if (L.length < from + q[c]) return Infinity;
    for (var i = from; i < from + q[c]; i++) tot += L[i];
  }
  return tot;
}
function cleanName(n) { return String(n == null ? "" : n).replace(/[\u0000-\u001f\u007f-\u009f\u00ad\u034f\u115f\u1160\u17b4\u17b5\u180e\u200b-\u200f\u202a-\u202e\u2060-\u206f\u3164\ufeff\uffa0<>]/g, "").replace(/\s+/g, " ").trim().slice(0, 14); }
function autoHero(S, team, type, avoid, extra) {
  avoid = (avoid || []).concat(extra || []);
  var ok = S.cfg.heroes.filter(function (h) { return avoid.indexOf(h) < 0 && pickable(S, h, team, type, extra); });
  if (!ok.length) {      /* 条件を満たす人がいなければ、取られていない中で、役割が満員でなく、いちばん安いヒーロー(強いヒーローを引かないように) */
    ok = S.cfg.heroes.filter(function (h) { return avoid.indexOf(h) < 0 && !taken(S, h, team, type); });
    if (type === "P" && (S.cfg.cost || S.cfg.hcls) && ok.length) {
      var mineF = pickedBy(S, team).concat(extra || []), cf = { C: 0, J: 0, L: 0 }, qf = quotaOf(S.cfg);
      if (S.cfg.hcls) mineF.forEach(function (x) { cf[S.cfg.hcls[x]]++; });
      var sc = function (h) { return (S.cfg.hcls && cf[S.cfg.hcls[h]] >= qf[S.cfg.hcls[h]] ? 1000 : 0) + (S.cfg.cost ? S.cfg.cost[h] : 0); };
      var best = Math.min.apply(null, ok.map(sc));
      ok = ok.filter(function (h) { return sc(h) === best; });
    }
  }
  return ok.length ? ok[Math.floor(Math.random() * ok.length)] : "";
}
function finishDraft(S) { S.phase = "done"; S.deadline = 0; S.cands = {}; S.pausedAt = 0; S.bp = null; S.code = 3001 + Math.floor(Math.random() * 6999); S.leaders = S.leaders || null; }      /* 3001〜9999 */
function emptyBp() { return { home: { hs: [], ok: false, auto: false }, away: { hs: [], ok: false, auto: false } }; }
/* ブラインドの「1回に出す範囲」[s,e): 1行ずつなら2手番、まとめてなら、同じ種類(BAN/PICK)が続く行ぜんぶ */
function stepEnd(cfg, s) {
  if (!cfg.blindAll) return s + 2;
  var r = Math.floor(s / 2), t = cfg.rows[r];
  while (r < cfg.rows.length && cfg.rows[r] === t) r++;
  return r * 2;
}
function stepN(S) { return (stepEnd(S.cfg, S.turn) - S.turn) / 2; }      /* 各チームが、この回に選ぶ数 */
/* ランダムプール: 今回使えるヒーローを、固定BAN・使用済みを除いた中から無作為に選ぶ(ロール別のときは、各役割が足りるように) */
function sampleAvail(S) {
  var cfg = S.cfg, pool = cfg.heroes.filter(function (h) { return cfg.fixedBans.indexOf(h) < 0 && (S.used || []).indexOf(h) < 0; });
  var n = Math.min(cfg.randPool, pool.length);
  if (n >= pool.length) return pool;
  var bans = cfg.rows.split("").filter(function (x) { return x === "B"; }).length;
  for (var t = 0; t < 60; t++) {
    var a = pool.slice().sort(function () { return Math.random() - 0.5; }).slice(0, n);
    if (cfg.hcls) {
      var q = quotaOf(cfg), cnt = { C: 0, J: 0, L: 0 };
      a.forEach(function (h) { cnt[cfg.hcls[h]]++; });
      if (!["C", "J", "L"].every(function (c) { return cnt[c] >= q[c] * 2 + 1; })) continue;
    }
    if (cfg.hcls || cfg.cost) {      /* BANの前の時点で、両チームが予算と役割を満たして埋められるプールだけを採用 */
      var sv = S.avail, st = S.turn; S.avail = a; S.turn = 1e9;
      var okp = ["home", "away"].every(function (t) { return feasible(S, "", t, [], { C: 0, J: 0, L: 0 }, 0, true); });
      S.avail = sv; S.turn = st;
      if (!okp) continue;
    }
    if (a.length >= cfg.size * 2 + bans * 2) return a;
  }
  return pool.slice().sort(function () { return Math.random() - 0.5; }).slice(0, n);
}
function advance(S, hero, auto, by) {
  var D = derive(S);
  if (extMode(S) === "pool" && D.cur && S.pool) {      /* 制限時間を超えた分は、そのチームの時間プールから消費する */
    var tm = D.cur.team, over = Math.max(0, clock(S) - S.deadline);
    S.pool[tm] = Math.max(0, (S.pool[tm] || 0) - over);
  }
  S.log[S.turn] = { hero: hero, by: by || "", auto: !!auto };
  S.pendingHero = ""; S.pendingTurn = -1; S.turn++;
  S.cands = S.cands || {};
  Object.keys(S.cands).forEach(function (k) { if (hero && S.cands[k] === hero) delete S.cands[k]; });
  if (S.turn >= D.turns.length) finishDraft(S);
  else {
    S.deadline = clock(S) + S.cfg.timer * 1000;      /* 一時停止中は、再開するまで時間が進まない */
    var nt = D.turns[S.turn]; delete S.cands[D.actorAt(nt)];
  }
}
/* ブラインド: 両チームの確定が揃った(または時間切れ)ので、この回の手番ぶんをまとめて公開して進める */
function resolveBlind(S, D) {
  var bp = S.bp || emptyBp(), e = stepEnd(S.cfg, S.turn), used = { home: 0, away: 0 };
  for (var i = S.turn; i < e; i++) {
    var tn = D.turns[i], b = bp[tn.team];
    S.log[i] = { hero: b.hs[used[tn.team]++] || "", by: D.actorAt(tn) || "", auto: !!b.auto };
  }
  S.bp = emptyBp(); S.cands = {}; S.pendingHero = ""; S.pendingTurn = -1; S.turn = e;
  if (S.turn >= D.turns.length) finishDraft(S);
  else S.deadline = clock(S) + S.cfg.timer * 1000;
}
/* 自分(uid)が、いま操作できる手番 {team,type,ix}。なければ null。ブラインドでは、確定後は null。
   ブラインドで1回に2つ以上選ぶとき(まとめて)は、代表者モードなら代表者、そうでなければチームの誰でも */
function myAct(S, D, uid) {
  if (S.phase !== "draft" || !D.cur) return null;
  if (!S.cfg.blind) return D.actor === uid ? { team: D.cur.team, type: D.cur.type, ix: S.turn } : null;
  var e = stepEnd(S.cfg, S.turn), n = (e - S.turn) / 2;
  for (var i = S.turn; i < e; i++) {
    var tn = D.turns[i], b = (S.bp || {})[tn.team];
    var mine = n > 1 ? (S.cfg.leader ? D.leaders[tn.team] === uid : D.seats[tn.team].indexOf(uid) >= 0) : D.actorAt(tn) === uid;
    if (mine) return b && b.ok ? null : { team: tn.team, type: tn.type, ix: i, n: n };
  }
  return null;
}
function clock(S) { return S.pausedAt || Date.now(); }      /* 一時停止中は止まった時刻 */
function extMode(S) { return (S.cfg && S.cfg.ext) || "reset"; }
function newState(cfg, host) {
  return { cfg: cfg, host: host, created: Date.now(), v: 0, phase: "lobby", turn: 0, deadline: 0, startsAt: 0,
    pendingHero: "", pendingTurn: -1, resetT: -1, pausedAt: 0, pool: { home: 0, away: 0 }, cands: {}, log: {}, seats: { home: [], away: [] }, members: {}, toks: {}, used: [], game: 1,
    avail: null, bp: null, res: "", tn: { home: "", away: "" }, series: cfg.series ? { wins: { A: 0, B: 0 }, hist: [], flip: false } : null };
}

/* 送信者 uid の操作 m を検証して反映する。変化したら changed:true、メッセージは msg */
function handle(S, uid, m, body) {
  var D = derive(S), now = Date.now(), mem = S.members, me = mem[uid], ph = S.phase;
  var seated = D.all.indexOf(uid) >= 0, isHost = uid === S.host, changed = false;
  if (!me || body.hello) {
    if (!me && Object.keys(mem).length >= MAX_MEMBERS) return { msg: "ルームが満員です" };
    if (!me) me = mem[uid] = { name: "", team: "", t: now, ready: false, left: false, spec: false };
    var nm = cleanName(body.name);
    if (nm) me.name = nm; else if (!me.name) me.name = "Player";      /* (名前なしは opSync / opCreate が先に弾く) */
    me.left = false; me.spec = seated ? false : !!body.spec;
    changed = true;
  }
  var msg = null, LD = !!S.cfg.leader;
  if (body.hello && LD && !me.spec) { var wl = !!body.leader; if (!me.team && me.wantLeader !== wl) { me.wantLeader = wl; changed = true; } }
  var takeLeader = function (team) {      /* 代表者として着席。すでにいれば一般参加者として着席 */
    if (LD && me.wantLeader) {
      var cur = D.leaders[team];
      if (!cur || cur === uid) me.leader = true; else { me.leader = false; msg = "そのチームにはすでに代表者がいます(一般の参加者として入りました)"; }
    } else me.leader = false;
  };
  if (body.hello && (body.team === "home" || body.team === "away") && ph === "lobby" && !me.spec && !me.team) {
    if (D.seats[body.team].length >= D.N) msg = "そのチームは満席です";
    else { me.team = body.team; me.t = now; me.ready = false; me.locked = true; takeLeader(body.team); changed = true; }      /* チーム別の参加リンクから入った人は、そのまま着席(席を離れられない) */
  }
  if (!m || typeof m.a !== "string") return { changed: changed, msg: msg };
  switch (m.a) {
    case "sit":
      if (ph !== "lobby" || me.spec || (m.team !== "home" && m.team !== "away")) break;
      if (me.team === m.team) break;
      if (D.seats[m.team].length >= D.N) { msg = "満席です"; break; }
      if (LD && m.leader === true && isHost) me.wantLeader = true;      /* ホストは、ロビーのボタンから代表者として着席できる */
      me.team = m.team; me.t = now; me.ready = false; takeLeader(m.team); changed = true; break;
    case "stand":
      if (ph !== "lobby" || me.locked) break;
      me.team = ""; me.ready = false; me.leader = false; changed = true; break;
    case "role":      /* 自分のロール/色の意思表示(任意) */
      if (!seated) break;
      var RL = S.cfg.size === 3 ? ROLES3 : ROLES5;
      if ((m.r !== "" && RL.indexOf(m.r) < 0) || ["", "red", "blue", "yellow"].indexOf(m.c) < 0) break;
      me.role = m.r; me.color = m.c; changed = true; break;
    case "ready":
      if ((ph !== "lobby" && ph !== "countdown") || !seated) break;
      if (LD && !me.leader) break;
      me.ready = !me.ready; changed = true; break;
    case "leave":
      me.left = true;
      if (ph === "lobby" || ph === "countdown") me.ready = false;
      if (ph === "lobby") { me.team = ""; me.leader = false; me.locked = false; }
      changed = true; break;
    case "kick":
      if (!isHost || ph !== "lobby" || typeof m.u !== "string" || !mem[m.u] || m.u === uid) break;
      mem[m.u].team = ""; mem[m.u].ready = false; mem[m.u].leader = false; mem[m.u].locked = false; changed = true; break;
    case "move": {      /* 席順の入れ替え(ロビーのみ): ホスト、または代表者モードでの、そのチームの代表者 */
      if (ph !== "lobby" || typeof m.u !== "string" || !mem[m.u] || !mem[m.u].team || (m.d !== -1 && m.d !== 1)) break;
      var mvT = mem[m.u].team;
      if (!(isHost || (LD && me.leader && me.team === mvT))) break;
      var mvOrd = rankOf(S, mvT), mvI = mvOrd.indexOf(m.u), mvJ = mvI + m.d;
      if (mvI < 0 || mvJ < 0 || mvJ >= mvOrd.length) break;
      if (LD && (mem[mvOrd[mvI]].leader || mem[mvOrd[mvJ]].leader)) break;      /* 代表者の席は固定 */
      var mvTs = mvOrd.map(function (u) { return mem[u].t; }).sort(function (a, b) { return a - b; }), mvPrev = -Infinity;
      mvOrd.splice(mvI, 1); mvOrd.splice(mvJ, 0, m.u);
      mvOrd.forEach(function (u, i) { var v = Math.max(mvTs[i], mvPrev + 0.001); mem[u].t = v; mvPrev = v; });
      changed = true; break;
    }
    case "tname":      /* チーム名: ホストがロビーで入力する。全員に表示される */
      if (!isHost || ph !== "lobby" || (m.t !== "home" && m.t !== "away") || typeof m.n !== "string") break;
      S.tn = S.tn || { home: "", away: "" };
      var nn = m.n.replace(/[\u0000-\u001f\u007f<>]/g, "").trim().slice(0, 12);
      if (S.tn[m.t] !== nn) { S.tn[m.t] = nn; changed = true; }
      break;
    case "next":      /* 次の試合へ(フィアレス、またはシリーズ戦の部屋): フィアレスなら今の試合のピック(設定ならBANも)を使用済みにして、ロビーへ */
    case "reset":
      if (!isHost || ph !== "done") break;
      if (m.a === "next") {
        var sr = S.series, need = S.cfg.series === 99 ? 1e9 : Math.ceil((S.cfg.series || 0) / 2), swp = S.cfg.swap === true;
        if (sr && (sr.wins.A >= need || sr.wins.B >= need)) { msg = "シリーズは決着しています。「最初から」で新しいシリーズを始めてください"; break; }
        var nu = (S.used || []).slice(), nb = S.cfg.rows.split("").filter(function (x) { return x === "B"; }).length;
        if (S.cfg.fearless) {
          Object.keys(S.log || {}).forEach(function (k) {
            var tn = D.turns[+k], hh = S.log[k].hero;
            if (!tn || !hh || nu.indexOf(hh) >= 0) return;
            if (tn.type === "P" || S.cfg.fearlessBan) nu.push(hh);
          });
          var left = S.cfg.heroes.filter(function (h) { return S.cfg.fixedBans.indexOf(h) < 0 && nu.indexOf(h) < 0; }).length;
          if (left < S.cfg.size * 2 + nb * 2) { msg = "使えるヒーローが足りなくなるため、次の試合へは進めません。「最初から」で使用済みをリセットしてください"; break; }
          S.used = nu;
        }
        S.game = (S.game || 1) + 1;
        if (sr) { if (S.res) sr.hist.push({ g: S.game - 1, w: (S.res === "home") !== sr.flip ? "A" : "B" }); if (swp) sr.flip = !sr.flip; }
        S.res = "";
        if (swp && S.tn) { var tt = S.tn.home; S.tn.home = S.tn.away; S.tn.away = tt; }      /* チーム名もチームについていく */
        if (swp) Object.keys(mem).forEach(function (k) { if (mem[k].team === "home") mem[k].team = "away"; else if (mem[k].team === "away") mem[k].team = "home"; });      /* 次の試合は、ホームとアウェイを入れ替える(席の並び・代表者はそのまま) */
      } else {
        if (S.cfg.fearless) S.used = [];      /* 「最初から」: 使用済みを全部戻す */
        S.game = 1;
        S.series = S.cfg.series ? { wins: { A: 0, B: 0 }, hist: [], flip: false } : null;
        S.res = "";
      }
      S.phase = "lobby"; S.turn = 0; S.deadline = 0; S.startsAt = 0; S.pendingHero = ""; S.pendingTurn = -1; S.resetT = -1; S.pausedAt = 0;
      S.cands = {}; S.log = {}; S.leaders = null; S.code = 0; S.seats = { home: [], away: [] }; S.pool = { home: 0, away: 0 }; S.avail = null; S.bp = null;
      Object.keys(mem).forEach(function (k) { mem[k].ready = false; });
      changed = true; break;
    case "result":      /* シリーズ戦: ホストが、その試合の勝者を記録する(押し直すと変更、もう一度同じ側を押すと取り消し) */
      if (!isHost || ph !== "done" || !S.series || (m.w !== "home" && m.w !== "away" && m.w !== "")) break;
      var id1 = function (side) { return (side === "home") !== S.series.flip ? "A" : "B"; };
      var w2 = m.w === S.res ? "" : m.w;
      if (S.res) S.series.wins[id1(S.res)]--;
      if (w2) S.series.wins[id1(w2)]++;
      S.res = w2; changed = true; break;
    case "cand":      /* 手番でない人の候補。同じチームの人にだけ配信される */
      if (ph !== "draft" || !seated || !D.cur || myAct(S, D, uid)) break;
      var ct = D.seats.home.indexOf(uid) >= 0 ? "home" : "away";
      S.cands = S.cands || {};
      if (m.h === "") { if (S.cands[uid]) { delete S.cands[uid]; changed = true; } break; }
      if (S.cfg.heroes.indexOf(m.h) < 0 || !pickable(S, m.h, ct, D.cur.type)) break;
      S.cands[uid] = m.h; changed = true; break;
    case "pick":
      var ac = myAct(S, D, uid);
      if (!ac || S.cfg.heroes.indexOf(m.h) < 0) break;
      if (S.cfg.blind) {
        S.bp = S.bp || emptyBp();
        var hb = S.bp[ac.team], at = hb.hs.indexOf(m.h);
        if (at >= 0) { hb.hs.splice(at, 1); changed = true; break; }      /* 選び直し(押すと外れる) */
        var others = hb.hs.slice();
        if (ac.n === 1) others = [];
        if (!pickable(S, m.h, ac.team, ac.type, others)) break;
        if (ac.n === 1) hb.hs = [m.h]; else if (hb.hs.length < ac.n) hb.hs.push(m.h); else break;
      } else {
        if (!pickable(S, m.h, ac.team, ac.type)) break;
        S.pendingTurn = S.turn; S.pendingHero = m.h;
      }
      changed = true; break;
    case "confirm":
      var ac2 = myAct(S, D, uid);
      if (!ac2) break;
      if (S.cfg.blind) {
        S.bp = S.bp || emptyBp();
        var mb = S.bp[ac2.team], ob = S.bp[ac2.team === "home" ? "away" : "home"];
        if (mb.ok || mb.hs.length !== ac2.n) break;
        var good = [];
        mb.hs.forEach(function (h) { if (pickable(S, h, ac2.team, ac2.type, good)) good.push(h); });
        if (good.length !== mb.hs.length) { mb.hs = good; changed = true; break; }
        if (ac2.type === "P" && !S.cfg.mirror && ob.ok) {
          var keep = mb.hs.filter(function (h) { return ob.hs.indexOf(h) < 0; });
          if (keep.length !== mb.hs.length) { mb.hs = keep; msg = "相手が先に確定したヒーローは選べません。選び直してください"; changed = true; break; }
        }
        mb.ok = true; mb.auto = false;
        if (S.bp.home.ok && S.bp.away.ok) resolveBlind(S, D);
        changed = true; break;
      }
      if (S.pendingTurn !== S.turn || !S.pendingHero || !pickable(S, S.pendingHero, ac2.team, ac2.type)) break;
      advance(S, S.pendingHero, false, uid); changed = true; break;
    case "pause":      /* ホストだけ: 制限時間の一時停止 / 再開 */
      if (!isHost || ph !== "draft") break;
      if (S.pausedAt) { S.deadline += Math.max(0, now - S.pausedAt); S.pausedAt = 0; }
      else S.pausedAt = now;
      changed = true; break;
    case "extend":
      if (ph !== "draft" || D.actor !== uid || S.resetT === S.turn || clock(S) >= S.deadline) break;
      if (extMode(S) === "pool" || extMode(S) === "none") break;      /* 時間プール方式と「なし」では延長ボタンはない */
      if (extMode(S) === "half") S.deadline += Math.round(S.cfg.timer / 2) * 1000;   /* 半分(四捨五入)を残り時間に追加 */
      else S.deadline = clock(S) + S.cfg.timer * 1000;               /* リセット: 制限時間いっぱいに戻す */
      S.resetT = S.turn; changed = true; break;
  }
  return { changed: changed, msg: msg };
}

/* 時間経過で進む状態遷移 + 切断した人の準備解除。変化したら true */
function step(S, seen, selfUid, ev) {
  var now = Date.now(), ch = false;
  if (S.phase === "lobby" || S.phase === "countdown") {
    Object.keys(S.members).forEach(function (u) {
      var m = S.members[u];
      if (m.ready && u !== selfUid && !(seen[u] && now - seen[u] < PRESENCE_MS)) { m.ready = false; ch = true; }
    });
  }
  var D = derive(S), both = D.teamReady.home && D.teamReady.away;
  if (S.phase === "lobby") {
    if (both) { if (ev) ev.cd = true; S.phase = "countdown"; S.startsAt = now + 5000; S.seats = { home: D.seats.home.slice(), away: D.seats.away.slice() }; S.leaders = { home: D.leaders.home, away: D.leaders.away }; ch = true; }
  } else if (S.phase === "countdown") {
    if (!both) { S.phase = "lobby"; S.startsAt = 0; ch = true; }
    else if (now >= S.startsAt) {
      if (ev) ev.started = true;
      S.phase = "draft"; S.turn = 0; S.deadline = now + S.cfg.timer * 1000; S.pendingHero = ""; S.pendingTurn = -1; S.resetT = -1; S.pausedAt = 0; S.cands = {}; ch = true;
      var pm = (S.cfg.pool || 0) * 1000; S.pool = { home: pm, away: pm };
      S.avail = S.cfg.randPool ? sampleAvail(S) : null; S.bp = S.cfg.blind ? emptyBp() : null; S.log = {};
    }
  } else if (S.phase === "draft" && D.cur && !S.pausedAt && now > S.deadline + (extMode(S) === "pool" ? ((S.pool && S.pool[D.cur.team]) || 0) : 0)) {
    if (S.cfg.blind) {      /* ブラインド: まだ確定していないチームは、選んだ分を残して、足りないぶんはピックならランダム・BANはなし */
      S.bp = S.bp || emptyBp();
      var avoid = [], nn = stepN(S);
      ["home", "away"].sort(function () { return Math.random() - 0.5; }).forEach(function (t) {
        var b = S.bp[t];
        if (!b.ok) {
          if (D.cur.type === "P" && !S.cfg.mirror) b.hs = b.hs.filter(function (h) { return avoid.indexOf(h) < 0; });
          var okset = [];
          b.hs.forEach(function (h) { if (pickable(S, h, t, D.cur.type, okset)) okset.push(h); });
          b.hs = okset;
          while (D.cur.type === "P" && b.hs.length < nn) {
            var ah = autoHero(S, t, "P", S.cfg.mirror ? [] : avoid, b.hs);
            if (!ah) break;
            b.hs.push(ah);
          }
          b.ok = true; b.auto = true;
        }
        b.hs.forEach(function (h) { avoid.push(h); });
      });
      resolveBlind(S, D);
    } else advance(S, D.cur.type === "P" ? autoHero(S, D.cur.team, "P") : "", true, "");
    ch = true;      /* 時間切れ: ピックはランダム、BANは選択なしで次へ */
  }
  return ch;
}

/* 受信者ごとに状態を絞る: 候補(cands)は同じチームの人にしか送らない。トークンは送らない */
function viewFor(S, uid, seen, now) {
  var D = derive(S);
  var teamOf = function (u) { return D.seats.home.indexOf(u) >= 0 ? "home" : (D.seats.away.indexOf(u) >= 0 ? "away" : ""); };
  var t = teamOf(uid), c = {}, src = S.cands || {}, v = {};
  Object.keys(src).forEach(function (k) { if (k === uid || (t && teamOf(k) === t)) c[k] = src[k]; });
  for (var k in S) { if (k !== "toks" && k !== "fip") v[k] = S[k]; }
  v.cands = c;
  if (S.bp && S.phase === "draft") {      /* ブラインド: 相手の選択は見せない(確定したかどうかだけ) */
    v.bp = {};
    ["home", "away"].forEach(function (tm) { var b = S.bp[tm]; v.bp[tm] = tm === t ? { hs: b.hs, ok: b.ok } : { ok: b.ok }; });
    v.se = stepEnd(S.cfg, S.turn);
  }
  if (!t) delete v.code;      /* ランダム数字は着席した参加者にだけ送る(ホスト・観戦者には送らない) */
  v.members = {};
  Object.keys(S.members).forEach(function (u) {
    var m = S.members[u], o = {};
    for (var f in m) o[f] = m[f];
    if (u !== uid && !(t && teamOf(u) === t)) { delete o.role; delete o.color; }      /* ロール・色は同じチームの人にしか送らない(相手・観戦者には見せない) */
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
  if (!(timer >= 5 && timer <= 120 && timer === Math.floor(timer))) return null;      /* 5秒〜120秒(1秒単位) */
  if (!/^[BP]+$/.test(rows) || rows.length > 60) return null;
  var bans = rows.split("").filter(function (x) { return x === "B"; }).length;
  if (rows.length - bans !== size) return null;      /* BANの人数の上限は、ヒーロー数に収まるかどうか(下で確認) */
  if (rows[rows.length - 1] !== "P" || (bans > 0 && rows[0] !== "B")) return null;
  var hs = c.heroes;
  if (!Array.isArray(hs) || hs.length < 10 || hs.length > 150) return null;
  var seen = {};
  for (var i = 0; i < hs.length; i++) {
    if (typeof hs[i] !== "string" || !hs[i] || hs[i].length > 30 || seen[hs[i]]) return null;
    seen[hs[i]] = 1;
  }
  var fx = Array.isArray(c.fixedBans) ? c.fixedBans.filter(function (h, i, a) { return hs.indexOf(h) >= 0 && a.indexOf(h) === i; }) : [];
  if (hs.length - fx.length < size * 2 + bans * 2) return null;       /* ヒーローが足りない */
  var ext = c.ext === undefined ? "pool" : String(c.ext);
  if (["pool", "reset", "half", "none"].indexOf(ext) < 0) return null;      /* none = 延長なし */
  var pool = Math.round(+c.pool);
  if (ext === "pool" && !(pool >= 10 && pool <= 600)) return null;
  if (ext !== "pool") pool = 0;
  var fl = c.fearless === true;
  var blind = c.blind === true;
  if (blind) { ext = "none"; pool = 0; }      /* ブラインドは、同時に出すので延長はなし */
  var sv = Math.round(+c.series), series = !(sv > 0) ? 0 : (sv === 99 || (sv >= 3 && sv <= 21 && sv % 2 === 1)) ? sv : 99;      /* シリーズ戦の記録: 0=なし、99=回数指定なし、奇数=BO○ */
  var hcls = null, cost = null, budget = 0, randPool = 0;
  if (c.rolePick === true) {
    hcls = {};
    for (var j = 0; j < hs.length; j++) { var cl = c.hcls && c.hcls[hs[j]]; if (["C", "J", "L"].indexOf(cl) < 0) return null; hcls[hs[j]] = cl; }
  }
  if (+c.budget > 0) {
    budget = Math.round(+c.budget);
    if (!(budget >= size && budget <= size * 5)) return null;
    cost = {};
    for (var m2 = 0; m2 < hs.length; m2++) { var cv = Math.round(+(c.cost && c.cost[hs[m2]])); if (!(cv >= 1 && cv <= 5)) return null; cost[hs[m2]] = cv; }
    if (budget < minBudget(hs, fx, cost, hcls, size, c.mirror === true)) return null;      /* コスト表では、チームを組めない予算 */
  }
  if (+c.randPool > 0) {
    randPool = Math.round(+c.randPool);
    var minPool = size * 2 + bans * 2 + (hcls ? 4 : 0);
    if (randPool < minPool || randPool > hs.length - fx.length) return null;
  }
  var first = typeof c.first === "string" && c.first.length === rows.length && /^[HA]+$/.test(c.first) ? c.first : defaultFirst(rows);
  return { first: first, mirror: c.mirror === true, blind: blind, blindAll: blind && c.blindAll === true, swap: c.swap === true, rolePick: !!hcls, hcls: hcls, budget: budget, cost: cost, randPool: randPool, series: series, size: size, timer: timer, rows: rows, fixedBans: fx, heroes: hs.slice(), ext: ext, pool: pool, leader: c.leader === true, fearless: fl, fearlessBan: fl && c.fearlessBan === true };
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
  if (!HOST_PASSWORD && !HOST_ACCOUNTS.length) return "nopw";
  var ip = String((req.headers && (req.headers["x-forwarded-for"] || req.headers["x-real-ip"])) || "ip").split(",")[0].trim().slice(0, 64);
  var key = "vgd:f:" + ip;
  var cnt = +((await redis([["GET", key]]))[0] || 0);
  if (cnt >= 10) return "limit";
  var ok = HOST_PASSWORD ? (same(b.hp, HOST_PASSWORD) & (HOST_USER ? same(b.hu, HOST_USER) : true)) : 0;
  HOST_ACCOUNTS.forEach(function (a) { ok = ok | (same(b.hp, a.p) & same(b.hu, a.u)); });
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

async function opCreate(b, res, extra) {
  if (!okUid(b.uid) || !okTok(b.tok)) return send(res, 400, { err: "bad" });
  var cfg = validCfg(b.cfg);
  if (!cfg) return send(res, 400, { err: "cfg" });
  if (!cleanName(b.name)) return send(res, 400, { err: "name" });
  for (var k = 0; k < 8; k++) {
    var id = genId(), S = newState(cfg, b.uid);
    S.toks[b.uid] = b.tok;
    if (extra && extra.free) { S.free = true; S.fip = extra.ip; }      /* 無料版: ドラフト開始の回数は、作った人の回線で数える */
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

  if (b.hello && !b.spec && !cleanName(b.name) && !(S.members[uid] && S.members[uid].name && S.members[uid].name !== "Player")) return send(res, 400, { err: "name" });
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
      var ev = {};
      if (step(S, seen, uid, ev)) ch = true;
      if (S.fip && (ev.cd || ev.started)) {      /* 無料版: ドラフトは同じ回線から24時間に FREE_DRAFTS 回まで */
        var dk = "vgd:d:" + S.fip, blocked = false;
        if (ev.started) {
          var dn = (await redis([["INCR", dk]]))[0];
          if (dn === 1) await redis([["EXPIRE", dk, String(FREE_WINDOW)]]);
          if (dn > FREE_DRAFTS) { await redis([["DECR", dk]]); blocked = true; }
          else delete S.blocked;
        } else if (+((await redis([["GET", dk]]))[0] || 0) >= FREE_DRAFTS) blocked = true;
        if (blocked) {
          S.phase = "lobby"; S.startsAt = 0; S.deadline = 0; S.turn = 0; S.cands = {}; S.pendingHero = ""; S.pendingTurn = -1; S.pool = { home: 0, away: 0 };
          Object.keys(S.members).forEach(function (k) { S.members[k].ready = false; });
          S.blocked = "freedraft"; msg = "無料版のドラフトは、同じ回線から24時間に3回までです。上限に達したため開始できません"; ch = true;
        }
      }
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
    if (b.op === "create") {
      var ip = "";
      if (b.free === true) {      /* free.html: ログイン不要。同じIPから FREE_LIMIT 回まで */
        if (!validCfg(b.cfg)) return send(res, 400, { err: "cfg" });
        if (!cleanName(b.name)) return send(res, 400, { err: "name" });
        ip = String((req.headers && (req.headers["x-forwarded-for"] || req.headers["x-real-ip"])) || "ip").split(",")[0].trim().slice(0, 64), fk = "vgd:c:" + ip;      /* 通常・代表者モードを合算して数える */
        var n = (await redis([["INCR", fk]]))[0];
        if (n === 1) await redis([["EXPIRE", fk, String(FREE_WINDOW)]]);
        if (n > FREE_LIMIT) return send(res, 429, { err: "freelimit" });
      }
      if (b.free !== true) { var st2 = await checkHost(req, b); if (st2 !== "ok") return hostFail(res, st2); }
      return await opCreate(b, res, b.free === true ? { free: true, ip: ip } : null);
    }
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
