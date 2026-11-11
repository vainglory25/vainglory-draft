/* Upstash(Redis)の使用量を返すAPI。usage.html から呼ばれます。
   Vercelの環境変数(Settings > Environment Variables):
     UPSTASH_EMAIL    … Upstashに登録したメールアドレス(必須)
     UPSTASH_API_KEY  … Upstashの Account > Management API で発行したAPIキー(必須)
     UPSTASH_DB_ID    … データベースID(任意。未設定なら、このアプリのRedisのURLから自動で探します)
     USAGE_PASS       … 使用量ページのパスコード(任意。設定すると、入力した人だけ見られます)
     USAGE_LIMIT      … 月間の上限(任意。初期値 500000) */
var crypto = require("crypto");
var BASE = process.env.UPSTASH_API_BASE || "https://api.upstash.com";
var EMAIL = process.env.UPSTASH_EMAIL, KEY = process.env.UPSTASH_API_KEY, DBID = process.env.UPSTASH_DB_ID;
var PASS = process.env.USAGE_PASS || "";
var LIMIT = +process.env.USAGE_LIMIT > 0 ? +process.env.USAGE_LIMIT : 500000;
var REST_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || "";
var PERIODS = ["1h", "3h", "12h", "1d", "3d", "7d", "30d"];
var cache = { id: null };

function send(res, code, o) { res.statusCode = code; res.setHeader("Content-Type", "application/json; charset=utf-8"); res.end(JSON.stringify(o)); }
function same(a, b) {
  var x = crypto.createHash("sha256").update(String(a)).digest(), y = crypto.createHash("sha256").update(String(b)).digest();
  return crypto.timingSafeEqual(x, y);
}
async function up(path) {
  var r = await fetch(BASE + path, { headers: { Authorization: "Basic " + Buffer.from(EMAIL + ":" + KEY).toString("base64") } });
  var t = await r.text(), j = null;
  try { j = JSON.parse(t); } catch (e) { /* 本文がJSONでない */ }
  return { status: r.status, json: j, text: t };
}
async function findDb() {
  if (DBID) return { id: DBID };
  if (cache.id) return cache;
  var r = await up("/v2/redis/databases");
  if (r.status !== 200 || !Array.isArray(r.json)) throw { code: r.status === 401 ? "auth" : "list", status: r.status };
  var host = "";
  try { host = new URL(REST_URL).hostname; } catch (e) { /* URLなし */ }
  var hit = r.json.filter(function (d) { return host && (d.endpoint && host.indexOf(d.endpoint) === 0); })[0] || (r.json.length === 1 ? r.json[0] : null);
  if (!hit) throw { code: "nodb", names: r.json.map(function (d) { return d.database_name + " (" + d.database_id + ")"; }) };
  cache = { id: hit.database_id, name: hit.database_name, region: hit.primary_region || hit.region, type: hit.type };
  return cache;
}

module.exports = async function (req, res) {
  res.setHeader("Cache-Control", "no-store");
  if (!EMAIL || !KEY) return send(res, 500, { err: "noenv", need: ["UPSTASH_EMAIL", "UPSTASH_API_KEY"] });
  if (PASS) {
    var given = req.headers["x-usage-pass"] || "";
    if (!given) return send(res, 401, { err: "pass", need: true });
    if (!same(given, PASS)) return send(res, 401, { err: "pass", bad: true });
  }
  var url = new URL(req.url, "http://x"), period = url.searchParams.get("period") || "7d";
  if (PERIODS.indexOf(period) < 0) period = "7d";
  try {
    var db = await findDb();
    var r = await up("/v2/redis/stats/" + encodeURIComponent(db.id) + "?period=" + period);
    if (r.status !== 200 && period !== "1d" && /plan|pro|upgrade|forbidden/i.test(r.text)) { period = "1d"; r = await up("/v2/redis/stats/" + encodeURIComponent(db.id) + "?period=1d"); }
    if (r.status !== 200 || !r.json) return send(res, 502, { err: "upstash", status: r.status, msg: String(r.text).slice(0, 200) });
    return send(res, 200, { limit: LIMIT, period: period, db: { id: db.id, name: db.name || "", region: db.region || "", type: db.type || "" }, now: Date.now(), stats: r.json });
  } catch (e) {
    if (e && e.code) return send(res, 502, { err: e.code, status: e.status, names: e.names });
    return send(res, 500, { err: "fail", msg: String(e && e.message || e).slice(0, 200) });
  }
};
