const GLOBALS = {
  NodeCache: new Map(),
  NodeHostIndexCache: new Map(),
  NodeHostIndexInflight: new Map(),
  NodeListCache: new Map(),
  NodeListInflight: new Map(),
  AuthFail: new Map(),
  Regex: {
    StaticExt:
      /\.(?:jpg|jpeg|gif|png|svg|ico|webp|js|css|woff2?|ttf|otf|map|webmanifest|srt|ass|vtt|sub)$/i,
    EmbyImages: /(?:\/Images\/|\/Icons\/|\/Branding\/|\/emby\/covers\/)/i,
    Streaming:
      /\.(?:mp4|m4v|m4s|m4a|ogv|webm|mkv|mov|avi|wmv|flv|ts|m3u8|mpd)$/i,
  },
};
const Config = {
  Defaults: {
    CacheTTL: 8000,
    ListCacheTTL: 15000,
    MaxRetryBodyBytes: 8 * 1024 * 1024,
  },
};
const FIXED_PROXY_RULES = {
  FORCE_EXTERNAL_PROXY: true,
  PAN_302_DIRECT: false,
  PRESERVE_PAN_HEADERS: true,
  WANGPAN_REFERER: "",
  WANGPAN_KEYWORDS: [
    "aliyundrive",
    "alipan",
    "quark",
    "baidupcs",
    "pan.baidu.com",
    "115.com",
    "123684.com",
    "uc.cn",
    "drive.google.com",
    "googleusercontent.com",
    "1drv.ms",
    "onedrive.live.com",
    "sharepoint.com",
  ],
};
const DIRECT_RULES = {
  DEFAULT_UA:
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
  ADAPTERS: [
    {
      name: "tianyi",
      keywords: ["cloud.189.cn", "189.cn", "ctyun", "e.189.cn", "ctyunxs.cn"],
      forceProxy: true,
      referer: "https://cloud.189.cn/",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "115",
      keywords: ["115.com", "anxia.com", "115cdn"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "pikpak",
      keywords: ["mypikpak.com", "pikpak"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "aliyun",
      keywords: ["aliyundrive", "alipan"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "quark",
      keywords: ["quark", "uc.cn"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "baidu",
      keywords: ["pan.baidu.com", "baidupcs"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "google-drive",
      keywords: [
        "drive.google.com",
        "googleusercontent.com",
        "googledrive",
        "gvt1.com",
      ],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "onedrive",
      keywords: [
        "onedrive.live.com",
        "1drv.ms",
        "sharepoint.com",
        "sharepoint-df.com",
      ],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
    {
      name: "generic-pan",
      keywords: ["123684.com"],
      forceProxy: true,
      referer: "",
      keepOrigin: false,
      keepReferer: false,
    },
  ],
};
function getKV(env) {
  const db = env.EMBY_D1 || env.D1 || env.DB;
  if (!db) return null;
  return {
    async get(key, opts = {}) {
      const row = await db
        .prepare("SELECT v FROM proxy_kv WHERE k = ?1")
        .bind(String(key))
        .first();
      if (!row) return null;
      const val = row.v;
      if (opts && opts.type === "json") {
        try {
          return JSON.parse(val);
        } catch {
          return null;
        }
      }
      return val;
    },
    async put(key, value) {
      const k = String(key);
      const v = typeof value === "string" ? value : JSON.stringify(value);
      const now = Date.now();
      await db
        .prepare(
          `
        INSERT INTO proxy_kv (k, v, updated_at)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(k) DO UPDATE SET
          v = excluded.v,
          updated_at = excluded.updated_at
      `,
        )
        .bind(k, v, now)
        .run();
    },
    async delete(key) {
      await db
        .prepare("DELETE FROM proxy_kv WHERE k = ?1")
        .bind(String(key))
        .run();
    },
    async list(opts = {}) {
      const o = opts || {};
      const p = String(o.prefix || "");
      const off = Number(o.cursor || 0) || 0;
      const lim = Math.max(1, Math.min(Number(o.limit || 1000), 1000));
      const rows = await db
        .prepare(
          `
    SELECT k
    FROM proxy_kv
    WHERE k LIKE ?1
    ORDER BY k
    LIMIT ?2 OFFSET ?3
  `,
        )
        .bind(p + "%", lim + 1, off)
        .all();
      const arr = Array.isArray(rows?.results) ? rows.results : [];
      const hasMore = arr.length > lim;
      const slice = hasMore ? arr.slice(0, lim) : arr;
      return {
        keys: slice.map((r) => ({ name: r.k })),
        list_complete: !hasMore,
        cursor: hasMore ? String(off + lim) : undefined,
      };
    },
  };
}
function safeEqual(a, b) {
  a = String(a || "");
  b = String(b || "");
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}
const Auth = {
  extractToken(request) {
    const auth = request.headers.get("Authorization") || "";
    if (/^Bearer\s+/i.test(auth)) return auth.replace(/^Bearer\s+/i, "").trim();
    const x = request.headers.get("X-Admin-Token");
    return String(x || "").trim();
  },
  unauthorized() {
    return {
      ok: false,
      uid: "",
      role: "",
      response: new Response(JSON.stringify({ error: "UNAUTHORIZED" }), {
        status: 401,
        headers: { "Content-Type": "application/json;charset=utf-8" },
      }),
    };
  },
  check(request, env) {
    const ip = request.headers.get("CF-Connecting-IP") || "unknown";
    const now = Date.now();
    const win = 10 * 60 * 1000; // 10分钟
    const maxFail = 20;
    let rec = GLOBALS.AuthFail.get(ip);
    if (!rec || now - rec.ts > win) rec = { n: 0, ts: now };
    if ((now & 63) === 0) {
      for (const [k, v] of GLOBALS.AuthFail) {
        if (!v || now - Number(v.ts || 0) > win) {
          GLOBALS.AuthFail.delete(k);
        }
      }
    }
    if (rec.n >= maxFail) {
      return {
        ok: false,
        uid: "",
        role: "",
        response: new Response(JSON.stringify({ error: "TOO_MANY_REQUESTS" }), {
          status: 429,
          headers: { "Content-Type": "application/json;charset=utf-8" },
        }),
      };
    }
    const got = this.extractToken(request);
    const admin = String(env.ADMIN_TOKEN || "").trim();
    if (got && admin && safeEqual(got, admin)) {
      GLOBALS.AuthFail.delete(ip);
      return { ok: true, uid: "admin", role: "admin", response: null };
    }
    rec.n += 1;
    rec.ts = now;
    GLOBALS.AuthFail.set(ip, rec);
    return this.unauthorized();
  },
};
const Validators = {
  NAME_RE: /^[a-z0-9_-]{1,32}$/i,
  SECRET_RE: /^[^\/?#\s]{0,128}$/,
  normalizeName(v) {
    return String(v || "")
      .trim()
      .toLowerCase();
  },
  validateName(v) {
    const name = this.normalizeName(v);
    if (!this.NAME_RE.test(name)) {
      return {
        ok: false,
        error: "name 非法：仅允许 a-z / 0-9 / _ / -，长度 1~32",
      };
    }
    return { ok: true, value: name };
  },
  splitTargets(v) {
    return String(v || "")
      .split(/\r?\n|[;,，；|]+/g)
      .map((s) => s.trim())
      .filter(Boolean);
  },
  validateTarget(v) {
    const arr = this.splitTargets(v);
    if (!arr.length) return { ok: false, error: "target 不能为空" };
    if (arr.length > 20)
      return { ok: false, error: "target 数量过多（最多20）" };
    const out = [];
    for (const t of arr) {
      if (t.length > 2048) return { ok: false, error: "target 过长" };
      try {
        const u = new URL(t);
        if (!/^https?:$/i.test(u.protocol)) {
          return { ok: false, error: "target 只允许 http/https" };
        }
        out.push(t.replace(/\/+$/, ""));
      } catch {
        return { ok: false, error: "target 不是合法 URL：" + t };
      }
    }
    return { ok: true, value: [...new Set(out)].join("\n") };
  },
  validateMode(v) {
    const m = String(v || "")
      .trim()
      .toLowerCase();
    if (m && m !== "normal" && m !== "split") {
      return { ok: false, error: "mode 仅支持 normal/split" };
    }
    return { ok: true, value: m || "split" };
  },
  validateSecret(v) {
    const secret = String(v || "").trim();
    if (!this.SECRET_RE.test(secret)) {
      return {
        ok: false,
        error: "secret 非法：不能包含 / ? # 或空白字符，最长128",
      };
    }
    return { ok: true, value: secret };
  },
  validateTag(v) {
    let tag = String(v || "").trim();
    if (tag.length > 64) tag = tag.slice(0, 64);
    return { ok: true, value: tag };
  },
  validateNote(v) {
    let note = String(v || "").trim();
    if (note.length > 64) note = note.slice(0, 64);
    return { ok: true, value: note };
  },
  validateDisplayName(v) {
    let name = String(v || "")
      .trim()
      .replace(/\s+/g, " ");
    if (name.length > 32) name = name.slice(0, 32);
    return { ok: true, value: name };
  },
  validateNodeInput(n) {
    if (!n || typeof n !== "object" || Array.isArray(n)) {
      return { ok: false, error: "节点项不是对象" };
    }
    const rn = this.validateName(n.name);
    if (!rn.ok) return { ok: false, error: rn.error };
    const rt = this.validateTarget(n.target);
    if (!rt.ok) return { ok: false, error: rt.error };
    const rm = this.validateMode(n.mode);
    if (!rm.ok) return { ok: false, error: rm.error };
    const streamTarget = String(n.streamTarget || "").trim();
    const rs = this.validateSecret(n.secret || "");
    if (!rs.ok) return { ok: false, error: rs.error };
    const rg = this.validateTag(n.tag || "");
    const rn2 = this.validateNote(n.note || "");
    const rd = this.validateDisplayName(n.displayName || "");
    let embyUser = String(n.embyUser || "").trim();
    let embyPass = String(n.embyPass || "").trim();
    if (embyUser.length > 128) embyUser = embyUser.slice(0, 128);
    if (embyPass.length > 256) embyPass = embyPass.slice(0, 256);
    return {
      ok: true,
      value: {
        name: rn.value,
        displayName: rd.value,
        target: rt.value,
        mode: rm.value,
        streamTarget,
        fav: !!n.fav,
        rank: Number.isFinite(Number(n.rank)) ? Number(n.rank) : undefined,
        secret: rs.value,
        tag: rg.value,
        note: rn2.value,
        embyUser,
        embyPass,
        directExternal: !!n.directExternal,
      },
    };
  },
};
const Database = {
  PREFIX: "node:",
  nodePrefix(uid = "admin") {
    uid = String(uid || "admin")
      .trim()
      .toLowerCase();
    return "u:" + uid + ":" + this.PREFIX;
  },
  userPrefix(uid = "admin") {
    return this.nodePrefix(uid);
  },
  nodeKey(uid, name) {
    return this.nodePrefix(uid) + String(name || "").toLowerCase();
  },
  memKey(uid, name) {
    return (
      String(uid || "admin").toLowerCase() +
      ":" +
      String(name || "").toLowerCase()
    );
  },
  listCacheKey(uid = "admin") {
    return "list:" + String(uid || "admin").toLowerCase();
  },
  cacheUrl(uid, name) {
    return (
      "https://internal-cache/node/" +
      encodeURIComponent(String(uid || "admin").toLowerCase()) +
      "/" +
      encodeURIComponent(String(name || "").toLowerCase())
    );
  },
  getKV(env) {
    return getKV(env);
  },
  packNode(n) {
    const o = { t: String(n?.target || "").trim() };
    if (n?.mode) o.m = String(n.mode);
    if (n?.streamTarget) o.st = String(n.streamTarget).trim();
    if (n?.fav) o.f = 1;
    if (Number.isFinite(Number(n?.rank))) o.r = Number(n.rank);
    if (n?.secret) o.s = String(n.secret);
    if (n?.tag) o.g = String(n.tag);
    if (n?.note) o.n = String(n.note);
    if (n?.displayName) o.d = String(n.displayName);
    if (n?.embyUser) o.eu = String(n.embyUser);
    if (n?.embyPass) o.ep = String(n.embyPass);
    if (n?.directExternal) o.de = 1;
    return JSON.stringify(o);
  },
  unpackNode(name, raw) {
    if (!raw || typeof raw !== "object") return null;
    const target = String(raw.t ?? raw.target ?? "").trim();
    if (!target) return null;
    const rm = Validators.validateMode(raw.m ?? raw.mode ?? "");
    if (!rm.ok) return null;
    const mode = rm.value;
    const streamTarget = String(raw.st ?? raw.streamTarget ?? "").trim();
    return {
      name: String(name || "")
        .trim()
        .toLowerCase(),
      displayName: String(raw.d ?? raw.displayName ?? ""),
      target,
      mode,
      streamTarget,
      fav: !!(raw.f ?? raw.fav ?? false),
      rank: Number.isFinite(Number(raw.r ?? raw.rank))
        ? Number(raw.r ?? raw.rank)
        : undefined,
      secret: String(raw.s ?? raw.secret ?? ""),
      tag: String(raw.g ?? raw.tag ?? ""),
      note: String(raw.n ?? raw.note ?? ""),
      embyUser: String(raw.eu ?? raw.embyUser ?? ""),
      embyPass: String(raw.ep ?? raw.embyPass ?? ""),
      directExternal: !!(raw.de ?? raw.directExternal ?? false),
    };
  },
  async getNode(nodeName, env, ctx, uid = "admin") {
    nodeName = String(nodeName || "").toLowerCase();
    uid = String(uid || "admin").toLowerCase();
    const kv = this.getKV(env);
    if (!kv) return null;
    const now = Date.now();
    const mk = this.memKey(uid, nodeName);
    const mem = GLOBALS.NodeCache.get(mk);
    if (mem && mem.exp > now) return mem.data;
    const cache = caches.default;
    const cacheUrl = new URL(this.cacheUrl(uid, nodeName));
    const cached = await cache.match(cacheUrl);
    if (cached) {
      const data = await cached.json();
      GLOBALS.NodeCache.set(mk, { data, exp: now + Config.Defaults.CacheTTL });
      return data;
    }
    const raw = await kv.get(this.nodeKey(uid, nodeName), { type: "json" });
    const nodeData = this.unpackNode(nodeName, raw);
    if (nodeData) {
      const putPromise = cache.put(
        cacheUrl,
        new Response(JSON.stringify(nodeData), {
          headers: {
            "Cache-Control": "public, max-age=5, stale-while-revalidate=30",
          },
        }),
      );
      if (ctx && typeof ctx.waitUntil === "function") {
        ctx.waitUntil(putPromise);
      } else {
        putPromise.catch(() => {});
      }
      GLOBALS.NodeCache.set(mk, {
        data: nodeData,
        exp: now + Config.Defaults.CacheTTL,
      });
      return nodeData;
    }
    return null;
  },
  async listAllNodes(env, uid = "admin", ttlOverride) {
    uid = String(uid || "admin").toLowerCase();
    const kv = this.getKV(env);
    if (!kv) return [];
    const key = this.listCacheKey(uid);
    const now = Date.now();
    const ttl =
      typeof ttlOverride === "number"
        ? Math.max(0, ttlOverride)
        : Number(Config?.Defaults?.ListCacheTTL || 15000);
    const hit = GLOBALS.NodeListCache.get(key);
    if (hit && hit.exp > now) return hit.data;
    const inflight = GLOBALS.NodeListInflight.get(key);
    if (inflight) {
      if (hit?.data) return hit.data;
      return await inflight;
    }
    const task = (async () => {
      const prefix = this.nodePrefix(uid);
      let cursor = undefined;
      const allKeys = [];
      do {
        const list = await kv.list({ prefix, cursor, limit: 1000 });
        if (Array.isArray(list?.keys)) allKeys.push(...list.keys);
        cursor = list?.list_complete ? undefined : list?.cursor;
      } while (cursor);
      const now2 = Date.now();
      const nodes = await Promise.all(
        allKeys.map(async (k) => {
          const name = String(k?.name || "").replace(prefix, "");
          if (!name) return null;
          const mk = this.memKey(uid, name);
          const mem = GLOBALS.NodeCache.get(mk);
          let v = mem && mem.exp > now2 ? mem.data : null;
          if (!v) {
            const raw = await kv.get(this.nodeKey(uid, name), { type: "json" });
            v = this.unpackNode(name, raw);
            if (v)
              GLOBALS.NodeCache.set(mk, {
                data: v,
                exp: now2 + Config.Defaults.CacheTTL,
              });
          }
          return v;
        }),
      );
      const out = nodes.filter(Boolean);
      out.sort((a, b) => {
        const af = !!a?.fav;
        const bf = !!b?.fav;
        if (af !== bf) return af ? -1 : 1;
        const ar = Number.isFinite(Number(a?.rank)) ? Number(a.rank) : 1e9;
        const br = Number.isFinite(Number(b?.rank)) ? Number(b.rank) : 1e9;
        if (ar !== br) return ar - br;
        return String(a?.name || "").localeCompare(
          String(b?.name || ""),
          "zh-Hans-CN",
          { sensitivity: "base" },
        );
      });
      GLOBALS.NodeListCache.set(key, { data: out, exp: Date.now() + ttl });
      return out;
    })().finally(() => {
      GLOBALS.NodeListInflight.delete(key);
    });
    GLOBALS.NodeListInflight.set(key, task);
    if (hit?.data) return hit.data;
    return await task;
  },
  async checkOne(target, timeoutMs = 4500) {
    const start = Date.now();
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await fetch(String(target || ""), {
        method: "GET",
        redirect: "manual",
        signal: ctrl.signal,
        headers: { "User-Agent": "cf-emby-proxy-check/1.0" },
      });
      const rt = Date.now() - start;
      clearTimeout(timer);
      const ok = r.status >= 200 && r.status < 500;
      return {
        ok,
        online: ok,
        status: r.status,
        rt,
        latency: rt,
        error: "",
      };
    } catch (e) {
      clearTimeout(timer);
      const rt = Date.now() - start;
      return {
        ok: false,
        online: false,
        status: 0,
        rt,
        latency: rt,
        error:
          e?.name === "AbortError" ? "TIMEOUT" : e?.message || "CHECK_FAILED",
      };
    }
  },
  async handleApi(request, env) {
    const auth = Auth.check(request, env);
    if (!auth.ok) return auth.response;
    const uid = "admin";
    const kv = this.getKV(env);
    if (!kv) {
      return new Response(
        JSON.stringify({ error: "D1未绑定! 请检查 EMBY_D1 / D1 / DB" }),
        {
          status: 500,
          headers: { "Content-Type": "application/json;charset=utf-8" },
        },
      );
    }
    let data = {};
    try {
      data = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: "Invalid JSON" }), {
        status: 400,
        headers: { "Content-Type": "application/json;charset=utf-8" },
      });
    }
    const cache = caches.default;
    const invalidate = async (name) => {
      GLOBALS.NodeCache.delete(this.memKey(uid, name));
      GLOBALS.NodeHostIndexCache.delete(uid);
      GLOBALS.NodeHostIndexInflight.delete(uid);
      const lk = this.listCacheKey(uid);
      GLOBALS.NodeListCache.delete(lk);
      GLOBALS.NodeListInflight.delete(lk);
      await cache.delete(this.cacheUrl(uid, name));
    };
    switch (data.action) {
      case "list": {
        const nodes = await this.listAllNodes(env, uid, 0);
        return new Response(JSON.stringify({ nodes, uid }), {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        });
      }
      case "toggleFav": {
        const vn = Validators.validateName(data.name);
        if (!vn.ok) {
          return new Response(JSON.stringify({ error: vn.error }), {
            status: 400,
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        }
        const name = vn.value;
        const raw = await kv.get(this.nodeKey(uid, name), { type: "json" });
        const node = this.unpackNode(name, raw);
        if (!node) {
          return new Response(JSON.stringify({ error: "节点不存在" }), {
            status: 404,
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        }
        node.fav = !node.fav;
        await kv.put(this.nodeKey(uid, name), this.packNode(node));
        await invalidate(name);
        return new Response(
          JSON.stringify({ success: true, name, fav: node.fav }),
          {
            headers: { "Content-Type": "application/json;charset=utf-8" },
          },
        );
      }
      case "saveOrder": {
        const names = Array.isArray(data.names) ? data.names : [];
        if (!names.length) {
          return new Response(JSON.stringify({ success: true, saved: 0 }), {
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        }
        let saved = 0;
        for (let i = 0; i < names.length; i++) {
          const vn = Validators.validateName(names[i]);
          if (!vn.ok) continue;
          const name = vn.value;
          const raw = await kv.get(this.nodeKey(uid, name), { type: "json" });
          const node = this.unpackNode(name, raw);
          if (!node) continue;
          node.rank = i + 1;
          await kv.put(this.nodeKey(uid, name), this.packNode(node));
          await invalidate(name);
          saved++;
        }
        return new Response(JSON.stringify({ success: true, saved }), {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        });
      }
      case "save":
      case "import": {
        const items = data.action === "save" ? [data] : data.nodes;
        if (!Array.isArray(items)) {
          return new Response(JSON.stringify({ error: "nodes 必须为数组" }), {
            status: 400,
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        }
        let saved = 0;
        const errors = [];
        for (const raw of items) {
          const v = Validators.validateNodeInput(raw);
          if (!v.ok) {
            errors.push({ name: raw?.name || "", error: v.error });
            continue;
          }
          const n = v.value;
          const oldNameRaw = String(raw?.oldName || "")
            .trim()
            .toLowerCase();
          const oldName = Validators.NAME_RE.test(oldNameRaw) ? oldNameRaw : "";
          const newKey = this.nodeKey(uid, n.name);
          if (data.action === "save") {
            const exists = await kv.get(newKey);
            if (!oldName && exists) {
              errors.push({
                name: n.name,
                error: "请求路径重复：该节点已存在",
              });
              continue;
            }
            if (oldName && oldName !== n.name && exists) {
              errors.push({
                name: n.name,
                error: "请求路径重复：该节点已存在",
              });
              continue;
            }
          }
          let toSave = n;
          if (data.action === "save") {
            const prevName = oldName || n.name;
            const prevRaw = await kv.get(this.nodeKey(uid, prevName), {
              type: "json",
            });
            const prevNode = this.unpackNode(prevName, prevRaw);
            const hasFavInPayload =
              raw && Object.prototype.hasOwnProperty.call(raw, "fav");
            const hasRankInPayload =
              raw && Object.prototype.hasOwnProperty.call(raw, "rank");
            const keepFav = hasFavInPayload ? !!n.fav : !!prevNode?.fav;
            let keepRank;
            if (hasRankInPayload) {
              keepRank = Number.isFinite(Number(n.rank))
                ? Number(n.rank)
                : undefined;
            } else {
              keepRank = Number.isFinite(Number(prevNode?.rank))
                ? Number(prevNode.rank)
                : undefined;
            }
            toSave = { ...n, fav: keepFav, rank: keepRank };
          }
          await kv.put(newKey, this.packNode(toSave));
          await invalidate(n.name);
          if (data.action === "save" && oldName && oldName !== n.name) {
            await kv.delete(this.nodeKey(uid, oldName));
            await invalidate(oldName);
          }
          saved++;
        }
        return new Response(
          JSON.stringify({
            success: true,
            saved,
            failed: errors.length,
            errors,
          }),
          {
            headers: { "Content-Type": "application/json;charset=utf-8" },
          },
        );
      }
      case "delete": {
        const vn = Validators.validateName(data.name);
        if (!vn.ok) {
          return new Response(JSON.stringify({ error: vn.error }), {
            status: 400,
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        }
        const name = vn.value;
        await kv.delete(this.nodeKey(uid, name));
        await invalidate(name);
        return new Response(JSON.stringify({ success: true }), {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        });
      }
      case "batchDelete": {
        const names = Array.isArray(data.names) ? data.names : [];
        let count = 0;
        for (const n of names) {
          const vn = Validators.validateName(n);
          if (!vn.ok) continue;
          const name = vn.value;
          await kv.delete(this.nodeKey(uid, name));
          await invalidate(name);
          count++;
        }
        return new Response(JSON.stringify({ success: true, count }), {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        });
      }
      case "batchTag": {
        const names = Array.isArray(data.names) ? data.names : [];
        const tag = Validators.validateTag(data.tag || "").value;
        let count = 0;
        for (const n of names) {
          const vn = Validators.validateName(n);
          if (!vn.ok) continue;
          const name = vn.value;
          const key = this.nodeKey(uid, name);
          const cur = await kv.get(key, { type: "json" });
          const node = this.unpackNode(name, cur);
          if (node) {
            node.tag = tag;
            await kv.put(key, this.packNode(node));
            await invalidate(name);
            count++;
          }
        }
        return new Response(JSON.stringify({ success: true, count }), {
          headers: { "Content-Type": "application/json;charset=utf-8" },
        });
      }
      case "compactAll": {
        const prefix = this.nodePrefix(uid);
        const limit = Math.max(1, Math.min(1000, Number(data.limit) || 500));
        const cursor = data.cursor ? String(data.cursor) : undefined;
        const dryRun = !!data.dryRun;
        const list = await kv.list({ prefix, cursor, limit });
        let scanned = 0,
          rewritten = 0,
          skipped = 0,
          invalid = 0;
        for (const k of list.keys || []) {
          scanned++;
          const name = k.name.slice(prefix.length);
          const raw = await kv.get(k.name, { type: "json" });
          const node = this.unpackNode(name, raw);
          if (!node) {
            invalid++;
            continue;
          }
          const isOldFormat =
            raw &&
            typeof raw === "object" &&
            ("target" in raw ||
              "secret" in raw ||
              "tag" in raw ||
              "note" in raw);
          if (!isOldFormat) {
            skipped++;
            continue;
          }
          if (!dryRun) {
            await kv.put(k.name, this.packNode(node));
            await invalidate(name);
          }
          rewritten++;
        }
        const done = !!list.list_complete;
        return new Response(
          JSON.stringify({
            success: true,
            dryRun,
            scanned,
            rewritten,
            skipped,
            invalid,
            done,
            nextCursor: done ? null : list.cursor,
          }),
          {
            headers: { "Content-Type": "application/json;charset=utf-8" },
          },
        );
      }
      case "checkStatus": {
        try {
          let target = [];
          if (Array.isArray(data.names) && data.names.length > 0) {
            const names = data.names
              .map((x) => Validators.validateName(x))
              .filter((x) => x.ok)
              .map((x) => x.value);
            const uniq = [...new Set(names)];
            const got = await Promise.all(
              uniq.map((n) => this.getNode(n, env, null, uid)),
            );
            target = got.filter(Boolean);
          } else {
            target = await this.listAllNodes(env, uid);
          }
          const results = [];
          const concurrency = 6;
          let idx = 0;
          const worker = async () => {
            while (idx < target.length) {
              const i = idx++;
              const n = target[i];
              if (!n || !n.name) {
                results.push({
                  name: n?.name || "",
                  ok: false,
                  online: false,
                  status: 0,
                  rt: 0,
                  latency: 0,
                  error: "NO_NODE",
                });
                continue;
              }
              const targets = Validators.splitTargets(String(n.target || ""));
              const urlToCheck = String(targets[0] || "").trim();
              if (!urlToCheck) {
                results.push({
                  name: n.name || "",
                  ok: false,
                  online: false,
                  status: 0,
                  rt: 0,
                  latency: 0,
                  checked: "",
                  error: "NO_TARGET",
                });
                continue;
              }
              const r = await this.checkOne(urlToCheck, 4500);
              results.push({
                name: n.name || "",
                ok: !!r.ok,
                online: !!r.online,
                status: r.status || 0,
                rt: r.rt || 0,
                latency: r.latency || r.rt || 0,
                checked: urlToCheck,
                error: r.error || "",
              });
            }
          };
          await Promise.all(
            Array.from(
              { length: Math.min(concurrency, Math.max(1, target.length)) },
              () => worker(),
            ),
          );
          return new Response(JSON.stringify({ success: true, results }), {
            headers: { "Content-Type": "application/json;charset=utf-8" },
          });
        } catch (e) {
          return new Response(
            JSON.stringify({
              success: false,
              results: [],
              error: "CHECK_STATUS_FAIL: " + (e?.message || String(e)),
            }),
            {
              status: 200,
              headers: { "Content-Type": "application/json;charset=utf-8" },
            },
          );
        }
      }
      default:
        return new Response("Invalid Action", { status: 400 });
    }
  },
  getHostIndexTTL() {
    return 6 * 60 * 60 * 1000;
  },
  async rebuildHostIndex(env, uid = "admin") {
    uid = String(uid || "admin").toLowerCase();
    const nodes = await this.listAllNodes(env, uid);
    const hostMap = new Map();
    for (const n of nodes) {
      const targets = String(n?.target || "")
        .split(/\r?\n|[;,，；|]+/g)
        .map((s) => s.trim())
        .filter(Boolean);
      for (const t of targets) {
        try {
          const h = new URL(t).host.toLowerCase();
          if (!hostMap.has(h)) {
            hostMap.set(h, { uid, name: n.name, secret: n.secret || "" });
          }
        } catch {}
      }
    }
    GLOBALS.NodeHostIndexCache.set(uid, {
      hostMap,
      exp: Date.now() + this.getHostIndexTTL(),
    });
    return hostMap;
  },
  async getHostIndex(env, uid = "admin") {
    uid = String(uid || "admin").toLowerCase();
    const now = Date.now();
    const hit = GLOBALS.NodeHostIndexCache.get(uid);
    if (hit && hit.exp > now) return hit.hostMap;
    if (hit && hit.hostMap) {
      if (!GLOBALS.NodeHostIndexInflight.has(uid)) {
        const p = this.rebuildHostIndex(env, uid).finally(() => {
          GLOBALS.NodeHostIndexInflight.delete(uid);
        });
        GLOBALS.NodeHostIndexInflight.set(uid, p);
      }
      return hit.hostMap;
    }
    let p = GLOBALS.NodeHostIndexInflight.get(uid);
    if (!p) {
      p = this.rebuildHostIndex(env, uid).finally(() => {
        GLOBALS.NodeHostIndexInflight.delete(uid);
      });
      GLOBALS.NodeHostIndexInflight.set(uid, p);
    }
    return await p;
  },
};
const ProxyHandler = {
  getNodeTargets(node) {
    return String(node?.target || "")
      .split(/\r?\n|[;,，；|]+/g)
      .map((s) => s.trim())
      .filter(Boolean);
  },
  buildRawAllowHosts(node, env) {
    const set = new Set();
    for (const t of this.getNodeTargets(node)) {
      try {
        set.add(new URL(t).host.toLowerCase());
      } catch {}
    }
    const extra = String(env.RAW_ALLOW_HOSTS || "").trim();
    if (extra) {
      for (const h of extra.split(",")) {
        const v = h.trim().toLowerCase();
        if (v) set.add(v);
      }
    }
    return set;
  },
  routePrefix(name, key, uid = "admin") {
    const n = encodeURIComponent(String(name || ""));
    const k = String(key || "");
    const u = String(uid || "admin").toLowerCase();
    const base =
      u && u !== "admin" ? `/u/${encodeURIComponent(u)}/${n}` : `/${n}`;
    return k ? `${base}/${encodeURIComponent(k)}` : base;
  },
  sameHost(a, b) {
    try {
      const ua = new URL(a);
      const ub = new URL(b);
      const ha = ua.hostname.toLowerCase();
      const hb = ub.hostname.toLowerCase();
      if (ha !== hb) return false;
      const pa = ua.port || (ua.protocol === "https:" ? "443" : "80");
      const pb = ub.port || (ub.protocol === "https:" ? "443" : "80");
      return pa === pb;
    } catch {
      return false;
    }
  },
  isEmosNode(node, targetUrl, env) {
    const tag = String(node?.tag || "").toLowerCase();
    if (tag.includes("emos")) return true;
    const hosts = String(env.EMOS_MATCH_HOSTS || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
    let h = "";
    try {
      h = String(targetUrl?.hostname || "").toLowerCase();
    } catch {}
    return !!h && hosts.includes(h);
  },
  applyEmosHeaders(h, request, env) {
    const pid = String(env.EMOS_PROXY_ID || "").trim();
    const pname = String(env.EMOS_PROXY_NAME || "").trim();
    if (pid) h.set("EMOS-PROXY-ID", pid);
    if (pname) h.set("EMOS-PROXY-NAME", pname);
    const cip =
      request.headers.get("CF-Connecting-IP") ||
      request.headers.get("X-Forwarded-For") ||
      request.headers.get("X-Real-IP") ||
      "";
    if (cip) h.set("X-FORWARDED-FOR", cip);
    const rg = request.headers.get("Range");
    if (rg) h.set("Range", rg);
  },
  isPanUrl(urlLike) {
    try {
      const u = typeof urlLike === "string" ? new URL(urlLike) : urlLike;
      const hay = `${u.host}${u.pathname}${u.search}`.toLowerCase();
      return FIXED_PROXY_RULES.WANGPAN_KEYWORDS.some((k) =>
        hay.includes(String(k).toLowerCase()),
      );
    } catch {
      return false;
    }
  },
  getDirectAdapter(urlLike) {
    try {
      const u = typeof urlLike === "string" ? new URL(urlLike) : urlLike;
      const hay = `${u.host}${u.pathname}${u.search}`.toLowerCase();
      for (const a of DIRECT_RULES.ADAPTERS) {
        if (
          (a.keywords || []).some((k) => hay.includes(String(k).toLowerCase()))
        ) {
          return a;
        }
      }
      return {
        name: this.isPanUrl(u) ? "generic-pan" : "generic",
        forceProxy: !!FIXED_PROXY_RULES.FORCE_EXTERNAL_PROXY,
        referer: FIXED_PROXY_RULES.WANGPAN_REFERER || "",
        keepOrigin: false,
        keepReferer: false,
      };
    } catch {
      return {
        name: "generic",
        forceProxy: !!FIXED_PROXY_RULES.FORCE_EXTERNAL_PROXY,
        referer: "",
        keepOrigin: false,
        keepReferer: false,
      };
    }
  },
  shouldProxyDirectUrl(urlLike) {
    const adapter = this.getDirectAdapter(urlLike);
    if (adapter && typeof adapter.forceProxy === "boolean") {
      return adapter.forceProxy;
    }
    return !!FIXED_PROXY_RULES.FORCE_EXTERNAL_PROXY;
  },
  isNodeDirectExternal(node) {
    return !!node?.directExternal;
  },
  buildDirectOutboundHeaders(
    request,
    targetUrl,
    env,
    node = null,
    mode = "normal",
  ) {
    const u = typeof targetUrl === "string" ? new URL(targetUrl) : targetUrl;
    const adapter = this.getDirectAdapter(u);
    const h = new Headers(request.headers);
    this.stripClientIpHeaders(h);
    [
      "cf-connecting-ip",
      "cf-ipcountry",
      "cf-ray",
      "cf-visitor",
      "cf-worker",
      "x-forwarded-for",
      "x-real-ip",
      "x-forwarded-proto",
      "x-forwarded-host",
      "x-forwarded-port",
      "forwarded",
      "sec-fetch-site",
      "sec-fetch-mode",
      "sec-fetch-dest",
      "sec-fetch-user",
      "connection",
      "content-length",
      "origin",
      "referer",
    ].forEach((k) => h.delete(k));
    h.set("Host", u.host);
    const ua = h.get("User-Agent") || "";
    if (!ua || /mpv|ffmpeg|lavf|dart|okhttp/i.test(ua)) {
      h.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
    }
    const rg = request.headers.get("Range");
    if (rg) h.set("Range", rg);
    const ifRange = request.headers.get("If-Range");
    if (ifRange) h.set("If-Range", ifRange);
    if (adapter.keepReferer && adapter.referer) {
      h.set("Referer", adapter.referer);
    }
    if (adapter.keepOrigin && adapter.referer) {
      try {
        h.set("Origin", new URL(adapter.referer).origin);
      } catch {}
    }
    if (mode === "retry-no-origin") {
      h.delete("Origin");
      h.delete("Referer");
    }
    if (mode === "retry-browserish") {
      h.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
      if (!h.get("Referer") && adapter.keepReferer && adapter.referer) {
        h.set("Referer", adapter.referer);
      }
    }
    const isEmos = this.isEmosNode(node, u, env);
    if (isEmos) this.applyEmosHeaders(h, request, env);
    return h;
  },
  buildCleanProxyHeaders(request, targetUrl) {
    const h = new Headers(request.headers);
    [
      "cf-connecting-ip",
      "cf-ipcountry",
      "cf-ray",
      "cf-visitor",
      "cf-worker",
      "x-forwarded-for",
      "x-real-ip",
      "x-forwarded-proto",
      "x-forwarded-host",
      "x-forwarded-port",
      "forwarded",
      "true-client-ip",
      "origin",
      "referer",
      "sec-fetch-site",
      "sec-fetch-mode",
      "sec-fetch-dest",
      "sec-fetch-user",
      "connection",
      "content-length",
    ].forEach((k) => h.delete(k));
    h.set("Host", targetUrl.host);
    const ua = h.get("User-Agent") || "";
    if (!ua || /mpv|ffmpeg|lavf|dart|okhttp/i.test(ua)) {
      h.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
    }
    const rg = request.headers.get("Range");
    if (rg) h.set("Range", rg);
    const ifRange = request.headers.get("If-Range");
    if (ifRange) h.set("If-Range", ifRange);
    return h;
  },
  stripClientIpHeaders(h) {
    if (!h) return h;
    h.delete("CF-Connecting-IP");
    h.delete("X-Forwarded-For");
    h.delete("X-Real-IP");
    h.delete("True-Client-IP");
    h.delete("Forwarded");
    h.delete("X-Forwarded-Proto");
    h.delete("X-Forwarded-Host");
    h.delete("X-Forwarded-Port");
    return h;
  },
  async handle(request, node, path, name, key, env, uid = "admin") {
    const targets = this.getNodeTargets(node);
    if (!targets.length) {
      return new Response("Invalid node target", { status: 500 });
    }
    let lastRes = null;
    for (const t of targets) {
      try {
        const nodeTry = { ...node, target: t };
        const res = await this.handleOneTarget(
          request,
          nodeTry,
          path,
          name,
          key,
          env,
          uid,
        );
        if (res && res.status < 500) return res;
        lastRes = res;
      } catch (e) {}
    }
    return (
      lastRes ||
      new Response("Proxy Error: all targets failed", { status: 502 })
    );
  },
  async handleOneTarget(request, node, path, name, key, env, uid = "admin") {
    let base = new URL(node.target);
    const isEmos = this.isEmosNode(node, base, env);
    const ua = request.headers.get("User-Agent") || "";
    const isCapy = /CapyPlayer|Dart/i.test(ua);
    let forwardPath = path || "/";
    const capyStrip = String(env.CAPY_STRIP_EMBY || "0") === "1";
    if (capyStrip && isCapy && /^\/emby(\/|$)/i.test(forwardPath)) {
      forwardPath = forwardPath.replace(/^\/emby/i, "") || "/";
    }
    if (
      node.mode === "split" &&
      !node.streamTarget &&
      forwardPath.startsWith("/__raw__/")
    ) {
      let raw = forwardPath.slice("/__raw__/".length);
      try {
        raw = decodeURIComponent(raw);
      } catch {}
      let u;
      try {
        u = new URL(raw);
      } catch {
        return new Response("Bad raw url", { status: 400 });
      }
      if (!/^https?:$/i.test(u.protocol))
        return new Response("Forbidden", { status: 403 });
      const allowHosts = this.buildRawAllowHosts(node, env);
      const allowAnyWhenSplitNoStream =
        String(env.RAW_ALLOW_ANY || "0") === "1";
      if (!allowHosts.has(u.host.toLowerCase()) && !allowAnyWhenSplitNoStream) {
        return new Response("Forbidden raw host", { status: 403 });
      }
      return this.handleDirect(request, raw, env, node);
    }
    const reqUrl = new URL(request.url);
    const finalUrl = new URL(forwardPath, base);
    finalUrl.search = reqUrl.search;
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      return await this.handleWebSocket(finalUrl, request);
    }
    if (request.method === "OPTIONS") return this.renderCors(request, env);
    const isStreaming = GLOBALS.Regex.Streaming.test(forwardPath);
    const isStatic =
      (GLOBALS.Regex.StaticExt.test(forwardPath) ||
        GLOBALS.Regex.EmbyImages.test(forwardPath)) &&
      request.method === "GET";
    const h = new Headers(request.headers);
    const p = finalUrl.pathname.toLowerCase();
    const isAuthApi = p.includes("/users/authenticatebyname");
    const isPlaybackApi =
      p.includes("/videos/") ||
      p.includes("/playback/") ||
      p.includes("/sessions/playing") ||
      /\.m3u8$/i.test(p) ||
      /\.mpd$/i.test(p) ||
      /\.mkv$/i.test(p) ||
      /\.mp4$/i.test(p);
    const needCompatOrigin = isAuthApi || isPlaybackApi;
    if (isCapy && isAuthApi) {
      h.delete("X-Emby-Token");
      h.delete("X-MediaBrowser-Token");
      h.delete("X-Authorization");
      const az = h.get("Authorization") || "";
      if (/^(Bearer|Token)\s+/i.test(az)) h.delete("Authorization");
      if (!h.get("Content-Type"))
        h.set("Content-Type", "application/json;charset=utf-8");
    }
    h.set("Host", base.host);
    const authz = h.get("Authorization") || "";
    const xEmby = h.get("X-Emby-Authorization") || "";
    if (!isAuthApi && /^MediaBrowser\s+/i.test(authz) && !xEmby) {
      h.set("X-Emby-Authorization", authz);
    }
    if (!isAuthApi && !authz && xEmby) {
      h.set("Authorization", xEmby);
    }
    if (needCompatOrigin) {
      if (isPlaybackApi) {
        h.delete("Origin");
        h.delete("Referer");
        h.delete("Sec-Fetch-Site");
        h.delete("Sec-Fetch-Mode");
        h.delete("Sec-Fetch-Dest");
        h.delete("Sec-Fetch-User");
        h.set("Accept", "*/*");
      } else {
        if (!h.get("Origin")) h.set("Origin", reqUrl.origin);
        if (!h.get("Referer")) h.set("Referer", reqUrl.origin + "/");
        if (!h.get("Accept"))
          h.set("Accept", "application/json, text/plain, */*");
        if (isAuthApi && !h.get("X-Requested-With")) {
          h.set("X-Requested-With", "XMLHttpRequest");
        }
      }
    }
    const currentUa = h.get("User-Agent") || "";
    if (
      isPlaybackApi ||
      !currentUa ||
      /mpv|ffmpeg|lavf|dart|okhttp/i.test(currentUa)
    ) {
      h.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
    }
    [
      "cf-connecting-ip",
      "cf-ipcountry",
      "cf-ray",
      "cf-visitor",
      "cf-worker",
    ].forEach((x) => h.delete(x));
    if (isPlaybackApi) {
      [
        "sec-fetch-site",
        "sec-fetch-mode",
        "sec-fetch-dest",
        "sec-fetch-user",
        "priority",
      ].forEach((x) => h.delete(x));
    }
    if (isStatic) h.delete("Range");
    if (isEmos) this.applyEmosHeaders(h, request, env);
    let cf = { cacheEverything: false, cacheTtl: 0 };
    if (isStatic) {
      const ck = new URL(finalUrl.toString());
      [
        "X-Emby-Token",
        "api_key",
        "X-Emby-Authorization",
        "_",
        "t",
        "stamp",
        "random",
      ].forEach((k) => ck.searchParams.delete(k));
      ck.searchParams.sort();
      cf = {
        cacheEverything: true,
        cacheTtl: 86400 * 30,
        cacheKey: ck.toString(),
        cacheTtlByStatus: { "200-299": 86400 * 30, 404: 60, "500-599": 0 },
      };
    }
    if (isEmos) {
      const pp = finalUrl.pathname.toLowerCase();
      if (/^\/emby\/items\/.+\/images\//i.test(pp)) {
        cf = { ...(cf || {}), cacheEverything: true, cacheTtl: 600 };
      }
      if (pp === "/emby/system/ping") {
        cf = { ...(cf || {}), cacheEverything: true, cacheTtl: 30 };
      }
      if (pp.startsWith("/emby/sessions/playing/progress")) {
        cf = { ...(cf || {}), cacheEverything: false, cacheTtl: 0 };
      }
    }
    try {
      const method = request.method.toUpperCase();
      const replayBody =
        method === "GET" || method === "HEAD"
          ? null
          : await request.clone().arrayBuffer();
      const isImageApi = /\/emby\/items\/.+\/images\//i.test(finalUrl.pathname);
      const isAdditionalPartsApi = /\/emby\/videos\/.+\/additionalparts/i.test(
        finalUrl.pathname,
      );
      const nodeDirect = this.isNodeDirectExternal(node);
      if (
        nodeDirect &&
        (isPlaybackApi || isImageApi || isAdditionalPartsApi) &&
        (request.method === "GET" || request.method === "HEAD")
      ) {
        return new Response(null, {
          status: 302,
          headers: {
            Location: finalUrl.toString(),
            "Cache-Control": "no-store",
          },
        });
      }
      if (
        !nodeDirect &&
        (isPlaybackApi || isImageApi || isAdditionalPartsApi)
      ) {
        const hClean = this.buildCleanProxyHeaders(request, finalUrl);
        const method = request.method.toUpperCase();
        const body =
          method === "GET" || method === "HEAD"
            ? null
            : replayBody
              ? replayBody.slice(0)
              : null;
        const resClean = await this.fetchWithProtocolFallback(finalUrl, {
          method: request.method,
          headers: hClean,
          body,
          redirect: "follow",
          cf,
        });
        const headersClean = new Headers(resClean.headers);
        const aoClean = this.pickAllowOrigin(request, env);
        headersClean.set("Access-Control-Allow-Origin", aoClean);
        if (aoClean !== "*") headersClean.set("Vary", "Origin");
        return new Response(resClean.body, {
          status: resClean.status,
          statusText: resClean.statusText,
          headers: headersClean,
        });
      }
      let res = await this.fetchWithProtocolFallback(finalUrl, {
        method: request.method,
        headers: h,
        body: replayBody ? replayBody.slice(0) : null,
        redirect: "manual",
        cf,
      });
      if (res.status === 403 && needCompatOrigin) {
        const h2 = new Headers(h);
        if (isEmos) this.applyEmosHeaders(h2, request, env);
        const reqOrigin = new URL(request.url).origin;
        h2.set("Origin", reqOrigin);
        h2.set("Referer", reqOrigin + "/");
        res = await this.fetchWithProtocolFallback(finalUrl, {
          method: request.method,
          headers: h2,
          body: replayBody ? replayBody.slice(0) : null,
          redirect: "manual",
          cf,
        });
      }
      if (res.status === 403) {
        const h3 = new Headers(h);
        this.stripClientIpHeaders(h3);
        h3.set("Host", base.host);
        h3.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
        const rg = request.headers.get("Range");
        if (rg) h3.set("Range", rg);
        const ifRange = request.headers.get("If-Range");
        if (ifRange) h3.set("If-Range", ifRange);
        h3.delete("Origin");
        h3.delete("Referer");
        h3.delete("Sec-Fetch-Site");
        h3.delete("Sec-Fetch-Mode");
        h3.delete("Sec-Fetch-Dest");
        h3.delete("Sec-Fetch-User");
        if (isEmos) this.applyEmosHeaders(h3, request, env);
        res = await this.fetchWithProtocolFallback(finalUrl, {
          method: request.method,
          headers: h3,
          body: replayBody ? replayBody.slice(0) : null,
          redirect: "manual",
          cf,
        });
        if (res.status === 403) {
          const h4 = new Headers(h3);
          this.stripClientIpHeaders(h4);
          h4.set("Host", base.host);
          h4.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
          h4.set("Referer", base.origin + "/");
          h4.set("Origin", base.origin);
          const rg2 = request.headers.get("Range");
          if (rg2) h4.set("Range", rg2);
          const ifRange2 = request.headers.get("If-Range");
          if (ifRange2) h4.set("If-Range", ifRange2);
          h4.delete("Sec-Fetch-Site");
          h4.delete("Sec-Fetch-Mode");
          h4.delete("Sec-Fetch-Dest");
          h4.delete("Sec-Fetch-User");
          if (isEmos) this.applyEmosHeaders(h4, request, env);
          res = await this.fetchWithProtocolFallback(finalUrl, {
            method: request.method,
            headers: h4,
            body: replayBody ? replayBody.slice(0) : null,
            redirect: "manual",
            cf,
          });
          if (res.status === 403) {
            const h5 = new Headers(h4);
            this.stripClientIpHeaders(h5);
            h5.set("Host", base.host);
            h5.set("User-Agent", DIRECT_RULES.DEFAULT_UA);
            h5.set("Referer", base.origin + "/");
            h5.set("Origin", base.origin);
            const rg3 = request.headers.get("Range");
            if (rg3) h5.set("Range", rg3);
            const ifRange3 = request.headers.get("If-Range");
            if (ifRange3) h5.set("If-Range", ifRange3);
            h5.delete("Sec-Fetch-Site");
            h5.delete("Sec-Fetch-Mode");
            h5.delete("Sec-Fetch-Dest");
            h5.delete("Sec-Fetch-User");
            if (isEmos) this.applyEmosHeaders(h5, request, env);
            res = await this.fetchWithProtocolFallback(finalUrl, {
              method: request.method,
              headers: h5,
              body: replayBody ? replayBody.slice(0) : null,
              redirect: "manual",
              cf,
            });
          }
        }
      }
      const headers = new Headers(res.headers);
      const ao = this.pickAllowOrigin(request, env);
      headers.set("Access-Control-Allow-Origin", ao);
      if (ao !== "*") headers.set("Vary", "Origin");
      if (isStatic) {
        headers.set("Access-Control-Allow-Origin", "*");
        headers.delete("Vary");
        headers.delete("Set-Cookie");
        headers.set(
          "Cache-Control",
          "public, max-age=31536000, s-maxage=86400",
        );
      } else if (isStreaming) {
        headers.set("Cache-Control", "no-store");
      }
      let splitLocHit = false;
      let hostMap = null;
      try {
        hostMap = await Database.getHostIndex(env, uid);
      } catch {
        hostMap = null;
      }
      if (res.status >= 300 && res.status < 400) {
        const location = headers.get("Location");
        if (location) {
          try {
            const origin = new URL(request.url).origin;
            const selfPrefix = this.routePrefix(name, key, uid);
            const selfPrefixNoSlash = selfPrefix.endsWith("/")
              ? selfPrefix.slice(0, -1)
              : selfPrefix;
            const splitMode = node?.mode === "split";
            if (location.startsWith("/")) {
              const alreadyPrefixed =
                location === selfPrefixNoSlash ||
                location.startsWith(selfPrefixNoSlash + "/");
              if (!splitMode) {
              } else if (!alreadyPrefixed) {
                headers.set("Location", origin + selfPrefix + location);
              }
            } else {
              const loc = new URL(location);
              const locHost = loc.host.toLowerCase();
              const baseHost = String(base.host || "").toLowerCase();
              const alreadyPrefixed =
                loc.pathname === selfPrefixNoSlash ||
                loc.pathname.startsWith(selfPrefixNoSlash + "/");
              if (!alreadyPrefixed) {
                if (splitMode && !node?.streamTarget && locHost !== baseHost) {
                  const shouldProxy = this.shouldProxyDirectUrl(loc);
                  const nodeDirect = this.isNodeDirectExternal(node);
                  if (nodeDirect || !shouldProxy) {
                    headers.set("Location", loc.toString());
                  } else {
                    if (!FIXED_PROXY_RULES.PAN_302_DIRECT) {
                      return await this.handleDirect(
                        request,
                        loc.toString(),
                        env,
                        node,
                      );
                    } else {
                      headers.set(
                        "Location",
                        origin +
                          selfPrefix +
                          "/__raw__/" +
                          encodeURIComponent(loc.toString()),
                      );
                      splitLocHit = true;
                    }
                  }
                } else if (locHost !== baseHost) {
                  const match = hostMap ? hostMap.get(locHost) || null : null;
                  if (match) {
                    const prefix = this.routePrefix(
                      match.name,
                      match.secret || "",
                      uid,
                    );
                    headers.set(
                      "Location",
                      origin + prefix + loc.pathname + loc.search + loc.hash,
                    );
                  }
                } else if (splitMode) {
                  headers.set(
                    "Location",
                    origin + selfPrefix + loc.pathname + loc.search + loc.hash,
                  );
                }
              }
            }
          } catch {}
        }
      }
      const ct = (headers.get("content-type") || "").toLowerCase();
      if (
        res.status >= 200 &&
        res.status < 300 &&
        (ct.includes("application/vnd.apple.mpegurl") ||
          ct.includes("application/x-mpegurl") ||
          ct.includes("application/dash+xml"))
      ) {
        const raw = await res.text();
        const rewritten = await this.rewriteBodyLinks(
          raw,
          request.url,
          env,
          uid,
          node,
          name,
          key,
        );
        headers.delete("content-length");
        return new Response(rewritten, {
          status: res.status,
          statusText: res.statusText,
          headers,
        });
      }
      return new Response(res.body, {
        status: res.status,
        statusText: res.statusText,
        headers,
      });
    } catch (err) {
      return new Response("Proxy Error: " + (err?.message || ""), {
        status: 502,
      });
    }
  },
  async fetchWithProtocolFallback(urlObj, init = {}) {
    const u1 = new URL(urlObj.toString());
    const u2 = new URL(urlObj.toString());
    u2.protocol = u1.protocol === "https:" ? "http:" : "https:";
    const method = String(init.method || "GET").toUpperCase();
    const hasBody = method !== "GET" && method !== "HEAD" && init.body != null;
    const maxBytes = Number(
      Config?.Defaults?.MaxRetryBodyBytes || 8 * 1024 * 1024,
    );
    let allowFallback = true;
    let preparedBody = null;
    if (hasBody) {
      const cl = Number(
        (init.headers && new Headers(init.headers).get("content-length")) || 0,
      );
      if (cl > maxBytes) allowFallback = false;
      const b = init.body;
      const reusable =
        typeof b === "string" ||
        b instanceof ArrayBuffer ||
        ArrayBuffer.isView(b) ||
        b instanceof URLSearchParams ||
        b instanceof FormData ||
        b instanceof Blob;
      if (reusable) {
        let est = 0;
        if (typeof b === "string") est = new TextEncoder().encode(b).byteLength;
        else if (b instanceof ArrayBuffer) est = b.byteLength;
        else if (ArrayBuffer.isView(b)) est = b.byteLength;
        else if (b instanceof Blob) est = b.size;
        if (est > maxBytes) allowFallback = false;
        preparedBody = b;
      } else {
        preparedBody = await new Response(b).arrayBuffer();
        if (preparedBody.byteLength > maxBytes) allowFallback = false;
      }
    }
    const buildInit = () => ({
      ...init,
      headers: new Headers(init.headers || {}),
      body: hasBody
        ? preparedBody instanceof ArrayBuffer
          ? preparedBody.slice(0)
          : preparedBody
        : null,
    });
    let lastErr = null;
    let firstRes = null;
    try {
      firstRes = await fetch(u1.toString(), buildInit());
      if (![525, 526, 530].includes(firstRes.status)) return firstRes;
    } catch (e) {
      lastErr = e;
    }
    if (!allowFallback) {
      if (firstRes) return firstRes;
      throw lastErr || new Error("fetch failed");
    }
    try {
      return await fetch(u2.toString(), buildInit());
    } catch (e2) {
      throw e2 || lastErr || new Error("fetch failed");
    }
  },
  buildDirectCandidates(rawPath, search = "") {
    const p = String(rawPath || "").trim();
    const withQuery = (u) =>
      search ? u + (u.includes("?") ? "&" : "?") + search.slice(1) : u;
    if (/^https?:\/\//i.test(p)) return [withQuery(p)];
    const hostPart = p.split("/")[0].split("?")[0].split("#")[0];
    if (/:80$/i.test(hostPart))
      return [withQuery(`http://${p}`), withQuery(`https://${p}`)];
    if (/:443$/i.test(hostPart))
      return [withQuery(`https://${p}`), withQuery(`http://${p}`)];
    return [withQuery(`https://${p}`), withQuery(`http://${p}`)];
  },
  async handleDirect(request, rawPath, env, node = null) {
    const reqUrl = new URL(request.url);
    const candidates = this.buildDirectCandidates(rawPath, reqUrl.search);
    const method = request.method.toUpperCase();
    if ((request.headers.get("Upgrade") || "").toLowerCase() === "websocket") {
      const first = candidates[0];
      return fetch(first, { headers: request.headers });
    }
    const hasBody = method !== "GET" && method !== "HEAD";
    const maxBytes = Number(
      Config?.Defaults?.MaxRetryBodyBytes || 8 * 1024 * 1024,
    );
    let allowFallback = true;
    let bodyBuf = null;
    if (hasBody) {
      const cl = Number(request.headers.get("content-length") || 0);
      if (cl > maxBytes) allowFallback = false;
      if (allowFallback) {
        bodyBuf = await request.clone().arrayBuffer();
        if (bodyBuf.byteLength > maxBytes) allowFallback = false;
      }
    }
    let lastErr = null;
    let lastRes = null;
    const targets = allowFallback ? candidates : candidates.slice(0, 1);
    for (const target of targets) {
      try {
        const u = new URL(target);
        const redirectMode = "follow";
        let h = this.buildDirectOutboundHeaders(
          request,
          u,
          env,
          node,
          "normal",
        );
        this.stripClientIpHeaders(h);
        let res = await fetch(target, {
          method,
          headers: h,
          body: hasBody ? (bodyBuf ? bodyBuf.slice(0) : request.body) : null,
          redirect: redirectMode,
        });
        if (res.status === 403) {
          const h2 = this.buildDirectOutboundHeaders(
            request,
            u,
            env,
            node,
            "retry-no-origin",
          );
          this.stripClientIpHeaders(h2);
          res = await fetch(target, {
            method,
            headers: h2,
            body: hasBody ? (bodyBuf ? bodyBuf.slice(0) : request.body) : null,
            redirect: redirectMode,
          });
        }
        if (res.status === 403) {
          const h3 = this.buildDirectOutboundHeaders(
            request,
            u,
            env,
            node,
            "retry-browserish",
          );
          this.stripClientIpHeaders(h3);
          res = await fetch(target, {
            method,
            headers: h3,
            body: hasBody ? (bodyBuf ? bodyBuf.slice(0) : request.body) : null,
            redirect: redirectMode,
          });
        }
        if ([525, 526, 530].includes(res.status)) {
          lastRes = res;
          continue;
        }
        const rh = new Headers(res.headers);
        try {
          const reqU = new URL(request.url);
          const i = reqU.pathname.indexOf("/__raw__/");
          const selfPrefix = i >= 0 ? reqU.pathname.slice(0, i) : "";
          if (res.status >= 300 && res.status < 400) {
            const loc = rh.get("Location");
            if (loc && selfPrefix) {
              const abs = new URL(loc, target);
              if (/^https?:$/i.test(abs.protocol)) {
                const shouldProxy = this.shouldProxyDirectUrl(abs);
                if (!shouldProxy) {
                  rh.set("Location", abs.toString());
                } else {
                  if (!FIXED_PROXY_RULES.PAN_302_DIRECT) {
                    return await this.handleDirect(
                      request,
                      abs.toString(),
                      env,
                      node,
                    );
                  } else {
                    rh.set(
                      "Location",
                      reqU.origin +
                        selfPrefix +
                        "/__raw__/" +
                        encodeURIComponent(abs.toString()),
                    );
                  }
                }
              }
            }
          }
        } catch {}
        const ao2 = this.pickAllowOrigin(request, env);
        rh.set("Access-Control-Allow-Origin", ao2);
        if (ao2 !== "*") rh.set("Vary", "Origin");
        return new Response(res.body, {
          status: res.status,
          statusText: res.statusText,
          headers: rh,
        });
      } catch (e) {
        lastErr = e;
      }
    }
    if (lastRes) return lastRes;
    return new Response("Proxy Error: " + (lastErr?.message || "unknown"), {
      status: 502,
    });
  },
  async handleWebSocket(url, request) {
    try {
      const u = new URL(url);
      if (u.protocol === "ws:") u.protocol = "http:";
      if (u.protocol === "wss:") u.protocol = "https:";
      const h = new Headers(request.headers);
      h.set("Connection", "Upgrade");
      h.set("Upgrade", "websocket");
      const req = new Request(u.toString(), {
        method: "GET",
        headers: h,
        redirect: "manual",
      });
      const resp = await fetch(req);
      if (resp.status !== 101) {
        return new Response("WS upstream rejected", { status: 502 });
      }
      return resp;
    } catch {
      return new Response("WS Error", { status: 502 });
    }
  },
  async rewriteBodyLinks(
    text,
    requestUrl,
    env,
    uid,
    currentNode,
    currentName,
    currentKey,
  ) {
    if (!text || typeof text !== "string") return text;
    const origin = new URL(requestUrl).origin;
    const urlRe = /https?:\/\/[^\s"'<>\\]+/gi;
    const urls = [...new Set(text.match(urlRe) || [])];
    if (!urls.length) return text;
    const map = new Map();
    let hostMap = null;
    try {
      hostMap = await Database.getHostIndex(env, uid);
    } catch {
      hostMap = null;
    }
    const curBaseHosts = new Set();
    for (const t of this.getNodeTargets(currentNode)) {
      try {
        curBaseHosts.add(new URL(t).host.toLowerCase());
      } catch {}
    }
    const selfPrefix = this.routePrefix(currentName, currentKey, uid);
    const splitNoStream = FIXED_PROXY_RULES.FORCE_EXTERNAL_PROXY;
    for (const full of urls) {
      let u;
      try {
        u = new URL(full);
      } catch {
        continue;
      }
      if (
        splitNoStream &&
        curBaseHosts.size &&
        !curBaseHosts.has(u.host.toLowerCase())
      ) {
        const shouldProxy = this.shouldProxyDirectUrl(u);
        const nodeDirect = this.isNodeDirectExternal(currentNode);
        if (nodeDirect || !shouldProxy) {
          map.set(full, full);
        } else {
          map.set(
            full,
            origin + selfPrefix + "/__raw__/" + encodeURIComponent(full),
          );
        }
        continue;
      }
      const match = hostMap ? hostMap.get(u.host.toLowerCase()) || null : null;
      if (match) {
        const prefix = this.routePrefix(match.name, match.secret || "", uid);
        map.set(full, origin + prefix + u.pathname + u.search + u.hash);
        continue;
      }
      map.set(
        full,
        `${origin}/${u.protocol}//${u.host}${u.pathname}${u.search}${u.hash}`,
      );
    }
    let out = text;
    for (const [from, to] of map.entries()) out = out.split(from).join(to);
    return out;
  },
  pickAllowOrigin(request, env) {
    const reqOrigin = request.headers.get("Origin") || "";
    const allow = String(env.CORS_ALLOW_ORIGIN || "").trim();
    if (!reqOrigin) return "*";
    if (!allow) return reqOrigin;
    if (allow === "*") return "*";
    const set = new Set(
      allow
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean),
    );
    return set.has(reqOrigin) ? reqOrigin : "null";
  },
  renderCors(request, env) {
    const ao = this.pickAllowOrigin(request, env);
    const headers = {
      "Access-Control-Allow-Origin": ao,
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "*",
    };
    if (ao !== "*") headers["Vary"] = "Origin";
    return new Response(null, { headers });
  },
};
const UI = {
  renderAdmin() {
    const html = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Emby反代管理系统</title>
<style>
:root{
  --bg:#f3f6fb;
  --panel:#ffffff;
  --text:#1e293b;
  --text2:#0f172a;
  --muted:#64748b;
  --line:#dbe3ef;
  --icon:#64748b;
  --inbg:rgba(255,255,255,.92);
  --intext:#0f172a;
  --inborder:#d8e2f0;
  --blue:#3b82f6;
  --brand:#2563eb;
  --brand-soft:#eaf2ff;
  --card-bg:#ffffff;
  --card-bg2:#fbfdff;
  --card-text:#1e293b;
  --card-muted:#64748b;
  --card-line:#dbe4f2;
  --density-gap:12px;
  --density-card-pad:14px;
  --density-name-size:34px;
  --density-label-size:16px;
  --density-mono-size:14px;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{
  font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei","Noto Sans CJK SC",Arial,sans-serif;
  background:var(--bg); color:var(--text);
}
#bgLayer{position:fixed;inset:0;z-index:-3;background-size:cover;background-position:center;background-repeat:no-repeat;filter:brightness(var(--bg-brightness,100%)) blur(var(--bg-blur,0px));transform:scale(1.04);display:none}
#bgOverlay{position:fixed;inset:0;z-index:-2;pointer-events:none;background:rgba(0,0,0,var(--bg-overlay,0.2));display:none}
body.has-bg #bgLayer, body.has-bg #bgOverlay{display:block}
.glass{backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);background:color-mix(in oklab, var(--panel) 80%, transparent)}
.wrap{max-width:min(96vw,1880px);margin:0 auto;padding:12px 8px 88px}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.title{font-size:40px;font-weight:800;display:flex;gap:8px;align-items:flex-end;color:var(--text2)}
.title::after{content:"";display:inline-block;width:36px;height:4px;margin-left:8px;margin-bottom:8px;border-radius:999px;background:linear-gradient(90deg,var(--brand),#60a5fa)}
.title small{
  font-size:15px;
  font-weight:700;
  color:#2563eb;
  background:rgba(59,130,246,.14);
  border:1px solid rgba(59,130,246,.32);
  border-radius:999px;
  padding:4px 12px;
  line-height:1.35;
  margin:0 0 4px 0;
  letter-spacing:.2px;
}
.right-actions{display:flex;gap:2px;align-items:center}
.top-ver{
  font-size:12px;
  color:var(--muted);
  font-weight:600;
  margin-right:8px;
  user-select:none;
}
.icon-btn{border:none;background:transparent;color:var(--icon);padding:8px;border-radius:10px;cursor:pointer;line-height:0}
.icon-btn.is-fav{ color:#f59e0b; }
.icon-btn:hover{background:rgba(148,163,184,.16)}
.icon-btn.eye-toggle{transition:.15s ease}
.icon-btn.eye-toggle.on{
  color:#2563eb;
  background:rgba(37,99,235,.12);
}
.icon-btn.eye-toggle.off{
  color:var(--icon);
  opacity:.7;
}
.controls{display:grid;grid-template-columns:120px minmax(0,1fr) 140px 94px 94px 40px;gap:10px;margin-bottom:12px;width:100%}
.controls select,.controls input,.controls button{
  height:40px;
  border:1px solid var(--inborder);
  background:var(--inbg);
  color:var(--intext);
  border-radius:10px;
  padding:0 12px;
  font-size:13px;
  outline:none;
  min-width:0;
  font-weight:600;
}
.controls button{cursor:pointer}
.controls button:hover{border-color:#bcd0ee;background:#f7fbff}
.controls .fab{
  display:flex;
  align-items:center;
  justify-content:center;
  padding:0;
  background:var(--brand) !important;
  color:#fff !important;
  border:none !important;
}
@media (min-width:981px){
  .controls .fab{
    width:36px;
    height:36px;
    font-size:22px;
    line-height:36px;
  }
}
#tagFilterBtn,#btnCheckSel,#btnCheckAll{color:var(--intext);border-color:var(--inborder)}
#tagFilterBtn:hover,#btnCheckSel:hover,#btnCheckAll:hover{
  border-color:color-mix(in oklab, var(--inborder) 60%, #93c5fd 40%);
  background:color-mix(in oklab, var(--inbg) 85%, #1d4ed8 15%);
}
#searchInput{width:100%;min-width:0;max-width:none}
@media(max-width:900px){
  .controls{grid-template-columns:120px 1fr 140px}
}
@media(max-width:760px){
  .title{font-size:32px}
  .controls{grid-template-columns:1fr 1fr}
  #searchInput{grid-column:1/-1}
}
@media (max-width: 640px){
  .top{
    align-items:flex-start;
    gap:8px;
  }
  .title{
    display:flex;
    flex-wrap:wrap;
    align-items:flex-end;
    gap:6px;
    line-height:1.08;
    min-width:0;
    max-width:calc(100vw - 120px); 
  }
  #nodeCount,
  .right-actions{
    flex:0 0 auto;
  }
}
#toggleAllVisBtn{
  display:flex;align-items:center;justify-content:center;gap:6px;white-space:nowrap;
  background:var(--brand-soft);border-color:#cfe0ff;color:#1e40af;
}
#tagFilterBtn{
  display:flex;align-items:center;justify-content:center;white-space:nowrap;
}
.state-dot{width:8px;height:8px;border-radius:50%;display:inline-block;flex:0 0 8px}
.state-dot.off{background:#9ca3af}
.state-dot.on{background:#3b82f6}
.batchbar{display:none;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px;padding:10px;border:1px dashed var(--line);border-radius:12px}
.batchbar input,.batchbar button{height:36px;border:1px solid var(--inborder);background:var(--inbg);color:var(--intext);border-radius:10px;padding:0 10px;font-size:13px}
.batchbar button{cursor:pointer}
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
@media(max-width:1500px){.grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:960px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:720px){.grid{grid-template-columns:1fr}}
.page-hint{
  margin-top:18px;
  text-align:center;
  color:var(--muted);
  font-size:13px;
  opacity:.75;
  user-select:none;
}
.card[draggable="true"]{ cursor:grab; }
.card.dragging{ opacity:.55; }
.card.drag-over{ outline:2px dashed #60a5fa; outline-offset:-2px; }
.card{
  border:1px solid var(--card-line);
  border-radius:10px;
  padding:10px 10px 8px;
  box-shadow:0 1px 2px rgba(0,0,0,.04);
  min-height:120px;
  background:linear-gradient(180deg,var(--card-bg) 0%,var(--card-bg2) 100%);
  transition:all .18s ease;
}
.card:hover{
  border-color:color-mix(in oklab, var(--card-line) 70%, #93c5fd 30%);
  box-shadow:0 6px 18px rgba(37,99,235,.12);
  transform:translateY(-1px);
}
.row{
  display:flex;
  justify-content:space-between;
  align-items:flex-start;
  gap:8px;
  min-width:0;
}
.left-wrap{
  min-width:0;
  flex:1 1 auto;
  overflow:hidden;
}
.left-head{
  display:flex;
  align-items:flex-start;
  gap:8px;
  min-width:0;
  flex:1 1 auto;
}
.selbox{ margin-top:4px; flex:0 0 auto; }
.selbox input{ width:16px; height:16px; cursor:pointer; }
.info{
  min-width:0;
  flex:1 1 auto;
}
.name{
  margin:0;
  font-size:var(--density-name-size);
  line-height:1.15;
  font-weight:800;
  color:var(--card-text);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  max-width:100%;
}
.path-tip{
  margin-top:2px;
  font-size:12px;
  color:var(--muted);
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
  max-width:100%;
}
.actions{
  display:flex;
  gap:4px;
  flex:0 0 auto;
  margin-left:8px;
}
.actions .icon-btn{ padding:6px; }
.badges{
  display:flex;
  flex-wrap:wrap;
  gap:6px;
  margin-top:6px;
}
.badge{
  display:inline-flex;
  align-items:center;
  height:18px;
  padding:0 8px;
  border-radius:999px;
  font-size:11px;
  line-height:18px;
  font-weight:700;
  white-space:nowrap;
}
.b-mode-normal{ background:#dbeafe; color:#1d4ed8; }
.b-green{  background:#dcfce7; color:#166534; }
.b-blue{   background:#e0f2fe; color:#075985; }
.b-orange{ background:#ffedd5; color:#9a3412; }
.b-gray{   background:#e5e7eb; color:#374151; }
.b-note{   background:#ede9fe; color:#5b21b6; }
.status{
  margin-top:8px;
  display:flex;
  align-items:center;
  gap:6px;
  font-size:12px;
  color:var(--card-muted);
}
.dot{
  width:7px;
  height:7px;
  border-radius:50%;
  display:inline-block;
  flex:0 0 7px;
}
.dot.online{  background:#16a34a; }
.dot.offline{ background:#ef4444; }
.dot.unknown{ background:#94a3b8; }
.line{margin-top:10px;display:grid;grid-template-columns:56px minmax(0,1fr) 24px 24px 24px;gap:6px;align-items:start}
.label{
  font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  font-size:12.5px;
  font-weight:500;
  color: color-mix(in srgb, var(--card-text) 88%, #000 12%);
  line-height:1.35;
  letter-spacing:0.1px;
}
.k{
  font-size:12px;
  color:var(--card-muted);
  font-weight:500;
}
.mono{
  font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  font-size:12.5px;
  font-weight:500;
  color: color-mix(in srgb, var(--card-text) 88%, #000 12%);
  line-height:1.35;
  letter-spacing:0.1px;
  cursor:copy;
  white-space:normal;
  word-break:break-all;
  overflow:hidden;
  display:-webkit-box;
  -webkit-line-clamp:2;      
  -webkit-box-orient:vertical;
}
.mono.muted{
  font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  font-size:12.5px;
  font-weight:500;
  line-height:1.35;
  letter-spacing:0.1px;
  color: color-mix(in srgb, var(--card-text) 88%, #000 12%);
}
.app-row{margin-top:6px;display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap}
.app-btn{
  border:1px solid var(--inborder);
  background:var(--inbg);
  color:var(--intext);
  border-radius:8px;
  padding:0 6px;
  height:24px;
  font-size:11px;
  font-weight:700;
  cursor:pointer;
  white-space:nowrap;
}
.app-btn:hover{
  border-color:#93c5fd;
  background:#eef4ff;
}
.app-btn.capy{opacity:.9}
.menu{position:relative;z-index:90}
.right-actions{position:relative;overflow:visible}
.menu-panel{
  position:absolute;
  right:0;
  left:auto;
  top:calc(100% + 8px);
  min-width:190px;
  max-width:min(92vw,280px);
  max-height:min(72vh,520px);
  overflow:auto;
  padding:6px;
  border:1px solid var(--line);
  border-radius:10px;
  display:none;
  z-index:120;
  box-shadow:0 8px 20px rgba(0,0,0,.08);
}
@media (max-width:640px){
  .menu-panel{
    right:-4px;
    top:calc(100% + 6px);
    max-width:calc(100vw - 12px);
    max-height:70vh;
  }
}
.menu-panel button{width:100%;border:none;background:transparent;color:var(--text);text-align:left;padding:8px 10px;border-radius:8px;cursor:pointer}
.menu-panel button:hover{background:rgba(148,163,184,.14)}
.fab{
  position:fixed;
  right:20px;
  bottom:24px;
  top:auto;
  width:52px;
  height:52px;
  border:none;
  border-radius:999px;
  font-size:28px;
  background:var(--brand);
  color:#fff;
  cursor:pointer;
  box-shadow:0 10px 22px rgba(59,130,246,.35);
  z-index:80;
}
@media (min-width:981px){
  .fab{
    position:static;
    right:auto;
    bottom:auto;
    top:auto;
    width:34px;
    height:34px;
    font-size:20px;
    line-height:34px;
    box-shadow:none;
    margin-left:6px;
    flex:0 0 auto;
  }
}
.modal-mask{display:none;position:fixed;inset:0;background:rgba(15,23,42,.38);z-index:60}
.modal{
  display:none;position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:min(92vw,560px);
  border:1px solid var(--line);border-radius:14px;padding:14px;z-index:61
}
.modal h3{margin:0 0 10px;color:var(--text)}
.modal input:not([type="checkbox"]),.modal select{
  width:100%;height:40px;border:1px solid var(--inborder);background:#fff;color:var(--intext);
  border-radius:10px;padding:0 10px;margin-bottom:8px;outline:none
}
 .pass-wrap{
  position:relative;
  margin-bottom:8px;
}
.pass-wrap #inPass{
  margin-bottom:0;
  padding-right:64px;
}
.pass-eye{
  position:absolute;
  right:8px;
  top:50%;
  transform:translateY(-50%);
  border:1px solid var(--inborder);
  background:#fff;
  color:var(--muted);
  border-radius:8px;
  height:28px;
  min-width:48px;
  padding:0 8px;
  cursor:pointer;
} 
 #editor #targetList .target-item{
  width:100%;
  display:block;
} 
.field-title{
  font-size:15px;
  font-weight:700;
  color:#334155;
  margin:4px 0 8px;
  letter-spacing:.2px;
}
.req{
  color:#ef4444;
  font-weight:800;
  margin-right:4px;
}
.tagbar{position:relative;margin-bottom:8px}
.tagbar input{margin:0;padding-right:12px}
#inTag::-webkit-calendar-picker-indicator{
  opacity:0;
  display:none;
}
#inTag{
  appearance:textfield;
  -moz-appearance:textfield;
}
.project-links{
  width: min(1100px, calc(100% - 24px));
  margin: 10px auto 8px;
  padding: 0;                 
  border: none;               
  background: transparent;    
  box-shadow: none;           
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: center;
  gap: 10px;
  font: inherit;
  font-size: 14px;
  color: var(--muted);
}
.project-links .label{
  color: var(--muted);
  margin-right: 2px;
  font: inherit;              
}
.project-links a{
  text-decoration: none;
  font: inherit;              
  font-size: 14px;
  color: var(--text2);
  border: 1px solid var(--line);
  background: #fff;
  border-radius: 999px;
  padding: 6px 10px;
  line-height: 1;
  transition: .15s ease;
}
.project-links a:hover{
  border-color: var(--blue);
  color: var(--blue);
  transform: translateY(-1px);
}
@media (max-width:768px){
  .project-links{
    margin: 8px 12px 6px;
    font-size: 13px;
  }
  .project-links a{
    font-size: 13px;
  }
}
.disclaimer{
  width: min(1100px, calc(100% - 24px));
  margin: 16px auto 12px;   
  padding: 10px 12px;
  border: 1px dashed #cbd5e1;
  border-radius: 10px;
  font-size: 12px;
  line-height: 1.6;
  color: #64748b;
  background: rgba(255,255,255,.55);
  text-align: center;       
}
@media (max-width:768px){
  .disclaimer{ margin: 12px; font-size: 11.5px; }
}
.modal .btns{display:flex;justify-content:flex-end;gap:8px;margin-top:4px}
.btn{border:none;border-radius:10px;padding:9px 14px;cursor:pointer}
.btn-p{background:var(--blue);color:#fff}.btn-g{background:rgba(148,163,184,.2);color:var(--text)}
.range{display:grid;grid-template-columns:90px 1fr 46px;gap:8px;align-items:center;margin:6px 0}
.small{font-size:12px;color:var(--muted)}
.tag-list{max-height:280px;overflow:auto;border:1px solid var(--line);border-radius:10px;padding:8px;margin-bottom:8px}
.tag-item{display:flex;align-items:center;gap:8px;padding:6px 4px;border-radius:8px}
.tag-item:hover{background:#f8fafc}
.tag-item input{margin:0}
.tag-item{
  display:flex;
  align-items:center;
  justify-content:flex-start;
  gap:8px;
}
.tag-item input[type="checkbox"]{
  width:16px;
  height:16px;
  margin:0;
  flex:0 0 auto;
}
.tag-empty{font-size:13px;color:var(--muted);padding:8px}
.gate{position:fixed;inset:0;background:rgba(15,23,42,.45);display:flex;align-items:center;justify-content:center;z-index:70}
.gate-box{width:min(92vw,360px);background:#fff;border:1px solid #d7dce4;border-radius:14px;padding:16px}
.gate-box h3{margin:0 0 10px}
.gate-box input{width:100%;height:42px;border:1px solid #d7dce4;border-radius:10px;padding:0 10px;outline:none}
.gate-box button{width:100%;height:42px;border:none;border-radius:10px;background:#111827;color:#fff;cursor:pointer;margin-top:10px}
.tip{min-height:16px;margin-top:7px;font-size:12px;color:#ef4444}
.tip.ok{color:#16a34a}
.toast-wrap{position:fixed;right:16px;bottom:16px;z-index:90;display:flex;flex-direction:column;gap:8px}
.toast{
  min-width:180px;max-width:360px;background:#111827;color:#fff;border-radius:10px;padding:10px 12px;font-size:13px;
  box-shadow:0 8px 18px rgba(0,0,0,.2);opacity:.97
}
.toast.success{background:#065f46}.toast.warn{background:#92400e}.toast.error{background:#991b1b}
:root{
  --density-name-size: clamp(22px, 2.2vw, 30px);
  --density-label-size: clamp(14px, 1.2vw, 16px);
  --density-mono-size: clamp(12px, 1vw, 14px);
}
.gate-box,
.modal,
.menu-panel{
  background: var(--panel) !important;
  color: var(--text) !important;
  border: 1px solid var(--line) !important;
  border-radius: 12px !important;
  box-shadow: 0 8px 24px rgba(15,23,42,.08) !important;
}
.gate-box input,
.modal input:not([type="checkbox"]),
.modal select,
.modal textarea,
.menu-panel input,
.menu-panel select{
  background: var(--inbg) !important;
  color: var(--intext) !important;
  border: 1px solid var(--inborder) !important;
  border-radius: 10px !important;
}
.gate-box button{
  background: var(--brand) !important;
  color: #fff !important;
  border: none !important;
  border-radius: 10px !important;
}
button:focus-visible,
.btn:focus-visible,
input:focus-visible,
select:focus-visible,
textarea:focus-visible,
[role="button"]:focus-visible{
  outline: 2px solid color-mix(in oklab, var(--brand) 72%, white 28%);
  outline-offset: 1px;
  box-shadow: 0 0 0 3px color-mix(in oklab, var(--brand) 18%, transparent 82%);
}
.card,
.menu-panel,
.modal,
button,
.btn{
  transition: background-color .16s ease, border-color .16s ease, box-shadow .16s ease, transform .16s ease;
}
.card:hover{
  transform: translateY(-1px);
  box-shadow: 0 10px 22px rgba(15,23,42,.08);
}
button:hover,.btn:hover{
  transform: translateY(-1px);
}
#editor{
  font-size:14px;
  line-height:1.45;
}
#editor h3{
  font-size:24px;
  font-weight:800;
  line-height:1.2;
  margin:0 0 14px 0;
  color:var(--text2);
}
#editor .field-title{
  font-size:18px;
  font-weight:700;
  line-height:1.3;
  margin:10px 0 8px;
  color:var(--text2);
}
#editor .req{
  font-size:16px;
  margin-right:4px;
}
#editor input:not([type="checkbox"]),
#editor textarea,
#editor select{
  height:42px;
  font-size:15px;
  border-radius:10px;
  padding:0 12px;
  margin-bottom:8px;
}
#editor input::placeholder,
#editor textarea::placeholder{
  color:#9aa4b2;
  font-size:14px;
}
#editor .field-help{
  font-size:12px;
  line-height:1.5;
  color:var(--muted);
  margin:2px 0 10px 24px;
}
#editor .check-row{
  display:flex;
  align-items:flex-start;
  gap:8px;
  margin:6px 0 2px;
}
#editor .check-row input[type="checkbox"]{
  margin-top:3px;
}
#editor .check-row span{
  font-size:15px;
  line-height:1.4;
}
#editor .btn-row{
  display:flex;
  gap:8px;
  margin:4px 0 8px;
}
#editor .btn-row .btn{
  height:36px;
  padding:0 12px;
  font-size:14px;
  border-radius:10px;
}
#editor .btns .btn{
  min-width:80px;
  height:38px;
  font-size:15px;
  font-weight:700;
}
#editor .check-row{
  display:flex;
  align-items:center;
  gap:10px;
  margin:6px 0 4px;
}
#editor .check-row input[type="checkbox"]{
  width:18px;
  height:18px;
  margin:0;
  accent-color: var(--blue);
  cursor:pointer;
}
#editor .check-row span{
  font-size:16px;
  line-height:1.25;
}
#editor .pass-wrap{
  position: relative;
}
#editor .pass-wrap #inPass{
  padding-right: 34px; 
}
#editor .pass-eye{
  position: absolute;
  right: 10px;
  top: 50%;
  transform: translateY(-50%);
  width: 20px;
  height: 20px;
  padding: 0;
  margin: 0;
  border: none;
  background: transparent;
  color: #7b8798;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  line-height: 1;
}
#editor .pass-eye:hover{
  color: #4b5563;
}
#editor .pass-eye:focus{
  outline: none;
}
#editor #togglePassIcon,
#editor #togglePassIcon svg{
  width: 16px;
  height: 16px;
  display: block;
}
#editor .check-row input[type="checkbox"]{
  width: 20px;
  height: 20px;
  margin: 0;
  accent-color: var(--blue);
}
#editor .pass-eye{
  width: 24px;
  height: 24px;
  right: 10px;
  top: 50%;
  transform: translateY(-50%);
}
#editor #togglePassIcon,
#editor #togglePassIcon svg{
  width: 22px;
  height: 22px;
}
</style>
</head>
<body>
<div id="bgLayer"></div>
<div id="bgOverlay"></div>
<div id="gate" class="gate">
  <div class="gate-box">
    <h3>管理员登录</h3>
    <input id="gatePwd" type="password" placeholder="请输入 ADMIN_TOKEN" />
<button id="gateBtn" onclick="window.Gate && Gate.check && Gate.check()">进入面板</button>
    <div id="gateTip" class="tip">请先在 Worker 环境变量设置 ADMIN_TOKEN</div>
  </div>
</div>
<div id="app" style="display:none">
  <div class="wrap">
    <div class="top">
      <div class="title">
  Emby反代管理系统
  <small id="nodeCount">0个</small>
</div>
      <div class="right-actions">
  <span class="top-ver">v1.3</span>
  <button class="icon-btn" title="切换主题" onclick="App.quickTheme()">
          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3a9 9 0 1 0 9 9 7 7 0 0 1-9-9z"></path></svg>
        </button>
        <div class="menu">
          <button class="icon-btn" onclick="App.toggleMenu()">
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="3" y1="6" x2="21" y2="6"></line><line x1="3" y1="12" x2="21" y2="12"></line><line x1="3" y1="18" x2="21" y2="18"></line></svg>
          </button>
          <div id="menuPanel" class="menu-panel glass">
            <button onclick="App.exportData()">导出配置</button>
            <button onclick="document.getElementById('fIn').click()">导入配置</button>
            <button onclick="App.openBgModal()">背景高级设置</button>
            <button onclick="App.setDensity('compact')">密度：紧凑</button>
            <button onclick="App.setDensity('cozy')">密度：舒适</button>
            <button onclick="App.setPreset('deepblue')">主题：深蓝</button>
            <button onclick="App.setPreset('graphite')">主题：石墨</button>
            <button onclick="App.setPreset('light')">主题：浅灰</button>
            <button onclick="Gate.logout()">退出登录</button>
            <input type="file" id="fIn" hidden accept=".json" onchange="App.importFile(this)">
          </div>
        </div>
      </div>
    </div>
    <div class="controls">
      <button id="tagFilterBtn" class="glass" onclick="App.openTagPicker()">标签：全部</button>
<input id="searchInput" class="full-sm glass" placeholder="搜索节点名称、备注..." oninput="App.filter(this.value)">
      <button id="toggleAllVisBtn" class="glass" onclick="App.toggleAllVisibility()"><span class="state-dot off"></span>显示全部地址</button>
      <button id="btnCheckSel" class="glass" onclick="App.checkSelectedStatus()">检测选中</button>
      <button id="btnCheckAll" class="glass" onclick="App.checkAllStatus()">检测全部</button>
    </div>
    <div id="batchBar" class="batchbar glass">
      <span class="small">已选 <b id="selCount">0</b> 项</span>
      <button onclick="App.selectAllFiltered()">全选筛选结果</button>
      <button onclick="App.clearSelection()">清空选择</button>
      <input id="batchTagInput" placeholder="批量标签（如 公益服）" />
<button onclick="App.applyBatchTag()">批量打标签</button>
<button onclick="App.applyBatchCred()">批量账号密码</button>
<button style="color:#ef4444" onclick="App.clearBatchCred()">清空账号密码</button>
<button style="color:#ef4444" onclick="App.batchDelete()">批量删除</button>
    </div>
    <div id="list" class="grid"></div>
  </div>
  <button class="fab" onclick="App.openEditor()">＋</button>
  <div id="mask" class="modal-mask" onclick="App.closeAllModals()"></div>
  <div id="editor" class="modal glass">
<h3 id="editorTitle">新增节点</h3>
<div class="field-title"><span class="req">*</span> 请求路径（英文）</div>
<input id="inName" placeholder="请输入唯一英文路径（a-z0-9_-，1~32）">
<div class="field-title">显示名称（可中文）</div>
<input id="inDisplayName" placeholder="自定义">
<div class="field-title">标签</div>
<div class="tagbar">
  <input id="inTag" list="tagSuggestions" placeholder="标签（如 公费服 / 公益服 / 白名单 / 等）">
</div>
<datalist id="tagSuggestions"></datalist>
<input id="inNote" placeholder="备注（如 保号规则 / 等）">
<div class="field-title"><span class="req">*</span> 目标地址（可多个）</div>
<div id="targetList"></div>
<div style="display:flex;gap:8px;margin-top:8px;">
<div class="btn-row">
<button type="button" class="btn btn-g" onclick="App.addTargetInput()">+ 添加目标地址</button>
<button type="button" class="btn btn-g" onclick="App.removeTargetInput()">- 删除一栏</button>
</div>
</div>
<input id="inSec" placeholder="密钥路径（可选，不能含 / ? #）">
<div class="field-title">播放策略</div>
<label class="check-row">
  <input id="inDirectExternal" type="checkbox">
  <span>网盘播放直连</span>
</label>
<div class="field-help">
  开启后，网盘外链由播放器直接访问（不经 Worker 反代），可能更快但受客户端网络影响。
</div>
<div class="field-title">Emby 账号（用于一键导入）</div>
<input id="inUser" placeholder="用户名（可留空）">
<div class="pass-wrap">
  <input id="inPass" type="password" placeholder="密码（可留空）">
  <button type="button" id="togglePassBtn" class="pass-eye" onclick="App.toggleEditorPass()" title="显示/隐藏密码">
  <span id="togglePassIcon"></span>
</button>
</div>
<div class="btns">
  <button class="btn btn-g" onclick="App.closeAllModals()">取消</button>
  <button class="btn btn-p" onclick="App.save()">保存</button>
</div>
  </div>
  <div id="tagPicker" class="modal glass">
    <h3>标签多选筛选</h3>
    <div id="tagPickerList" class="tag-list"></div>
    <div class="btns">
      <button class="btn btn-g" onclick="App.clearTagFilter()">清空</button>
      <button class="btn btn-g" onclick="App.closeAllModals()">取消</button>
      <button class="btn btn-p" onclick="App.applyTagFilter()">应用</button>
    </div>
  </div>
  <div id="bgModal" class="modal glass">
    <h3>背景高级设置</h3>
    <input id="bgUrl" placeholder="背景图URL（https://...）">
    <div class="range"><label>亮度</label><input id="bgBrightness" type="range" min="40" max="140" step="1"><span id="bgBrightnessVal">100%</span></div>
    <div class="range"><label>模糊</label><input id="bgBlur" type="range" min="0" max="20" step="1"><span id="bgBlurVal">0px</span></div>
    <div class="range"><label>遮罩</label><input id="bgOverlayRange" type="range" min="0" max="80" step="1"><span id="bgOverlayVal">20%</span></div>
    <div class="small">建议：深色主题 + 亮度 75~90 + 模糊 4~8</div>
    <div class="btns">
      <button class="btn btn-g" onclick="App.clearBg()">清除背景</button>
      <button class="btn btn-g" onclick="App.closeAllModals()">关闭</button>
      <button class="btn btn-p" onclick="App.saveBg()">保存背景设置</button>
    </div>
  </div>
  <div id="toastWrap" class="toast-wrap"></div>
</div>
<script>
const $ = (s)=>document.querySelector(s);
function mountFabToControls(){
  const controls = document.querySelector('.controls');
  const fab = document.querySelector('.fab');
  if (!controls || !fab || fab.dataset.moved === '1') return;
  controls.appendChild(fab);   
  fab.dataset.moved = '1';
}
const SVG = {
  edit: '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20h9"></path><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z"></path></svg>',
  trash: '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg>',
  eye: '<svg xmlns="http://www.w3.org/2000/svg" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8S1 12 1 12z"></path><circle cx="12" cy="12" r="3"></circle></svg>',
  eyeOff: '<svg xmlns="http://www.w3.org/2000/svg" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"></path><line x1="1" y1="1" x2="23" y2="23"></line></svg>',
  copy: '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>',
  link: '<svg xmlns="http://www.w3.org/2000/svg" width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l2.92-2.92a5 5 0 0 0-7.07-7.07L11.7 5.23"></path><path d="M14 11a5 5 0 0 0-7.54-.54L3.54 13.38a5 5 0 0 0 7.07 7.07l1.69-1.69"></path></svg>',
  ping: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3a9 9 0 1 0 9 9"></path><path d="M12 7v5l3 3"></path></svg>',
  star: '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 15 9 22 9 17 14 19 22 12 18 5 22 7 14 2 9 9 9 12 2"></polygon></svg>',
  starOn: '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.2"><polygon points="12 2 15 9 22 9 17 14 19 22 12 18 5 22 7 14 2 9 9 9 12 2"></polygon></svg>',
};
const PRESETS = {
  light: {
    "--bg":"#f3f6fb","--panel":"#ffffff","--text":"#1e293b","--text2":"#0f172a","--muted":"#64748b","--line":"#dbe3ef","--icon":"#64748b",
    "--inbg":"rgba(255,255,255,.92)","--intext":"#0f172a","--inborder":"#d8e2f0","--blue":"#3b82f6",
    "--card-bg":"#ffffff","--card-bg2":"#fbfdff","--card-text":"#1e293b","--card-muted":"#64748b","--card-line":"#dbe4f2"
  },
  deepblue: {
    "--bg":"#0b1830","--panel":"#18263b","--text":"#ecf3ff","--text2":"#d8e8ff","--muted":"#9fb4cc","--line":"#2f3f58","--icon":"#b8c7da",
    "--inbg":"rgba(19,33,54,.72)","--intext":"#e8f1ff","--inborder":"rgba(51,71,97,.9)","--blue":"#4f8fff",
    "--card-bg":"#17263b","--card-bg2":"#1b2d45","--card-text":"#eaf2ff","--card-muted":"#a9bdd7","--card-line":"#2f4564"
  },
  graphite: {
    "--bg":"#121417","--panel":"#1a1e24","--text":"#f3f4f6","--text2":"#e5e7eb","--muted":"#9ca3af","--line":"#323844","--icon":"#c1c7d0",
    "--inbg":"rgba(23,27,33,.72)","--intext":"#f3f4f6","--inborder":"rgba(58,67,80,.9)","--blue":"#4f7cff",
    "--card-bg":"#1b2028","--card-bg2":"#212733","--card-text":"#f3f4f6","--card-muted":"#a6b0bf","--card-line":"#3a4352"
  }
};
const Gate = {
  key: 'emby_admin_token',
getToken() { return (sessionStorage.getItem(this.key) || '').trim(); },
setToken(v) { sessionStorage.setItem(this.key, String(v || '').trim()); },
clearToken() { sessionStorage.removeItem(this.key); },
bindEvents() {
  const btn = $('#gateBtn');
  const input = $('#gatePwd');
  if (btn) {
    if (!btn.__bound) {
      btn.__bound = true;
      btn.addEventListener('click', () => this.check());
    }
  }
  if (input && !input.__bound) {
    input.__bound = true;
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') this.check();
    });
  }
},
async check() {
  $('#gateTip').classList.remove('ok');
  $('#gateTip').innerText = '登录中...';
  const v = ($('#gatePwd').value || '').trim();
  if (!v) { $('#gateTip').innerText = '请输入 ADMIN_TOKEN'; return; }
  this.setToken(v);
  const d = await API.listCached({ ttl: 0, force: true });
  if (d && !d.error) {
    $('#gateTip').classList.add('ok');
    $('#gateTip').innerText = '登录成功';
    $('#gate').style.display='none';
    $('#app').style.display='block';
    App.init(d);
    return;
  }
  this.clearToken();
  $('#gateTip').classList.remove('ok');
  if (d && d.error === 'UNAUTHORIZED') $('#gateTip').innerText = '令牌错误';
  else $('#gateTip').innerText = d?.error || '登录失败';
},
async boot() {
  this.bindEvents();
    const token = this.getToken();
    if (!token) { $('#gate').style.display='flex'; $('#app').style.display='none'; return; }
    const d = await API.listCached({ ttl: 0, force: true });
    if (d && !d.error) {
      $('#gate').style.display='none';
      $('#app').style.display='block';
      App.init(d);
    } else {
      this.clearToken();
      $('#gate').style.display='flex';
      $('#app').style.display='none';
    }
  },
  logout() { this.clearToken(); location.reload(); }
};
const API = {
  _listCache: null,
  _listCacheAt: 0,
  _listInflight: null,
  async req(data) {
    const headers = { 'Content-Type':'application/json' };
    const token = Gate.getToken();
    if (token) headers['Authorization'] = 'Bearer ' + token;
    let r;
    try {
      r = await fetch('/admin', { method:'POST', headers, body: JSON.stringify(data) });
    } catch {
      return { error:'网络异常' };
    }
    let d = {};
    try { d = await r.json(); } catch {}
    if (!r.ok) return { error: d.error || ('HTTP_' + r.status), status: r.status };
    return d;
  },
  async listCached({ ttl = 10000, force = false } = {}) {
    const now = Date.now();
    if (!force && this._listCache && (now - this._listCacheAt) < ttl) {
      return this._listCache;
    }
    if (!force && this._listInflight) {
      return await this._listInflight;
    }
    const p = this.req({ action:'list' }).finally(() => {
      this._listInflight = null;
    });
    this._listInflight = p;
    const d = await p;
    if (d && !d.error) {
      this._listCache = d;
      this._listCacheAt = Date.now();
    }
    return d;
  },
  clearListCache() {
    this._listCache = null;
    this._listCacheAt = 0;
    this._listInflight = null;
  }
};
const App = {
  nodes: [],
  filterText: '',
  selectedTags: new Set(),
  allTags: [],
  selected: new Set(),
  visibleMap: new Set(),
  expandMap: new Set(),
  statusMap: {},
  editingOldName: null,
    kThemePreset: 'emby_theme_preset',
  kDensity: 'emby_density',
  kBg: 'emby_bg_cfg',
async init(prefetchedList = null){
  this.loadPrefs();
  if (prefetchedList && !prefetchedList.error) {
    this.applyListData(prefetchedList);
  } else {
    await this.refresh();
  }
  mountFabToControls();
  this.bindBgRangePreview();
},
openTagSuggest(){
  const inp = $('#inTag');
  if(!inp) return;
  inp.focus();
  try { inp.dispatchEvent(new KeyboardEvent('keydown', { key:'ArrowDown', bubbles:true })); } catch {}
},
  loadPrefs(){
    this.setPreset(localStorage.getItem(this.kThemePreset) || 'light', false);
    this.setDensity(localStorage.getItem(this.kDensity) || 'compact', false);
    this.applyBg(this.getBgCfg());
  },
  setPreset(name, needToast=true){
    const p = PRESETS[name] || PRESETS.light;
    Object.keys(p).forEach(k=>document.documentElement.style.setProperty(k,p[k]));
    localStorage.setItem(this.kThemePreset,name);
    if(needToast) this.toast('主题已切换','success');
  },
  quickTheme(){
    const cur = localStorage.getItem(this.kThemePreset) || 'light';
    const next = cur==='light' ? 'deepblue' : (cur==='deepblue' ? 'graphite' : 'light');
    this.setPreset(next,true);
  },
  setDensity(mode, needToast=true){
    const compact = mode !== 'cozy';
    document.documentElement.style.setProperty('--density-gap', compact?'12px':'16px');
    document.documentElement.style.setProperty('--density-card-pad', compact?'14px':'18px');
    document.documentElement.style.setProperty('--density-name-size', compact?'34px':'38px');
    document.documentElement.style.setProperty('--density-label-size', compact?'16px':'17px');
    document.documentElement.style.setProperty('--density-mono-size', compact?'14px':'15px');
    localStorage.setItem(this.kDensity, compact?'compact':'cozy');
    if(needToast) this.toast('密度已切换','success');
  },
  getBgCfg(){ try{return JSON.parse(localStorage.getItem(this.kBg)||'{}');}catch{return {};} },
  applyBg(cfg){
    const url = String(cfg.url||'').trim();
    const brightness = Number(cfg.brightness ?? 100);
    const blur = Number(cfg.blur ?? 0);
    const overlay = Number(cfg.overlay ?? 20);
    document.documentElement.style.setProperty('--bg-brightness', brightness+'%');
    document.documentElement.style.setProperty('--bg-blur', blur+'px');
    document.documentElement.style.setProperty('--bg-overlay', String(overlay/100));
    $('#bgLayer').style.filter = 'brightness('+brightness+'%) blur('+blur+'px)';
    $('#bgOverlay').style.background = 'rgba(0,0,0,'+(Math.max(0,Math.min(80,overlay))/100)+')';
    if(url){
      $('#bgLayer').style.backgroundImage='url("'+url.replace(/"/g,'\\\\\\"')+'")';
      document.body.classList.add('has-bg');
    }else{
      $('#bgLayer').style.backgroundImage='none';
      document.body.classList.remove('has-bg');
    }
  },
  openBgModal(){
    const cfg = this.getBgCfg();
    $('#bgUrl').value = cfg.url || '';
    $('#bgBrightness').value = String(cfg.brightness ?? 100);
    $('#bgBlur').value = String(cfg.blur ?? 0);
    $('#bgOverlayRange').value = String(cfg.overlay ?? 20);
    this.refreshBgRangeText();
    this.openModal('bgModal');
  },
  bindBgRangePreview(){
    ['bgBrightness','bgBlur','bgOverlayRange'].forEach(id=>{
      const el = document.getElementById(id);
      if(!el) return;
      el.addEventListener('input',()=>{
        this.refreshBgRangeText();
        this.applyBg({
          url: ($('#bgUrl').value || '').trim(),
          brightness: Number($('#bgBrightness').value || 100),
          blur: Number($('#bgBlur').value || 0),
          overlay: Number($('#bgOverlayRange').value || 20)
        });
      });
    });
  },
  refreshBgRangeText(){
    $('#bgBrightnessVal').innerText = ($('#bgBrightness').value || 100)+'%';
    $('#bgBlurVal').innerText = ($('#bgBlur').value || 0)+'px';
    $('#bgOverlayVal').innerText = ($('#bgOverlayRange').value || 20)+'%';
  },
  saveBg(){
    const cfg = {
      url: ($('#bgUrl').value || '').trim(),
      brightness: Number($('#bgBrightness').value || 100),
      blur: Number($('#bgBlur').value || 0),
      overlay: Number($('#bgOverlayRange').value || 20)
    };
    localStorage.setItem(this.kBg, JSON.stringify(cfg));
    this.applyBg(cfg);
    this.toast('背景设置已保存','success');
    this.closeAllModals();
  },
  clearBg(){
    const cfg = this.getBgCfg();
    cfg.url = '';
    localStorage.setItem(this.kBg, JSON.stringify(cfg));
    this.applyBg(cfg);
    this.toast('背景已清除','warn');
  },
applyListData(d){
  this.nodes = (d.nodes || []).map(n=>({ ...n, tag:n.tag||'', note:n.note||'' }));
  $('#nodeCount').innerText = this.nodes.length + '个';
  this.buildTagFilter();
  this.updateTagSuggestions();
  this.renderList();
  this.updateGlobalVisibilityBtn();
  this.updateBatchBar();
},
async refresh(){
  const d = await API.listCached({ ttl: 0, force: true });
  if(d.error){
    if (d.error === 'UNAUTHORIZED') { this.toast('登录失效，请重新登录','error'); Gate.logout(); return; }
    this.toast(d.error,'error');
    return;
  }
  this.applyListData(d);
},
  buildTagFilter(){
    this.allTags = Array.from(new Set(this.nodes.map(n=>(n.tag||'').trim()).filter(Boolean))).sort((a,b)=>a.localeCompare(b,'zh-CN'));
    this.selectedTags = new Set([...this.selectedTags].filter(t => this.allTags.includes(t)));
    this.updateTagFilterBtn();
  },
  updateTagFilterBtn(){
    const btn = $('#tagFilterBtn');
    if(!btn) return;
    if(this.selectedTags.size===0){ btn.textContent = '标签：全部'; return; }
    const arr = [...this.selectedTags];
    btn.textContent = arr.length===1 ? ('标签：' + arr[0]) : ('标签：' + arr[0] + ' +' + (arr.length-1));
  },
  openTagPicker(){
    const box = $('#tagPickerList');
    if(!this.allTags.length){
      box.innerHTML = '<div class="tag-empty">暂无标签</div>';
    }else{
      box.innerHTML = this.allTags.map((t)=>{
        const ck = this.selectedTags.has(t) ? 'checked' : '';
        return '<label class="tag-item"><input type="checkbox" data-tag="'+this.escapeHtml(t)+'" '+ck+'> <span>'+this.escapeHtml(t)+'</span></label>';
      }).join('');
    }
    this.openModal('tagPicker');
  },
  applyTagFilter(){
    const boxes = document.querySelectorAll('#tagPickerList input[type="checkbox"]');
    const set = new Set();
    boxes.forEach(el=>{ if(el.checked) set.add(el.getAttribute('data-tag') || ''); });
    this.selectedTags = set;
    this.updateTagFilterBtn();
    this.closeAllModals();
    this.renderList();
    this.updateBatchBar();
  },
  clearTagFilter(){
    this.selectedTags.clear();
    this.updateTagFilterBtn();
    this.closeAllModals();
    this.renderList();
    this.updateBatchBar();
  },
updateTagSuggestions(){
  const dl = $('#tagSuggestions');
  if(!dl) return;
  const freq = new Map();
  this.nodes.forEach(n => {
    const t = (n.tag || '').trim();
    if (!t) return;
    freq.set(t, (freq.get(t) || 0) + 1);
  });
  const tags = [...freq.entries()]
    .sort((a,b)=>(b[1]-a[1]) || a[0].localeCompare(b[0], 'zh-CN'))
    .slice(0, 30)
    .map(x => x[0]);
  dl.innerHTML = tags.map(t => '<option value="'+this.escapeHtml(t)+'"></option>').join('');
},
  getFiltered() {
  const txt = (this.filterText || '').toLowerCase();
  return this.nodes.filter((n) => {
    const tag = (n.tag || '').trim();
    const okTag = this.selectedTags.size === 0 || this.selectedTags.has(tag);
    const okText =
      !txt ||
      n.name.toLowerCase().includes(txt) ||
      (n.displayName || '').toLowerCase().includes(txt) ||
      (n.note || '').toLowerCase().includes(txt);
    return okTag && okText;
  });
},
  filter(v){ this.filterText=v||''; this.renderList(); this.updateBatchBar(); },
  dragName: '',
  sortByOrder(arr) {
    return [...arr].sort((a, b) => {
      const af = !!a.fav, bf = !!b.fav;
      if (af !== bf) return af ? -1 : 1; 
      const ar = Number.isFinite(a.rank) ? a.rank : 1e9;
      const br = Number.isFinite(b.rank) ? b.rank : 1e9;
      if (ar !== br) return ar - br;
      return String(a.name || '').localeCompare(String(b.name || ''), 'zh-CN');
    });
  },
async moveOrder(dragName, targetName) {
  if (!dragName || !targetName || dragName === targetName) return;
  const all = this.sortByOrder(this.nodes).map(n => n.name);
  const from = all.indexOf(dragName);
  const to = all.indexOf(targetName);
  if (from < 0 || to < 0) return;
  all.splice(from, 1);
  all.splice(to, 0, dragName);
  const rankMap = new Map(all.map((name, i) => [name, i + 1]));
  this.nodes = this.nodes.map(n => ({ ...n, rank: rankMap.get(n.name) ?? n.rank }));
  this.renderList();
  const r = await API.req({ action: 'saveOrder', names: all });
  if (!r.success) {
    this.toast(r.error || '保存排序失败', 'error');
    await this.refresh(); 
    return;
  }
  this.toast('排序已保存', 'success');
},
bindCardDrag(card, name) {
  card.setAttribute('draggable', 'true');
  card.addEventListener('dragstart', (e) => {
    this.dragName = name;
    card.classList.add('dragging');
    try { e.dataTransfer.setData('text/plain', name); } catch {}
    if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
  });
  card.addEventListener('dragend', () => {
    this.dragName = '';
    card.classList.remove('dragging');
    document.querySelectorAll('.card.drag-over').forEach(el => el.classList.remove('drag-over'));
  });
  card.addEventListener('dragenter', (e) => {
    e.preventDefault();
    if (this.dragName && this.dragName !== name) card.classList.add('drag-over');
  });
  card.addEventListener('dragover', (e) => {
    e.preventDefault();
    if (this.dragName && this.dragName !== name) card.classList.add('drag-over');
  });
  card.addEventListener('dragleave', () => card.classList.remove('drag-over'));
  card.addEventListener('drop', async (e) => {
    e.preventDefault();
    e.stopPropagation();
    card.classList.remove('drag-over');
    const drag = this.dragName || (e.dataTransfer ? e.dataTransfer.getData('text/plain') : '');
    if (!drag || drag === name) return;
    await this.moveOrder(drag, name);
  });
},
  tagClass(tag){
    const t = (tag||'').toLowerCase();
    if (t.includes('公益') || t.includes('公费')) return 'b-green';
    if (t.includes('白名单')) return 'b-blue';
    if (t.includes('机场')) return 'b-orange';
    return 'b-gray';
  },
isMobileOS(){
  const ua = navigator.userAgent || '';
  const isAndroid = /Android/i.test(ua);
  const isiOS = /iPhone|iPad|iPod/i.test(ua) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  return isAndroid || isiOS;
},
  statusText(name){
    const s = this.statusMap[name];
    if(!s) return {cls:'unknown',txt:'未检测'};
    if(s.online) return {cls:'online',txt:'在线 '+s.latency+'ms'};
    return {cls:'offline',txt:'离线'};
  },
  escapeHtml(s){
    if(s==null) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;');
  },
  iconBtn(svg, title, onClick, extraStyle){
    const b = document.createElement('button');
    b.className = 'icon-btn';
    if (title) b.title = title;
    if (extraStyle) b.style.cssText = extraStyle;
    b.innerHTML = svg;
    b.addEventListener('click', onClick);
    return b;
  },
  parseServerInfoFromUrl(fullUrl){
    try{
      const u = new URL(fullUrl);
      const host = u.hostname;
      const port = u.port ? Number(u.port) : (u.protocol === 'https:' ? 443 : 80);
      const path = (u.pathname && u.pathname !== '/') ? u.pathname : '';
      const https = u.protocol === 'https:';
      return { host, port, path, https, raw: fullUrl };
    }catch{
      return null;
    }
  },
  buildManualImportText(fullUrl, username, password){
  const info = this.parseServerInfoFromUrl(fullUrl);
  if (!info) return '';
  const scheme = info.https ? 'https' : 'http';
  const port = info.port;
  const hasCustomPort =
    (scheme === 'https' && Number(port) !== 443) ||
    (scheme === 'http' && Number(port) !== 80);
  const origin = scheme + '://' + info.host + (hasCustomPort ? (':' + String(port)) : '');
  const pathOnly = info.path || '/';
  const NL = String.fromCharCode(10);
  return (
    '地址: ' + origin + NL +
    '用户名: ' + String(username || '') + NL +
    '密码: ' + String(password || '') + NL +
    '路径: ' + pathOnly
  );
},
  buildHillsImportUrls(fullUrl, username, password){
  const info = this.parseServerInfoFromUrl(fullUrl);
  if (!info) return null;
  const scheme = info.https ? 'https' : 'http';
  const port = info.port;
  const pathOnly = info.path || '';
  const normPath = pathOnly ? (pathOnly.startsWith('/') ? pathOnly : ('/' + pathOnly)) : '';
  const hasCustomPort =
    (scheme === 'https' && Number(port) !== 443) ||
    (scheme === 'http' && Number(port) !== 80);
  const origin = scheme + '://' + info.host + (hasCustomPort ? (':' + String(port)) : '');
  const mobileQs = new URLSearchParams({
    type: 'emby',
    scheme: String(scheme || ''),
    host: String(info.host || ''),
    port: String(port || ''),
    username: String(username || ''),
    password: String(password || '')
  });
  const mobileUrl = 'hills://import?' + mobileQs.toString();
  const windowsUrl =
    'hills://import?type=emby' +
    '&scheme=' + encodeURIComponent(String(scheme || '')) +
    '&host=' + encodeURIComponent(String(info.host || '')) +
    '&port=' + encodeURIComponent(String(port || '')) +
    '&username=' + encodeURIComponent(String(username || '')) +
    '&password=' + encodeURIComponent(String(password || ''));
  return { mobileUrl, windowsUrl, pathOnly };
},
buildEmbyImportUrl(app, fullUrl, username, password){
  const info = this.parseServerInfoFromUrl(fullUrl);
  if (!info) return null;
  let scheme = info.https ? 'https' : 'http';
  let port = info.port;
  if (app === 'forward') {
    scheme = 'https';
    port = 443;
  }
  const qs = new URLSearchParams({
    type: 'emby',
    scheme: String(scheme || ''),
    host: String(info.host || ''),
    port: String(port || ''),
    username: String(username || ''),
    password: String(password || '')
  });
  let prefix = '';
  if (app === 'hills') prefix = 'hills://import?';
  else if (app === 'forward') prefix = 'forward://import?';
  else return null;
  return {
    schemeUrl: prefix + qs.toString(),
    pathOnly: info.path || ''
  };
},
  openAppScheme(scheme){
    window.location.href = scheme;
  },
showPathModal(text, appName, onOpen, opts = {}){
  if (!text) { onOpen(); return; }
  const isManual = !!opts.manual;
  const old = document.getElementById('pathModalMask');
  if (old) old.remove();
  const mask = document.createElement('div');
  mask.id = 'pathModalMask';
  mask.style.position = 'fixed';
  mask.style.inset = '0';
  mask.style.zIndex = '99999';
  mask.style.background = 'rgba(0,0,0,.45)';
  mask.style.display = 'flex';
  mask.style.alignItems = 'center';
  mask.style.justifyContent = 'center';
  mask.style.padding = '16px';
  const box = document.createElement('div');
  box.style.width = 'min(92vw,420px)';
  box.style.background = 'var(--panel)';
  box.style.color = 'var(--text)';
  box.style.border = '1px solid var(--line)';
  box.style.borderRadius = '14px';
  box.style.padding = '14px';
  const title = document.createElement('div');
  title.style.fontWeight = '700';
  title.style.marginBottom = '8px';
  title.textContent = appName + (isManual ? ' 导入信息提示' : ' 路径填写提示');
  const sub = document.createElement('div');
  sub.style.fontSize = '13px';
  sub.style.opacity = '.9';
  sub.style.marginBottom = '8px';
  sub.textContent = isManual
    ? '该播放器可能不支持自动导入，请复制以下信息手动填写：'
    : '该播放器暂不支持自动写入 Path，请复制后粘贴到 Path：';
  const code = document.createElement('div');
  code.style.padding = '10px';
  code.style.border = '1px dashed #64748b';
  code.style.borderRadius = '10px';
  code.style.wordBreak = 'break-all';
  code.style.whiteSpace = 'pre-wrap';
  code.style.marginBottom = '12px';
  code.textContent = text;
  const btnRow = document.createElement('div');
  btnRow.style.display = 'flex';
  btnRow.style.gap = '8px';
  btnRow.style.justifyContent = 'flex-end';
  function mkBtn(txt, primary){
    const b = document.createElement('button');
    b.textContent = txt;
    b.style.padding = '8px 12px';
    b.style.borderRadius = '10px';
    b.style.cursor = 'pointer';
    if (primary) {
      b.style.border = 'none';
      b.style.background = '#3b82f6';
      b.style.color = '#fff';
    } else {
      b.style.border = '1px solid #64748b';
      b.style.background = 'transparent';
      b.style.color = 'inherit';
    }
    return b;
  }
  const btnCancel = mkBtn('取消', false);
  const btnCopy = mkBtn(isManual ? '复制信息' : '复制路径', false);
  const btnOpen = mkBtn('打开 ' + appName, true);
  const close = () => mask.remove();
  btnCancel.onclick = close;
  btnCopy.onclick = async () => {
    try { await navigator.clipboard.writeText(text); } catch {}
    this.toast(isManual ? '导入信息已复制' : '路径已复制：' + text, 'success');
  };
  btnOpen.onclick = () => {
    close();
    onOpen();
  };
  mask.addEventListener('click', (e) => { if (e.target === mask) close(); });
  btnRow.appendChild(btnCancel);
  btnRow.appendChild(btnCopy);
  btnRow.appendChild(btnOpen);
  box.appendChild(title);
  box.appendChild(sub);
  box.appendChild(code);
  box.appendChild(btnRow);
  mask.appendChild(box);
  document.body.appendChild(mask);
},
openWithPathModal(app, schemeUrl, text, opts = {}){
  const nameMap = {
    sen: 'SenPlayer',
    epx: 'EPlayerX',
    hills: 'Hills',
    rodel: '小幻(Win)',
    forward: 'Forward'
  };
  const appName = nameMap[app] || app;
  this.showPathModal(text, appName, () => {
    this.openAppScheme(schemeUrl);
    this.toast('已打开 ' + appName, 'success');
  }, opts);
},
buildPathQuery(pathOnly){
  const p = String(pathOnly || '').trim();
  if (!p) return '';
  const norm = p.startsWith('/') ? p : ('/' + p);
  const ep = encodeURIComponent(norm);
  return '&path=' + ep + '&basePath=' + ep + '&serverPath=' + ep;
},
toggleEditorPass(){
  const ip = $('#inPass');
  const icon = $('#togglePassIcon');
  if (!ip) return;
  const show = ip.type === 'password';
  ip.type = show ? 'text' : 'password';
  if (icon) {
    icon.innerHTML = show ? SVG.eyeOff : SVG.eye;
  }
  const btn = $('#togglePassBtn'); if (btn) btn.blur();
},
async quickAddThirdParty(app, node, fullUrl){
  const address = String(fullUrl || '').trim();
  if (!address) {
    this.toast('缺少代理地址', 'error');
    return;
  }
  let origin = address;
  let full = address;
  let pathOnly = '';
  let host = '';
  let port = '';
  let scheme = '';
  try {
    const u = new URL(address);
    origin = u.origin;
    full = u.origin + (u.pathname || '/');
    pathOnly = (u.pathname && u.pathname !== '/') ? u.pathname : '';
    host = u.hostname || '';
    scheme = (u.protocol || '').replace(':', '');
    port = u.port || (u.protocol === 'https:' ? '443' : '80');
  } catch {}
  const userTrim = String((node && node.embyUser) || '').trim();
  const passTrim = String((node && node.embyPass) || '').trim();
  const uName = encodeURIComponent(userTrim);
  const pWord = encodeURIComponent(passTrim);
  const pqs = this.buildPathQuery(pathOnly);
  if (app === 'sen') {
    const url =
      'senplayer://importserver?type=emby' +
      '&address=' + encodeURIComponent(origin) +
      '&username=' + uName +
      '&password=' + pWord +
      '&scheme=' + encodeURIComponent(scheme) +
      '&host=' + encodeURIComponent(host) +
      '&port=' + encodeURIComponent(String(port)) +
      pqs;
    this.openWithPathModal('sen', url, pathOnly);
    return;
  }
  if (app === 'epx') {
    const url =
      'eplayerx://add-or-update?type=emby' +
      '&href=' + encodeURIComponent(full) +
      '&username=' + uName +
      '&password=' + pWord +
      '&scheme=' + encodeURIComponent(scheme) +
      '&host=' + encodeURIComponent(host) +
      '&port=' + encodeURIComponent(String(port)) +
      pqs;
    this.openWithPathModal('epx', url, pathOnly);
    return;
  }
  if (app === 'capy') {
    try { await navigator.clipboard.writeText(address); } catch {}
    this.toast('已复制代理地址（Capy 请手动粘贴）', 'warn');
    return;
  }
 if (app === 'hills') {
  const built = this.buildHillsImportUrls(address, userTrim, passTrim);
  if (!built) {
    this.toast('生成 Hills 导入链接失败', 'error');
    return;
  }
  const ua = (navigator.userAgent || '').toLowerCase();
  const isWindows = ua.includes('windows nt');
  const hillsUrl = isWindows ? built.windowsUrl : built.mobileUrl;
  console.log('HILLS URL =>', hillsUrl);
  if (isWindows) {
    const manualText = this.buildManualImportText(address, userTrim, passTrim);
    let copied = false;
    try {
      await navigator.clipboard.writeText(manualText);
      copied = true;
    } catch (e) {}
    if (copied) {
      this.toast('已复制手动导入信息（地址/用户名/密码/路径）', 'warn');
    } else {
      this.toast('剪贴板被浏览器拦截，已弹出手动复制框', 'warn');
      window.prompt('请复制以下导入信息：', manualText);
    }
    this.openWithPathModal('hills', hillsUrl, manualText, { manual: true });
    return;
  }
  this.openWithPathModal('hills', hillsUrl, built.pathOnly);
  return;
}
  if (app === 'rodel') {
    const built = this.buildHillsImportUrls(address, userTrim, passTrim);
    if (!built) {
      this.toast('生成小幻导入链接失败', 'error');
      return;
    }
    const rodelUrl = String(built.windowsUrl || '').replace('hills://', 'rodelplayer://');
    const ua = (navigator.userAgent || '').toLowerCase();
    const isWindows = ua.includes('windows nt');
    if (isWindows) {
      const manualText = this.buildManualImportText(address, userTrim, passTrim);
      let copied = false;
      try {
        await navigator.clipboard.writeText(manualText);
        copied = true;
      } catch {}
      if (copied) {
        this.toast('已复制手动导入信息（地址/用户名/密码/路径）', 'warn');
      } else {
        this.toast('剪贴板被浏览器拦截，已弹出手动复制框', 'warn');
        window.prompt('请复制以下导入信息：', manualText);
      }
      this.openWithPathModal('rodel', rodelUrl, manualText, { manual: true });
      return;
    }
    this.openWithPathModal('rodel', rodelUrl, built.pathOnly);
    return;
  }
  if (app === 'forward') {
    const built = this.buildEmbyImportUrl(app, address, userTrim, passTrim);
    if (!built) {
      this.toast('生成导入链接失败', 'error');
      return;
    }
    this.openWithPathModal('forward', built.schemeUrl, built.pathOnly);
    return;
  }
},
  getAllVisibilityKeys() {
    const keys = [];
    for (const n of this.nodes) {
      keys.push(n.name + ':target');
      keys.push(n.name + ':proxyVis');
    }
    return keys;
  },
  areAllVisible() {
    const keys = this.getAllVisibilityKeys();
    if (!keys.length) return false;
    return keys.every(k => this.visibleMap.has(k));
  },
  updateGlobalVisibilityBtn() {
    const btn = document.getElementById('toggleAllVisBtn');
    if (!btn) return;
    const allVisible = this.areAllVisible();
    btn.innerHTML = allVisible
      ? '<span class="state-dot on"></span>隐藏全部地址'
      : '<span class="state-dot off"></span>显示全部地址';
  },
  toggleAllVisibility() {
    const keys = this.getAllVisibilityKeys();
    if (!keys.length) return this.toast('暂无节点', 'warn');
    const allVisible = keys.every(k => this.visibleMap.has(k));
    if (allVisible) {
      keys.forEach(k => this.visibleMap.delete(k));
      this.toast('已隐藏全部节点地址', 'success');
    } else {
      keys.forEach(k => this.visibleMap.add(k));
      this.toast('已显示全部节点地址', 'success');
    }
    this.renderList();
  },
  renderList(){
    const arr = this.sortByOrder(this.getFiltered());
    const list = $('#list');
    list.innerHTML = '';
    if(!arr.length){
      const empty = document.createElement('div');
      empty.className = 'card glass';
      empty.style.gridColumn = '1 / -1';
      empty.style.textAlign = 'center';
      empty.style.color = 'var(--muted)';
      empty.textContent = '暂无节点';
      list.appendChild(empty);
      this.updateGlobalVisibilityBtn();
      return;
    }
    for(const n of arr){
     const normalUrl = location.origin + '/' + encodeURIComponent(n.name);
     const fullUrl = n.secret ? (normalUrl + '/' + encodeURIComponent(n.secret)) : normalUrl;
      const kTarget = n.name + ':target';
      const kProxyVis = n.name + ':proxyVis';
      const kProxyExp = n.name + ':proxyExp';
      const showTarget = this.visibleMap.has(kTarget);
      const showProxy = this.visibleMap.has(kProxyVis);
      const targetExpanded = this.expandMap.has(kTarget);
      const proxyExpanded = this.expandMap.has(kProxyExp);
      const st = this.statusText(n.name);
      const card = document.createElement('div');
      card.className = 'card glass';
      this.bindCardDrag(card, n.name);
      const row = document.createElement('div');
      row.className = 'row';
      const leftWrap = document.createElement('div');
leftWrap.className = 'left-wrap';
const leftHead = document.createElement('div');
leftHead.className = 'left-head';
const sel = document.createElement('label');
sel.className = 'selbox';
const cb = document.createElement('input');
cb.type = 'checkbox';
cb.checked = this.selected.has(n.name);
cb.addEventListener('change', () => this.toggleSelect(n.name, cb.checked));
sel.appendChild(cb);
const info = document.createElement('div');
info.className = 'info';
const h3 = document.createElement('h3');
h3.className = 'name';
h3.textContent = (n.displayName || '').trim() || n.name;
h3.title = ((n.displayName || '').trim() || n.name) + '\\n/' + n.name;
const pathTip = document.createElement('div');
pathTip.className = 'path-tip';
pathTip.textContent = '/' + n.name;
const badges = document.createElement('div');
badges.className = 'badges';
const bm = document.createElement('span');
bm.className = 'badge b-mode-normal';
bm.textContent = '反代';
badges.appendChild(bm);
if ((n.tag || '').trim()) {
  const b1 = document.createElement('span');
  b1.className = 'badge ' + this.tagClass(n.tag);
  b1.textContent = n.tag;
  badges.appendChild(b1);
}
if ((n.note || '').trim()) {
  const b2 = document.createElement('span');
  b2.className = 'badge b-note';
  b2.textContent = n.note;
  badges.appendChild(b2);
}
const status = document.createElement('div');
status.className = 'status';
const dot = document.createElement('span');
dot.className = 'dot ' + st.cls;
const txt = document.createElement('span');
txt.textContent = st.txt;
status.appendChild(dot);
status.appendChild(txt);
status.appendChild(this.iconBtn(SVG.ping, '检测此节点', () => this.checkNode(n.name)));
info.appendChild(h3);
info.appendChild(pathTip);
info.appendChild(badges);
info.appendChild(status);
leftHead.appendChild(sel);
leftHead.appendChild(info);
leftWrap.appendChild(leftHead);
      const actions = document.createElement('div');
actions.className = 'actions';
const btnFav = this.iconBtn(n.fav ? SVG.starOn : SVG.star, n.fav ? '取消收藏' : '收藏置顶', () => this.toggleFav(n.name));
if (n.fav) btnFav.classList.add('is-fav');
actions.appendChild(btnFav);
actions.appendChild(this.iconBtn(SVG.edit, '编辑', () => this.openEditor(n.name)));
actions.appendChild(this.iconBtn(SVG.trash, '删除', () => this.del(n.name), 'color:#ef4444'));
      row.appendChild(leftWrap);
      row.appendChild(actions);
      card.appendChild(row);
      const line1 = document.createElement('div');
      line1.className = 'line';
      const l1 = document.createElement('div'); l1.className = 'label'; l1.textContent = '目标地址';
      const v1 = document.createElement('div');
v1.className = 'mono ' + (showTarget ? '' : 'muted');
v1.textContent = showTarget ? (n.target || '') : '******';
v1.title = '单击复制目标地址';
v1.addEventListener('click', () => {
  if (!showTarget) return this.toast('请先显示目标地址', 'warn');
  this.copyText(n.target || '', '已复制目标地址');
});
const eye1 = this.iconBtn(showTarget ? SVG.eyeOff : SVG.eye, showTarget ? '隐藏目标地址' : '显示目标地址', () => this.toggleVisibility(kTarget));
eye1.classList.add('eye-toggle', showTarget ? 'on' : 'off');
      line1.appendChild(l1); line1.appendChild(v1); line1.appendChild(eye1);
      line1.appendChild(document.createElement('span')); line1.appendChild(document.createElement('span'));
      card.appendChild(line1);
      const line2 = document.createElement('div');
      line2.className = 'line';
      const l2 = document.createElement('div'); l2.className = 'label'; l2.textContent = '代理地址';
      const v2 = document.createElement('div');
v2.className = 'mono ' + (showProxy ? '' : 'muted');
v2.textContent = showProxy ? fullUrl : '******';
v2.title = '单击复制代理地址';
v2.addEventListener('click', () => {
  if (!showProxy) return this.toast('请先显示代理地址', 'warn');
  this.copyText(fullUrl, '已复制代理地址');
});
const eye2 = this.iconBtn(showProxy ? SVG.eyeOff : SVG.eye, showProxy ? '隐藏代理地址' : '显示代理地址', () => this.toggleVisibility(kProxyVis));
eye2.classList.add('eye-toggle', showProxy ? 'on' : 'off');
      const c1 = this.iconBtn(SVG.copy, '复制普通', () => this.copyText(normalUrl, '已复制普通链接'));
      const c2 = this.iconBtn(SVG.link, '复制完整', () => this.copyText(fullUrl, '已复制完整链接'));
      line2.appendChild(l2); line2.appendChild(v2); line2.appendChild(eye2); line2.appendChild(c1); line2.appendChild(c2);
      card.appendChild(line2);
const sen = document.createElement('button');
sen.className = 'app-btn';
sen.innerText = 'Sen';
sen.title = 'SenPlayer 一键添加';
sen.addEventListener('click', () => this.quickAddThirdParty('sen', n, fullUrl));
const capy = document.createElement('button');
capy.className = 'app-btn capy';
capy.innerText = 'Capy';
capy.title = 'CapyPlayer 复制配置';
capy.addEventListener('click', () => this.quickAddThirdParty('capy', n, fullUrl));
const epx = document.createElement('button');
epx.className = 'app-btn';
epx.innerText = 'Epx';
epx.title = 'EPlayerX 一键添加';
epx.addEventListener('click', () => this.quickAddThirdParty('epx', n, fullUrl));
const ua = (navigator.userAgent || '').toLowerCase();
const isWindows = ua.includes('windows nt');
const hills = document.createElement('button');
hills.className = 'app-btn';
hills.innerText = isWindows ? 'Hills(Win)' : 'Hills';
hills.title = isWindows ? 'Hills Windows 导入' : 'Hills 一键导入';
hills.addEventListener('click', () => this.quickAddThirdParty('hills', n, fullUrl));
const forward = document.createElement('button');
forward.className = 'app-btn';
forward.innerText = 'Forward';
forward.title = 'Forward 一键导入';
forward.addEventListener('click', () => this.quickAddThirdParty('forward', n, fullUrl));
const appRow = document.createElement('div');
appRow.className = 'app-row';
appRow.appendChild(sen);
appRow.appendChild(capy);
appRow.appendChild(epx);
appRow.appendChild(hills);
if (isWindows) {
  const rodel = document.createElement('button');
  rodel.className = 'app-btn';
  rodel.innerText = '小幻(Win)';
  rodel.title = '小幻播放器 Windows 导入';
  rodel.addEventListener('click', () => this.quickAddThirdParty('rodel', n, fullUrl));
  appRow.appendChild(rodel);
}
appRow.appendChild(forward);
if (appRow.childElementCount > 0) {
  card.appendChild(appRow);
}
      list.appendChild(card);
    }
    if (arr.length < 6) { 
      const hint = document.createElement('div');
      hint.className = 'page-hint';
      hint.style.gridColumn = '1 / -1';
      hint.textContent = '可点击右上 + 新增节点';
      list.appendChild(hint);
    }
    this.updateGlobalVisibilityBtn();
  },
  toggleSelect(name, checked){
    if(checked) this.selected.add(name); else this.selected.delete(name);
    this.updateBatchBar();
  },
  selectAllFiltered(){
    this.getFiltered().forEach(n=>this.selected.add(n.name));
    this.renderList();
    this.updateBatchBar();
    this.toast('已全选当前筛选结果','success');
  },
  clearSelection(){
    this.selected.clear();
    this.renderList();
    this.updateBatchBar();
  },
  updateBatchBar(){
    $('#selCount').innerText = String(this.selected.size);
    $('#batchBar').style.display = this.selected.size > 0 ? 'flex' : 'none';
  },
    async applyBatchCred(){
    const names = Array.from(this.selected || []);
    if (!names.length) return this.toast('请先选择节点', 'warn');
    const user = prompt('批量设置用户名（留空不修改）', '');
    if (user === null) return;
    const pass = prompt('批量设置密码（留空不修改）', '');
    if (pass === null) return;
    const setUser = String(user || '').trim();
    const setPass = String(pass || '').trim();
    if (!setUser && !setPass) {
      return this.toast('用户名和密码至少填写一个', 'warn');
    }
    let ok = 0, fail = 0;
    for (const name of names) {
      const n = this.nodes.find(x => x.name === name);
      if (!n) { fail++; continue; }
      const payload = {
        action: 'save',
        oldName: n.name,
        name: n.name,
        displayName: n.displayName || '',
        target: n.target || '',
        mode: n.mode || 'split',
        secret: n.secret || '',
        tag: n.tag || '',
        note: n.note || '',
        rank: Number.isFinite(Number(n.rank)) ? Number(n.rank) : undefined,
        fav: !!n.fav,
        embyUser: setUser ? setUser : (n.embyUser || ''),
        embyPass: setPass ? setPass : (n.embyPass || ''),
        directExternal: !!n.directExternal
      };
      const r = await API.req(payload);
      if (r && r.success && (!r.failed || r.failed === 0)) ok++;
      else fail++;
    }
    API.clearListCache();
    await this.refresh();
    this.toast('批量账号密码完成：成功 ' + ok + '，失败 ' + fail, fail ? 'warn' : 'success');
  },
  async clearBatchCred(){
    const names = Array.from(this.selected || []);
    if (!names.length) return this.toast('请先选择节点', 'warn');
    if (!confirm('确认清空已选 ' + names.length + ' 个节点的 Emby 账号密码？')) return;
    let ok = 0, fail = 0;
    for (const name of names) {
      const n = this.nodes.find(x => x.name === name);
      if (!n) { fail++; continue; }
      const payload = {
        action: 'save',
        oldName: n.name,
        name: n.name,
        displayName: n.displayName || '',
        target: n.target || '',
        mode: n.mode || 'split',
        secret: n.secret || '',
        tag: n.tag || '',
        note: n.note || '',
        rank: Number.isFinite(Number(n.rank)) ? Number(n.rank) : undefined,
        fav: !!n.fav,
        embyUser: '',
        embyPass: '',
        directExternal: !!n.directExternal
      };
      const r = await API.req(payload);
      if (r && r.success && (!r.failed || r.failed === 0)) ok++;
      else fail++;
    }
    API.clearListCache();
    await this.refresh();
    this.toast('清空账号密码完成：成功 ' + ok + '，失败 ' + fail, fail ? 'warn' : 'success');
  },
  async batchDelete(){
    const names = Array.from(this.selected);
    if(!names.length) return this.toast('请先选择节点','warn');
    if(!confirm('确认删除选中节点（'+names.length+'）?')) return;
    const r = await API.req({ action:'batchDelete', names });
    if(r.success){
      this.selected.clear();
      this.toast('已删除 '+(r.count||names.length)+' 个节点','success');
      await this.refresh();
    }else{
      this.toast(r.error || '批量删除失败','error');
    }
  },
  async checkNode(name){
    const r = await API.req({ action:'checkStatus', names:[name] });
    if(r.success && r.results && r.results[0]){
      this.statusMap[name] = r.results[0];
      this.renderList();
      this.toast(name + ': ' + (r.results[0].online ? ('在线 '+r.results[0].latency+'ms') : '离线'), r.results[0].online?'success':'warn');
    }else{
      this.toast(r.error || '检测失败','error');
    }
  },
  async checkSelectedStatus(){
    const names = Array.from(this.selected);
    if(!names.length) return this.toast('先选择节点再检测','warn');
    const r = await API.req({ action:'checkStatus', names });
    if(r.success){
      (r.results || []).forEach(x=>{ this.statusMap[x.name] = x; });
      this.renderList();
      this.toast('选中节点检测完成','success');
    }else{
      this.toast(r.error || '检测失败','error');
    }
  },
  async checkAllStatus(){
    const r = await API.req({ action:'checkStatus' });
    if(r.success){
      (r.results || []).forEach(x=>{ this.statusMap[x.name] = x; });
      this.renderList();
      this.toast('全部节点检测完成','success');
    }else{
      this.toast(r.error || '检测失败','error');
    }
  },
  openModal(id){
    $('#mask').style.display='block';
    $('#'+id).style.display='block';
  },
  closeAllModals(){
    $('#mask').style.display='none';
    ['editor','bgModal','tagPicker'].forEach(id=>{ const e=$('#'+id); if(e) e.style.display='none'; });
  },
splitTargetsText(v){
  return String(v || '')
    .split(/\\r?\\n|[;,，；|]+/g)
    .map(s => s.trim())
    .filter(Boolean);
},
ensureTargetRows(min = 1){
  const list = $('#targetList');
  if (!list) return;
  while (list.children.length < min) this.addTargetInput('');
},
addTargetInput(val = ''){
  const list = $('#targetList');
  if (!list) return;
  const input = document.createElement('input');
  input.className = 'target-item';
  input.placeholder = 'https://example.com';
  input.value = String(val || '');
  input.style.marginTop = '8px';
  list.appendChild(input);
},
removeTargetInput(){
  const list = $('#targetList');
  if (!list) return;
  if (list.children.length <= 1) return;
  list.removeChild(list.lastElementChild);
},
setTargetInputsFromText(text){
  const list = $('#targetList');
  if (!list) return;
  list.innerHTML = '';
  const arr = this.splitTargetsText(text);
  if (!arr.length) {
  this.addTargetInput('');
  return;
}
for (const t of arr) this.addTargetInput(t);
this.ensureTargetRows(1);
},
collectTargetsText(){
  const list = $('#targetList');
  if (!list) return '';
  const arr = [...list.querySelectorAll('input')]
    .map(i => (i.value || '').trim())
    .filter(Boolean);
  return arr.join('\\n');
},
openEditor(name){
  this.editingOldName = '';
  this.currentMode = 'split';
  $('#editorTitle').innerText = '新增节点';
  $('#inName').value = '';
  $('#inDisplayName').value = '';
  $('#inTag').value = '';
  $('#inNote').value = '';
  this.setTargetInputsFromText('');
  $('#inSec').value = '';
  $('#inUser').value = '';
  $('#inPass').value = '';
  $('#inDirectExternal').checked = false;
  if(name){
    const n = this.nodes.find(x=>x.name===name);
    if(n){
      this.editingOldName = n.name;
      $('#editorTitle').innerText = '编辑节点';
      $('#inName').value = n.name || '';
      $('#inDisplayName').value = n.displayName || '';
      $('#inTag').value = n.tag || '';
      $('#inNote').value = n.note || '';
      this.setTargetInputsFromText(n.target || '');
      $('#inSec').value = n.secret || '';
      $('#inUser').value = n.embyUser || '';
      $('#inPass').value = n.embyPass || '';
      $('#inDirectExternal').checked = !!n.directExternal;
      this.currentMode = 'split';
    }
  }
    const tagInput = $('#inTag');
  if (tagInput && !tagInput.dataset.autoSuggestBound) {
    tagInput.addEventListener('focus', () => this.openTagSuggest());
    tagInput.dataset.autoSuggestBound = '1';
  }
  $('#inPass').type = 'password';
const ticon = $('#togglePassIcon');
if (ticon) ticon.innerHTML = SVG.eye;
  this.ensureTargetRows(1);
  this.openModal('editor');
},
async save(){
  const name = ($('#inName').value || '').trim();
  const displayName = ($('#inDisplayName').value || '').trim();
  const tag = ($('#inTag').value || '').trim();
  const note = ($('#inNote').value || '').trim();
  const target = this.collectTargetsText();
  const secret = ($('#inSec').value || '').trim();
  const embyUser = ($('#inUser').value || '').trim();
  const embyPass = ($('#inPass').value || '').trim();
  const directExternal = !!$('#inDirectExternal').checked;
  if(!name || !target) return this.toast('请求路径和目标地址必填','warn');
  const lower = name.toLowerCase();
  const existed = this.nodes.some(x => String(x.name || '').toLowerCase() === lower);
  if (!this.editingOldName && existed) {
    return this.toast('请求路径重复：该节点已存在，请换一个路径', 'warn');
  }
  if (this.editingOldName && this.editingOldName.toLowerCase() !== lower && existed) {
    return this.toast('请求路径重复：该节点已存在，请换一个路径', 'warn');
  }
  const editingNode = this.editingOldName
    ? this.nodes.find(x => String(x.name || '').toLowerCase() === String(this.editingOldName).toLowerCase())
    : null;
  const rank = Number.isFinite(Number(editingNode?.rank)) ? Number(editingNode.rank) : undefined;
  const fav = !!editingNode?.fav;
  const mode = 'split';
  const r = await API.req({
    action:'save',
    name, displayName, target, mode,
    secret, tag, note, rank, fav,
    embyUser, embyPass,
    directExternal,
    oldName: this.editingOldName || ''
  });
  if(!r.success) return this.toast(r.error || '保存失败','error');
  if (r.failed > 0 && Array.isArray(r.errors) && r.errors[0]) {
    return this.toast('保存失败：' + r.errors[0].error, 'error');
  }
  API.clearListCache();
  this.closeAllModals();
  this.toast('保存成功','success');
  await this.refresh();
},
  async toggleFav(name){
  const r = await API.req({ action:'toggleFav', name });
  if(!r.success) return this.toast(r.error || '操作失败','error');
  API.clearListCache();   
  await this.refresh();
},
  async del(name){
    if(!confirm('删除节点: '+name+' ?')) return;
    const r = await API.req({ action:'delete', name });
    if(r.success){
      this.selected.delete(name);
      this.toast('删除成功','success');
      await this.refresh();
    }else{
      this.toast(r.error || '删除失败','error');
    }
  },
  async exportData(){
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([JSON.stringify(this.nodes, null, 2)], { type:'application/json' }));
    a.download = 'nodes_ui.json';
    a.click();
    this.toast('导出完成','success');
  },
  importFile(input){
    const file = input.files && input.files[0];
    if(!file) return;
    const reader = new FileReader();
    reader.onload = async (e)=>{
      try{
        const nodes = JSON.parse(e.target.result);
        const r = await API.req({ action:'import', nodes });
        if(r.success){
          if (r.failed > 0) this.toast('导入完成：成功 '+r.saved+'，失败 '+r.failed,'warn');
          else this.toast('导入成功','success');
          await this.refresh();
        } else {
          this.toast(r.error || '导入失败','error');
        }
      }catch{
        this.toast('导入文件格式错误','error');
      }finally{
        input.value='';
      }
    };
    reader.readAsText(file);
  },
toggleMenu(){
  const m = $('#menuPanel');
  const open = m.style.display !== 'block';
  m.style.display = open ? 'block' : 'none';
  if (open) {
    const vh = Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0);
    m.style.maxHeight = Math.max(220, Math.floor(vh * 0.72)) + 'px';
  }
},
  toggleVisibility(k){
    if(this.visibleMap.has(k)) this.visibleMap.delete(k);
    else this.visibleMap.add(k);
    this.renderList();
  },
  toggleExpand(k){
    if(this.expandMap.has(k)) this.expandMap.delete(k);
    else this.expandMap.add(k);
    this.renderList();
  },
  async copyText(text, msg){
    try{
      await navigator.clipboard.writeText(text);
      this.toast(msg || '复制成功','success');
    }catch{
      this.toast('复制失败','error');
    }
  },
  toast(text, type){
    const wrap = $('#toastWrap');
    const el = document.createElement('div');
    el.className = 'toast ' + (type || 'success');
    el.textContent = text;
    wrap.appendChild(el);
    setTimeout(()=>el.remove(),2200);
  }
};
window.Gate = Gate;
window.App = App;
Gate.boot();
</script>
<div class="project-links">
  <span class="label">项目地址：</span>
  <a href="https://github.com/chenhr454/emby---worker" target="_blank" rel="noopener noreferrer">GitHub</a>
  <span class="label">频道：</span>
  <a href="https://t.me/+tdZEbQpmAktkZmY1" target="_blank" rel="noopener noreferrer">Telegram</a>
</div>
<div class="disclaimer">
  <strong>免责声明：</strong>
  本项目仅供学习与技术测试使用，请遵守当地法律法规。使用者对配置、转发内容与访问行为承担全部责任，开发者不对任何直接或间接损失负责。
</div>
</body>
</html>`;
    return new Response(html, {
      headers: { "Content-Type": "text/html;charset=utf-8" },
    });
  },
};
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/favicon.ico")
      return new Response("", { status: 204 });
    if (url.pathname === "/")
      return new Response(null, {
        status: 302,
        headers: { Location: "/admin" },
      });
    let segments = [];
    try {
      segments = url.pathname
        .split("/")
        .filter(Boolean)
        .map(decodeURIComponent);
    } catch {
      return new Response("Bad Request: invalid URL encoding", { status: 400 });
    }
    const root = segments[0];
    if (root === "admin") {
      if (request.method === "POST") return Database.handleApi(request, env);
      return UI.renderAdmin();
    }
    if (root && root !== "admin") {
      const nodeName = String(root || "")
        .trim()
        .toLowerCase();
      if (/^[a-z0-9_-]{1,32}$/i.test(nodeName)) {
        const nodeData = await Database.getNode(nodeName, env, ctx, "admin");
        if (nodeData) {
          const secret = nodeData.secret || "";
          let valid = true;
          let strip = 1;
          if (secret) {
            if (segments[1] === secret) strip = 2;
            else valid = false;
          }
          if (valid) {
            let remaining = "/" + segments.slice(strip).join("/");
            if (remaining === "/" && !url.pathname.endsWith("/")) {
              const redir = new URL(url.href);
              redir.pathname = redir.pathname + "/";
              return new Response(null, {
                status: 301,
                headers: { Location: redir.toString() },
              });
            }
            if (url.pathname.endsWith("/") && remaining !== "/")
              remaining += "/";
            if (remaining === "") remaining = "/";
            return ProxyHandler.handle(
              request,
              nodeData,
              remaining,
              nodeName,
              secret,
              env,
              "admin",
            );
          }
          return new Response("Node Not Found", { status: 404 });
        }
      }
    }
    const enableDirect = String(env.ENABLE_DIRECT_PROXY || "0") === "1";
    if (!enableDirect) return new Response("Node Not Found", { status: 404 });
    let directRaw = url.pathname.slice(1);
    try {
      directRaw = decodeURIComponent(directRaw);
    } catch {}
    const looksLikeHost =
      /^https?:\/\//i.test(directRaw) || /[.:]/.test(root || "");
    if (!looksLikeHost) return new Response("Node Not Found", { status: 404 });
    return ProxyHandler.handleDirect(request, directRaw, env);
  },
};
