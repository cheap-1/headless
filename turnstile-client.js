/**
 * Receiver client for Lucrehulk cloudflare-turnstile-solver-2026 token server.
 *
 * Protocol (binary, little-endian) — receiver side only:
 *   Request solve:  [1, solver_idx u32, ua_len u8, ua..., (name_len u8, name..., val_len u8, val...)*]
 *   Token reply:    [solver_idx u32, token_bytes...]  OR length 4 = fail, length 1 = [0] no solvers
 *   Solvers count:  send [3] → reply [count u32, 0]
 *
 * The actual browser solvers + Rust token-server + clicker must run separately
 * (GUI machine). This module only *requests* tokens.
 *
 * Env:
 *   TURNSTILE_SERVER   ws://host:8080
 *   TURNSTILE_TIMEOUT  ms (default 45000)
 */

const { WebSocket } = require("ws");

function u32le(n) {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n >>> 0, 0);
  return b;
}

function encodeFields(fields) {
  if (!fields || typeof fields !== "object") return Buffer.alloc(0);
  const parts = [];
  for (const [name, value] of Object.entries(fields)) {
    const nb = Buffer.from(String(name), "utf8");
    const vb = Buffer.from(String(value), "utf8");
    if (nb.length > 255 || vb.length > 255) {
      throw new Error(`field name/value too long: ${name}`);
    }
    parts.push(Buffer.from([nb.length]), nb, Buffer.from([vb.length]), vb);
  }
  return Buffer.concat(parts);
}

function buildSolveRequest({ solverIdx = 0, userAgent = "", fields = {} } = {}) {
  const ua = Buffer.from(userAgent || "", "utf8");
  if (ua.length > 255) throw new Error("userAgent too long");
  return Buffer.concat([
    Buffer.from([1]),
    u32le(solverIdx),
    Buffer.from([ua.length]),
    ua,
    encodeFields(fields)
  ]);
}

/**
 * Request one Turnstile token from the token server.
 * @returns {Promise<{ token: string, solverIdx: number }>}
 */
function requestTurnstileToken(options = {}) {
  const serverUrl =
    options.serverUrl ||
    process.env.TURNSTILE_SERVER ||
    "ws://127.0.0.1:8080";
  const timeoutMs =
    options.timeoutMs ||
    parseInt(process.env.TURNSTILE_TIMEOUT || "45000", 10) ||
    45000;
  const solverIdx = options.solverIdx || 0;
  const userAgent = options.userAgent || "";
  const fields = options.fields || {};

  return new Promise((resolve, reject) => {
    let settled = false;
    let ws;
    const timer = setTimeout(() => {
      fail(new Error(`turnstile token timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    function finish(err, result) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        if (ws && ws.readyState === WebSocket.OPEN) ws.close();
      } catch {}
      if (err) reject(err);
      else resolve(result);
    }
    function fail(err) {
      finish(err);
    }

    try {
      ws = new WebSocket(serverUrl);
    } catch (e) {
      return fail(e);
    }

    ws.binaryType = "nodebuffer";

    ws.on("open", () => {
      try {
        ws.send(buildSolveRequest({ solverIdx, userAgent, fields }));
      } catch (e) {
        fail(e);
      }
    });

    ws.on("message", (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      // No solvers available
      if (buf.length === 1 && buf[0] === 0) {
        return fail(new Error("turnstile: no solvers available"));
      }
      // Available-solvers response (ignore if we didn't ask) length 5
      if (buf.length === 5 && buf[4] === 0) {
        return;
      }
      // Failed solve: only solver_idx, no token
      if (buf.length === 4) {
        return fail(new Error("turnstile: solver returned empty token"));
      }
      if (buf.length < 4) {
        return fail(new Error(`turnstile: unexpected packet len=${buf.length}`));
      }
      const idx = buf.readUInt32LE(0);
      const token = buf.slice(4).toString("utf8");
      if (!token) {
        return fail(new Error("turnstile: empty token string"));
      }
      finish(null, { token, solverIdx: idx });
    });

    ws.on("error", (err) => fail(err));
    ws.on("close", () => {
      if (!settled) fail(new Error("turnstile: connection closed before token"));
    });
  });
}

/**
 * Query how many solvers are currently available.
 */
function getAvailableSolvers(serverUrl) {
  const url = serverUrl || process.env.TURNSTILE_SERVER || "ws://127.0.0.1:8080";
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const timer = setTimeout(() => {
      try {
        ws.close();
      } catch {}
      reject(new Error("solvers count timeout"));
    }, 8000);
    ws.binaryType = "nodebuffer";
    ws.on("open", () => ws.send(Buffer.from([3])));
    ws.on("message", (data) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (buf.length >= 4) {
        clearTimeout(timer);
        const n = buf.readUInt32LE(0);
        try {
          ws.close();
        } catch {}
        resolve(n);
      }
    });
    ws.on("error", (e) => {
      clearTimeout(timer);
      reject(e);
    });
  });
}

module.exports = {
  requestTurnstileToken,
  getAvailableSolvers,
  buildSolveRequest
};
