/**
 * Receiver client for Lucrehulk token-server protocol.
 * Env: TURNSTILE_SERVER, TURNSTILE_TIMEOUT, TURNSTILE_CONCURRENCY
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

function requestTurnstileTokenOnce(options = {}) {
  const serverUrl =
    options.serverUrl ||
    process.env.TURNSTILE_SERVER ||
    "ws://127.0.0.1:8080";
  const timeoutMs =
    options.timeoutMs ||
    parseInt(process.env.TURNSTILE_TIMEOUT || "60000", 10) ||
    60000;
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

      // Solvers unavailable: single byte 0 (or any 1-byte packet)
      if (buf.length === 1) {
        return fail(new Error("turnstile: no solvers available"));
      }
      // Available-solvers response length 5
      if (buf.length === 5 && buf[4] === 0) {
        return;
      }
      // Failed solve: only solver_idx
      if (buf.length === 4) {
        return fail(new Error("turnstile: solver returned empty token"));
      }
      if (buf.length < 5) {
        return fail(
          new Error(
            `turnstile: unexpected packet len=${buf.length} hex=${buf.toString("hex")}`
          )
        );
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

// Limit parallel solves — Lucrehulk only has as many browser solvers as you opened
const MAX_IN_FLIGHT = Math.max(
  1,
  parseInt(process.env.TURNSTILE_CONCURRENCY || "2", 10) || 2
);
let inFlight = 0;
const waitQueue = [];

function pumpQueue() {
  while (inFlight < MAX_IN_FLIGHT && waitQueue.length) {
    const job = waitQueue.shift();
    inFlight++;
    requestTurnstileTokenOnce(job.options)
      .then((r) => {
        inFlight--;
        job.resolve(r);
        pumpQueue();
      })
      .catch((e) => {
        inFlight--;
        // Retry once after short delay if no solvers
        if (
          job.retries < 4 &&
          e &&
          /no solvers available/i.test(String(e.message || e))
        ) {
          job.retries++;
          setTimeout(() => {
            waitQueue.push(job);
            pumpQueue();
          }, 1500 + job.retries * 500);
        } else {
          job.reject(e);
        }
        pumpQueue();
      });
  }
}

function requestTurnstileToken(options = {}) {
  return new Promise((resolve, reject) => {
    waitQueue.push({ options, resolve, reject, retries: 0 });
    pumpQueue();
  });
}

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
