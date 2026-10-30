import "dotenv/config";

import crypto from "node:crypto";
import { PassThrough } from "node:stream";
import express from "express";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import { Pool } from "pg";
import {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "").replace(/\/+$/, "");
const PUBLIC_CATALOG_ENABLED = process.env.PUBLIC_CATALOG_ENABLED === "true";
const INDEXING_ENABLED = process.env.INDEXING_ENABLED === "true";
const DISTRIBUTION_AUTHORIZED = process.env.DISTRIBUTION_AUTHORIZED === "true";
const TELEGRAM_API_ID = Number(process.env.TELEGRAM_API_ID || 0);
const TELEGRAM_API_HASH = process.env.TELEGRAM_API_HASH || "";
const TELEGRAM_SESSION = process.env.TELEGRAM_SESSION || "";
const SOURCE_CHATS = (process.env.TELEGRAM_SOURCE_CHATS || "")
  .split(",").map((value) => value.trim()).filter(Boolean);
const INDEX_MESSAGE_LIMIT = Math.min(5000, Math.max(1, Number(process.env.INDEX_MESSAGE_LIMIT || 500)));
const INDEX_INTERVAL_MS = Math.max(5, Number(process.env.INDEX_INTERVAL_MINUTES || 30)) * 60_000;
const STREAM_SIGNING_SECRET = process.env.STREAM_SIGNING_SECRET || "";

const S3_CONFIGURED = Boolean(
  process.env.S3_ENDPOINT &&
  process.env.S3_BUCKET &&
  process.env.S3_ACCESS_KEY_ID &&
  process.env.S3_SECRET_ACCESS_KEY
);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 8,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
});

const s3 = S3_CONFIGURED
  ? new S3Client({
      endpoint: process.env.S3_ENDPOINT,
      region: process.env.S3_REGION || "auto",
      forcePathStyle: process.env.S3_FORCE_PATH_STYLE !== "false",
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY_ID,
        secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
      },
    })
  : null;

let telegramClient = null;
let telegramReady = false;
let indexerTimer = null;
const activeCacheFills = new Map();
const AUDIO_EXTENSIONS = new Set([
  "mp3", "m4a", "aac", "flac", "ogg", "opus", "wav", "alac", "aiff", "ape", "wma",
]);

function log(level, event, fields = {}) {
  // Never pass secrets, Telegram session strings, signed URLs, or full request URLs here.
  const safe = { time: new Date().toISOString(), level, event, ...fields };
  process.stdout.write(JSON.stringify(safe) + "\n");
}

function requirePublicConfiguration() {
  if (!PUBLIC_CATALOG_ENABLED) return;
  const missing = [];
  if (!DISTRIBUTION_AUTHORIZED) missing.push("DISTRIBUTION_AUTHORIZED=true");
  if (!PUBLIC_BASE_URL.startsWith("https://")) missing.push("PUBLIC_BASE_URL (HTTPS)");
  if (!process.env.DATABASE_URL) missing.push("DATABASE_URL");
  if (!TELEGRAM_API_ID || !TELEGRAM_API_HASH || !TELEGRAM_SESSION) missing.push("Telegram API credentials and session");
  if (!SOURCE_CHATS.length) missing.push("TELEGRAM_SOURCE_CHATS");
  if (!STREAM_SIGNING_SECRET || STREAM_SIGNING_SECRET.length < 32) missing.push("STREAM_SIGNING_SECRET (32+ characters)");
  if (!S3_CONFIGURED) missing.push("S3-compatible object storage settings");
  if (missing.length) throw new Error("Public catalogue cannot start; missing configuration: " + missing.join(", "));
}

function makeManifest() {
  return {
    id: "app.heobeep.telegram-library",
    name: "Telegram Music Library",
    version: "0.1.0",
    description: "A shared music catalogue from approved Telegram sources.",
    resources: ["search", "stream"],
    allowDownloads: 0,
  };
}

function signMediaId(id, expires) {
  return crypto.createHmac("sha256", STREAM_SIGNING_SECRET)
    .update(id + "." + expires).digest("hex");
}

function safeEqualHex(a, b) {
  if (!/^[a-f0-9]{64}$/i.test(a || "") || !/^[a-f0-9]{64}$/i.test(b || "")) return false;
  return crypto.timingSafeEqual(Buffer.from(a, "hex"), Buffer.from(b, "hex"));
}

function mediaUrlFor(req, id) {
  const base = PUBLIC_BASE_URL || (req.protocol + "://" + req.get("host"));
  const expires = Math.floor(Date.now() / 1000) + 10 * 60;
  const sig = signMediaId(id, expires);
  return base + "/media/" + encodeURIComponent(id) +
    "?expires=" + expires + "&sig=" + sig;
}

function formatFrom(filename, mimeType) {
  const extension = (filename || "").split(".").pop()?.toLowerCase();
  if (extension && AUDIO_EXTENSIONS.has(extension)) return extension;
  const subtype = (mimeType || "").split("/")[1]?.toLowerCase();
  return subtype === "mpeg" ? "mp3" : (subtype || "mp3");
}

async function createSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracks (
      id TEXT PRIMARY KEY,
      source_chat TEXT NOT NULL,
      message_id BIGINT NOT NULL,
      title TEXT NOT NULL,
      artist TEXT,
      album TEXT,
      duration INTEGER,
      filename TEXT,
      mime_type TEXT,
      format TEXT NOT NULL DEFAULT 'mp3',
      file_size BIGINT,
      indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (source_chat, message_id)
    );
    CREATE INDEX IF NOT EXISTS tracks_search_idx
      ON tracks USING GIN (to_tsvector('simple',
        coalesce(title, '') || ' ' || coalesce(artist, '') || ' ' || coalesce(album, '')
      ));
    CREATE INDEX IF NOT EXISTS tracks_indexed_at_idx ON tracks (indexed_at DESC);
  `);
}

async function searchTracks(query) {
  const result = await pool.query(
    `SELECT id, title, artist, album, duration, format
       FROM tracks
      WHERE to_tsvector('simple',
        coalesce(title, '') || ' ' || coalesce(artist, '') || ' ' || coalesce(album, '')
      ) @@ plainto_tsquery('simple', $1)
         OR title ILIKE $2
         OR artist ILIKE $2
      ORDER BY
        CASE WHEN lower(title) = lower($1) THEN 0 ELSE 1 END,
        title ASC
      LIMIT 40`,
    [query, "%" + query.replace(/[\\%_]/g, "\\$&") + "%"]
  );

  return result.rows.map((track) => ({
    id: track.id,
    title: track.title,
    artist: track.artist || undefined,
    album: track.album || undefined,
    duration: track.duration || undefined,
    format: track.format,
    audioQuality: ["flac", "alac", "ape", "wav", "aiff"].includes(track.format)
      ? "LOSSLESS" : "HIGH",
  }));
}

async function getTrack(id) {
  const result = await pool.query(
    "SELECT * FROM tracks WHERE id = $1 LIMIT 1", [id]
  );
  return result.rows[0] || null;
}

async function initialiseTelegram() {
  if (!TELEGRAM_API_ID || !TELEGRAM_API_HASH || !TELEGRAM_SESSION) return false;

  const { TelegramClient } = await import("teleproto");
  const { StringSession } = await import("teleproto/sessions");
  telegramClient = new TelegramClient(
    new StringSession(TELEGRAM_SESSION),
    TELEGRAM_API_ID,
    TELEGRAM_API_HASH,
    { connectionRetries: 5, maxConcurrentDownloads: 4 }
  );
  await telegramClient.connect();
  telegramReady = await telegramClient.checkAuthorization();

  if (!telegramReady) {
    await telegramClient.disconnect();
    telegramClient = null;
    throw new Error("The configured Telegram session is not authorised.");
  }

  log("info", "telegram_connected");
  return true;
}

function extractAudio(message, sourceChat) {
  const document = message?.media?.document || message?.document;
  if (!document) return null;

  const attributes = document.attributes || [];
  const audioAttribute = attributes.find((item) =>
    item && (typeof item.duration === "number" || item.title || item.performer)
  );
  const filenameAttribute = attributes.find((item) =>
    item && typeof item.fileName === "string"
  );
  const filename = filenameAttribute?.fileName || "";
  const format = formatFrom(filename, document.mimeType || "");
  const isAudio = (document.mimeType || "").startsWith("audio/") ||
    Boolean(audioAttribute) || AUDIO_EXTENSIONS.has(format);
  if (!isAudio) return null;

  const fallbackTitle = filename.replace(/\.[^.]+$/, "") || "Untitled track";
  const title = (audioAttribute?.title || fallbackTitle || message.message || "").trim();
  const artist = (audioAttribute?.performer || "").trim() || null;
  const id = crypto.createHash("sha256")
    .update(String(sourceChat) + ":" + String(message.id))
    .digest("hex").slice(0, 32);

  return {
    id,
    source_chat: String(sourceChat),
    message_id: String(message.id),
    title: title.slice(0, 300),
    artist: artist?.slice(0, 200) || null,
    album: null,
    duration: Number(audioAttribute?.duration || 0) || null,
    filename: filename.slice(0, 500) || null,
    mime_type: document.mimeType || "application/octet-stream",
    format,
    file_size: document.size ? String(document.size) : null,
  };
}

async function indexSourceChat(sourceChat) {
  let count = 0;
  for await (const message of telegramClient.iterMessages(sourceChat, {
    limit: INDEX_MESSAGE_LIMIT,
  })) {
    const track = extractAudio(message, sourceChat);
    if (!track) continue;

    await pool.query(
      `INSERT INTO tracks
        (id, source_chat, message_id, title, artist, album, duration, filename, mime_type, format, file_size, indexed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
       ON CONFLICT (source_chat, message_id) DO UPDATE SET
         title=EXCLUDED.title, artist=EXCLUDED.artist, album=EXCLUDED.album,
         duration=EXCLUDED.duration, filename=EXCLUDED.filename,
         mime_type=EXCLUDED.mime_type, format=EXCLUDED.format,
         file_size=EXCLUDED.file_size, indexed_at=NOW()`,
      [
        track.id, track.source_chat, track.message_id, track.title, track.artist,
        track.album, track.duration, track.filename, track.mime_type,
        track.format, track.file_size,
      ]
    );
    count += 1;
  }
  return count;
}

async function runIndexPass() {
  if (!INDEXING_ENABLED || !telegramReady || !SOURCE_CHATS.length || !DISTRIBUTION_AUTHORIZED) return;
  const lock = await pool.query("SELECT pg_try_advisory_lock(72638142) AS locked");
  if (!lock.rows[0]?.locked) {
    log("info", "indexer_lock_held_elsewhere");
    return;
  }

  try {
    let indexed = 0;
    for (const chat of SOURCE_CHATS) {
      indexed += await indexSourceChat(chat);
    }
    log("info", "index_pass_complete", { indexed, sources: SOURCE_CHATS.length });
  } catch (error) {
    log("error", "index_pass_failed", { error: error?.name || "Error" });
  } finally {
    await pool.query("SELECT pg_advisory_unlock(72638142)");
  }
}

async function startIndexer() {
  await runIndexPass();
  indexerTimer = setInterval(() => {
    runIndexPass().catch((error) => log("error", "indexer_unhandled", { error: error?.name || "Error" }));
  }, INDEX_INTERVAL_MS);
  indexerTimer.unref?.();
}

function isObjectMissing(error) {
  return error?.$metadata?.httpStatusCode === 404 ||
    error?.name === "NotFound" || error?.Code === "NoSuchKey";
}

async function signedCachedObjectUrl(id) {
  const command = new GetObjectCommand({
    Bucket: process.env.S3_BUCKET,
    Key: "tracks/" + id,
  });
  return getSignedUrl(s3, command, { expiresIn: 5 * 60 });
}

async function streamAndCacheFromTelegram(track, response) {
  const messages = await telegramClient.getMessages(track.source_chat, {
    ids: [Number(track.message_id)],
  });
  const message = Array.isArray(messages) ? messages[0] : messages;
  if (!message || !(message.media?.document || message.document)) {
    response.status(404).end();
    return;
  }

  const body = new PassThrough({ highWaterMark: 1024 * 1024 });
  const upload = new Upload({
    client: s3,
    params: {
      Bucket: process.env.S3_BUCKET,
      Key: "tracks/" + track.id,
      Body: body,
      ContentType: track.mime_type || "application/octet-stream",
      Metadata: { "track-id": track.id },
    },
    queueSize: 2,
    partSize: 5 * 1024 * 1024,
    leavePartsOnError: false,
  });

  response.status(200);
  response.setHeader("Content-Type", track.mime_type || "application/octet-stream");
  response.setHeader("Cache-Control", "private, no-store");
  if (track.file_size) response.setHeader("Content-Length", String(track.file_size));
  body.pipe(response);

  const uploadPromise = upload.done();
  try {
    await telegramClient.downloadMedia(message, { outputFile: body });
    if (!body.writableEnded) body.end();
    await uploadPromise;
    log("info", "media_cached", { trackId: track.id });
  } catch (error) {
    body.destroy();
    log("error", "media_cache_failed", { trackId: track.id, error: error?.name || "Error" });
    if (!response.headersSent) response.status(502).end();
    else response.destroy();
    throw error;
  }
}

const app = express();
app.disable("x-powered-by");
app.set("trust proxy", 1);
app.use(helmet({ crossOriginResourcePolicy: { policy: "cross-origin" } }));
app.use(express.json({ limit: "32kb" }));

app.get("/", (_req, res) => res.json({
  service: "bitchord-telegram-library",
  status: "online",
  manifest: "/manifest.json",
  health: "/healthz",
}));

app.get("/healthz", async (_req, res) => {
  let database = "ok";
  try {
    await pool.query("SELECT 1");
  } catch {
    database = "error";
  }

  const ready = database === "ok" &&
    (!PUBLIC_CATALOG_ENABLED || (telegramReady && Boolean(s3)));
  res.status(ready ? 200 : 503).json({
    status: ready ? "ok" : "setup_required",
    database,
    publicCatalogEnabled: PUBLIC_CATALOG_ENABLED,
    telegramConnected: telegramReady,
    objectStorageConfigured: Boolean(s3),
    indexingEnabled: INDEXING_ENABLED,
  });
});

app.get("/manifest.json", (_req, res) => {
  res.set("Cache-Control", "public, max-age=300");
  res.json(makeManifest());
});

const publicApiLimiter = rateLimit({
  windowMs: 60_000,
  limit: 90,
  standardHeaders: "draft-8",
  legacyHeaders: false,
});

app.get("/search", publicApiLimiter, async (req, res, next) => {
  try {
    if (!PUBLIC_CATALOG_ENABLED) return res.json({ tracks: [] });
    const query = String(req.query.q || "").trim().slice(0, 120);
    if (query.length < 2) return res.json({ tracks: [] });
    const tracks = await searchTracks(query);
    res.set("Cache-Control", "public, max-age=60");
    return res.json({ tracks });
  } catch (error) {
    return next(error);
  }
});

app.get("/stream/:id", publicApiLimiter, async (req, res, next) => {
  try {
    if (!PUBLIC_CATALOG_ENABLED) return res.status(503).json({ error: "Catalogue is not enabled." });
    const track = await getTrack(req.params.id);
    if (!track) return res.status(404).end();
    const format = track.format || "mp3";
    return res.json({
      url: mediaUrlFor(req, track.id),
      format,
      codec: format === "mp3" ? "mp3" : format,
      container: format,
      manifest: "none",
      encrypted: false,
    });
  } catch (error) {
    return next(error);
  }
});

app.get("/media/:id", publicApiLimiter, async (req, res, next) => {
  try {
    if (!PUBLIC_CATALOG_ENABLED) return res.status(503).end();
    const expires = Number(req.query.expires);
    const signature = String(req.query.sig || "");
    const now = Math.floor(Date.now() / 1000);
    if (!Number.isSafeInteger(expires) || expires < now || expires > now + 11 * 60) {
      return res.status(403).end();
    }
    if (!safeEqualHex(signature, signMediaId(req.params.id, expires))) {
      return res.status(403).end();
    }

    const track = await getTrack(req.params.id);
    if (!track) return res.status(404).end();

    try {
      await s3.send(new HeadObjectCommand({
        Bucket: process.env.S3_BUCKET,
        Key: "tracks/" + track.id,
      }));
      return res.redirect(302, await signedCachedObjectUrl(track.id));
    } catch (error) {
      if (!isObjectMissing(error)) throw error;
    }

    const existing = activeCacheFills.get(track.id);
    if (existing) {
      await existing;
      return res.redirect(302, await signedCachedObjectUrl(track.id));
    }

    const fillPromise = streamAndCacheFromTelegram(track, res);
    activeCacheFills.set(track.id, fillPromise);
    try {
      await fillPromise;
    } finally {
      activeCacheFills.delete(track.id);
    }
  } catch (error) {
    return next(error);
  }
});

app.use((error, _req, res, _next) => {
  log("error", "request_failed", { error: error?.name || "Error" });
  if (res.headersSent) return res.destroy();
  return res.status(500).json({ error: "Internal server error." });
});

async function shutdown(signal) {
  log("info", "shutdown", { signal });
  if (indexerTimer) clearInterval(indexerTimer);
  try {
    if (telegramClient) await telegramClient.disconnect();
    if (s3) s3.destroy();
    await pool.end();
  } finally {
    process.exit(0);
  }
}

async function main() {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL is required.");
  requirePublicConfiguration();
  await createSchema();

  if (TELEGRAM_SESSION) await initialiseTelegram();

  const server = app.listen(PORT, "0.0.0.0", () => {
    log("info", "http_server_started", { port: PORT });
  });

  if (INDEXING_ENABLED) await startIndexer();

  process.once("SIGTERM", () => {
    server.close(() => shutdown("SIGTERM"));
  });
  process.once("SIGINT", () => {
    server.close(() => shutdown("SIGINT"));
  });
}

main().catch((error) => {
  log("fatal", "startup_failed", { error: error?.message || error?.name || "Error" });
  process.exit(1);
});
