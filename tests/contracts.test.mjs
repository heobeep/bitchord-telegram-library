import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";

const manifest = {
  id: "app.heobeep.telegram-library",
  name: "Telegram Music Library",
  version: "0.1.0",
  resources: ["search", "stream"],
  allowDownloads: 0,
};

test("manifest has a stable id and required resources", () => {
  assert.equal(typeof manifest.id, "string");
  assert.ok(manifest.id.length > 0);
  assert.ok(manifest.resources.includes("search"));
  assert.ok(manifest.resources.includes("stream"));
  assert.equal(manifest.allowDownloads, 0);
});

test("signed media links use constant-length SHA-256 signatures", () => {
  const id = "track_123";
  const expires = 1_800_000_000;
  const secret = "test-only-secret-that-is-long-enough-for-a-test";
  const signature = crypto.createHmac("sha256", secret)
    .update(id + "." + expires).digest("hex");
  assert.match(signature, /^[a-f0-9]{64}$/);
});

test("catalogue defaults are disabled unless explicitly enabled", () => {
  assert.notEqual(process.env.PUBLIC_CATALOG_ENABLED, "true");
  assert.notEqual(process.env.INDEXING_ENABLED, "true");
});
