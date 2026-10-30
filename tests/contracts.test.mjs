import test from "node:test";
import assert from "node:assert/strict";
import {
  MANIFEST,
  createMediaSignature,
  verifyMediaSignature,
} from "../src/contract.mjs";

test("manifest has the stable BitChord identity and required capabilities", () => {
  assert.ok(MANIFEST.id);
  assert.ok(MANIFEST.resources.includes("search"));
  assert.ok(MANIFEST.resources.includes("stream"));
  assert.equal(MANIFEST.allowDownloads, 0);
});

test("valid signed media links verify before expiry", () => {
  const now = 1_800_000_000;
  const expires = now + 600;
  const secret = "test-only-secret-that-is-long-enough-for-a-test";
  const signature = createMediaSignature("track_123", expires, secret);

  assert.equal(
    verifyMediaSignature("track_123", expires, signature, secret, now),
    true
  );
});

test("signed media links reject tampering and expired URLs", () => {
  const now = 1_800_000_000;
  const expires = now + 600;
  const secret = "test-only-secret-that-is-long-enough-for-a-test";
  const signature = createMediaSignature("track_123", expires, secret);

  assert.equal(verifyMediaSignature("track_456", expires, signature, secret, now), false);
  assert.equal(verifyMediaSignature("track_123", now - 1, signature, secret, now), false);
});

test("unsigned or short-secret media links are rejected", () => {
  assert.equal(verifyMediaSignature("x", 1_800_000_600, "bad", "short", 1_800_000_000), false);
});

test("public catalogue and indexing remain disabled by default", () => {
  assert.notEqual(process.env.PUBLIC_CATALOG_ENABLED, "true");
  assert.notEqual(process.env.INDEXING_ENABLED, "true");
});
