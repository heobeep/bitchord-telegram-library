import crypto from "node:crypto";

export const MANIFEST = Object.freeze({
  id: "app.heobeep.telegram-library",
  name: "Telegram Music Library",
  version: "0.1.0",
  description: "A shared music catalogue from approved Telegram sources.",
  resources: ["search", "stream"],
  allowDownloads: 0,
});

export function createMediaSignature(id, expires, secret) {
  return crypto.createHmac("sha256", secret)
    .update(String(id) + "." + String(expires))
    .digest("hex");
}

export function verifyMediaSignature(id, expires, signature, secret, now = Math.floor(Date.now() / 1000)) {
  if (!Number.isSafeInteger(expires) || expires < now || expires > now + 11 * 60) return false;
  if (typeof secret !== "string" || secret.length < 32) return false;
  if (typeof signature !== "string" || !/^[a-f0-9]{64}$/i.test(signature)) return false;

  const expected = createMediaSignature(id, expires, secret);
  return crypto.timingSafeEqual(Buffer.from(signature, "hex"), Buffer.from(expected, "hex"));
}
