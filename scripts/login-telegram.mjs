import "dotenv/config";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { TelegramClient } from "teleproto";
import { StringSession } from "teleproto/sessions";

const apiId = Number(process.env.TELEGRAM_API_ID || 0);
const apiHash = process.env.TELEGRAM_API_HASH || "";

if (!apiId || !apiHash) {
  process.stderr.write("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in your local environment first.\n");
  process.exit(1);
}

const rl = createInterface({ input, output });
const client = new TelegramClient(
  new StringSession(""),
  apiId,
  apiHash,
  { connectionRetries: 5 }
);

try {
  await client.start({
    phoneNumber: async () => rl.question("Telegram phone number (with country code): "),
    phoneCode: async () => rl.question("Code sent by Telegram: "),
    password: async () => rl.question("Two-step verification password (if enabled): "),
    onError: () => {
      process.stderr.write("Telegram authentication failed. Check the code/password and retry.\n");
      return false;
    },
  });

  const session = client.session.save();
  process.stdout.write("\nTelegram login succeeded. Copy the value below directly into your hosting provider's TELEGRAM_SESSION secret.\n");
  process.stdout.write("Do not paste it into chat, GitHub, screenshots, or logs.\n\n");
  process.stdout.write(session + "\n");
} catch (error) {
  process.stderr.write("Telegram login failed: " + (error?.name || "Error") + "\n");
  process.exitCode = 1;
} finally {
  rl.close();
  await client.disconnect().catch(() => {});
}
