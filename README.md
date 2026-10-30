# BitChord Telegram Music Library

A shared BitChord addon backend. Every listener adds the same public HTTPS manifest URL. A single server-managed Telegram account reads only the source chats listed by the operator, indexes audio metadata in PostgreSQL, and caches authorised audio to private S3-compatible storage.

**This repository is a first implementation foundation, not a live public catalogue.** Public search, indexing and playback are disabled by default. Do not enable them until you have authority to redistribute every included recording.

## BitChord endpoints

The service implements the current three-route addon contract:

- `GET /manifest.json` — addon identity and capabilities.
- `GET /search?q=...` — returns `{ "tracks": [...] }`.
- `GET /stream/:id` — returns a short-lived signed HTTPS media URL and codec/container hints.

The media URL is served by `GET /media/:id`. On the first play, audio is streamed from Telegram while being cached into a **private** S3-compatible bucket. Subsequent plays are redirected to a short-lived signed object URL, reducing repeated Telegram downloads. The first cache-fill response is a normal full response; byte-range seeking is available once the object has been cached and the object-storage provider supports HTTP Range requests.

The manifest sets `allowDownloads: 0` to mark the source as streaming-only in BitChord's interface. This is not DRM or access control: because the catalogue is public, a listener can still retrieve a playable stream URL. Never put material in the catalogue that must remain private.

## Architecture

- **Node.js 22+ / Express** — the public addon API.
- **PostgreSQL** — track metadata and an indexing coordination lock.
- **teleproto (MTProto)** — one central user-account session; no Telegram login per listener.
- **Private S3-compatible bucket** — audio cache, with signed playback URLs.
- **BitChord** — only needs one public manifest URL; it never receives Telegram credentials.

GramJS's original `telegram` package is archived/deprecated; this project uses its actively maintained compatible successor, `teleproto`.

## Local setup

1. Install Node.js 22 or newer and PostgreSQL.
2. Copy `.env.example` to `.env` and fill in the database URL, Telegram API ID/hash, and object-storage settings.
3. Obtain a Telegram API ID/hash from [my.telegram.org](https://my.telegram.org).
4. Generate the central account session locally:

   ```sh
   npm install
   npm run login:telegram
   ```

   The command prints a long-lived Telegram session string once. Put it directly into your hosting provider's secret setting named `TELEGRAM_SESSION`. Do not commit it or send it in chat.

5. Set `TELEGRAM_SOURCE_CHATS` to a comma-separated list of channel usernames or peer IDs. The central account must already have access to each source.
6. Configure a private S3-compatible bucket and its endpoint, region, bucket name, access key and secret key. Do not make the bucket public.
7. Keep the following off until permissions and configuration are verified:

   ```env
   PUBLIC_CATALOG_ENABLED=false
   INDEXING_ENABLED=false
   DISTRIBUTION_AUTHORIZED=false
   ```

8. Start the API with `npm start`. Run `npm test` for the local contract tests.

## Enabling the public catalogue

Only after you have permission from the relevant rights holders/channel operators to make the files available to the public, set:

```env
DISTRIBUTION_AUTHORIZED=true
PUBLIC_CATALOG_ENABLED=true
INDEXING_ENABLED=true
```

Also set `PUBLIC_BASE_URL` to the service's actual HTTPS URL and `STREAM_SIGNING_SECRET` to a random secret of at least 32 characters. Restart/redeploy after changing secrets.

The app refuses to start with public serving enabled unless the authorisation acknowledgement, HTTPS URL, database, Telegram session, source list, signing key and S3-compatible storage are configured.

## Deployment notes

A public production deployment needs a persistent PostgreSQL database and private object storage. A single small web instance is suitable for initial validation, not a guarantee of capacity for hundreds of simultaneous listeners. Test real concurrency, Telegram flood-wait behavior, storage egress/costs, and provider limits before broad release. Keep a single indexer owner; the PostgreSQL advisory lock prevents two instances in this app from indexing the same catalogue simultaneously.

The hosting workspace must be explicitly confirmed before provisioning resources. Do not copy Telegram credentials into GitHub or a checked-in `render.yaml`.

## Privacy and operations

- The central Telegram account is a service credential. Limit who can access it and revoke its active sessions immediately if compromised.
- Only list channels whose content you are authorised to redistribute.
- Avoid bulk joins, aggressive scans, or retry loops. Telegram may impose `FLOOD_WAIT` limits.
- The public API intentionally does not expose setup/login/status routes or Telegram secrets.
- Logs contain event names and safe identifiers only; never log tokens, session strings, signed URLs, or full request URLs.
- Review storage costs: after the first cache fill, playback traffic is served by the object-storage provider, not Telegram.

## Current status

The repository contains the API foundation and setup tooling. It has not been connected to a Telegram session, configured with a storage bucket, load-tested, or deployed. The catalogue remains disabled until those steps are completed.
