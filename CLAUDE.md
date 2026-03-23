# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ABM Post-Event lead intelligence pipeline. Two Google Apps Script (GAS) files process Slack channel messages from a BrowserStack ABM event to extract attendee intel and populate a Google Sheets CRM.

## Architecture

### Two-Script Pipeline

**Script A (`partA-read-slack-channels.js`)** — Deployed as a Slack webhook (Apps Script Web App).
- Receives Slack event POSTs; parses `sync` commands mentioning the bot
- Fetches channel history via `conversations.history`, resolves user IDs to names
- Enriches messages containing image attachments by calling Gemini 2.5 Flash-Lite (batch 5 in parallel) to extract attendee ID card fields (`first_name`, `last_name`, `company_name`)
- Writes rows to a `Slack Transcript` sheet (columns: `timestamp`, `user`, `text`, `thread_ts`, `attachments`, `id_card_intel`)
- Also exposes a `resolveUsers` POST action for external name resolution calls from Script B

**Script B (`partB-testing-overall-intel.js`)** — Deployed on the event Google Sheet. Menu-driven.
- Reads the `Slack Transcript` sheet plus an attendee roster from the active sheet
- Filters noise messages (`isNoisyMessage` / `getNoiseReason`)
- Groups clean messages by SPOC (Slack user = BrowserStack rep), then calls Gemini once per SPOC with all their messages
- Matches extracted intel against the roster using multi-stage anti-hallucination matching (email → name+company → full name → domain → fuzzy)
- Writes results to a `Slack Summary` sheet and adds tracking columns back to `Slack Transcript`
- Generates a `Processing Report` sheet with match-quality statistics
- Provides a `retryFailedMessages` function for re-processing API failures

### Data Flow
```
Slack Channel → Script A (GAS Webhook)
                  → Gemini (ID card images)
                  → "Slack Transcript" sheet
                       ↓
              Script B (Sheets menu)
                  → Gemini (text summarisation per SPOC)
                  → Roster matching (multi-stage)
                  → "Slack Summary" sheet
                  → "Slack Transcript" (tracking columns written back)
                  → "Processing Report" sheet
```

### Key Concepts

**SPOC** = BrowserStack sales rep who sent the Slack message. Messages are grouped by SPOC before sending to Gemini so one API call covers all context for that rep.

**messageLog** = In-memory `{ts: {status, extractedCount, summaries, warnings, matchQuality}}` object built during `buildSlackSummaryFromTranscript`. Written back to the transcript sheet as 5 new columns after processing.

**Tracking columns** added to `Slack Transcript`: `Processing Status`, `Extracted Count`, `Extracted Summaries`, `Match Quality`, `Warnings/Notes`. Status values use emoji prefixes: `✅ Processed`, `❌ Failed`, `🔇 Filtered as Noise`, `⚠️ No Intel Extracted`.

**Roster enrichment** — if Gemini omits an email but the name+company match a roster entry, the email is filled in and flagged as `Enriched`. Hallucinated names (no roster match) are rejected and logged.

## Secrets & Configuration

All secrets are stored via Apps Script `PropertiesService.getScriptProperties()` — never in code:
- `SLACK_BOT_TOKEN` — used by Script A for all Slack API calls
- `GEMINI_API_KEY` — used by both scripts

Script-level constants to update when deploying:
- `DEFAULT_TRANSCRIPT_SPREADSHEET_ID` (Script A) — fallback sheet for channels without an explicit sync URL
- `SLACK_RESOLVER_WEBAPP_URL` (Script B) — deployed URL of Script A's Web App
- `QUAL_DOC_ID` (Script B) — qualification criteria Google Doc ID
- Bot user ID hardcoded in `isBotOrCommandMessage`: `<@U0A49H0TBFY>` — update if bot changes

## Deployment

These are Google Apps Script projects, not Node.js. There is no `npm`, no build step, and no local test runner. Deployment is done via the Apps Script IDE or `clasp`:

```bash
# Push changes (requires clasp login and .clasp.json)
clasp push

# Open in browser editor
clasp open
```

Script A must be deployed as a **Web App** (Execute as: Me, Who has access: Anyone) and the URL registered as a Slack Event Subscription endpoint.

Script B is bound to the event Google Sheet and runs via the custom `Slack` menu or `Participant Tools` menu created by `onOpen()`.

## Workflow for Adding/Modifying Features

See `TRACKING_IMPLEMENTATION.md` for the message-level tracking implementation reference — it documents the full pattern for `messageLog` initialisation, per-message status updates, `writeProcessingResultsToTranscript`, and `generateProcessingReport`.

When modifying noise filter patterns, update **both** `isNoisyMessage` (array of pattern strings) and `getNoiseReason` (array of `{pattern, reason}` objects) in Script B — they must stay in sync.

When adding new Gemini calls, follow the existing pattern: `muteHttpExceptions: true`, check `getResponseCode() !== 200` before parsing, log failures to `messageLog` with `retryable: true`.
