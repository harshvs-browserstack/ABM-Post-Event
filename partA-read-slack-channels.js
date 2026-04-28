// Code.gs – Slack Transcript + Gemini ID card extraction (with user names)
//
// Configuration Storage:
// - Channel-sheet mappings & sync state → Config sheet (tab in DEFAULT_TRANSCRIPT_SPREADSHEET_ID)
// - Event deduplication state → PropertiesService (time-limited, internal)
// - Secrets (tokens, API keys) → PropertiesService (secure)

const DEFAULT_TRANSCRIPT_SPREADSHEET_ID = '1SKPYcbjgSg7YPwSw2I5VBBurSFaYYjU1USvLfY9gvlA';
const MAX_MESSAGES_PER_RUN = 400;
const MAX_THREAD_FETCHES = 40; //caps how many threads are we expanding per run (our 6 min execution limit is a problem)
const GEMINI_MODEL = 'gemini-2.5-flash-lite';
const GEMINI_ENDPOINT =
  'https://generativelanguage.googleapis.com/v1beta/models/' +
  GEMINI_MODEL +
  ':generateContent';

const ID_CARD_PROMPT =
  'You are made for reading photos of BrowserStack event ID cards. ' +
  'If you don\'t see a very clear picture of an ID card - return exactly: NO_ID_CARD\n' +
  'Each image may contain one or more attendee ID cards. ' +
  'For each visible ID card, extract: first_name, last_name, company_name. ' +
  'Return the result as plain text ONLY in this exact format:\n' +
  'ID Card 1: {"first_name": "...", "last_name": "...", "company_name": "..."}\n' +
  'ID Card 2: {"first_name": "...", "last_name": "...", "company_name": "..."}\n' +
  'The company_name cannot be BrowserStack — that is the logo on the ID card, do not confuse the two. ' +
  'Use lower_snake_case keys. ' +
  'If a field is unknown, set it to an empty string. ' +
  'Do not add any prose or explanation.';

const NO_ID_CARD = 'NO_ID_CARD';

// Simple in-memory cache for Slack userId -> name per execution
const userNameCache = {};

// ─── SheetLogger ──────────────────────────────────────────────────────────────

const LOGGING_ENABLED  = true;   // set false in prod to silence sheet logs
const LOG_SHEET_NAME   = 'Logs';
const LOG_BUFFER       = [];
const LOG_FLUSH_SIZE   = 20;

function sheetLog(level, message) {
  if (!LOGGING_ENABLED) return;
  const ts = new Date().toISOString();
  LOG_BUFFER.push([ts, level, message]);
  if (LOG_BUFFER.length >= LOG_FLUSH_SIZE) flushLogs();
}

function flushLogs() {
  if (!LOGGING_ENABLED || LOG_BUFFER.length === 0) return;
  try {
    const ss    = SpreadsheetApp.openById(DEFAULT_TRANSCRIPT_SPREADSHEET_ID);
    let   sheet = ss.getSheetByName(LOG_SHEET_NAME);
    if (!sheet) {
      sheet = ss.insertSheet(LOG_SHEET_NAME);
      sheet.appendRow(['timestamp', 'level', 'message']);
    }
    const rows = LOG_BUFFER.splice(0, LOG_BUFFER.length);
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, 3).setValues(rows);
  } catch (e) {
    console.error('flushLogs error', e);
  }
}

// ─── Idempotency ──────────────────────────────────────────────────────────────

function isDuplicateEvent(eventId) {
  if (!eventId) return false;
  const key   = 'handledEvent:' + eventId;
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty(key)) {
    sheetLog('INFO', 'isDuplicateEvent: duplicate event_id=' + eventId + ', skipping');
    return true;
  }
  props.setProperty(key, String(Date.now()));
  return false;
}

// ─── doPost ───────────────────────────────────────────────────────────────────

function doPost(e) {
  sheetLog('INFO', 'doPost: invoked');
  try {
    const body = JSON.parse(e.postData.contents || '{}');

    if (body.action === 'resolveUsers' && Array.isArray(body.userIds)) {
      sheetLog('INFO', 'doPost: routing to handleResolveUsers');
      return handleResolveUsers(body);
    }

    if (body.type === 'url_verification') {
      sheetLog('INFO', 'doPost: url_verification challenge');
      return ContentService.createTextOutput(body.challenge);
    }

    if (isDuplicateEvent(body.event_id)) {
      return ContentService.createTextOutput('OK');
    }

    const event = body.event;
    if (!event || !event.text) {
      sheetLog('WARN', 'doPost: no event or text in payload');
      return ContentService.createTextOutput('No event');
    }

    const text      = event.text.trim();
    const channelId = event.channel;
    const triggerTs = event.ts;
    const threadTs  = event.thread_ts || event.ts;

    sheetLog('INFO', 'doPost: channel=' + channelId + ' | triggerTs=' + triggerTs + ' | text=' + text.substring(0, 80));

    const cmd = parseSyncCommand(text);
    if (!cmd || !cmd.isSync) {
      sheetLog('INFO', 'doPost: not a sync command, ignoring');
      return ContentService.createTextOutput('Not a sync command');
    }

    const sheetInfo = cmd.sheetUrl
      ? getSheetFromUrl(cmd.sheetUrl)
      : getDefaultTranscriptSheetForChannel(channelId);

    if (!sheetInfo) {
      sheetLog('ERROR', 'doPost: could not open target sheet');
      sendThreadReply(channelId, threadTs, 'Sync failed – could not open target Google Sheet.');
      return ContentService.createTextOutput('Sheet open error');
    }

    const oldestTs = getLastSyncedTs(channelId, sheetInfo.ss.getId());
    sheetLog('INFO', 'doPost: sheetName=' + sheetInfo.name + ' | oldestTs=' + oldestTs);

    sendThreadReply(
      channelId,
      threadTs,
      oldestTs
        ? `Starting sync into sheet '${sheetInfo.name}' from ts ${oldestTs}...`
        : `Starting initial sync into sheet '${sheetInfo.name}'...`
    );

    const messages = fetchRecentMessagesFromSlackChannel(
      channelId,
      MAX_MESSAGES_PER_RUN,
      oldestTs,
      triggerTs
    );

    if (!messages || messages.length === 0) {
      sheetLog('INFO', 'doPost: no new messages found');
      sendThreadReply(channelId, threadTs, 'No new messages found to sync.');
      return ContentService.createTextOutput('No messages');
    }

    sheetLog('INFO', 'doPost: fetched ' + messages.length + ' messages, enriching...');
    const enriched = enrichMessagesWithIdCards(messages);
    const written  = writeTranscriptToSheet(sheetInfo.sheet, enriched);

    const newestTs = messages.reduce(function (max, m) {
      const n = Number(m.ts);
      return n > max ? n : max;
    }, 0);

    if (newestTs) {
      setLastSyncedTs(channelId, sheetInfo.ss.getId(), String(newestTs));
      sheetLog('INFO', 'doPost: updated lastSyncedTs=' + newestTs);
    }

    sendThreadReply(
      channelId,
      threadTs,
      `Done – fetched ${written} messages. Last synced ts = ${newestTs}.`
    );

    sheetLog('INFO', 'doPost: completed successfully | written=' + written);
    flushLogs();
    return ContentService.createTextOutput('OK');

  } catch (err) {
    sheetLog('ERROR', 'doPost: uncaught error – ' + err.message);
    flushLogs();
    console.error(err);
    try {
      const body     = JSON.parse(e.postData.contents || '{}');
      const event    = body.event || {};
      const cId      = event.channel || '';
      const tTs      = event.thread_ts || event.ts || null;
      if (cId && tTs) {
        sendThreadReply(cId, tTs, 'Sync failed with an internal error. Please check Apps Script logs.');
      }
    } catch (ignore) {}
    return ContentService.createTextOutput('Error: ' + err.message);
  }
}

// ─── Command + sheet helpers ──────────────────────────────────────────────────

function parseSyncCommand(text) {
  const cleaned = text.replace(/<@[^>]+>/g, '').trim();
  const parts   = cleaned.split(/\s+/);
  if (parts[0] !== 'sync') return null;
  return { isSync: true, sheetUrl: parts[1] || null };
}

function getSheetFromUrl(url) {
  try {
    const idMatch = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    if (!idMatch) return null;
    const ss        = SpreadsheetApp.openById(idMatch[1]);
    const sheetName = 'Slack Transcript';
    let   sheet     = ss.getSheetByName(sheetName);
    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
      sheet.appendRow(['timestamp', 'user', 'text', 'thread_ts', 'attachments', 'id_card_intel']);
    }
    return { ss, sheet, name: sheetName };
  } catch (e) {
    sheetLog('ERROR', 'getSheetFromUrl: ' + e.message);
    return null;
  }
}

function getDefaultTranscriptSheetForChannel(channelId) {
  const ss = SpreadsheetApp.openById(DEFAULT_TRANSCRIPT_SPREADSHEET_ID);

  // Check if we have a config mapping for this channel
  let config = getConfigRow(channelId, DEFAULT_TRANSCRIPT_SPREADSHEET_ID);

  if (config) {
    // Use existing mapped sheet
    sheetLog('INFO', 'getDefaultTranscriptSheetForChannel: found existing config for channel=' +
      channelId + ' | sheetName=' + config.sheetName);
    let sheet = ss.getSheetByName(config.sheetName);
    if (!sheet) {
      // Config exists but sheet was deleted; recreate it
      sheetLog('WARN', 'getDefaultTranscriptSheetForChannel: sheet ' + config.sheetName +
        ' not found, recreating');
      sheet = ss.insertSheet(config.sheetName);
      sheet.appendRow(['timestamp', 'user', 'text', 'thread_ts', 'attachments', 'id_card_intel']);
    }
    return { ss, sheet, name: config.sheetName };
  }

  // No config exists; create new sheet and register it
  const channelName = getSlackChannelName(channelId);
  const sheetName   = `${channelName}-transcript`;
  let   sheet       = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow(['timestamp', 'user', 'text', 'thread_ts', 'attachments', 'id_card_intel']);
  }

  // Add config row to track this channel-sheet mapping
  setConfigRow(channelId, DEFAULT_TRANSCRIPT_SPREADSHEET_ID, sheetName, null);
  sheetLog('INFO', 'getDefaultTranscriptSheetForChannel: created sheet ' + sheetName +
    ' and registered in Config');

  return { ss, sheet, name: sheetName };
}

// ─── Slack API helpers ────────────────────────────────────────────────────────

function getSlackChannelName(channelId) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  const resp  = UrlFetchApp.fetch('https://slack.com/api/conversations.info', {
    method: 'post',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/x-www-form-urlencoded' },
    payload: { channel: channelId },
    muteHttpExceptions: true
  });
  const json = JSON.parse(resp.getContentText());
  if (!json.ok) { sheetLog('ERROR', 'getSlackChannelName: ' + JSON.stringify(json)); return channelId; }
  return json.channel.name || channelId;
}

function getSlackUserName(userId) {
  if (!userId) return '';
  if (userNameCache[userId]) return userNameCache[userId];

  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  try {
    const resp = UrlFetchApp.fetch('https://slack.com/api/users.info', {
      method: 'post',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/x-www-form-urlencoded' },
      payload: { user: userId },
      muteHttpExceptions: true
    });
    const json = JSON.parse(resp.getContentText());
    if (!json.ok || !json.user) {
      sheetLog('WARN', 'getSlackUserName: users.info failed for ' + userId);
      userNameCache[userId] = userId;
      return userId;
    }
    const profile     = json.user.profile || {};
    const realName    = profile.real_name_normalized || profile.real_name || '';
    const displayName = profile.display_name_normalized || profile.display_name || '';
    const name        = realName || displayName || userId;
    userNameCache[userId] = name;
    return name;
  } catch (e) {
    sheetLog('ERROR', 'getSlackUserName: ' + e.message);
    userNameCache[userId] = userId;
    return userId;
  }
}

function sendThreadReply(channelId, threadTs, text) {
  if (!channelId || !threadTs) return;
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  const resp  = UrlFetchApp.fetch('https://slack.com/api/chat.postMessage', {
    method: 'post',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
    payload: JSON.stringify({ channel: channelId, text: text, thread_ts: threadTs }),
    muteHttpExceptions: true
  });
  const json = JSON.parse(resp.getContentText());
  if (!json.ok) {
    sheetLog('ERROR', 'sendThreadReply: chat.postMessage failed – ' + JSON.stringify(json));
  } else {
    sheetLog('INFO', 'sendThreadReply: message posted successfully');
  }
}

// ─── Gemini enrichment ────────────────────────────────────────────────────────

function enrichMessagesWithIdCards(messages) {
  sheetLog('INFO', 'enrichMessagesWithIdCards: processing ' + messages.length + ' messages');

  // Build one task per image (one Gemini call per image)
  const tasks = [];

  messages.forEach(function (msg, msgIndex) {
    const imageFiles = (msg.files || []).filter(function (f) {
      return f.mimetype && f.mimetype.startsWith('image/');
    });

    if (imageFiles.length === 0) {
      msg.id_card_intel = '';
      return;
    }

    imageFiles.forEach(function (file, fileIndex) {
      tasks.push({ msgIndex: msgIndex, file: file, fileIndex: fileIndex });
    });
  });

  sheetLog('INFO', 'enrichMessagesWithIdCards: ' + tasks.length + ' image task(s) to process');

  if (tasks.length === 0) return messages;

  const apiKey     = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  const allResults = new Array(tasks.length).fill(null);

  // Build requests for all tasks
  const allRequests = tasks.map(function (task, i) {
    sheetLog('INFO', 'enrichMessagesWithIdCards: building request ' + (i + 1) + '/' + tasks.length +
      ' | msgIndex=' + task.msgIndex);
    const payload = buildGeminiPayloadForIdCards([task.file]);
    return {
      url: GEMINI_ENDPOINT + '?key=' + encodeURIComponent(apiKey),
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify(payload)
    };
  });

  // Run in batches of 5
  const batchSize = 5;
  for (let i = 0; i < allRequests.length; i += batchSize) {
    const batchRequests = allRequests.slice(i, i + batchSize);
    const batchTasks    = tasks.slice(i, i + batchSize);

    sheetLog('INFO', 'enrichMessagesWithIdCards: firing batch ' +
      Math.floor(i / batchSize + 1) + ' (' + batchRequests.length + ' requests)');

    const responses = UrlFetchApp.fetchAll(batchRequests);

    responses.forEach(function (resp, idx) {
      try {
        if (resp.getResponseCode() !== 200) {
          sheetLog('ERROR', 'enrichMessagesWithIdCards: Gemini HTTP ' +
            resp.getResponseCode() + ' for task ' + (i + idx));
          allResults[i + idx] = NO_ID_CARD;
          return;
        }
        const data = JSON.parse(resp.getContentText());
        const text = extractTextFromGeminiResponse(data);
        allResults[i + idx] = text || NO_ID_CARD;
        sheetLog('INFO', 'enrichMessagesWithIdCards: task ' + (i + idx) +
          ' response=' + String(text).substring(0, 120));
      } catch (e) {
        sheetLog('ERROR', 'enrichMessagesWithIdCards: parse error task ' + (i + idx) + ' – ' + e.message);
        allResults[i + idx] = NO_ID_CARD;
      }
    });
  }

  // Assemble: group raw card lines per message, then renumber globally
  const msgCardLines = {};

  tasks.forEach(function (task, i) {
    const raw = allResults[i];
    if (!raw || raw === NO_ID_CARD) return;
    const lines = extractRawCardLines(raw);
    if (!lines.length) return;
    if (!msgCardLines[task.msgIndex]) msgCardLines[task.msgIndex] = [];
    msgCardLines[task.msgIndex] = msgCardLines[task.msgIndex].concat(lines);
  });

  messages.forEach(function (msg, msgIndex) {
    const lines = msgCardLines[msgIndex];
    if (!lines || lines.length === 0) {
      if (msg.id_card_intel === undefined) msg.id_card_intel = '';
      return;
    }
    // Renumber sequentially from 1
    const renumbered = lines.map(function (jsonPart, n) {
      return 'ID Card ' + (n + 1) + ': ' + jsonPart;
    });
    msg.id_card_intel = renumbered.join('\n');
    sheetLog('INFO', 'enrichMessagesWithIdCards: msgIndex=' + msgIndex +
      ' | cards=' + lines.length + ' | preview=' + msg.id_card_intel.substring(0, 100));
  });

  sheetLog('INFO', 'enrichMessagesWithIdCards: done');
  return messages;
}

function buildGeminiPayloadForIdCards(files) {
  sheetLog('INFO', 'buildGeminiPayloadForIdCards: building payload for ' + files.length + ' file(s)');
  const parts = [{ text: ID_CARD_PROMPT }];

  files.forEach(function (file, idx) {
    sheetLog('INFO', 'buildGeminiPayloadForIdCards: fetching image ' + (idx + 1) +
      ' / ' + files.length + ' | url = ' + (file.url_private || file.url_private_download || '').substring(0, 80));

    const imageData = getImageDataFromUrl(file);
    if (!imageData) {
      sheetLog('WARN', 'buildGeminiPayloadForIdCards: no image data for file ' + (idx + 1));
      return;
    }

    // Size guard: skip if still > 4 MB after compression
    const approxBytes = imageData.data.length * 0.75;
    if (approxBytes > 4 * 1024 * 1024) {
      sheetLog('WARN', 'buildGeminiPayloadForIdCards: skipping oversized image (~' +
        Math.round(approxBytes / 1024 / 1024) + ' MB) for file ' + (idx + 1));
      return;
    }

    sheetLog('INFO', 'buildGeminiPayloadForIdCards: image ' + (idx + 1) +
      ' ~' + Math.round(approxBytes / 1024) + ' KB | mimeType=' + imageData.mimeType);

    parts.push({
      inlineData: {
        mimeType: imageData.mimeType,
        data: imageData.data
      }
    });
  });

  return { contents: [{ role: 'user', parts: parts }] };
}

/**
 * Fetches a Slack-private image using Slack's pre-generated thumbnails
 * to avoid large payloads without needing a Drive round-trip.
 *
 * Slack file objects include thumb_1024, thumb_720, thumb_480, thumb_360
 * which are already compressed JPEGs — we use the largest available.
 * Falls back to url_private only if no thumbnail exists.
 */
function getImageDataFromUrl(file) {
  if (!file) return null;

  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');

  // Prefer Slack's pre-generated thumbnails (already compressed JPEGs)
  const url = file.thumb_1024 || file.thumb_720 || file.thumb_480 || file.thumb_360
            || file.url_private || file.url_private_download;

  if (!url) return null;

  const usedThumb = !!(file.thumb_1024 || file.thumb_720 || file.thumb_480 || file.thumb_360);
  sheetLog('INFO', 'getImageDataFromUrl: fetching | usedThumb=' + usedThumb + ' | url=' + url.substring(0, 80));

  const resp = UrlFetchApp.fetch(url, {
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    sheetLog('ERROR', 'getImageDataFromUrl: fetch failed HTTP ' + resp.getResponseCode());
    return null;
  }

  const blob     = resp.getBlob();
  const approxKB = Math.round(blob.getBytes().length / 1024);
  sheetLog('INFO', 'getImageDataFromUrl: fetched ~' + approxKB + ' KB | mimeType=' + blob.getContentType());

  if (approxKB > 4096) {
    sheetLog('WARN', 'getImageDataFromUrl: skipping oversized image (~' + approxKB + ' KB)');
    return null;
  }

  return {
    mimeType: blob.getContentType(),
    data: Utilities.base64Encode(blob.getBytes())
  };
}

function extractTextFromGeminiResponse(data) {
  try {
    const candidate = data.candidates && data.candidates[0];
    if (!candidate || !candidate.content || !candidate.content.parts) return '';
    return candidate.content.parts
      .map(function (p) { return p.text || ''; })
      .filter(function (t) { return t; })
      .join('\n')
      .trim();
  } catch (e) {
    sheetLog('ERROR', 'extractTextFromGeminiResponse: ' + e.message + ' | raw=' + JSON.stringify(data).substring(0, 200));
    return '';
  }
}

/**
 * Extract bare JSON strings from a Gemini response,
 * stripping any "ID Card N:" prefix so we can renumber globally.
 */
function extractRawCardLines(raw) {
  if (!raw || raw === NO_ID_CARD) return [];

  const lines = raw.split(/\r?\n/).map(function (l) { return l.trim(); }).filter(Boolean);
  const result = [];

  lines.forEach(function (line) {
    const m = line.match(/id\s*card\s*\d*\s*:\s*(\{.*\})/i);
    if (m) {
      result.push(m[1].trim());
    } else if (line.startsWith('{') && line.endsWith('}')) {
      result.push(line);
    }
  });

  return result;
}

// ─── Write transcript ─────────────────────────────────────────────────────────

function writeTranscriptToSheet(sheet, messages) {
  if (!messages.length) return 0;
  sheetLog('INFO', 'writeTranscriptToSheet: writing ' + messages.length + ' rows');

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(['timestamp', 'user', 'text', 'thread_ts', 'attachments', 'id_card_intel']);
  }

  const rows = messages
    .sort(function (a, b) { return parseFloat(a.ts) - parseFloat(b.ts); })
    .map(function (m) {
      return [m.ts, m.user, m.text, m.thread_ts, JSON.stringify(m.files || []), m.id_card_intel || ''];
    });

  sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
  sheetLog('INFO', 'writeTranscriptToSheet: wrote ' + rows.length + ' rows');
  return rows.length;
}

// ─── Config sheet management ─────────────────────────────────────────────────

const CONFIG_SHEET_NAME = 'Config';

/**
 * Ensures the Config sheet exists with headers.
 */
function ensureConfigSheet() {
  try {
    const ss    = SpreadsheetApp.openById(DEFAULT_TRANSCRIPT_SPREADSHEET_ID);
    let   sheet = ss.getSheetByName(CONFIG_SHEET_NAME);
    if (!sheet) {
      sheet = ss.insertSheet(CONFIG_SHEET_NAME);
      sheet.appendRow(['channelId', 'spreadsheetId', 'sheetName', 'lastSyncedTs']);
      sheetLog('INFO', 'ensureConfigSheet: created Config sheet');
    }
    return sheet;
  } catch (e) {
    sheetLog('ERROR', 'ensureConfigSheet: ' + e.message);
    return null;
  }
}

/**
 * Reads config for a channel-sheet pair from the Config sheet.
 * Returns { channelId, spreadsheetId, sheetName, lastSyncedTs } or null if not found.
 */
function getConfigRow(channelId, sheetId) {
  try {
    const sheet = ensureConfigSheet();
    if (!sheet) return null;

    const data = sheet.getDataRange().getValues();
    for (let i = 1; i < data.length; i++) {
      const row = data[i];
      if (row[0] === channelId && row[1] === sheetId) {
        return {
          channelId: row[0],
          spreadsheetId: row[1],
          sheetName: row[2],
          lastSyncedTs: row[3] || null
        };
      }
    }
    return null;
  } catch (e) {
    sheetLog('ERROR', 'getConfigRow: ' + e.message);
    return null;
  }
}

/**
 * Creates or updates a config row for a channel-sheet pair.
 */
function setConfigRow(channelId, sheetId, sheetName, lastSyncedTs) {
  try {
    const sheet = ensureConfigSheet();
    if (!sheet) return;

    const data = sheet.getDataRange().getValues();
    let   found = false;

    // Update existing row
    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === channelId && data[i][1] === sheetId) {
        sheet.getRange(i + 1, 4).setValue(lastSyncedTs || '');
        sheetLog('INFO', 'setConfigRow: updated row ' + (i + 1) +
          ' | channel=' + channelId + ' | lastSyncedTs=' + lastSyncedTs);
        found = true;
        break;
      }
    }

    // Insert new row if not found
    if (!found) {
      sheet.appendRow([channelId, sheetId, sheetName, lastSyncedTs || '']);
      sheetLog('INFO', 'setConfigRow: inserted new row | channel=' + channelId +
        ' | sheet=' + sheetName + ' | lastSyncedTs=' + lastSyncedTs);
    }
  } catch (e) {
    sheetLog('ERROR', 'setConfigRow: ' + e.message);
  }
}

// ─── Last synced ts (now using Config sheet) ───────────────────────────────────

function getLastSyncedTs(channelId, sheetId) {
  const config = getConfigRow(channelId, sheetId);
  return config ? config.lastSyncedTs : null;
}

function setLastSyncedTs(channelId, sheetId, ts) {
  // First read the config to get the sheetName, then update
  const config = getConfigRow(channelId, sheetId);
  if (config) {
    setConfigRow(channelId, sheetId, config.sheetName, ts);
  } else {
    // If no config exists yet, we'll create one (should have been created by now)
    sheetLog('WARN', 'setLastSyncedTs: no config found for channel=' + channelId +
      ' | sheetId=' + sheetId);
  }
}

/**
 * Reset sync timestamps for a specific sheet.
 * Run with target sheet ID, e.g.: clearSyncTimestampsForSheet('1OuraOJpqEg4Wja74Mp067yys0kMznp3_egRHeUHx7Lw')
 */
function clearSyncTimestampsForSheet(sheetId) {
  try {
    const sheet = ensureConfigSheet();
    if (!sheet) return;

    const data = sheet.getDataRange().getValues();
    const rowsToDelete = [];

    for (let i = 1; i < data.length; i++) {
      if (data[i][1] === sheetId) {
        rowsToDelete.push(i + 1);
      }
    }

    // Delete in reverse order to avoid shifting indices
    for (let i = rowsToDelete.length - 1; i >= 0; i--) {
      sheet.deleteRow(rowsToDelete[i]);
      console.log('Deleted row: ' + rowsToDelete[i]);
    }

    sheetLog('INFO', 'clearSyncTimestampsForSheet: cleared ' + rowsToDelete.length +
      ' row(s) for sheetId=' + sheetId);
  } catch (e) {
    sheetLog('ERROR', 'clearSyncTimestampsForSheet: ' + e.message);
  }
}

/**
 * Clear all config for a specific channel.
 * Run with channel ID, e.g.: clearChannelConfig('C01234567')
 */
function clearChannelConfig(channelId) {
  try {
    const sheet = ensureConfigSheet();
    if (!sheet) return;

    const data = sheet.getDataRange().getValues();
    const rowsToDelete = [];

    for (let i = 1; i < data.length; i++) {
      if (data[i][0] === channelId) {
        rowsToDelete.push(i + 1);
      }
    }

    for (let i = rowsToDelete.length - 1; i >= 0; i--) {
      sheet.deleteRow(rowsToDelete[i]);
      console.log('Deleted row: ' + rowsToDelete[i]);
    }

    sheetLog('INFO', 'clearChannelConfig: cleared ' + rowsToDelete.length +
      ' row(s) for channelId=' + channelId);
  } catch (e) {
    sheetLog('ERROR', 'clearChannelConfig: ' + e.message);
  }
}

/**
 * MIGRATION FUNCTION: One-time use.
 * Copies all lastSyncedTs mappings from PropertiesService to Config sheet,
 * then deletes them from PropertiesService.
 *
 * Call this once after deploying the new Config sheet code:
 *   migratePropertiesServiceToConfigSheet()
 */
function migratePropertiesServiceToConfigSheet() {
  try {
    const props = PropertiesService.getScriptProperties();
    const all   = props.getProperties();

    const configSheet = ensureConfigSheet();
    if (!configSheet) {
      console.error('migratePropertiesServiceToConfigSheet: could not access Config sheet');
      return;
    }

    let migrated = 0;
    let errors   = 0;

    console.log('migratePropertiesServiceToConfigSheet: scanning properties...');

    // Iterate over all properties looking for lastSyncedTs:channelId:sheetId format
    Object.keys(all).forEach(function (key) {
      if (!key.startsWith('lastSyncedTs:')) return;

      try {
        // Parse key: lastSyncedTs:channelId:sheetId
        const parts = key.split(':');
        if (parts.length !== 3) {
          console.warn('migratePropertiesServiceToConfigSheet: skipping malformed key ' + key);
          return;
        }

        const channelId = parts[1];
        const sheetId   = parts[2];
        const ts        = all[key];

        // Check if this mapping already exists in Config sheet
        const existing = getConfigRow(channelId, sheetId);
        if (existing) {
          console.log('migratePropertiesServiceToConfigSheet: config already exists for ' +
            channelId + ':' + sheetId + ', skipping');
          return;
        }

        // Try to get the sheet name from the target spreadsheet
        let sheetName = 'Unknown Sheet';
        try {
          const ss = SpreadsheetApp.openById(sheetId);
          // Assume the default transcript sheet pattern or first sheet
          sheetName = ss.getSheetByName('Slack Transcript')?.getName() || 'Slack Transcript';
        } catch (e) {
          console.warn('migratePropertiesServiceToConfigSheet: could not open sheet ' + sheetId +
            ', using default name');
        }

        // Insert into Config sheet
        setConfigRow(channelId, sheetId, sheetName, ts);
        console.log('migratePropertiesServiceToConfigSheet: migrated ' + channelId +
          ':' + sheetId + ' | ts=' + ts);
        migrated++;
      } catch (e) {
        console.error('migratePropertiesServiceToConfigSheet: error processing key ' + key +
          ' – ' + e.message);
        errors++;
      }
    });

    console.log('migratePropertiesServiceToConfigSheet: migrated ' + migrated +
      ' mapping(s), errors=' + errors);

    // Only delete properties after successful migration
    if (migrated > 0) {
      console.log('migratePropertiesServiceToConfigSheet: deleting migrated keys from PropertiesService...');
      let deleted = 0;

      Object.keys(all).forEach(function (key) {
        if (!key.startsWith('lastSyncedTs:')) return;
        try {
          props.deleteProperty(key);
          console.log('migratePropertiesServiceToConfigSheet: deleted ' + key);
          deleted++;
        } catch (e) {
          console.error('migratePropertiesServiceToConfigSheet: failed to delete ' + key +
            ' – ' + e.message);
        }
      });

      console.log('migratePropertiesServiceToConfigSheet: deleted ' + deleted +
        ' property(ies) from PropertiesService');
    }

    sheetLog('INFO', 'migratePropertiesServiceToConfigSheet: complete | migrated=' +
      migrated + ' | errors=' + errors);

  } catch (e) {
    console.error('migratePropertiesServiceToConfigSheet: uncaught error – ' + e.message);
  }
}

// ─── Fetch messages ───────────────────────────────────────────────────────────

function fetchRecentMessagesFromSlackChannel(channelId, limit, oldestTs, excludeTs) {
  sheetLog('INFO', 'fetchRecentMessagesFromSlackChannel: channel=' + channelId +
    ' | limit=' + limit + ' | oldestTs=' + oldestTs + ' | excludeTs=' + excludeTs);

  const props   = PropertiesService.getScriptProperties();
  const token   = props.getProperty('SLACK_BOT_TOKEN');
  const payload = { channel: channelId, limit: limit };

  if (oldestTs) {
    payload.oldest    = oldestTs;
    payload.inclusive = false;
  }

  const resp = UrlFetchApp.fetch('https://slack.com/api/conversations.history', {
    method: 'post',
    headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/x-www-form-urlencoded' },
    payload: payload,
    muteHttpExceptions: true
  });

  const json = JSON.parse(resp.getContentText());
  if (!json.ok) {
    sheetLog('ERROR', 'fetchRecentMessagesFromSlackChannel: ' + JSON.stringify(json));
    return [];
  }

  const messages = json.messages
    .filter(function (m) {
      if (excludeTs && m.ts === excludeTs) return false;
      if (isBotOrCommandMessage(m)) return false;
      return true;
    })
    .map(function (m) {
      const userName = getSlackUserName(m.user || '');
      return {
        ts:        m.ts,
        user:      userName,
        text:      m.text || '',
        thread_ts: m.thread_ts || '',
        files:     m.files || m.attachments || [],
        reply_count: m.reply_count || 0
      };
    });

  sheetLog('INFO', 'fetchRecentMessagesFromSlackChannel: ' + messages.length + ' messages after filter');

  // Fetch thread replies for any threaded messages
  const allThreadParents = messages.filter(m => m.reply_count > 0);
  const threadParents = allThreadParents.slice(0, MAX_THREAD_FETCHES);
  sheetLog('INFO', 'fetchRecentMessages: total messages=' + messages.length +
    ' | threadParents found=' + allThreadParents.length +
    ' | fetching=' + threadParents.length + ' (cap=' + MAX_THREAD_FETCHES + ')');
  threadParents.forEach(function(parent) {
    const resp = UrlFetchApp.fetch('https://slack.com/api/conversations.replies', {
      method: 'post',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/x-www-form-urlencoded' },
      payload: { channel: channelId, ts: parent.ts, limit: 200 },
      muteHttpExceptions: true
    });
    const json = JSON.parse(resp.getContentText());
    if (!json.ok) return;

    // slice(1) skips the parent which is already in messages
    json.messages.slice(1).forEach(function(reply) {
      if (isBotOrCommandMessage(reply)) return;
      messages.push({
        ts:        reply.ts,
        user:      getSlackUserName(reply.user || ''),
        text:      reply.text || '',
        thread_ts: reply.thread_ts || '',
        files:     reply.files || reply.attachments || []
      });
    });
  });
  return messages;
}

function isBotOrCommandMessage(m) {
  const text = String(m.text || '').trim();
  if (!text) return false;
  if (text.indexOf('<@U0A49H0TBFY>') !== -1) return true;
  const cleaned = text.replace(/<@[^>]+>/g, '').trim();
  const parts   = cleaned.split(/\s+/);
  if (parts[0] && parts[0].toLowerCase() === 'sync') return true;
  return false;
}

// ─── Resolve users (external POST action) ────────────────────────────────────

function handleResolveUsers(body) {
  sheetLog('INFO', 'handleResolveUsers: invoked');
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');

  if (!token) {
    return ContentService.createTextOutput(
      JSON.stringify({ error: 'SLACK_BOT_TOKEN not configured' })
    ).setMimeType(ContentService.MimeType.JSON);
  }

  const userIds   = body.userIds || [];
  const uniqueIds = Array.from(new Set(userIds)).filter(function (id) {
    return id && typeof id === 'string';
  });

  sheetLog('INFO', 'handleResolveUsers: resolving ' + uniqueIds.length + ' unique user(s)');

  const results = [];
  const BATCH   = 40;

  for (let i = 0; i < uniqueIds.length; i += BATCH) {
    uniqueIds.slice(i, i + BATCH).forEach(function (userId) {
      const name = fetchSlackUserNameOnce(userId, token);
      results.push({ user_id: userId, name: name });
    });
  }

  sheetLog('INFO', 'handleResolveUsers: done, returning ' + results.length + ' result(s)');
  flushLogs();

  return ContentService.createTextOutput(
    JSON.stringify({ users: results })
  ).setMimeType(ContentService.MimeType.JSON);
}

function fetchSlackUserNameOnce(userId, token) {
  try {
    const resp = UrlFetchApp.fetch('https://slack.com/api/users.info', {
      method: 'post',
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/x-www-form-urlencoded' },
      payload: { user: userId },
      muteHttpExceptions: true
    });
    const json = JSON.parse(resp.getContentText());
    if (!json.ok || !json.user) {
      sheetLog('WARN', 'fetchSlackUserNameOnce: failed for ' + userId);
      return userId;
    }
    const profile     = json.user.profile || {};
    const realName    = profile.real_name_normalized || profile.real_name || '';
    const displayName = profile.display_name_normalized || profile.display_name || '';
    return realName || displayName || userId;
  } catch (e) {
    sheetLog('ERROR', 'fetchSlackUserNameOnce: ' + e.message);
    return userId;
  }
}
