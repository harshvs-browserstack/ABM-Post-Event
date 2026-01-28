// Code.gs – Slack Transcript + Gemini ID card extraction (with user names)

const DEFAULT_TRANSCRIPT_SPREADSHEET_ID = '1SKPYcbjgSg7YPwSw2I5VBBurSFaYYjU1USvLfY9gvlA';
const MAX_MESSAGES_PER_RUN = 400;
const GEMINI_MODEL = 'gemini-2.5-flash-lite';
const GEMINI_ENDPOINT =
  'https://generativelanguage.googleapis.com/v1beta/models/' +
  GEMINI_MODEL +
  ':generateContent';

// Simple in-memory cache for Slack userId -> name per execution
const userNameCache = {};

/**
 * Idempotency: mark an event_id as handled to avoid duplicate processing.
 */
function isDuplicateEvent(eventId) {
  if (!eventId) return false;
  const key = 'handledEvent:' + eventId;
  const props = PropertiesService.getScriptProperties();
  if (props.getProperty(key)) {
    return true;
  }
  props.setProperty(key, String(Date.now()));
  return false;
}

/**
 * Slack entrypoint – configure Slack to POST events here.
 */
function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || '{}');

    // Route: external name-lookup request
    if (body.action === 'resolveUsers' && Array.isArray(body.userIds)) {
      return handleResolveUsers(body);
    }

    // Slack URL verification handshake
    if (body.type === 'url_verification') {
      return ContentService.createTextOutput(body.challenge);
    }

    // Deduplicate by event_id
    if (isDuplicateEvent(body.event_id)) {
      return ContentService.createTextOutput('OK');
    }

    const event = body.event;
    if (!event || !event.text) {
      return ContentService.createTextOutput('No event');
    }

    const text = event.text.trim();
    const channelId = event.channel;
    const triggerTs = event.ts;                 // ts of @bot sync message
    const threadTs = event.thread_ts || event.ts; // thread root

    const cmd = parseSyncCommand(text);
    if (!cmd || !cmd.isSync) {
      return ContentService.createTextOutput('Not a sync command');
    }

    const sheetInfo = cmd.sheetUrl
      ? getSheetFromUrl(cmd.sheetUrl)
      : getDefaultTranscriptSheetForChannel(channelId);

    if (!sheetInfo) {
      sendThreadReply(
        channelId,
        threadTs,
        'Sync failed – could not open target Google Sheet.'
      );
      return ContentService.createTextOutput('Sheet open error');
    }

    // Determine oldest ts for this channel+sheet combo
    const oldestTs = getLastSyncedTs(channelId, sheetInfo.ss.getId());

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
      triggerTs                // exclude the sync command message
    );
    if (!messages || messages.length === 0) {
      sendThreadReply(
        channelId,
        threadTs,
        'No new messages found to sync.'
      );
      return ContentService.createTextOutput('No messages');
    }

    const enriched = enrichMessagesWithIdCards(messages);
    const written = writeTranscriptToSheet(sheetInfo.sheet, enriched);

    // Update last synced ts to the newest message we just wrote
    const newestTs = messages.reduce(function (max, m) {
      const tsNum = Number(m.ts);
      return tsNum > max ? tsNum : max;
    }, 0);
    if (newestTs) {
      setLastSyncedTs(channelId, sheetInfo.ss.getId(), String(newestTs));
    }

    sendThreadReply(
      channelId,
      threadTs,
      `Done – fetched ${written} messages. Last synced ts = ${newestTs}.`
    );

    return ContentService.createTextOutput('OK');
  } catch (err) {
    console.error(err);
    try {
      const body = JSON.parse(e.postData.contents || '{}');
      const event = body.event || {};
      const channelId = event.channel || '';
      const threadTs = event.thread_ts || event.ts || null;
      if (channelId && threadTs) {
        sendThreadReply(
          channelId,
          threadTs,
          'Sync failed with an internal error. Please check Apps Script logs.'
        );
      }
    } catch (ignore) {}
    return ContentService.createTextOutput('Error: ' + err.message);
  }
}

// 2. Command parsing and sheet helpers

function parseSyncCommand(text) {
  // Remove bot mention like "<@U123ABC>"
  const cleaned = text.replace(/<@[^>]+>/g, '').trim(); // "sync" or "sync https://.."
  const parts = cleaned.split(/\s+/);
  if (parts[0] !== 'sync') return null;
  return {
    isSync: true,
    sheetUrl: parts[1] || null
  };
}

/**
 * Open transcript sheet by URL.
 * Always creates/uses dedicated "Slack Transcript" in that spreadsheet.
 */
function getSheetFromUrl(url) {
  try {
    const idMatch = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
    if (!idMatch) return null;
    const spreadsheetId = idMatch[1];

    const ss = SpreadsheetApp.openById(spreadsheetId);
    const sheetName = 'Slack Transcript';
    let sheet = ss.getSheetByName(sheetName);

    if (!sheet) {
      sheet = ss.insertSheet(sheetName);
      sheet.appendRow([
        'timestamp',
        'user',
        'text',
        'thread_ts',
        'attachments',
        'id_card_intel'
      ]);
    }

    return { ss, sheet, name: sheetName };
  } catch (e) {
    console.error(e);
    return null;
  }
}

function getDefaultTranscriptSheetForChannel(channelId) {
  const ss = SpreadsheetApp.openById(DEFAULT_TRANSCRIPT_SPREADSHEET_ID);
  const channelName = getSlackChannelName(channelId);
  const sheetName = `${channelName}-transcript`;

  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
    sheet.appendRow([
      'timestamp',
      'user',
      'text',
      'thread_ts',
      'attachments',
      'id_card_intel'
    ]);
  }
  return { ss, sheet, name: sheetName };
}

// 3. Slack Web API helpers

function getSlackChannelName(channelId) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  const url = 'https://slack.com/api/conversations.info';

  const params = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    payload: {
      channel: channelId
    },
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, params);
  const json = JSON.parse(resp.getContentText());
  if (!json.ok) {
    console.error(json);
    return channelId;
  }
  return json.channel.name || channelId;
}

/**
 * Resolve Slack user ID to a human-readable name (real_name or display_name).
 * Results are cached in-memory per script execution.
 */
function getSlackUserName(userId) {
  if (!userId) return '';
  if (userNameCache[userId]) {
    return userNameCache[userId];
  }

  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  const url = 'https://slack.com/api/users.info';

  const params = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    payload: {
      user: userId
    },
    muteHttpExceptions: true
  };

  try {
    const resp = UrlFetchApp.fetch(url, params);
    const json = JSON.parse(resp.getContentText());
    if (!json.ok || !json.user) {
      console.error('users.info failed for', userId, json);
      userNameCache[userId] = userId; // fallback to ID
      return userId;
    }
    const profile = json.user.profile || {};
    const realName = profile.real_name_normalized || profile.real_name || '';
    const displayName = profile.display_name_normalized || profile.display_name || '';
    const name = realName || displayName || userId;
    userNameCache[userId] = name;
    return name;
  } catch (e) {
    console.error('users.info error for', userId, e);
    userNameCache[userId] = userId;
    return userId;
  }
}

/**
 * Post a message in the same thread as the sync command.
 */
function sendThreadReply(channelId, threadTs, text) {
  if (!channelId || !threadTs) return;

  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');

  const url = 'https://slack.com/api/chat.postMessage';
  const payload = {
    channel: channelId,
    text: text,
    thread_ts: threadTs
  };

  const params = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, params);
  const json = JSON.parse(resp.getContentText());
  if (!json.ok) {
    console.error('chat.postMessage failed', json);
  }
}

// 4. Gemini enrichment – batched, parallel calls

function enrichMessagesWithIdCards(messages) {
  const tasks = [];

  messages.forEach(function (msg, index) {
    const imageFiles = (msg.files || []).filter(function (f) {
      return f.mimetype && f.mimetype.startsWith('image/');
    });

    if (imageFiles.length === 0) {
      msg.id_card_intel = ''; // no images
      return;
    }

    // One Gemini call per message that can see all of its images
    tasks.push({
      msgIndex: index,
      files: imageFiles
    });
  });

  if (tasks.length === 0) {
    return messages;
  }

  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  const allRequests = [];

  tasks.forEach(function (task) {
    const payload = buildGeminiPayloadForIdCards(task.files);
    allRequests.push({
      url: GEMINI_ENDPOINT + '?key=' + encodeURIComponent(apiKey),
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify(payload)
    });
  });

  // Run in batches of 5 in parallel
  const batchSize = 5;
  for (let i = 0; i < allRequests.length; i += batchSize) {
    const batchRequests = allRequests.slice(i, i + batchSize);
    const batchTasks = tasks.slice(i, i + batchSize);

    const responses = UrlFetchApp.fetchAll(batchRequests);

    responses.forEach(function (resp, idx) {
      const t = batchTasks[idx];
      const msg = messages[t.msgIndex];

      try {
        if (resp.getResponseCode() !== 200) {
          console.error('Gemini error HTTP', resp.getResponseCode(), resp.getContentText());
          msg.id_card_intel = '';
          return;
        }

        const data = JSON.parse(resp.getContentText());
        const text = extractTextFromGeminiResponse(data);
        msg.id_card_intel = normalizeIdCardOutput(text);
      } catch (e) {
        console.error('Gemini parse error', e);
        msg.id_card_intel = '';
      }
    });
  }

  return messages;
}

function buildGeminiPayloadForIdCards(files) {
  const parts = [];

  parts.push({
    text:
      'You are made for reading photos of BrowserStack event ID cards. If you dont see a very clear picture of an ID card - return empty json.' +
      'Each image may contain one or more attendee ID cards. ' +
      'For each visible ID card, extract: first_name, last_name, company_name. ' +
      'Return the result as plain text ONLY in this exact format:\n' +
      'ID Card 1: {"first_name": "...", "last_name": "...", "company_name": "..."}\n' +
      'ID Card 2: {"first_name": "...", "last_name": "...", "company_name": "..."}\n' +
      'The company_name here cannot be BrowserStack, that is the logo on the ID card, do not confuse the two' +
      'Use lower_snake_case keys. ' +
      'If a field is unknown, set it to an empty string. ' +
      'Do not add any prose or explanation.'
  });

  files.forEach(function (file) {
    const imageData = getImageDataFromUrl(file.url_private || file.url_private_download);
    if (!imageData) return;
    parts.push({
      inlineData: {
        mimeType: imageData.mimeType,
        data: imageData.data
      }
    });
  });

  const payload = {
    contents: [
      {
        role: 'user',
        parts: parts
      }
    ]
  };

  return payload;
}

function getImageDataFromUrl(url) {
  if (!url) return null;
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');

  const resp = UrlFetchApp.fetch(url, {
    headers: {
      Authorization: 'Bearer ' + token
    },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    console.error('Image fetch error', resp.getResponseCode(), resp.getContentText());
    return null;
  }

  const blob = resp.getBlob();
  return {
    mimeType: blob.getContentType(),
    data: Utilities.base64Encode(blob.getBytes())
  };
}

function extractTextFromGeminiResponse(data) {
  try {
    const candidate = data.candidates && data.candidates[0];
    if (!candidate || !candidate.content || !candidate.content.parts) return '';
    const parts = candidate.content.parts;
    const texts = parts
      .map(function (p) {
        return p.text || '';
      })
      .filter(function (t) {
        return t;
      });
    return texts.join('\n').trim();
  } catch (e) {
    console.error('extractTextFromGeminiResponse error', e, JSON.stringify(data));
    return '';
  }
}

function normalizeIdCardOutput(raw) {
  if (!raw) return '';

  const lines = raw
    .split(/\r?\n/)
    .map(function (l) {
      return l.trim();
    })
    .filter(function (l) {
      return l;
    });

  if (lines.length === 0) return '';

  const normalizedLines = [];
  let cardCount = 0;

  lines.forEach(function (line) {
    const m = line.match(/id\s*card\s*(\d*)\s*:\s*(\{.*\})/i);
    if (m) {
      cardCount++;
      const jsonPart = m[2].trim();
      normalizedLines.push(`ID Card ${cardCount}: ${jsonPart}`);
    } else if (line.startsWith('{') && line.endsWith('}')) {
      cardCount++;
      normalizedLines.push(`ID Card ${cardCount}: ${line}`);
    }
  });

  if (normalizedLines.length === 0) {
    return raw;
  }

  return normalizedLines.join('\n');
}

// 5. Write transcript with ID Card Intel

function writeTranscriptToSheet(sheet, messages) {
  if (!messages.length) return 0;

  const lastRow = sheet.getLastRow();
  if (lastRow === 0) {
    sheet.appendRow([
      'timestamp',
      'user',
      'text',
      'thread_ts',
      'attachments',
      'id_card_intel'
    ]);
  }

  const rows = messages
    .sort(function (a, b) {
      return parseFloat(a.ts) - parseFloat(b.ts);
    })
    .map(function (m) {
      const attachmentsJson = JSON.stringify(m.files || []);
      const idCardIntel = m.id_card_intel || '';
      // m.user is now a human-readable name, resolved earlier
      return [m.ts, m.user, m.text, m.thread_ts, attachmentsJson, idCardIntel];
    });

  sheet
    .getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length)
    .setValues(rows);
  return rows.length;
}

// 6. Only fetch messages after last saved timestamp

function getLastSyncedTs(channelId, sheetId) {
  const key = `lastSyncedTs:${channelId}:${sheetId}`;
  return PropertiesService.getScriptProperties().getProperty(key) || null;
}

function setLastSyncedTs(channelId, sheetId, ts) {
  const key = `lastSyncedTs:${channelId}:${sheetId}`;
  PropertiesService.getScriptProperties().setProperty(key, ts);
}

/**
 * Fetch recent messages from channel since oldestTs (exclusive) and
 * exclude the sync command message itself. Also resolve user IDs to names.
 */
function fetchRecentMessagesFromSlackChannel(channelId, limit, oldestTs, excludeTs) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  const url = 'https://slack.com/api/conversations.history';

  const payload = {
    channel: channelId,
    limit: limit
  };
  if (oldestTs) {
    payload.oldest = oldestTs;
    payload.inclusive = false;
  }

  const params = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    payload: payload,
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, params);
  const json = JSON.parse(resp.getContentText());
  if (!json.ok) {
    console.error(json);
    return [];
  }

  return json.messages
  .filter(function (m) {
    if (excludeTs && m.ts === excludeTs) return false;
    if (isBotOrCommandMessage(m)) return false;
    return true;
  })
  .map(function (m) {
    const files = m.files || m.attachments || [];
    // Store raw user ID here, not the name
    return {
      ts: m.ts,
      user: m.user || '',       // Slack user ID
      text: m.text || '',
      thread_ts: m.thread_ts || '',
      files: files
    };
  });
}

/**
 * Returns true if the message should be excluded from transcript:
 * - contains a mention of the bot user id (e.g. <@U0A49H0TBFY>)
 * - or looks like a sync command.
 */
function isBotOrCommandMessage(m) {
  const text = String(m.text || '').trim();
  if (!text) return false;

  // Skip any message mentioning the bot user ID
  if (text.indexOf('<@U0A49H0TBFY>') !== -1) {
    return true;
  }

  // Skip messages that are essentially the sync command
  const cleaned = text.replace(/<@[^>]+>/g, '').trim(); // remove any mentions
  const parts = cleaned.split(/\s+/);
  if (parts[0] && parts[0].toLowerCase() === 'sync') {
    return true;
  }

  return false;
}



/**
 * Handle external POST { action: "resolveUsers", userIds: [...] }
 * Returns JSON { users: [ { user_id, name }, ... ] }
 */
function handleResolveUsers(body) {
  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');
  if (!token) {
    return ContentService.createTextOutput(
      JSON.stringify({ error: 'SLACK_BOT_TOKEN not configured' })
    ).setMimeType(ContentService.MimeType.JSON);
  }

  const userIds = body.userIds || [];
  const uniqueIds = Array.from(new Set(userIds)).filter(function (id) {
    return id && typeof id === 'string';
  });

  const results = [];
  const BATCH = 40;

  for (let i = 0; i < uniqueIds.length; i += BATCH) {
    const batch = uniqueIds.slice(i, i + BATCH);
    batch.forEach(function (userId) {
      const name = fetchSlackUserNameOnce(userId, token);
      results.push({ user_id: userId, name: name });
    });
  }

  return ContentService.createTextOutput(
    JSON.stringify({ users: results })
  ).setMimeType(ContentService.MimeType.JSON);
}

/**
 * Single users.info lookup with basic error handling.
 */
function fetchSlackUserNameOnce(userId, token) {
  const url = 'https://slack.com/api/users.info';

  const params = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    payload: {
      user: userId
    },
    muteHttpExceptions: true
  };

  try {
    const resp = UrlFetchApp.fetch(url, params);
    const json = JSON.parse(resp.getContentText());
    if (!json.ok || !json.user) {
      console.error('users.info failed for', userId, json);
      return userId;
    }
    const profile = json.user.profile || {};
    const realName =
      profile.real_name_normalized || profile.real_name || '';
    const displayName =
      profile.display_name_normalized || profile.display_name || '';
    return realName || displayName || userId;
  } catch (e) {
    console.error('users.info error for', userId, e);
    return userId;
  }
}

