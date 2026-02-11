// ============================================================================
// Script B – Event spreadsheet side
// Anti-hallucination improvements + structured output + noise filtering
// ============================================================================

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SLACK_RESOLVER_WEBAPP_URL =
  'https://script.google.com/macros/s/AKfycbyyUcDokrL3X1t9YSFfrf2RbWQSQXPe7pVYsOgOPYtTRHRiSvxKnEoSFM1TvWVzPQtM/exec';

const QUAL_DOC_ID = '1sL5qJN-8MgcTawIsSChGGVRlt4lORPKwOL_Hbj0P6Dg';

const GEMINI_MODEL_SUMMARY = 'gemini-2.5-flash-lite';
const QUAL_MODEL = 'gemini-2.5-flash';

const GEMINI_ENDPOINT_SUMMARY =
  'https://generativelanguage.googleapis.com/v1beta/models/' +
  GEMINI_MODEL_SUMMARY +
  ':generateContent';

// ---------------------------------------------------------------------------
// Menu
// ---------------------------------------------------------------------------

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  // Your existing Slack menu
  ui.createMenu('Slack')
    .addItem('Resolve Slack Names', 'resolveSlackNamesViaScriptA')
    .addItem('Build Slack Summary', 'buildSlackSummaryFromTranscript')
    .addItem('Populate Slack Intel', 'populateSlackIntelFromSummary')
    .addItem('Qualify Leads (Batch)', 'batchQualifyLeads')
    .addSeparator()
    .addItem('🔄 Retry Failed Messages', 'retryFailedMessages')
    .addToUi();

  // Your new Participant Tools menu
  ui.createMenu('🚀 Participant Tools')
    .addItem('Map Participant Emails', 'mapParticipantEmails')
    .addToUi();
}

// ============================================================================
// 1. Build Slack Summary from Slack Transcript (Gemini 2.5 Flash-Lite)
// ============================================================================

/**
 * Build "Slack Summary" from "Slack Transcript" using Gemini 2.5 Flash-Lite.
 * IMPROVED: SPOC-based batching + MESSAGE-LEVEL TRACKING for auditing.
 */
function buildSlackSummaryFromTranscript() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const collatedSheet = ss.getActiveSheet();
  const transcriptSheet = ss.getSheetByName('Slack Transcript');
  if (!transcriptSheet) {
    SpreadsheetApp.getActive().toast(
      "Transcript sheet 'Slack Transcript' not found.",
      'Slack Summary',
      5
    );
    return;
  }

  // Initialize message-level tracking log
  const messageLog = {};

  // 1) Read roster from collated sheet
  const collatedRange = collatedSheet.getDataRange();
  const collatedValues = collatedRange.getValues();
  if (collatedValues.length < 2) {
    SpreadsheetApp.getActive().toast('No attendee rows found.', 'Slack Summary', 5);
    return;
  }

  const header = collatedValues[0];
  const colIdx = {
    firstName: header.indexOf('First Name'),
    lastName: header.indexOf('Last Name'),
    email: header.indexOf('Email'),
    account: header.indexOf('Account')
  };

  const roster = [];
  for (let i = 1; i < collatedValues.length; i++) {
    const row = collatedValues[i];
    const firstName =
      colIdx.firstName >= 0 ? String(row[colIdx.firstName]).trim() : '';
    const lastName =
      colIdx.lastName >= 0 ? String(row[colIdx.lastName]).trim() : '';
    const email = colIdx.email >= 0 ? String(row[colIdx.email]).trim() : '';
    const account =
      colIdx.account >= 0 ? String(row[colIdx.account]).trim() : '';
    if (!(firstName || lastName || email || account)) continue;
    roster.push({
      first_name: firstName,
      last_name: lastName,
      email: email,
      account: account
    });
  }

  if (roster.length === 0) {
    SpreadsheetApp.getActive().toast(
      'Roster is empty after filtering.',
      'Slack Summary',
      5
    );
    return;
  }

  // 2) Read transcript messages in chronological order
  const tRange = transcriptSheet.getDataRange();
  const tValues = tRange.getValues();
  if (tValues.length < 2) {
    SpreadsheetApp.getActive().toast(
      'No transcript rows to process.',
      'Slack Summary',
      5
    );
    return;
  }

  const tHeader = tValues[0];
  const tIdx = {
    ts: tHeader.indexOf('timestamp'),
    user: tHeader.indexOf('user'),
    text: tHeader.indexOf('text'),
    thread_ts: tHeader.indexOf('thread_ts'),
    id_card_intel: tHeader.indexOf('id_card_intel')
  };

  const messages = [];
  for (let i = 1; i < tValues.length; i++) {
    const row = tValues[i];
    const text = tIdx.text >= 0 ? String(row[tIdx.text] || '').trim() : '';
    const ts = tIdx.ts >= 0 ? String(row[tIdx.ts] || '') : '';
    
    if (!text) {
      // Track empty messages
      if (ts) {
        messageLog[ts] = {
          status: 'Skipped - Empty Text',
          extractedCount: 0,
          summaries: [],
          warnings: [],
          matchQuality: []
        };
      }
      continue;
    }
    
    messages.push({
      ts: ts,
      user: tIdx.user >= 0 ? String(row[tIdx.user] || '') : '',
      text: text,
      id_card_intel:
        tIdx.id_card_intel >= 0
          ? String(row[tIdx.id_card_intel] || '').trim()
          : ''
    });
  }

  if (messages.length === 0) {
    SpreadsheetApp.getActive().toast(
      'No non-empty transcript messages.',
      'Slack Summary',
      5
    );
    return;
  }

  // ← Track original SPOCs before noise filtering
  const originalSPOCs = new Set();
  messages.forEach(function(msg) {
    if (msg.user) originalSPOCs.add(msg.user);
  });
  console.log(`Original unique SPOCs before filtering: ${originalSPOCs.size}`);

  // 3) FILTER OUT NOISE MESSAGES (with tracking)
  const cleanMessages = messages.filter(function(msg) {
    const isNoise = isNoisyMessage(msg);
    if (isNoise) {
      // Log filtered messages
      const noiseReason = getNoiseReason(msg);
      messageLog[msg.ts] = {
        status: '🔇 Filtered as Noise',
        extractedCount: 0,
        summaries: [],
        warnings: [noiseReason],
        matchQuality: []
      };
      console.log(`Filtered message ${msg.ts}: ${noiseReason}`);
    }
    return !isNoise;
  });
  
  console.log(`Filtered ${messages.length - cleanMessages.length} noise messages, ${cleanMessages.length} remaining`);

  // 4) GROUP MESSAGES BY SPOC (user)
  const messagesBySPOC = {};
  cleanMessages.forEach(function(msg) {
    const spoc = msg.user;
    if (!spoc) {
      messageLog[msg.ts] = {
        status: 'Skipped - No SPOC',
        extractedCount: 0,
        summaries: [],
        warnings: ['Message has no user/SPOC identifier'],
        matchQuality: []
      };
      return;
    }
    if (!messagesBySPOC[spoc]) {
      messagesBySPOC[spoc] = [];
    }
    messagesBySPOC[spoc].push(msg);
  });

  // ← Track which SPOCs got completely filtered out
  const noisedOutSPOCs = [...originalSPOCs].filter(spoc => !messagesBySPOC[spoc]);
  if (noisedOutSPOCs.length > 0) {
    console.warn(`⚠️  ${noisedOutSPOCs.length} SPOCs had ALL messages filtered as noise:`);
    console.warn(noisedOutSPOCs.join(', '));
  }

  // 5) SORT EACH SPOC'S MESSAGES CHRONOLOGICALLY
  Object.keys(messagesBySPOC).forEach(function(spoc) {
    messagesBySPOC[spoc].sort(function(a, b) {
      return parseFloat(a.ts) - parseFloat(b.ts);
    });
  });

  console.log(`Grouped messages into ${Object.keys(messagesBySPOC).length} SPOCs`);

  // 6) Call Gemini ONCE PER SPOC with all their messages (with tracking)
  const summaryRows = buildSlackSummaryBySPOC(roster, messagesBySPOC, messageLog);

  // 7) Write Slack Summary tab
  writeSlackSummarySheet(ss, summaryRows);

  // 8) Write processing results back to transcript sheet
  writeProcessingResultsToTranscript(ss, transcriptSheet, messageLog);

  // 9) Generate processing report
  generateProcessingReport(ss, messageLog, originalSPOCs.size, Object.keys(messagesBySPOC).length);

  SpreadsheetApp.getActive().toast(
    `Slack Summary built with ${summaryRows.length} rows from ${Object.keys(messagesBySPOC).length} SPOCs. Check transcript for details.`,
    'Slack Summary',
    8
  );
}

/**
 * Get the specific noise reason for a message
 */
function getNoiseReason(msg) {
  const text = msg.text.toLowerCase();
  const trimmedText = msg.text.trim();
  
  const noisePatterns = [
    { pattern: 'has joined the channel', reason: 'System: User joined channel' },
    { pattern: 'has left the channel', reason: 'System: User left channel' },
    { pattern: 'has renamed the channel', reason: 'System: Channel renamed' },
    { pattern: 'set the channel topic', reason: 'System: Topic changed' },
    { pattern: 'pinned a message', reason: 'System: Message pinned' },
    { pattern: 'flight booking', reason: 'Logistics: Flight booking' },
    { pattern: 'hotel booking confirmation', reason: 'Logistics: Hotel booking' },
    { pattern: 'lemon tree bengaluru', reason: 'Logistics: Hotel mention' },
    { pattern: 'check in: 19th aug', reason: 'Logistics: Check-in' },
    { pattern: 'check out: 20th aug', reason: 'Logistics: Check-out' },
    { pattern: 'please arrive at the venue by', reason: 'Logistics: Venue arrival' },
    { pattern: 'registration desk', reason: 'Logistics: Registration' },
    { pattern: 'onboarding session for', reason: 'Internal: Onboarding' },
    { pattern: 'sharing the recording of', reason: 'Internal: Recording share' },
    { pattern: 'business cards for the internal team', reason: 'Internal: Business cards' },
    { pattern: 'food counter will be open', reason: 'Logistics: Food' },
    { pattern: 'breakfast will be included', reason: 'Logistics: Breakfast' }
  ];

  for (let i = 0; i < noisePatterns.length; i++) {
    if (text.includes(noisePatterns[i].pattern)) {
      return noisePatterns[i].reason;
    }
  }

  if (/^(thanks?|thank you|noted|sure|okay|ok|will do|sounds good|got it)$/i.test(trimmedText)) {
    return 'Pure acknowledgment/greeting';
  }

  if (trimmedText.length < 10 && !/(interested|demo|lca|tcm|tm|a11y|percy|ai|automate)/i.test(trimmedText)) {
    return `Too short (${trimmedText.length} chars) with no intel keywords`;
  }

  return 'Matched noise filter';
}

/**
 * Filter out noise messages that don't contain lead intelligence.
 */
function isNoisyMessage(msg) {
  const text = msg.text.toLowerCase();
  const trimmedText = msg.text.trim();
  
  const noisePatterns = [
    'has joined the channel',
    'has left the channel',  
    'has renamed the channel',
    'set the channel topic',
    'pinned a message',
    'flight booking',
    'hotel booking confirmation',
    'lemon tree bengaluru',
    'check in: 19th aug',
    'check out: 20th aug',
    'please arrive at the venue by',
    'registration desk',
    'onboarding session for',
    'sharing the recording of',
    'business cards for the internal team',
    'food counter will be open',
    'breakfast will be included'
  ];

  const isSystemNoise = noisePatterns.some(function(pattern) {
    return text.includes(pattern);
  });

  const pureChatter = /^(thanks?|thank you|noted|sure|okay|ok|will do|sounds good|got it)$/i.test(trimmedText);
  
  const isMeaninglessShort = trimmedText.length < 10 && 
    !/(interested|demo|lca|tcm|tm|a11y|percy|ai|automate)/i.test(trimmedText);

  return isSystemNoise || pureChatter || isMeaninglessShort;
}

/**
 * Process messages grouped by SPOC - one API call per SPOC (with tracking)
 */
function buildSlackSummaryBySPOC(roster, messagesBySPOC, messageLog) {
  const apiKey =
    PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) {
    throw new Error('GEMINI_API_KEY not set in Script Properties.');
  }

  const allTranscriptSPOCs = new Set();
  Object.keys(messagesBySPOC).forEach(spoc => allTranscriptSPOCs.add(spoc));
  console.log(`Total unique SPOCs to process: ${allTranscriptSPOCs.size}`);

  // Build roster lookup maps for enrichment
  const rosterByEmail = {};
  const rosterByNameAccount = {};
  
  roster.forEach(function (r) {
    const emailLower = r.email.toLowerCase();
    const firstLower = r.first_name.toLowerCase();
    const accountLower = r.account.toLowerCase();
    
    if (emailLower) {
      rosterByEmail[emailLower] = r;
    }
    
    if (firstLower && accountLower) {
      const key = firstLower + '|' + accountLower;
      if (!rosterByNameAccount[key]) {
        rosterByNameAccount[key] = [];
      }
      rosterByNameAccount[key].push(r);
    }
  });

  const allRows = [];
  const seen = {};
  const failedSPOCs = [];
  const hallucinatedSPOCs = [];
  
  const spocList = Object.keys(messagesBySPOC);
  let processedCount = 0;
  
  spocList.forEach(function(spoc) {
    const spocMessages = messagesBySPOC[spoc];
    
    console.log(`Processing SPOC: ${spoc} (${spocMessages.length} messages)`);
    processedCount++;
    
    const payload = buildGeminiSummaryPayload(roster, spocMessages);

    const options = {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify(payload)
    };

    const resp = UrlFetchApp.fetch(
      GEMINI_ENDPOINT_SUMMARY + '?key=' + encodeURIComponent(apiKey),
      options
    );
    
    if (resp.getResponseCode() !== 200) {
      const httpCode = resp.getResponseCode();
      console.error(
        `SPOC ${spoc}: Gemini HTTP error`,
        httpCode,
        resp.getContentText()
      );
      
      failedSPOCs.push({
        spoc: spoc,
        reason: 'API_ERROR',
        messages: spocMessages.length,
        httpCode: httpCode
      });
      
      // Mark all messages from this SPOC as failed
      spocMessages.forEach(function(msg) {
        messageLog[msg.ts] = {
          status: '❌ Failed - API Error',
          extractedCount: 0,
          summaries: [],
          warnings: [`HTTP ${httpCode}: ${resp.getContentText().substring(0, 100)}`],
          matchQuality: [],
          retryable: true
        };
      });
      
      return;
    }

    let parsed;
    try {
      const root = JSON.parse(resp.getContentText());
      const candidate = root.candidates && root.candidates[0];
      if (!candidate || !candidate.content || !candidate.content.parts) {
        console.error(`SPOC ${spoc}: No candidate or parts in Gemini response`);
        
        failedSPOCs.push({
          spoc: spoc,
          reason: 'EMPTY_RESPONSE',
          messages: spocMessages.length
        });
        
        spocMessages.forEach(function(msg) {
          messageLog[msg.ts] = {
            status: '❌ Failed - Empty Response',
            extractedCount: 0,
            summaries: [],
            warnings: ['Gemini returned no candidate content'],
            matchQuality: [],
            retryable: true
          };
        });
        
        return;
      }

      const jsonText = (candidate.content.parts[0].text || '').trim();
      if (!jsonText) {
        console.error(`SPOC ${spoc}: Empty JSON text from Gemini`);
        
        failedSPOCs.push({
          spoc: spoc,
          reason: 'EMPTY_JSON',
          messages: spocMessages.length
        });
        
        spocMessages.forEach(function(msg) {
          messageLog[msg.ts] = {
            status: '❌ Failed - Empty JSON',
            extractedCount: 0,
            summaries: [],
            warnings: ['Gemini returned empty JSON'],
            matchQuality: [],
            retryable: true
          };
        });
        
        return;
      }

      parsed = JSON.parse(jsonText);
      if (!Array.isArray(parsed)) {
        console.error(
          `SPOC ${spoc}: Gemini JSON root is not array:`,
          jsonText.substring(0, 500)
        );
        
        failedSPOCs.push({
          spoc: spoc,
          reason: 'INVALID_JSON_FORMAT',
          messages: spocMessages.length
        });
        
        spocMessages.forEach(function(msg) {
          messageLog[msg.ts] = {
            status: '❌ Failed - Invalid JSON',
            extractedCount: 0,
            summaries: [],
            warnings: ['Gemini returned non-array JSON'],
            matchQuality: [],
            retryable: true
          };
        });
        
        return;
      }
    } catch (e) {
      console.error(`SPOC ${spoc}: Failed to parse Gemini JSON`, e);
      
      failedSPOCs.push({
        spoc: spoc,
        reason: 'PARSE_ERROR',
        messages: spocMessages.length,
        error: e.toString()
      });
      
      spocMessages.forEach(function(msg) {
        messageLog[msg.ts] = {
          status: '❌ Failed - Parse Error',
          extractedCount: 0,
          summaries: [],
          warnings: [`JSON parse error: ${e.toString()}`],
          matchQuality: [],
          retryable: true
        };
      });
      
      return;
    }

    // Initialize tracking for all messages in this SPOC batch
    spocMessages.forEach(function(msg) {
      if (!messageLog[msg.ts]) {
        messageLog[msg.ts] = {
          status: '⏳ Processing',
          extractedCount: 0,
          summaries: [],
          warnings: [],
          matchQuality: []
        };
      }
    });

    // Track extractions by timestamp
    const extractionsByTs = {};
    
    let spocLeadCount = 0;
    let hallucinatedCount = 0;
    let validCount = 0;
    let enrichedCount = 0;
    let fuzzyCount = 0;

    parsed.forEach(function (entry) {
      let email = String(entry.email || '').trim();
      const firstName = String(entry.first_name || '').trim();
      const lastName = String(entry.last_name || '').trim();
      const account = String(entry.account || '').trim();
      let summary = String(entry.summary || '').trim();
      const ts = String(entry.ts || '').trim();
      const spocFromEntry = String(entry.spoc || spoc).trim();

      if (!summary) return;
      
      // Initialize extraction tracking for this timestamp
      if (!extractionsByTs[ts]) {
        extractionsByTs[ts] = {
          summaries: [],
          warnings: [],
          matchQuality: []
        };
      }

      let wasEnriched = false;
      
      // ENRICHMENT: Fill in missing email from roster
      if (!email && firstName && account) {
        const lookupKey = firstName.toLowerCase() + '|' + account.toLowerCase();
        const matches = rosterByNameAccount[lookupKey];
        if (matches && matches.length > 0) {
          email = matches[0].email;
          wasEnriched = true;
          enrichedCount++;
          extractionsByTs[ts].warnings.push(`✏️ Enriched: Email filled from roster for ${firstName} ${account}`);
          console.log(`SPOC ${spoc}: Enriched email for ${firstName} ${account} → ${email}`);
        }
      }

      const rosterMatchResult = findRosterMatchWithStage(entry, roster);

      if (!rosterMatchResult.match) {
        console.warn(
          `SPOC ${spoc}: No roster match:`,
          firstName, lastName, email, account
        );
        hallucinatedCount++;
        extractionsByTs[ts].warnings.push(`❌ Hallucinated: ${firstName} ${lastName} from ${account} (no roster match)`);
        extractionsByTs[ts].matchQuality.push('Hallucinated');
        return;
      }

      const rosterMatch = rosterMatchResult.match;
      const matchStage = rosterMatchResult.stage;
      
      validCount++;

      // Track match quality
      if (matchStage >= 4) {
        fuzzyCount++;
        extractionsByTs[ts].warnings.push(`⚠️ Fuzzy Match: Stage ${matchStage} for ${firstName} ${account}`);
        extractionsByTs[ts].matchQuality.push('Fuzzy');
      } else if (wasEnriched) {
        extractionsByTs[ts].matchQuality.push('Enriched');
      } else {
        extractionsByTs[ts].matchQuality.push('Exact');
      }

      // Enrich with roster data if needed
      if (!email && rosterMatch.email) {
        email = rosterMatch.email;
      }

      // Ensure summary follows the standardized format
      const prefix = spocFromEntry
        ? `Intel from SPOC of the day ${spocFromEntry}: `
        : `Intel from SPOC of the day ${spoc}: `;
      if (!summary.startsWith('Intel from SPOC of the day')) {
        summary = prefix + summary;
      }

      // De-duplication
      const dedupKey = spoc + '|' + email + '|' + ts + '|' + summary;
      if (seen[dedupKey]) return;
      seen[dedupKey] = true;

      allRows.push([email, firstName, lastName, account, ts, spocFromEntry || spoc, summary]);
      spocLeadCount++;
      
      // Track the summary for this timestamp
      extractionsByTs[ts].summaries.push(summary);
    });

    // Write extraction results to messageLog
    Object.keys(extractionsByTs).forEach(function(ts) {
      const extraction = extractionsByTs[ts];
      if (messageLog[ts]) {
        messageLog[ts].status = extraction.summaries.length > 0 ? '✅ Processed' : '⚠️ No Intel Extracted';
        messageLog[ts].extractedCount = extraction.summaries.length;
        messageLog[ts].summaries = extraction.summaries;
        messageLog[ts].warnings = messageLog[ts].warnings.concat(extraction.warnings);
        messageLog[ts].matchQuality = extraction.matchQuality;
      }
    });

    // Mark messages with no extractions
    spocMessages.forEach(function(msg) {
      if (messageLog[msg.ts] && messageLog[msg.ts].extractedCount === 0 && messageLog[msg.ts].status === '⏳ Processing') {
        messageLog[msg.ts].status = '⚠️ No Intel Extracted';
        messageLog[msg.ts].warnings.push('Message processed but no valid intel extracted');
      }
    });

    // Track SPOCs where ALL entries were hallucinated
    if (parsed.length > 0 && validCount === 0) {
      hallucinatedSPOCs.push({
        spoc: spoc,
        attempted: parsed.length,
        hallucinated: hallucinatedCount,
        reason: 'ALL_HALLUCINATED'
      });
      console.error(`🚨 SPOC ${spoc}: ALL ${parsed.length} Gemini entries were hallucinated/invalid!`);
    }

    console.log(`SPOC ${spoc}: Extracted ${spocLeadCount} valid leads (${hallucinatedCount} hallucinated, ${validCount} valid, ${enrichedCount} enriched, ${fuzzyCount} fuzzy)`);
    
    // Rate limiting
    if (processedCount < spocList.length) {
      Utilities.sleep(500);
    }
  });

  // Track SPOCs in final output
  const spocsCovered = new Set();
  allRows.forEach(row => spocsCovered.add(row[5]));

  const missingSPOCs = [...allTranscriptSPOCs].filter(s => !spocsCovered.has(s));

  // Console diagnostic report (preserved)
  console.log('\n' + '='.repeat(70));
  console.log('SPOC PROCESSING SUMMARY');
  console.log('='.repeat(70));
  console.log(`Total SPOCs in transcript: ${allTranscriptSPOCs.size}`);
  console.log(`SPOCs successfully processed: ${processedCount}`);
  console.log(`Total summary rows generated: ${allRows.length}`);
  console.log(`SPOCs in final summary: ${spocsCovered.size}`);
  console.log(`SPOCs DROPPED (zero output rows): ${missingSPOCs.length}`);

  if (failedSPOCs.length > 0) {
    console.log(`\n⚠️  API/Parse Failures (${failedSPOCs.length} SPOCs):`);
    failedSPOCs.forEach(f => {
      console.log(`  - ${f.spoc}: ${f.reason} (${f.messages} messages)`);
    });
  }

  if (hallucinatedSPOCs.length > 0) {
    console.log(`\n⚠️  Full Hallucination (${hallucinatedSPOCs.length} SPOCs):`);
    hallucinatedSPOCs.forEach(h => {
      console.log(`  - ${h.spoc}: ${h.attempted} entries attempted, ALL invalid`);
    });
  }

  if (missingSPOCs.length > 0) {
    console.log(`\n⚠️  Missing SPOCs (zero rows in summary):`);
    console.log(`  ${missingSPOCs.join(', ')}`);
  }
  
  console.log('='.repeat(70) + '\n');

  console.log(`Total leads extracted: ${allRows.length} from ${processedCount} SPOCs`);
  return allRows;
}

/**
 * Enhanced findRosterMatch that returns both match and stage number
 */
function findRosterMatchWithStage(entry, roster) {
  const emailLower = String(entry.email || '').toLowerCase().trim();
  const firstLower = String(entry.first_name || '').toLowerCase().trim();
  const lastLower = String(entry.last_name || '').toLowerCase().trim();
  const accountLower = normalizeCompanyName(entry.account || '');

  // STAGE 1: Exact email match
  if (emailLower) {
    const emailMatch = roster.find(function(r) {
      return r.email.toLowerCase() === emailLower;
    });
    if (emailMatch) return { match: emailMatch, stage: 1 };
  }

  // STAGE 2: First name + normalized company
  if (firstLower && accountLower) {
    const nameCompanyMatch = roster.find(function(r) {
      return r.first_name.toLowerCase() === firstLower && 
             normalizeCompanyName(r.account) === accountLower;
    });
    if (nameCompanyMatch) return { match: nameCompanyMatch, stage: 2 };
  }

  // STAGE 3: Full name match
  if (firstLower && lastLower) {
    const fullNameMatch = roster.find(function(r) {
      return r.first_name.toLowerCase() === firstLower && 
             r.last_name.toLowerCase() === lastLower;
    });
    if (fullNameMatch) return { match: fullNameMatch, stage: 3 };
  }

  // STAGE 4: First name + email domain
  if (firstLower && emailLower) {
    const emailDomain = emailLower.split('@')[1];
    if (emailDomain) {
      const domainMatch = roster.find(function(r) {
        const rDomain = r.email.toLowerCase().split('@')[1];
        return r.first_name.toLowerCase() === firstLower && rDomain === emailDomain;
      });
      if (domainMatch) return { match: domainMatch, stage: 4 };
    }
  }

  // STAGE 5: Fuzzy company substring
  if (firstLower && accountLower && accountLower.length >= 4) {
    const fuzzyMatch = roster.find(function(r) {
      const rAccountLower = normalizeCompanyName(r.account);
      return r.first_name.toLowerCase() === firstLower && 
             (rAccountLower.includes(accountLower) || accountLower.includes(rAccountLower));
    });
    if (fuzzyMatch) return { match: fuzzyMatch, stage: 5 };
  }

  return { match: null, stage: 0 };
}

/**
 * Write processing results back to Slack Transcript sheet
 */
function writeProcessingResultsToTranscript(ss, transcriptSheet, messageLog) {
  console.log('Writing processing results back to transcript...');
  
  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
  
  if (values.length < 2) return;
  
  const header = values[0];
  const tsIdx = header.indexOf('timestamp');
  
  if (tsIdx === -1) {
    console.error('timestamp column not found in transcript');
    return;
  }
  
  // Find or create tracking columns
  let statusIdx = header.indexOf('Processing Status');
  let countIdx = header.indexOf('Extracted Count');
  let summariesIdx = header.indexOf('Extracted Summaries');
  let qualityIdx = header.indexOf('Match Quality');
  let warningsIdx = header.indexOf('Warnings/Notes');
  
  const needsNewColumns = (statusIdx === -1);
  
  if (needsNewColumns) {
    // Add new column headers
    statusIdx = header.length;
    countIdx = header.length + 1;
    summariesIdx = header.length + 2;
    qualityIdx = header.length + 3;
    warningsIdx = header.length + 4;
    
    values[0].push('Processing Status', 'Extracted Count', 'Extracted Summaries', 'Match Quality', 'Warnings/Notes');
  }
  
  // Update each row with processing results
  for (let i = 1; i < values.length; i++) {
    const ts = String(values[i][tsIdx] || '').trim();
    
    if (!ts || !messageLog[ts]) {
      // No tracking data for this message
      if (needsNewColumns) {
        values[i][statusIdx] = 'Not Processed';
        values[i][countIdx] = 0;
        values[i][summariesIdx] = '';
        values[i][qualityIdx] = '';
        values[i][warningsIdx] = '';
      }
      continue;
    }
    
    const log = messageLog[ts];
    
    values[i][statusIdx] = log.status || 'Unknown';
    values[i][countIdx] = log.extractedCount || 0;
    values[i][summariesIdx] = (log.summaries || []).join('\n---\n');
    values[i][qualityIdx] = (log.matchQuality || []).join(', ');
    values[i][warningsIdx] = (log.warnings || []).join('\n');
  }
  
  // Write all data back
  const newRange = transcriptSheet.getRange(1, 1, values.length, values[0].length);
  newRange.setValues(values);
  
  // Apply conditional formatting
  if (statusIdx >= 0) {
    for (let i = 2; i <= values.length; i++) {
      const status = String(values[i-1][statusIdx] || '');
      const cell = transcriptSheet.getRange(i, statusIdx + 1);
      
      if (status.includes('✅ Processed')) {
        cell.setBackground('#D9EAD3'); // Light green
      } else if (status.includes('Filtered as Noise')) {
        cell.setBackground('#FFF2CC'); // Light yellow
      } else if (status.includes('❌ Failed')) {
        cell.setBackground('#EA9999'); // Light red
      } else if (status.includes('⚠️')) {
        cell.setBackground('#FFE599'); // Light orange
      }
    }
  }
  
  console.log(`Updated ${values.length - 1} transcript rows with processing results`);
}

/**
 * Generate processing report in a new sheet
 */
function generateProcessingReport(ss, messageLog, originalSPOCs, processedSPOCs) {
  const reportName = 'Processing Report';
  let reportSheet = ss.getSheetByName(reportName);
  
  if (!reportSheet) {
    reportSheet = ss.insertSheet(reportName);
  } else {
    reportSheet.clear();
  }
  
  // Count statistics
  const stats = {
    total: 0,
    processed: 0,
    filtered: 0,
    failed: 0,
    noIntel: 0,
    skipped: 0,
    withIntel: 0,
    totalExtractions: 0,
    hallucinated: 0,
    fuzzy: 0,
    enriched: 0,
    exact: 0
  };
  
  Object.keys(messageLog).forEach(function(ts) {
    const log = messageLog[ts];
    stats.total++;
    
    if (log.status.includes('Processed')) {
      stats.processed++;
      if (log.extractedCount > 0) {
        stats.withIntel++;
        stats.totalExtractions += log.extractedCount;
      } else {
        stats.noIntel++;
      }
    } else if (log.status.includes('Filtered')) {
      stats.filtered++;
    } else if (log.status.includes('Failed')) {
      stats.failed++;
    } else if (log.status.includes('Skipped')) {
      stats.skipped++;
    }
    
    // Count match quality
    if (log.matchQuality) {
      log.matchQuality.forEach(function(quality) {
        if (quality === 'Hallucinated') stats.hallucinated++;
        else if (quality === 'Fuzzy') stats.fuzzy++;
        else if (quality === 'Enriched') stats.enriched++;
        else if (quality === 'Exact') stats.exact++;
      });
    }
  });
  
  // Write report
  const reportData = [
    ['Slack Summary Processing Report', ''],
    ['Generated', new Date().toString()],
    ['', ''],
    ['=== MESSAGE STATISTICS ===', ''],
    ['Total Messages in Transcript', stats.total],
    ['Successfully Processed', stats.processed],
    ['Filtered as Noise', stats.filtered],
    ['Failed (API/Parse Errors)', stats.failed],
    ['Skipped (Empty/No SPOC)', stats.skipped],
    ['', ''],
    ['=== INTEL EXTRACTION ===', ''],
    ['Messages with Intel Extracted', stats.withIntel],
    ['Messages with No Intel', stats.noIntel],
    ['Total Intel Entries Extracted', stats.totalExtractions],
    ['Average Extractions per Message', stats.withIntel > 0 ? (stats.totalExtractions / stats.withIntel).toFixed(2) : 0],
    ['', ''],
    ['=== MATCH QUALITY ===', ''],
    ['Exact Matches', stats.exact],
    ['Enriched Matches', stats.enriched],
    ['Fuzzy Matches', stats.fuzzy],
    ['Hallucinated (Rejected)', stats.hallucinated],
    ['', ''],
    ['=== SPOC STATISTICS ===', ''],
    ['Original SPOCs in Transcript', originalSPOCs],
    ['SPOCs Processed', processedSPOCs],
    ['SPOCs Filtered Out', originalSPOCs - processedSPOCs]
  ];
  
  reportSheet.getRange(1, 1, reportData.length, 2).setValues(reportData);
  
  // Format the report
  reportSheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);
  reportSheet.getRange('A4:A26').setFontWeight('bold');
  reportSheet.setColumnWidth(1, 300);
  reportSheet.setColumnWidth(2, 150);
  
  console.log('Processing report generated');
}

/**
 * RETRY FAILED MESSAGES - Allows reprocessing of failed messages
 */
function retryFailedMessages() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const transcriptSheet = ss.getSheetByName('Slack Transcript');
  
  if (!transcriptSheet) {
    SpreadsheetApp.getActive().toast('Slack Transcript sheet not found.', 'Retry', 5);
    return;
  }
  
  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
  
  if (values.length < 2) {
    SpreadsheetApp.getActive().toast('No transcript rows found.', 'Retry', 5);
    return;
  }
  
  const header = values[0];
  const statusIdx = header.indexOf('Processing Status');
  const tsIdx = header.indexOf('timestamp');
  
  if (statusIdx === -1) {
    SpreadsheetApp.getActive().toast('No processing status column found. Run Build Slack Summary first.', 'Retry', 5);
    return;
  }
  
  // Find failed messages
  const failedTimestamps = [];
  for (let i = 1; i < values.length; i++) {
    const status = String(values[i][statusIdx] || '');
    const ts = String(values[i][tsIdx] || '').trim();
    
    if (status.includes('❌ Failed') && ts) {
      failedTimestamps.push(ts);
    }
  }
  
  if (failedTimestamps.length === 0) {
    SpreadsheetApp.getActive().toast('No failed messages found to retry.', 'Retry', 5);
    return;
  }
  
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Retry Failed Messages',
    `Found ${failedTimestamps.length} failed messages. Retry processing?`,
    ui.ButtonSet.YES_NO
  );
  
  if (response !== ui.Button.YES) {
    return;
  }
  
  // Clear failed status markers so they get reprocessed
  for (let i = 1; i < values.length; i++) {
    const status = String(values[i][statusIdx] || '');
    
    if (status.includes('❌ Failed')) {
      values[i][statusIdx] = ''; // Clear status to trigger reprocessing
    }
  }
  
  range.setValues(values);
  
  SpreadsheetApp.getActive().toast(
    `Cleared ${failedTimestamps.length} failed statuses. Re-running Build Slack Summary...`,
    'Retry',
    5
  );
  
  // Re-run the main processing function
  Utilities.sleep(1000);
  buildSlackSummaryFromTranscript();
}


/**
 * Normalize company/account name for fuzzy matching
 */
function normalizeCompanyName(name) {
  if (!name) return '';
  return name.toLowerCase()
    .replace(/\s*-\s*parent$/i, '')
    .replace(/\s+inc\.?$/i, '')
    .replace(/\s+ltd\.?$/i, '')
    .replace(/\s+pvt\.?$/i, '')
    .replace(/\s+llc$/i, '')
    .replace(/\s+corporation$/i, '')
    .trim();
}

/**
 * IMPROVED: Multi-stage fuzzy roster matching
 */
function findRosterMatch(entry, roster) {
  const emailLower = String(entry.email || '').toLowerCase().trim();
  const firstLower = String(entry.first_name || '').toLowerCase().trim();
  const lastLower = String(entry.last_name || '').toLowerCase().trim();
  const accountLower = normalizeCompanyName(entry.account || '');

  // STAGE 1: Exact email match
  if (emailLower) {
    const emailMatch = roster.find(function(r) {
      return r.email.toLowerCase() === emailLower;
    });
    if (emailMatch) return emailMatch;
  }

  // STAGE 2: First name + normalized company
  if (firstLower && accountLower) {
    const nameCompanyMatch = roster.find(function(r) {
      return r.first_name.toLowerCase() === firstLower && 
             normalizeCompanyName(r.account) === accountLower;
    });
    if (nameCompanyMatch) return nameCompanyMatch;
  }

  // STAGE 3: Full name match
  if (firstLower && lastLower) {
    const fullNameMatch = roster.find(function(r) {
      return r.first_name.toLowerCase() === firstLower && 
             r.last_name.toLowerCase() === lastLower;
    });
    if (fullNameMatch) return fullNameMatch;
  }

  // STAGE 4: First name + email domain
  if (firstLower && emailLower) {
    const emailDomain = emailLower.split('@')[1];
    if (emailDomain) {
      const domainMatch = roster.find(function(r) {
        const rDomain = r.email.toLowerCase().split('@')[1];
        return r.first_name.toLowerCase() === firstLower && rDomain === emailDomain;
      });
      if (domainMatch) return domainMatch;
    }
  }

  // STAGE 5: Fuzzy company substring
  if (firstLower && accountLower && accountLower.length >= 4) {
    const fuzzyMatch = roster.find(function(r) {
      const rAccountLower = normalizeCompanyName(r.account);
      return r.first_name.toLowerCase() === firstLower && 
             (rAccountLower.includes(accountLower) || accountLower.includes(rAccountLower));
    });
    if (fuzzyMatch) return fuzzyMatch;
  }

  return null;
}


/**
 * Use Gemini 2.5 Flash-Lite to map Slack messages to roster entries
 * with structured JSON schema and anti-hallucination measures.
 * Enriches missing emails from roster after Gemini processing.
 */
function buildSlackSummaryWithGemini(roster, messages) {
  const apiKey =
    PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) {
    throw new Error('GEMINI_API_KEY not set in Script Properties.');
  }

  // Build roster lookup maps for enrichment
  const rosterByEmail = {};
  const rosterByNameAccount = {};
  
  roster.forEach(function (r) {
    const emailLower = r.email.toLowerCase();
    const firstLower = r.first_name.toLowerCase();
    const accountLower = r.account.toLowerCase();
    
    if (emailLower) {
      rosterByEmail[emailLower] = r;
    }
    
    if (firstLower && accountLower) {
      const key = firstLower + '|' + accountLower;
      if (!rosterByNameAccount[key]) {
        rosterByNameAccount[key] = [];
      }
      rosterByNameAccount[key].push(r);
    }
  });

  const allRows = [];
  const seen = {};
  const batchSize = 10;

  for (let i = 0; i < messages.length; i += batchSize) {
    const batch = messages.slice(i, i + batchSize);
    const payload = buildGeminiSummaryPayload(roster, batch);

    const options = {
      method: 'post',
      contentType: 'application/json',
      muteHttpExceptions: true,
      payload: JSON.stringify(payload)
    };

    const resp = UrlFetchApp.fetch(
      GEMINI_ENDPOINT_SUMMARY + '?key=' + encodeURIComponent(apiKey),
      options
    );
    if (resp.getResponseCode() !== 200) {
      console.error(
        'Gemini summary HTTP error',
        resp.getResponseCode(),
        resp.getContentText()
      );
      continue;
    }

    let parsed;
    try {
      const root = JSON.parse(resp.getContentText());
      const candidate = root.candidates && root.candidates[0];
      if (!candidate || !candidate.content || !candidate.content.parts) {
        console.error('No candidate or parts in Gemini response');
        continue;
      }

      const jsonText = (candidate.content.parts[0].text || '').trim();
      if (!jsonText) {
        console.error('Empty JSON text from Gemini');
        continue;
      }

      parsed = JSON.parse(jsonText);
      if (!Array.isArray(parsed)) {
        console.error(
          'Gemini JSON root is not array:',
          jsonText.substring(0, 500)
        );
        continue;
      }
    } catch (e) {
      console.error('Failed to parse Gemini summary JSON', e);
      continue;
    }

    parsed.forEach(function (entry) {
      let email = String(entry.email || '').trim();
      const firstName = String(entry.first_name || '').trim();
      const lastName = String(entry.last_name || '').trim();
      const account = String(entry.account || '').trim();
      let summary = String(entry.summary || '').trim();
      const ts = String(entry.ts || '').trim();
      const spoc = String(entry.spoc || '').trim();

      if (!summary) return;

      // ENRICHMENT: Fill in missing email from roster if we have first_name + account
      if (!email && firstName && account) {
        const lookupKey = firstName.toLowerCase() + '|' + account.toLowerCase();
        const matches = rosterByNameAccount[lookupKey];
        if (matches && matches.length > 0) {
          // Take the first match's email
          email = matches[0].email;
          console.log('Enriched missing email for', firstName, account, '→', email);
        }
      }

      // Verify this entry has valid roster mapping
      const isValidMapping = roster.some(function (r) {
        return (
          (email && r.email.toLowerCase() === email.toLowerCase()) ||
          (firstName &&
            account &&
            r.first_name.toLowerCase() === firstName.toLowerCase() &&
            r.account.toLowerCase() === account.toLowerCase())
        );
      });

      if (!isValidMapping) {
        console.warn(
          'Skipping hallucinated entry - no roster match:',
          firstName,
          lastName,
          email,
          account
        );
        return;
      }

      // Ensure summary follows the standardized format
      const prefix = spoc
        ? `Intel from SPOC of the day ${spoc}: `
        : 'Intel from SPOC of the day Unknown: ';
      if (!summary.startsWith('Intel from SPOC of the day')) {
        summary = prefix + summary;
      }

      // De-duplication: skip exact (email, ts, summary) repeats
      const dedupKey = email + '|' + ts + '|' + summary;
      if (seen[dedupKey]) return;
      seen[dedupKey] = true;

      allRows.push([email, firstName, lastName, account, ts, spoc, summary]);
    });
  }

  return allRows;
}

/**
 * Build Gemini payload with structured JSON schema and anti-hallucination instructions.
 */
function buildGeminiSummaryPayload(roster, batchMessages) {
  const systemInstruction =
    'You are an expert lead intelligence analyst for BrowserStack. Your SOLE task is to extract and structure lead intelligence from Slack conversations.\n\n' +
    '## CRITICAL ANTI-HALLUCINATION RULES\n' +
    '1. ROSTER IS TRUTH: Use ONLY the attendees present in ROSTER_JSON. Never invent names, emails, or companies.\n' +
    '2. NO GUESSING: If you cannot confidently map a message to a roster entry, SKIP IT entirely.\n' +
    '3. NOISE FILTERING: Skip messages about logistics, lunch, breaks, directions, internal team chat, or anything not about lead conversations.\n' +
    '4. VERIFY EACH FIELD: Before outputting an entry, verify first_name + account OR email exists in the roster.\n\n' +
    '## CONSOLIDATED LIST DETECTION (CRITICAL)\n' +
    'If a message contains multiple leads in ONE message, extract ALL of them separately:\n' +
    '- Phrases like "Consolidated Leads:", "TCM leads:", "From the [Product] Booth:"\n' +
    '- Numbered lists (1. Name - Company 2. Name - Company ...)\n' +
    '- Multiple "Name - Company" patterns in sequence\n' +
    '- Bullet points with company/person names\n' +
    'Each person mentioned = ONE separate JSON entry. Do NOT consolidate multiple leads into one entry.\n\n' +
    '## SPOC ATTRIBUTION RULES (CRITICAL)\n' +
    'The SPOC (BrowserStack employee) and LEAD (customer) are DIFFERENT:\n' +
    '- If message says "Spoke to X from Y" → X is the LEAD, message.user is the SPOC\n' +
    '- If message contains "cc: <name>" or "shared by <name>" → That name is likely the ACTUAL SPOC\n' +
    '- For consolidated lists WITHOUT individual SPOC names → Use message.user as SPOC for all entries\n' +
    '- NEVER put the customer name in the "spoc" field\n\n' +
    '## MISSING DATA HANDLING\n' +
    '- If first_name exists but email is missing: STILL EXTRACT if company/account is known\n' +
    '- If last_name is partial/abbreviated (e.g., "K", "S", "Kumar"): Use as-is, do not invent full names\n' +
    '- If account has " - Parent" suffix: Keep it exactly\n' +
    '- Extract EVERY mention of a company/person combo, even if data is incomplete\n' +
    '- Use id_card_intel to help identify names/companies when main text is vague\n\n' +
    '## MESSAGE CLASSIFICATION\n' +
    'PROCESS a message ONLY IF it contains:\n' +
    '- Discussion about a lead\'s technical needs, pain points, or product interest\n' +
    '- Demo requests, follow-up actions, or deal intel\n' +
    '- Competitive intel or urgency signals\n' +
    '- Person name + company mention with ANY context about their needs\n\n' +
    'SKIP messages about:\n' +
    '- "Heading to lunch", "left for break", "going to booth"\n' +
    '- Internal team coordination without lead names\n' +
    '- General event logistics\n' +
    '- Chit-chat or social conversation\n\n' +
    '## STRUCTURED OUTPUT FORMAT\n' +
    'Each valid lead interaction becomes ONE entry with this EXACT structure:\n' +
    '{\n' +
    '  "email": "exact email from roster or empty string",\n' +
    '  "first_name": "exact first_name from roster",\n' +
    '  "last_name": "exact last_name from roster",\n' +
    '  "account": "exact account from roster",\n' +
    '  "ts": "timestamp from message",\n' +
    '  "spoc": "message.user (BrowserStack rep name)",\n' +
    '  "summary": "Spoke to [Representative Name] from [Account]: [intel details]"\n' +
    '}\n\n' +
    '## SUMMARY FORMAT REQUIREMENTS\n' +
    'Start every summary with ONE of these patterns:\n' +
    '- "Spoke to [First Last] from [Company]: [intel]" (when name is known)\n' +
    '- "Spoke to representative from [Company]: [intel]" (when name unknown, company-only intel)\n\n' +
    'Then include relevant intel about:\n' +
    '- Current tools/solutions in use (e.g., "Using Katalon", "Currently on Zephyr")\n' +
    '- Pain points or technical challenges (e.g., "Regression takes 8 weeks")\n' +
    '- Feature interests (e.g., "Interested in AI agents", "Needs visual testing", "Exploring LCA")\n' +
    '- Demo requests or next steps (e.g., "Wants POC", "Scheduled follow-up call")\n' +
    '- Competitive context (e.g., "Using LambdaTest", "Evaluated Sauce Labs")\n' +
    '- Urgency signals or deal timing (e.g., "License expires in March", "Actively evaluating")\n' +
    '- Team size, role, or decision-making authority (e.g., "QA Manager with 10-person team")\n\n' +
    '## MAPPING LOGIC (HIERARCHICAL)\n' +
    '1. Priority 1 (Email exact match): If email mentioned, map to exact email in roster\n' +
    '2. Priority 2 (Name + Company): Map to roster entry where first_name AND account match (ignore case, normalize company suffixes)\n' +
    '3. Priority 3 (First name + Company fuzzy): Match first_name exactly and company name as substring\n' +
    '4. Priority 4 (Company only): If only company known, create entry for representative with that account\n' +
    '5. Use id_card_intel as an additional hint for name/company identification\n\n' +
    '## CONTEXT EXTRACTION PRIORITY\n' +
    'When extracting intel, prioritize:\n' +
    '1. Product mentions (LCA, TCM, TM, Percy, A11y, Visual Testing, etc.)\n' +
    '2. Action items ("demo", "POC", "trial", "follow-up", "scheduled call")\n' +
    '3. Current state ("using X", "on Y tool", "manual testing", "no automation")\n' +
    '4. Pain points ("flaky tests", "slow regression", "lack of reporting")\n' +
    '5. Decision signals ("license expiring", "actively evaluating", "budget approved")\n\n' +
    '## SELF-VERIFICATION CHECKLIST (run before outputting each entry)\n' +
    '☑ Does this message contain actual lead intel (not noise)?\n' +
    '☑ Can I find this person/company in the roster (or can map with high confidence)?\n' +
    '☑ Does the summary start with "Spoke to..."?\n' +
    '☑ Is the intel specific and actionable?\n' +
    '☑ Am I using the EXACT name/email/company from the roster (no variations)?\n' +
    '☑ If this is a consolidated list, did I extract EACH person as a SEPARATE entry?\n' +
    '☑ Is the "spoc" field the BrowserStack employee, NOT the customer?\n\n' +
    'If ANY checkbox fails → SKIP this entry.\n\n' +
    'Output must be a JSON array. No prose, no explanations.';

  const rosterJson = JSON.stringify(roster);
  const messagesJson = JSON.stringify(
    batchMessages.map(function (m) {
      return {
        ts: m.ts,
        user: m.user,
        text: m.text,
        id_card_intel: m.id_card_intel
      };
    })
  );

  const prompt =
    systemInstruction +
    '\n\n## INPUT DATA\n\nROSTER_JSON:\n' +
    rosterJson +
    '\n\nMESSAGES_JSON (chronological):\n' +
    messagesJson +
    '\n\n## YOUR TASK\n' +
    'Analyze each message in MESSAGES_JSON.\n' +
    'For EACH valid lead interaction mentioned in a message, output ONE JSON object following the exact schema above.\n' +
    'CRITICAL: If a message contains multiple leads (consolidated list, numbered list, multiple Name-Company patterns), extract EACH as a SEPARATE entry.\n' +
    'Return a JSON array with ALL extracted leads.';

  // Structured JSON schema (Pydantic-style for Gemini)
  const jsonSchema = {
    type: 'array',
    items: {
      type: 'object',
      properties: {
        email: {
          type: 'string',
          description: 'Exact email from roster, or empty string if unknown'
        },
        first_name: {
          type: 'string',
          description: 'Exact first_name from roster'
        },
        last_name: {
          type: 'string',
          description: 'Exact last_name from roster (can be abbreviated like "K" or "S")'
        },
        account: {
          type: 'string',
          description: 'Exact account/company from roster (include " - Parent" if present)'
        },
        ts: {
          type: 'string',
          description: 'Timestamp from the source message'
        },
        spoc: {
          type: 'string',
          description: 'BrowserStack rep name (message.user) - NEVER the customer name'
        },
        summary: {
          type: 'string',
          description:
            'Must start with "Spoke to [Name] from [Company]:" or "Spoke to representative from [Company]:" followed by intel details including products, pain points, next steps, etc.'
        }
      },
      required: ['first_name', 'account', 'ts', 'spoc', 'summary']
    }
  };

  const payload = {
    contents: [
      {
        role: 'user',
        parts: [{ text: prompt }]
      }
    ],
    generationConfig: {
      temperature: 0.1, // Lower temperature for more deterministic output
      response_mime_type: 'application/json',
      response_schema: jsonSchema // Enforce structured output
    }
  };

  return payload;
}

/**
 * Intelligent post-processing to extract additional leads from
 * consolidated list messages that Gemini might have missed
 */
function extractConsolidatedLeads(messages, geminiResults, roster, spoc) {
  const additionalLeads = [];
  
  messages.forEach(function(msg) {
    const text = msg.text;
    
    // Detect consolidated list patterns
    const isConsolidatedList = 
      /consolidated\s+leads?/i.test(text) ||
      /tcm\s+leads?/i.test(text) ||
      /from\s+the\s+.+\s+booth/i.test(text) ||
      /(^|\n)\d+\.\s+\w+/m.test(text); // Numbered lists
    
    if (!isConsolidatedList) return;
    
    console.log(`📋 Detected consolidated list in message from ${spoc}`);
    
    // Count how many company names appear in the message
    const companyMentions = roster.filter(function(r) {
      const accountNorm = normalizeCompanyName(r.account);
      if (!accountNorm || accountNorm.length < 4) return false;
      return text.toLowerCase().includes(accountNorm);
    });
    
    // Count how many Gemini already extracted
    const geminiExtractedCount = geminiResults.filter(function(result) {
      return result.ts === msg.ts;
    }).length;
    
    // If Gemini extracted significantly fewer than roster mentions, flag it
    if (companyMentions.length > geminiExtractedCount + 2) {
      console.warn(
        `⚠️  Possible extraction gap: ${companyMentions.length} companies mentioned, ` +
        `only ${geminiExtractedCount} extracted by Gemini`
      );
      console.warn(`   Companies in roster mentioned: ${companyMentions.map(c => c.account).join(', ')}`);
    }
    
    // Try to extract simple "Name - Company" patterns Gemini might have missed
    const simplePattern = /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[-–]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/g;
    let match;
    
    while ((match = simplePattern.exec(text)) !== null) {
      const firstName = match[1].trim().split(' ')[0];
      const companyRaw = match[2].trim();
      const companyNorm = normalizeCompanyName(companyRaw);
      
      // Check if this was already extracted
      const alreadyExtracted = geminiResults.some(function(r) {
        return r.first_name.toLowerCase() === firstName.toLowerCase() &&
               normalizeCompanyName(r.account) === companyNorm;
      });
      
      if (alreadyExtracted) continue;
      
      // Find roster match
      const rosterMatch = roster.find(function(r) {
        return r.first_name.toLowerCase() === firstName.toLowerCase() &&
               normalizeCompanyName(r.account) === companyNorm;
      });
      
      if (rosterMatch) {
        console.log(`✅ Recovered missed lead via pattern matching: ${firstName} - ${companyRaw}`);
        additionalLeads.push({
          first_name: rosterMatch.first_name,
          last_name: rosterMatch.last_name,
          email: rosterMatch.email,
          account: rosterMatch.account,
          summary: `Intel from SPOC of the day ${spoc}: Mentioned in consolidated list`,
          ts: msg.ts,
          spoc: spoc
        });
      }
    }
  });
  
  return additionalLeads;
}


/**
 * Write or overwrite "Slack Summary" sheet.
 */
function writeSlackSummarySheet(ss, summaryRows) {
  const name = 'Slack Summary';
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
  } else {
    sheet.clear();
  }

  const header = [
    'Email',
    'First Name',
    'Last Name',
    'Account',
    'Slack Ts',
    'SPOC',
    'Summary'
  ];

  const all = [header].concat(summaryRows);
  if (all.length === 1) {
    sheet.getRange(1, 1, 1, header.length).setValues([header]);
  } else {
    sheet.getRange(1, 1, all.length, header.length).setValues(all);
  }
}

// ============================================================================
// 2. Populate Slack Intel in Collated Sheet from Slack Summary
// ============================================================================

/**
 * Populate Slack Intel in the active collated sheet from Slack Summary.
 * Includes content-based de-duplication to avoid repeated intel lines.
 */
function populateSlackIntelFromSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const collatedSheet = ss.getActiveSheet();
  const summarySheet = ss.getSheetByName('Slack Summary');
  if (!summarySheet) {
    SpreadsheetApp.getActive().toast(
      'Slack Summary sheet not found.',
      'Slack Intel',
      5
    );
    return;
  }

  const collatedRange = collatedSheet.getDataRange();
  const collatedValues = collatedRange.getValues();
  if (collatedValues.length < 2) {
    SpreadsheetApp.getActive().toast(
      'No attendee rows found.',
      'Slack Intel',
      5
    );
    return;
  }

  const header = collatedValues[0].map(function (h) {
    return String(h).trim().toLowerCase();
  });
  const colIdx = {
    email: header.indexOf('email'),
    account: header.indexOf('account'),
    slackIntel: header.indexOf('slack intel')
  };
  if (colIdx.slackIntel === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'Slack Intel' not found in active sheet.",
      'Slack Intel',
      5
    );
    return;
  }

  const emailToRows = {};
  const accountToRows = {};

  for (let i = 1; i < collatedValues.length; i++) {
    const row = collatedValues[i];
    const email =
      colIdx.email >= 0
        ? String(row[colIdx.email] || '').trim().toLowerCase()
        : '';
    const account =
      colIdx.account >= 0
        ? String(row[colIdx.account] || '').trim().toLowerCase()
        : '';

    if (email) {
      if (!emailToRows[email]) emailToRows[email] = [];
      emailToRows[email].push(i);
    }
    if (account) {
      if (!accountToRows[account]) accountToRows[account] = [];
      accountToRows[account].push(i);
    }
  }

  const sRange = summarySheet.getDataRange();
  const sValues = sRange.getValues();
  if (sValues.length < 2) {
    SpreadsheetApp.getActive().toast(
      'No rows in Slack Summary.',
      'Slack Intel',
      5
    );
    return;
  }

  const sHeader = sValues[0].map(function (h) {
    return String(h).trim().toLowerCase();
  });
  const sIdx = {
    email: sHeader.indexOf('email'),
    account: sHeader.indexOf('account'),
    summary: sHeader.indexOf('summary')
  };

  if (sIdx.summary === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'Summary' not found in Slack Summary.",
      'Slack Intel',
      5
    );
    return;
  }

  const intelByRow = {};
  const seenByRow = {};

  for (let i = 1; i < sValues.length; i++) {
    const sRow = sValues[i];
    const summary = String(sRow[sIdx.summary] || '').trim();
    if (!summary) continue;

    const email =
      sIdx.email >= 0
        ? String(sRow[sIdx.email] || '').trim().toLowerCase()
        : '';
    const account =
      sIdx.account >= 0
        ? String(sRow[sIdx.account] || '').trim().toLowerCase()
        : '';

    let targetRowIdxs = [];

    if (email && emailToRows[email]) {
      targetRowIdxs = targetRowIdxs.concat(emailToRows[email]);
    }

    if (targetRowIdxs.length === 0 && account && accountToRows[account]) {
      targetRowIdxs = targetRowIdxs.concat(accountToRows[account]);
    }

    if (targetRowIdxs.length === 0) continue;

    targetRowIdxs.forEach(function (rowIdx) {
      if (!intelByRow[rowIdx]) intelByRow[rowIdx] = [];
      if (!seenByRow[rowIdx]) seenByRow[rowIdx] = {};

      if (seenByRow[rowIdx][summary]) return;

      seenByRow[rowIdx][summary] = true;
      intelByRow[rowIdx].push(summary);
    });
  }

  let updated = 0;

  Object.keys(intelByRow).forEach(function (rowIdxStr) {
    const rowIdx = parseInt(rowIdxStr, 10);
    const summaries = intelByRow[rowIdx];
    if (!summaries || summaries.length === 0) return;

    const existing = String(
      collatedValues[rowIdx][colIdx.slackIntel] || ''
    ).trim();
    const combined = summaries.join('\n---\n');
    const newValue = existing ? existing + '\n---\n' + combined : combined;

    collatedValues[rowIdx][colIdx.slackIntel] = newValue;
    updated++;
  });

  if (updated > 0) {
    collatedRange.setValues(collatedValues);
  }

  SpreadsheetApp.getActive().toast(
    `Slack Intel updated for ${updated} attendee rows from Slack Summary (duplicates removed).`,
    'Slack Intel',
    5
  );
}

// ============================================================================
// 3. Slack User Name Resolution
// ============================================================================

function resolveSlackUserNames() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const transcriptSheet = ss.getSheetByName('Slack Transcript');
  if (!transcriptSheet) {
    SpreadsheetApp.getActive().toast(
      "Transcript sheet 'Slack Transcript' not found.",
      'Slack Users',
      5
    );
    return;
  }

  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
  if (values.length < 2) {
    SpreadsheetApp.getActive().toast(
      'No transcript rows to process.',
      'Slack Users',
      5
    );
    return;
  }

  const header = values[0];
  const userColIdx = header.indexOf('user');
  if (userColIdx === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'user' not found in Slack Transcript.",
      'Slack Users',
      5
    );
    return;
  }

  const userIdSet = {};
  for (let i = 1; i < values.length; i++) {
    const userId = String(values[i][userColIdx] || '').trim();
    if (!userId || userId.startsWith('@')) continue;
    userIdSet[userId] = true;
  }

  const userIds = Object.keys(userIdSet);
  if (userIds.length === 0) {
    SpreadsheetApp.getActive().toast(
      'No new Slack user IDs to resolve.',
      'Slack Users',
      5
    );
    return;
  }

  const props = PropertiesService.getScriptProperties();
  const token = props.getProperty('SLACK_BOT_TOKEN');

  const BATCH = 40;
  const idToName = {};

  for (let i = 0; i < userIds.length; i += BATCH) {
    const batch = userIds.slice(i, i + BATCH);
    batch.forEach(function (userId) {
      const name = fetchSlackUserNameOnce(userId, token);
      idToName[userId] = name;
    });
  }

  let usersSheet = ss.getSheetByName('Slack Users');
  if (!usersSheet) {
    usersSheet = ss.insertSheet('Slack Users');
    usersSheet.appendRow(['user_id', 'name']);
  }
  const existingRange = usersSheet.getDataRange();
  const existingValues = existingRange.getValues();
  const existingMap = {};

  if (existingValues.length > 1) {
    for (let i = 1; i < existingValues.length; i++) {
      const id = String(existingValues[i][0] || '').trim();
      const name = String(existingValues[i][1] || '').trim();
      if (id) existingMap[id] = name;
    }
  }

  const toAppend = [];
  Object.keys(idToName).forEach(function (id) {
    if (!existingMap[id]) {
      toAppend.push([id, idToName[id]]);
    }
  });
  if (toAppend.length > 0) {
    usersSheet
      .getRange(usersSheet.getLastRow() + 1, 1, toAppend.length, 2)
      .setValues(toAppend);
  }

  for (let i = 1; i < values.length; i++) {
    const userId = String(values[i][userColIdx] || '').trim();
    if (!userId || userId.startsWith('@')) continue;
    if (idToName[userId]) {
      values[i][userColIdx] = idToName[userId];
    }
  }
  range.setValues(values);

  SpreadsheetApp.getActive().toast(
    `Resolved ${userIds.length} Slack user IDs.`,
    'Slack Users',
    5
  );
}

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

function resolveSlackNamesViaScriptA() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const transcriptSheet = ss.getSheetByName('Slack Transcript');
  if (!transcriptSheet) {
    SpreadsheetApp.getActive().toast(
      "Transcript sheet 'Slack Transcript' not found.",
      'Slack Users',
      5
    );
    return;
  }

  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
  if (values.length < 2) {
    SpreadsheetApp.getActive().toast(
      'No transcript rows to process.',
      'Slack Users',
      5
    );
    return;
  }

  const header = values[0];
  const userColIdx = header.indexOf('user');
  if (userColIdx === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'user' not found in Slack Transcript.",
      'Slack Users',
      5
    );
    return;
  }

  const userIdSet = {};
  for (let i = 1; i < values.length; i++) {
    const userId = String(values[i][userColIdx] || '').trim();
    if (!userId) continue;
    userIdSet[userId] = true;
  }

  const userIds = Object.keys(userIdSet);
  if (userIds.length === 0) {
    SpreadsheetApp.getActive().toast(
      'No Slack user IDs found to resolve.',
      'Slack Users',
      5
    );
    return;
  }

  if (!SLACK_RESOLVER_WEBAPP_URL) {
    SpreadsheetApp.getActive().toast(
      'Resolver Web App URL not configured in Script B.',
      'Slack Users',
      5
    );
    return;
  }

  const payload = {
    action: 'resolveUsers',
    userIds: userIds
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    payload: JSON.stringify(payload)
  };

  let resp;
  try {
    resp = UrlFetchApp.fetch(SLACK_RESOLVER_WEBAPP_URL, options);
  } catch (e) {
    console.error('Error calling resolver Web App', e);
    SpreadsheetApp.getActive().toast(
      'Error calling resolver Web App. Check logs.',
      'Slack Users',
      5
    );
    return;
  }

  if (resp.getResponseCode() !== 200) {
    console.error(
      'Resolver HTTP error',
      resp.getResponseCode(),
      resp.getContentText()
    );
    SpreadsheetApp.getActive().toast(
      'Resolver Web App returned an error. Check logs.',
      'Slack Users',
      5
    );
    return;
  }

  let data;
  try {
    data = JSON.parse(resp.getContentText());
  } catch (e) {
    console.error('Resolver response parse error', e, resp.getContentText());
    SpreadsheetApp.getActive().toast(
      'Resolver response was not valid JSON. Check logs.',
      'Slack Users',
      5
    );
    return;
  }

  const users = Array.isArray(data.users) ? data.users : [];
  if (users.length === 0) {
    SpreadsheetApp.getActive().toast(
      'Resolver returned no users.',
      'Slack Users',
      5
    );
    return;
  }

  const idToName = {};
  users.forEach(function (u) {
    if (!u || !u.user_id) return;
    idToName[String(u.user_id).trim()] =
      String(u.name || '').trim() || u.user_id;
  });

  let usersSheet = ss.getSheetByName('Slack Users');
  if (!usersSheet) {
    usersSheet = ss.insertSheet('Slack Users');
    usersSheet.appendRow(['user_id', 'name']);
  }
  const existingRange = usersSheet.getDataRange();
  const existingValues = existingRange.getValues();
  const existingMap = {};

  if (existingValues.length > 1) {
    for (let i = 1; i < existingValues.length; i++) {
      const id = String(existingValues[i][0] || '').trim();
      const name = String(existingValues[i][1] || '').trim();
      if (id) existingMap[id] = name;
    }
  }

  const toAppend = [];
  Object.keys(idToName).forEach(function (id) {
    if (!existingMap[id]) {
      toAppend.push([id, idToName[id]]);
    }
  });
  if (toAppend.length > 0) {
    usersSheet
      .getRange(usersSheet.getLastRow() + 1, 1, toAppend.length, 2)
      .setValues(toAppend);
  }

  for (let i = 1; i < values.length; i++) {
    const userId = String(values[i][userColIdx] || '').trim();
    if (!userId) continue;
    if (idToName[userId]) {
      values[i][userColIdx] = idToName[userId];
    }
  }
  range.setValues(values);

  SpreadsheetApp.getActive().toast(
    `Resolved ${userIds.length} Slack user IDs via Script A.`,
    'Slack Users',
    5
  );
}

// ============================================================================
// 4. Batch Lead Qualification
// ============================================================================

function batchQualifyLeads() {
  const apiKey =
    PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) {
    throw new Error('GEMINI_API_KEY not set in Script Properties.');
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getActiveSheet();

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    SpreadsheetApp.getActive().toast(
      'No data rows to qualify.',
      'Lead Qualifier',
      5
    );
    return;
  }

  const dataRange = sheet.getRange(1, 1, lastRow, sheet.getLastColumn());
  const data = dataRange.getValues();

  const header = data[0].map(function (h) {
    return String(h).trim().toLowerCase();
  });

  const colIdx = {
    memberStatus: header.indexOf('member status'),
    email: header.indexOf('email'),
    title: header.indexOf('title'),
    notes: header.indexOf('slack intel'),
    question1: header.indexOf('slido intel'),
    question2: header.indexOf('linkedin')
  };

  if (colIdx.memberStatus === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'Member Status' not found.",
      'Lead Qualifier',
      5
    );
    return;
  }

  if (colIdx.email === -1) {
    SpreadsheetApp.getActive().toast(
      "Column 'Email' not found.",
      'Lead Qualifier',
      5
    );
    return;
  }

  const attendeesSheet = ss.getSheetByName('Attendance');
  const attendedEmails = {};

  if (attendeesSheet) {
    const attendeesData = attendeesSheet.getDataRange().getValues();
    if (attendeesData.length > 1) {
      const attendeesHeader = attendeesData[0].map(function (h) {
        return String(h).trim().toLowerCase();
      });
      const emailColIdx = attendeesHeader.indexOf('email');

      if (emailColIdx >= 0) {
        for (let i = 1; i < attendeesData.length; i++) {
          const email = String(attendeesData[i][emailColIdx] || '')
            .trim()
            .toLowerCase();
          if (email) {
            attendedEmails[email] = true;
          }
        }
        console.log(
          'Built attendance index with',
          Object.keys(attendedEmails).length,
          'attendees'
        );
      }
    }
  } else {
    console.warn(
      'Attendees sheet not found - all leads will be treated as Registered - No Show'
    );
  }

  const promptInstructions = DocumentApp.openById(QUAL_DOC_ID)
    .getBody()
    .getText();

  const rowsToClassify = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];

    const memberStatus = String(row[colIdx.memberStatus] || '').trim();
    if (memberStatus) continue;

    const email =
      colIdx.email >= 0
        ? String(row[colIdx.email] || '').trim().toLowerCase()
        : '';
    const title =
      colIdx.title >= 0 ? String(row[colIdx.title] || '').trim() : '';
    const slackIntel =
      colIdx.notes >= 0 ? String(row[colIdx.notes] || '').trim() : '';
    const slidoIntel =
      colIdx.question1 >= 0 ? String(row[colIdx.question1] || '').trim() : '';
    const linkedin =
      colIdx.question2 >= 0 ? String(row[colIdx.question2] || '').trim() : '';

    const attended = email && attendedEmails[email] ? true : false;

    const intelParts = [];

    intelParts.push('--- LEAD DATA ---');
    intelParts.push(
      'Attendance Status: ' + (attended ? 'ATTENDED' : 'REGISTERED - NO SHOW')
    );

    if (title) {
      intelParts.push('Title (Column E): ' + title);
    }

    if (slackIntel) {
      intelParts.push('Notes/Context (Column M): ' + slackIntel);
    }

    if (slidoIntel) {
      intelParts.push('Question/Engagement (Column N): ' + slidoIntel);
    }

    if (linkedin) {
      intelParts.push('Question/Engagement (Column O): ' + linkedin);
    }

    const combinedIntel = intelParts.join('\n');

    rowsToClassify.push({
      rowIndex: i,
      combinedIntel: combinedIntel
    });
  }

  if (rowsToClassify.length === 0) {
    SpreadsheetApp.getActive().toast(
      'No rows needing qualification (status already set).',
      'Lead Qualifier',
      5
    );
    return;
  }

  const BATCH_SIZE = 3;
  const statusesByRowIndex = {};
  let failedClassifications = 0;

  for (let i = 0; i < rowsToClassify.length; i += BATCH_SIZE) {
    const batch = rowsToClassify.slice(i, i + BATCH_SIZE);

    const batchPromptParts = [];
    batch.forEach(function (item, idx) {
      batchPromptParts.push(
        '### LEAD ' + (idx + 1) + ' ###\n' + item.combinedIntel
      );
    });

    const batchPrompt =
      promptInstructions +
      '\n\n---\n' +
      'You will now classify multiple leads in one go.\n' +
      'For each LEAD i, analyze the data provided and return ONLY the chosen status string from the allowed list, in JSON.\n\n' +
      'LEADS:\n' +
      batchPromptParts.join('\n\n') +
      '\n\n' +
      'Return a JSON array of strings, in this exact format (one element per LEAD in order):\n' +
      '["Attended and Raised Hand (Priority)", "Attended", "Registered - No Show", ...]\n' +
      'Do NOT include any explanations or extra fields.\n' +
      'IMPORTANT: The array MUST have exactly ' +
      batch.length +
      ' elements (one per LEAD).';

    const statuses = callGeminiBatchClassification(batchPrompt, apiKey);

    if (!statuses) {
      console.error(
        'Gemini returned no statuses for batch starting at index',
        i
      );
      failedClassifications += batch.length;
      continue;
    }

    if (statuses.length !== batch.length) {
      console.warn(
        'Batch classification size mismatch; expected',
        batch.length,
        'got',
        statuses.length,
        '- mapping what we can'
      );
    }

    batch.forEach(function (item, idx) {
      if (idx < statuses.length) {
        const status = String(statuses[idx] || '').trim();
        if (status) {
          statusesByRowIndex[item.rowIndex] = status;
        } else {
          statusesByRowIndex[item.rowIndex] = 'Classification Failed';
          failedClassifications++;
        }
      } else {
        statusesByRowIndex[item.rowIndex] = 'Classification Failed';
        failedClassifications++;
      }
    });

    Utilities.sleep(1500);
  }

  let updated = 0;
  Object.keys(statusesByRowIndex).forEach(function (rowIndexStr) {
    const rowIdx = parseInt(rowIndexStr, 10);
    const status = statusesByRowIndex[rowIdx];
    data[rowIdx][colIdx.memberStatus] = status;
    updated++;
  });

  if (updated > 0) {
    dataRange.setValues(data);
  }

  const successCount = updated - failedClassifications;
  SpreadsheetApp.getActive().toast(
    `Member Status updated for ${successCount} leads. ${failedClassifications} failed (marked 'Classification Failed').`,
    'Lead Qualifier',
    8
  );
}

function callGeminiBatchClassification(prompt, apiKey) {
  const url =
    'https://generativelanguage.googleapis.com/v1beta/models/' +
    QUAL_MODEL +
    ':generateContent?key=' +
    encodeURIComponent(apiKey);

  const payload = {
    contents: [
      {
        role: 'user',
        parts: [{ text: prompt }]
      }
    ],
    generationConfig: {
      temperature: 0.1,
      response_mime_type: 'application/json'
    }
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, options);
  const text = resp.getContentText();
  const json = JSON.parse(text);

  if (json.error) {
    throw new Error(json.error.message || 'Gemini error');
  }

  let raw = '';
  try {
    const candidate = json.candidates && json.candidates[0];
    if (candidate && candidate.content && candidate.content.parts) {
      raw = candidate.content.parts
        .map(function (p) {
          return p.text || '';
        })
        .join('\n')
        .trim();
    }
  } catch (e) {
    console.error('Error extracting text from Gemini response', e, text);
    throw e;
  }

  if (!raw) {
    return [];
  }

  let arr;
  try {
    arr = JSON.parse(raw);
    if (!Array.isArray(arr)) {
      console.error('Expected JSON array from Gemini, got:', raw);
      return [];
    }
  } catch (e) {
    console.error('Failed to parse Gemini batch JSON:', e, raw);
    return [];
  }

  return arr.map(function (v) {
    return String(v || '').trim();
  });
}

/**
 * Maps emails from Attendance tab to the Current Sheet based on tiered logic.
 */
function runTieredEmailMapping() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const attendanceSheet = ss.getSheetByName("Attendance");
  const targetSheet = ss.getActiveSheet(); 

  if (!attendanceSheet) {
    SpreadsheetApp.getUi().alert("Error: 'Attendance' sheet not found.");
    return;
  }

  const attendanceData = attendanceSheet.getDataRange().getValues();
  const targetData = targetSheet.getDataRange().getValues();

  // Map Attendance Data: Full Name (0), First (1), Last (2), Email (3), Company (7)
  const attendanceMap = attendanceData.slice(1).map(row => ({
    fullName: String(row[0] || "").toLowerCase().trim(),
    firstName: String(row[1] || "").toLowerCase().trim(),
    lastName: String(row[2] || "").toLowerCase().trim(),
    email: row[3] || "",
    company: String(row[7] || "").toLowerCase().trim()
  }));

  const results = [];
  const fuzzyMatchRows = []; // To track which rows need highlighting

  // Loop through target sheet rows
  for (let i = 1; i < targetData.length; i++) {
    let tRow = targetData[i];
    let tName = String(tRow[1] || "").toLowerCase().trim();
    let tCompany = String(tRow[5] || "").toLowerCase().trim();
    
    let matchedEmail = "";
    let matchType = "none";

    // Tier 1: Exact Name + Exact Company
    let match = attendanceMap.find(a => a.fullName === tName && a.company === tCompany && tCompany !== "");
    if (match) matchType = "exact";

    // Tier 2: Exact Name Only
    if (!match) {
      match = attendanceMap.find(a => a.fullName === tName);
      if (match) matchType = "exact";
    }

    // Tier 3: Abbreviation Match
    if (!match) {
      match = attendanceMap.find(a => {
        const isAbbr = (tName.includes(a.lastName) && tName.includes(a.firstName.charAt(0))) || 
                       (tName.includes(a.firstName) && tName.includes(a.lastName.charAt(0)));
        return isAbbr && (a.company === tCompany || tCompany === "");
      });
      if (match) matchType = "exact";
    }

    // Tier 4: First Name + Company
    if (!match) {
      match = attendanceMap.find(a => a.firstName === tName.split(" ")[0] && a.company === tCompany && tCompany !== "");
      if (match) matchType = "exact";
    }

    // Tier 5: Fuzzy / AI Fallback
    if (!match) {
      matchedEmail = runFuzzyMatch(tName, tCompany, attendanceMap);
      if (matchedEmail && matchedEmail !== "NEEDS REVIEW") {
        matchType = "fuzzy";
        fuzzyMatchRows.push(i + 1); // Store row number for highlighting
      } else {
        matchedEmail = "NEEDS REVIEW";
      }
    } else {
      matchedEmail = match.email;
    }
    
    results.push([matchedEmail]);
  }

  // 1. Clear old highlighting in the Email Column (Column C)
  targetSheet.getRange(2, 3, targetData.length, 1).setBackground(null);

  // 2. Write the emails
  const emailRange = targetSheet.getRange(2, 3, results.length, 1);
  emailRange.setValues(results);

  // 3. Highlight Fuzzy Matches in Yellow
  fuzzyMatchRows.forEach(rowNum => {
    targetSheet.getRange(rowNum, 3).setBackground("#FFF2CC"); // Light Yellow
  });

  // 4. Highlight "NEEDS REVIEW" in Red
  results.forEach((res, idx) => {
    if (res[0] === "NEEDS REVIEW") {
      targetSheet.getRange(idx + 2, 3).setBackground("#F4CCCC"); // Light Red
    }
  });

  SpreadsheetApp.getUi().alert("Mapping complete. Yellow cells are fuzzy matches; Red cells need manual review.");
}

/**
 * Simple Fuzzy Match logic to act as the "AI" fallback
 */
function runFuzzyMatch(targetName, targetCompany, attendanceList) {
  // If you have a Gemini API key, you could call it here. 
  // Otherwise, we use a basic similarity score.
  let bestMatch = null;
  let highestScore = 0;

  attendanceList.forEach(person => {
    let score = 0;
    if (targetName.includes(person.lastName)) score += 0.5;
    if (targetCompany && person.company.includes(targetCompany)) score += 0.4;
    
    if (score > highestScore && score > 0.6) {
      highestScore = score;
      bestMatch = person.email;
    }
  });

  return bestMatch || "MANUAL CHECK REQUIRED";
}
