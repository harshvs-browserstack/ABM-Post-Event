# Message-Level Tracking Implementation Guide

## Overview
This document describes the message-level tracking features to be added to `partB-testing-overall-intel.js`.

## New Features

### 1. Menu Changes
Add retry menu item to existing Slack menu:

```javascript
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  
  ui.createMenu('Slack')
    .addItem('Resolve Slack Names', 'resolveSlackNamesViaScriptA')
    .addItem('Build Slack Summary', 'buildSlackSummaryFromTranscript')
    .addItem('Populate Slack Intel', 'populateSlackIntelFromSummary')
    .addItem('Qualify Leads (Batch)', 'batchQualifyLeads')
    .addSeparator()
    .addItem('🔄 Retry Failed Messages', 'retryFailedMessages')  // NEW
    .addToUi();

  // ... rest of menu code
}
```

### 2. Message Log Tracking
Initialize messageLog object in `buildSlackSummaryFromTranscript()`:

```javascript
function buildSlackSummaryFromTranscript() {
  const messageLog = {};  // NEW: Track all messages
  
  // ... existing roster and message reading code ...
  
  // Track empty messages
  for (let i = 1; i < tValues.length; i++) {
    const text = tIdx.text >= 0 ? String(row[tIdx.text] || '').trim() : '';
    const ts = tIdx.ts >= 0 ? String(row[tIdx.ts] || '') : '';
    
    if (!text) {
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
    // ... rest of message processing
  }
}
```

### 3. Noise Filtering with Tracking
```javascript
const cleanMessages = messages.filter(function(msg) {
  const isNoise = isNoisyMessage(msg);
  if (isNoise) {
    const noiseReason = getNoiseReason(msg);  // NEW function
    messageLog[msg.ts] = {
      status: '🔇 Filtered as Noise',
      extractedCount: 0,
      summaries: [],
      warnings: [noiseReason],
      matchQuality: []
    };
  }
  return !isNoise;
});
```

### 4. New Helper Function: getNoiseReason()
```javascript
function getNoiseReason(msg) {
  const text = msg.text.toLowerCase();
  const trimmedText = msg.text.trim();
  
  const noisePatterns = [
    { pattern: 'has joined the channel', reason: 'System: User joined channel' },
    { pattern: 'flight booking', reason: 'Logistics: Flight booking' },
    { pattern: 'hotel booking', reason: 'Logistics: Hotel booking' },
    // ... more patterns
  ];

  for (let i = 0; i < noisePatterns.length; i++) {
    if (text.includes(noisePatterns[i].pattern)) {
      return noisePatterns[i].reason;
    }
  }

  if (/^(thanks?|thank you|noted)$/i.test(trimmedText)) {
    return 'Pure acknowledgment/greeting';
  }

  return 'Matched noise filter';
}
```

### 5. Enhanced buildSlackSummaryBySPOC() with Tracking

Add tracking for failed API calls:

```javascript
if (resp.getResponseCode() !== 200) {
  console.error(`SPOC ${spoc}: Gemini HTTP error ...`);
  
  // NEW: Mark all messages as failed
  spocMessages.forEach(function(msg) {
    messageLog[msg.ts] = {
      status: '❌ Failed - API Error',
      extractedCount: 0,
      summaries: [],
      warnings: [`HTTP ${httpCode}: ...`],
      matchQuality: [],
      retryable: true  // NEW: Enable retry
    };
  });
  return;
}
```

Track extraction results per timestamp:

```javascript
const extractionsByTs = {};

parsed.forEach(function (entry) {
  const ts = String(entry.ts || '').trim();
  
  if (!extractionsByTs[ts]) {
    extractionsByTs[ts] = {
      summaries: [],
      warnings: [],
      matchQuality: []
    };
  }
  
  // ... process entry ...
  
  if (wasEnriched) {
    extractionsByTs[ts].warnings.push(`✏️ Enriched: Email filled from roster`);
    extractionsByTs[ts].matchQuality.push('Enriched');
  }
  
  if (!rosterMatch) {
    extractionsByTs[ts].warnings.push(`❌ Hallucinated: ${firstName} ${lastName}`);
    extractionsByTs[ts].matchQuality.push('Hallucinated');
    return;
  }
  
  extractionsByTs[ts].summaries.push(summary);
});

// Write back to messageLog
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
```

### 6. Enhanced Roster Matching with Stage Tracking

```javascript
function findRosterMatchWithStage(entry, roster) {
  // ... existing matching logic ...
  
  // Return both match and stage number
  if (emailMatch) return { match: emailMatch, stage: 1 };
  if (nameCompanyMatch) return { match: nameCompanyMatch, stage: 2 };
  if (fullNameMatch) return { match: fullNameMatch, stage: 3 };
  if (domainMatch) return { match: domainMatch, stage: 4 };
  if (fuzzyMatch) return { match: fuzzyMatch, stage: 5 };
  
  return { match: null, stage: 0 };
}
```

### 7. Write Processing Results to Transcript

```javascript
function writeProcessingResultsToTranscript(ss, transcriptSheet, messageLog) {
  console.log('Writing processing results back to transcript...');
  
  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
  const header = values[0];
  
  // Find or create tracking columns
  let statusIdx = header.indexOf('Processing Status');
  let countIdx = header.indexOf('Extracted Count');
  let summariesIdx = header.indexOf('Extracted Summaries');
  let qualityIdx = header.indexOf('Match Quality');
  let warningsIdx = header.indexOf('Warnings/Notes');
  
  const needsNewColumns = (statusIdx === -1);
  
  if (needsNewColumns) {
    statusIdx = header.length;
    countIdx = header.length + 1;
    summariesIdx = header.length + 2;
    qualityIdx = header.length + 3;
    warningsIdx = header.length + 4;
    
    values[0].push('Processing Status', 'Extracted Count', 'Extracted Summaries', 'Match Quality', 'Warnings/Notes');
  }
  
  // Update each row
  for (let i = 1; i < values.length; i++) {
    const ts = String(values[i][tsIdx] || '').trim();
    
    if (!ts || !messageLog[ts]) {
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
  
  // Write back
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
        cell.setBackground('#F4CCCC'); // Light yellow
      } else if (status.includes('❌ Failed')) {
        cell.setBackground('#EA9999'); // Light red
      } else if (status.includes('⚠️')) {
        cell.setBackground('#FFE599'); // Light orange
      }
    }
  }
}
```

### 8. Generate Processing Report

```javascript
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
  
  const reportData = [
    ['Slack Summary Processing Report', ''],
    ['Generated', new Date().toString()],
    ['', ''],
    ['=== MESSAGE STATISTICS ===', ''],
    ['Total Messages in Transcript', stats.total],
    ['Successfully Processed', stats.processed],
    ['Filtered as Noise', stats.filtered],
    ['Failed (API/Parse Errors)', stats.failed],
    ['', ''],
    ['=== INTEL EXTRACTION ===', ''],
    ['Messages with Intel Extracted', stats.withIntel],
    ['Messages with No Intel', stats.noIntel],
    ['Total Intel Entries Extracted', stats.totalExtractions],
    ['', ''],
    ['=== MATCH QUALITY ===', ''],
    ['Exact Matches', stats.exact],
    ['Enriched Matches', stats.enriched],
    ['Fuzzy Matches', stats.fuzzy],
    ['Hallucinated (Rejected)', stats.hallucinated]
  ];
  
  reportSheet.getRange(1, 1, reportData.length, 2).setValues(reportData);
  reportSheet.getRange(1, 1).setFontWeight('bold').setFontSize(14);
}
```

### 9. Retry Failed Messages Function

```javascript
function retryFailedMessages() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const transcriptSheet = ss.getSheetByName('Slack Transcript');
  
  if (!transcriptSheet) {
    SpreadsheetApp.getActive().toast('Slack Transcript sheet not found.', 'Retry', 5);
    return;
  }
  
  const range = transcriptSheet.getDataRange();
  const values = range.getValues();
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
  
  // Clear failed status markers
  for (let i = 1; i < values.length; i++) {
    const status = String(values[i][statusIdx] || '');
    if (status.includes('❌ Failed')) {
      values[i][statusIdx] = '';
    }
  }
  
  range.setValues(values);
  
  SpreadsheetApp.getActive().toast(
    `Cleared ${failedTimestamps.length} failed statuses. Re-running Build Slack Summary...`,
    'Retry',
    5
  );
  
  Utilities.sleep(1000);
  buildSlackSummaryFromTranscript();
}
```

### 10. Integration Points in buildSlackSummaryFromTranscript()

At the end of the function, add:

```javascript
// 7) Write Slack Summary tab
writeSlackSummarySheet(ss, summaryRows);

// 8) Write processing results back to transcript sheet (NEW)
writeProcessingResultsToTranscript(ss, transcriptSheet, messageLog);

// 9) Generate processing report (NEW)
generateProcessingReport(ss, messageLog, originalSPOCs.size, Object.keys(messagesBySPOC).length);

SpreadsheetApp.getActive().toast(
  `Slack Summary built with ${summaryRows.length} rows from ${Object.keys(messagesBySPOC).length} SPOCs. Check transcript for details.`,
  'Slack Summary',
  8
);
```

## Implementation Checklist

- [ ] Update `onOpen()` menu with Retry item
- [ ] Initialize `messageLog` object in main function
- [ ] Add `getNoiseReason()` helper function
- [ ] Update noise filtering to track reasons
- [ ] Add message tracking in `buildSlackSummaryBySPOC()`
- [ ] Create `findRosterMatchWithStage()` function
- [ ] Implement `writeProcessingResultsToTranscript()`
- [ ] Implement `generateProcessingReport()`
- [ ] Implement `retryFailedMessages()`
- [ ] Add function calls at end of main function
- [ ] Test all console.log statements are preserved
- [ ] Verify conditional formatting colors

## Testing

1. Run "Build Slack Summary" - verify new columns appear in transcript
2. Check Processing Report sheet is created
3. Verify colored cells match status (green/yellow/red/orange)
4. Find a failed message, use "Retry Failed Messages"
5. Verify console logs still show all diagnostic info

## Example Output

### Slack Transcript Columns (new):
- **Processing Status**: ✅ Processed / ❌ Failed - API Error / 🔇 Filtered as Noise
- **Extracted Count**: 2
- **Extracted Summaries**: Intel from SPOC... ---\n Intel from SPOC...
- **Match Quality**: Exact, Enriched
- **Warnings/Notes**: ✏️ Enriched: Email filled from roster for John Company

### Failed Message Example:
```
Status: ❌ Failed - Parse Error
Extracted Count: 0
Summaries: (empty)
Match Quality: (empty)
Warnings: JSON parse error: Unexpected token
Retryable: true
```
