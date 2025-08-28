// index.js (CommonJS)
require('dotenv/config');
const express = require('express');
const fs = require('fs');
const { PubSub } = require('@google-cloud/pubsub');
const { google } = require('googleapis');
const { GoogleSpreadsheet } = require('google-spreadsheet');
const { processFile } = require('./processFile.js');
const { getOrCreateDestinationFolder, propertyMap, getPropertyData } = require('./sheetUtils.js');
const credentials = require('./service-account.json');
const { LocalSemaphore } = require('./distributedSemaphore');

const MAX_GEMINI_CONCURRENT = parseInt(process.env.MAX_GEMINI_CONCURRENT) || 5;
const MAX_CONCURRENT_PROCESSING = parseInt(process.env.MAX_CONCURRENT_PROCESSING) || 5;

// Initialize local Gemini semaphore for rate limiting
const geminiSemaphore = new LocalSemaphore('gemini-api', MAX_GEMINI_CONCURRENT);

// Initialize local processing semaphore (separate from Gemini semaphore)
const processingSemaphore = new LocalSemaphore('processing', MAX_CONCURRENT_PROCESSING);

const app = express();
const PORT = process.env.PORT || 8081;

const PROJECT_ID = process.env.PROJECT_ID;
const SUBSCRIPTION_NAME = process.env.PUBSUB_SUBSCRIPTION;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME; // ProcessedFiles
const PROPERTY_SHEET_NAME = process.env.PROPERTY_SHEET_NAME; // Properties
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

if (!PROJECT_ID || !SUBSCRIPTION_NAME || !SPREADSHEET_ID || !SHEET_NAME || !PROPERTY_SHEET_NAME) {
  throw new Error("Missing required environment variables. Ensure PROJECT_ID, PUBSUB_SUBSCRIPTION, SPREADSHEET_ID, SHEET_NAME, and PROPERTY_SHEET_NAME are set.");
}

let propertyData = [];
let mainDestinationFolderId = null;
let drive; // will create after obtaining auth client
let authClient; // OAuth2 client for google-spreadsheet

// Initialize Pub/Sub client and subscription
const pubsub = new PubSub({ projectId: PROJECT_ID, keyFilename: './service-account.json' });
const subscription = pubsub.subscription(SUBSCRIPTION_NAME);

// Cache of processed files
const processedFileSet = new Set();
const processedFileNames = new Set();

// Function to add processed file to sheet
async function addProcessedFileToSheet(fileId, fileName) {
  try {
    const doc = new GoogleSpreadsheet(SPREADSHEET_ID);
    if (!authClient) throw new Error('Auth client not initialized for Google Sheets');
    await doc.useOAuth2Client(authClient);
    await doc.loadInfo();

    const sheet = doc.sheetsByTitle[SHEET_NAME];
    if (!sheet) throw new Error(`Sheet "${SHEET_NAME}" not found`);

    await sheet.addRow({
      'File ID': fileId,
      'File Name': fileName,
      'Processed Date': new Date().toISOString()
    });
    console.log(`Added processed file to sheet: ${fileName} (${fileId})`);
  } catch (error) {
    console.error('Error adding file to ProcessedFiles sheet:', error?.message ?? error);
  }
}

// Load existing files from Google Sheet
async function loadProcessedFiles() {
  const doc = new GoogleSpreadsheet(SPREADSHEET_ID);
  if (!authClient) throw new Error('Auth client not initialized for Google Sheets');
  await doc.useOAuth2Client(authClient);
  await doc.loadInfo();

  const sheet = doc.sheetsByTitle[SHEET_NAME];
  if (!sheet) throw new Error(`Sheet "${SHEET_NAME}" not found`);

  const rows = await sheet.getRows();
  for (const row of rows) {
    // google-spreadsheet rows may expose values as properties or via row.get
    const fileId = typeof row.get === 'function' ? row.get('File ID') : row['File ID'] ?? row.fileId;
    const fileName = typeof row.get === 'function' ? row.get('File Name') : row['File Name'] ?? row.fileName;
    if (fileId) processedFileSet.add(fileId);
    if (fileName) processedFileNames.add(String(fileName).toLowerCase());
  }
  console.log(`Loaded ${rows.length} rows: ${processedFileSet.size} file IDs and ${processedFileNames.size} file names from sheet.`);
}

// Queue for sequential message processing
let messageQueue = [];
let isProcessing = false;

// Process messages with parallel processing and concurrency control
async function processMessageQueue() {
  if (isProcessing || messageQueue.length === 0) return;
  
  isProcessing = true;
  const messagesToProcess = messageQueue.splice(0); // Take all messages
  
  console.log(`🚀 Processing ${messagesToProcess.length} messages in parallel (max concurrent: ${MAX_CONCURRENT_PROCESSING})`);
  console.log(`📊 Current Gemini semaphore usage: ${await geminiSemaphore.getCurrentUsage()}`);
  
  // Process messages concurrently with semaphore control
  const processingPromises = messagesToProcess.map(async (message, index) => {
    const releaseProcessing = await processingSemaphore.acquire();
    try {
      console.log(`📝 [${index + 1}/${messagesToProcess.length}] Starting message processing`);
      await processMessage(message);
      console.log(`✅ [${index + 1}/${messagesToProcess.length}] Message processed successfully`);
    } catch (error) {
      console.error(`❌ [${index + 1}/${messagesToProcess.length}] Error processing message:`, {
        error: error.message,
        stack: error.stack,
        messageData: message.data ? message.data.toString() : 'no data'
      });
      // Don't throw - we want to continue processing other messages
    } finally {
      releaseProcessing();
    }
  });
  
  await Promise.all(processingPromises);
  
  console.log(`🏁 Completed processing batch of ${messagesToProcess.length} messages`);
  isProcessing = false;
  
  // Process any new messages that arrived while we were processing
  if (messageQueue.length > 0) {
    setImmediate(() => processMessageQueue());
  }
}

// Process a single message
async function processMessage(message) {
  let payload = null;
  let fileId = null;
  let fileName = null;
  
  try {
    console.log('\n=== PROCESSING MESSAGE ===');
    
    try {
      payload = JSON.parse(Buffer.from(message.data, 'base64').toString('utf8'));
      fileId = payload.fileId;
      fileName = payload.fileName?.toLowerCase();
      
      console.log(`🔄 Processing file ID: ${fileId}, fileName: ${fileName}`);
    } catch (parseError) {
      console.error('❌ Error parsing message payload:', {
        error: parseError.message,
        rawData: message.data ? message.data.toString() : 'no data'
      });
      message.ack(); // Ack malformed messages
      return;
    }

    if (!fileId || !fileName) {
      console.log('⏭️  Missing fileId or fileName, skipping message');
      message.ack();
      return;
    }

    // Skip if already processed
    if (processedFileSet.has(fileId) || processedFileNames.has(fileName)) {
      console.log(`⏭️  Skipping already processed file: ${fileName}`);
      message.ack();
      return;
    }

    // Fetch metadata from Drive
    let file;
    try {
      const res = await drive.files.get({
        fileId,
        fields: 'id, name, mimeType, parents',
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });
      file = res.data;
      console.log(`🔍 Found file in Drive: ${file.name} (ID: ${file.id})`);
    } catch (driveError) {
      console.error('❌ Error fetching file from Drive:', {
        error: driveError.message,
        fileId: fileId,
        fileName: fileName,
        stack: driveError.stack
      });
      
      // Check if it's a not found error (permanent) vs temporary error
      if (driveError.message.includes('not found') || driveError.message.includes('404')) {
        console.log('💀 File not found in Drive, acknowledging message');
        message.ack();
      } else {
        console.log('🔄 Temporary Drive error, will retry');
        message.nack();
      }
      return;
    }

    try {
      console.log(`🚀 Starting file processing for: ${file.name}`);
      await processFile(file, propertyData, drive, mainDestinationFolderId, GEMINI_API_KEY, geminiSemaphore);

      // Add to processed set to avoid duplicates
      processedFileSet.add(file.id);
      processedFileNames.add(file.name.toLowerCase());

      // Add to ProcessedFiles sheet after successful processing and copying
      try {
        await addProcessedFileToSheet(file.id, file.name);
        console.log(`📊 File added to ProcessedFiles sheet: ${file.name}`);
      } catch (sheetError) {
        console.error('⚠️  Warning: Failed to add to sheet (file still processed):', {
          error: sheetError.message,
          fileName: file.name
        });
      }

      console.log(`✅ File processed successfully: ${file.name}\n`);
      message.ack();
    } catch (processError) {
      console.error('❌ Error during file processing:', {
        error: processError.message,
        stack: processError.stack,
        fileName: file.name,
        fileId: file.id,
        errorType: processError.constructor.name
      });

      // For certain errors, mark as processed to avoid infinite retries
      const msg = String(processError?.message ?? '');
      if (msg.includes('PDF too large') || msg.includes('timeout') || msg.includes('400 Bad Request') || msg.includes('rate limit') || msg.includes('429')) {
        console.log('💀 Marking file as processed due to permanent/rate-limit error');
        processedFileSet.add(file.id);
        processedFileNames.add(file.name.toLowerCase());
        
        try {
          await addProcessedFileToSheet(file.id, file.name + ' (ERROR: ' + msg + ')');
        } catch (sheetError) {
          console.error('⚠️  Failed to add error record to sheet:', sheetError.message);
        }
        
        message.ack();
      } else {
        console.log('🔄 Retryable error, will nack message');
        message.nack();
      }
    }
  } catch (error) {
    console.error('❌ Unexpected error processing message:', {
      error: error.message,
      stack: error.stack,
      fileId: fileId,
      fileName: fileName,
      payload: payload
    });
    message.nack();
  }
}

// Handle incoming messages by adding them to queue
function setupSubscription() {
  subscription.on('message', (message) => {
    // console.log('\n=== NEW MESSAGE RECEIVED - ADDING TO QUEUE ===');
    messageQueue.push(message);
    processMessageQueue(); // Start processing if not already running
  });

  subscription.on('error', (err) => console.error('Subscription error:', err));
}

async function initializeApp() {
  console.log('Starting application initialization...');

  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive.file'
    ],
    credentials
  });

  authClient = await auth.getClient();
  drive = google.drive({ version: 'v3', auth: authClient });

  await loadProcessedFiles();

  propertyData = await getPropertyData(SPREADSHEET_ID, PROPERTY_SHEET_NAME, credentials)
    .catch(err => {
      console.error('Failed to load property data:', err);
      return [];
    });

  console.log('Creating/getting main destination folder...');
  mainDestinationFolderId = await getOrCreateDestinationFolder(drive);
  console.log(`Main destination folder ID: ${mainDestinationFolderId}`);

  console.log('Setup complete. Starting subscriber...');

  setupSubscription();
  console.log('Subscriber is now listening for messages.');
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Status endpoint
app.get('/status', async (req, res) => {
  try {
    res.json({
      status: 'healthy',
      processedFiles: processedFileSet.size,
      processedFileNames: processedFileNames.size,
      queueLength: messageQueue.length,
      isProcessing: isProcessing,
      concurrency: {
        maxConcurrentProcessing: MAX_CONCURRENT_PROCESSING,
        maxGeminiConcurrent: MAX_GEMINI_CONCURRENT
      },
      geminiSemaphore: {
        limit: MAX_GEMINI_CONCURRENT,
        available: await geminiSemaphore.getAvailablePermits(),
        currentUsage: await geminiSemaphore.getCurrentUsage(),
        type: 'local'
      },
      processingType: 'local-parallel'
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      error: error.message
    });
  }
});

// Start the server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Subscriber service listening on port ${PORT}`);

  // Initialize app after server starts with error handling
  initializeApp().catch(error => {
    console.error('Failed to initialize app:', error);
    // Do NOT exit; keep server alive so Cloud Run health checks can fail gracefully
  });
});
