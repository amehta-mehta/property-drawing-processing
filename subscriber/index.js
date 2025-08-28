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

// Process messages sequentially
async function processMessageQueue() {
  if (isProcessing || messageQueue.length === 0) return;
  
  isProcessing = true;
  console.log(`📋 Starting to process ${messageQueue.length} messages in queue`);
  
  while (messageQueue.length > 0) {
    const message = messageQueue.shift();
    try {
      await processMessage(message);
    } catch (error) {
      console.error('Error in processMessage, continuing with next message:', error?.message ?? error);
      // Still ack the message to prevent infinite retries
      message.ack();
    }
  }
  
  console.log('✅ Finished processing all messages in queue');
  isProcessing = false;
}

// Process a single message
async function processMessage(message) {
  try {
    console.log('\n=== PROCESSING MESSAGE ===');
      const payload = JSON.parse(Buffer.from(message.data, 'base64').toString('utf8'));
      const fileId = payload.fileId;
      const fileName = payload.fileName?.toLowerCase();

      console.log(`🔄 Processing file ID: ${fileId}`);

      if (!fileId || !fileName) {
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
      const res = await drive.files.get({
        fileId,
        fields: 'id, name, mimeType, parents',
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });
      const file = res.data;

      try {
         await processFile(file, propertyData, drive, mainDestinationFolderId, GEMINI_API_KEY);

         // Add to processed set to avoid duplicates
         processedFileSet.add(file.id);
         processedFileNames.add(file.name.toLowerCase());

         // Add to ProcessedFiles sheet after successful processing and copying
         await addProcessedFileToSheet(file.id, file.name);
         console.log(`📊 File added to ProcessedFiles sheet\n`);

         message.ack();
      } catch (processError) {
        console.error('Error during file processing:', processError?.message ?? processError);

        // For certain errors, mark as processed to avoid infinite retries
        const msg = String(processError?.message ?? '');
        if (msg.includes('PDF too large') || msg.includes('timeout') || msg.includes('400 Bad Request')) {
          console.log('Marking file as processed due to permanent error');
          processedFileSet.add(file.id);
          processedFileNames.add(file.name.toLowerCase());
          await addProcessedFileToSheet(file.id, file.name + ' (ERROR: ' + msg + ')');
          message.ack();
        } else {
          // For other errors, nack to retry
          message.nack();
        }
      }
    } catch (error) {
      console.error('Error processing message:', error);
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

  // 1) Create Google auth client using service account credentials
  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive.file'
    ],
    credentials
  });

  // Obtain a client which can be used with googleapis and google-spreadsheet
  authClient = await auth.getClient();
  drive = google.drive({ version: 'v3', auth: authClient });

  // 2) Load existing processed files from sheet
  await loadProcessedFiles();

  // 3) Load property data from properties sheet (using sheetUtils.getPropertyData)
  propertyData = await getPropertyData(SPREADSHEET_ID, PROPERTY_SHEET_NAME, credentials)
    .catch(err => {
      console.error('Failed to load property data:', err);
      return [];
    });

  // 3) Get or create main destination folder
  console.log('Creating/getting main destination folder...');
  mainDestinationFolderId = await getOrCreateDestinationFolder(drive);
  console.log(`Main destination folder ID: ${mainDestinationFolderId}`);

  console.log('Setup complete. Starting subscriber...');

  // Start subscription only after initialization is complete
  setupSubscription();
  console.log('Subscriber is now listening for messages.');
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Status endpoint
app.get('/status', (req, res) => {
  res.json({
    status: 'running',
    processedFiles: processedFileSet.size,
    mainDestinationFolderId
  });
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
