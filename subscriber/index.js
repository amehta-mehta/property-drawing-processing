// index.js (CommonJS)
require('dotenv/config');
const express = require('express');
const fs = require('fs');
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
// Removed Pub/Sub subscription - now processing directly from Drive
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const SHEET_NAME = process.env.SHEET_NAME; // ProcessedFiles
const PROPERTY_SHEET_NAME = process.env.PROPERTY_SHEET_NAME; // Properties
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;

if (!PROJECT_ID || !GOOGLE_DRIVE_FOLDER_ID || !SPREADSHEET_ID || !SHEET_NAME || !PROPERTY_SHEET_NAME || !GEMINI_API_KEY) {
  throw new Error('Missing required environment variables. Please check your .env file.');
}

let propertyData = [];
let mainDestinationFolderId = null;
let drive; // will create after obtaining auth client
let authClient; // OAuth2 client for google-spreadsheet



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

// Batch processing configuration
const BATCH_SIZE = 100; // Process files in batches of 100
const PROCESSING_DELAY = 2000; // 2 second delay between batches
let isProcessing = false;
let currentBatchIndex = 0;
let allFiles = [];

// Statistics tracking
let totalFilesProcessed = 0;
let totalFilesSkipped = 0;
let startTime = Date.now();

// Fetch all files from Google Drive folder
async function fetchAllFiles() {
  console.log('📁 Fetching all files from Google Drive folder...');
  const files = [];
  let pageToken = null;
  
  do {
    try {
      const response = await drive.files.list({
        q: `'${GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false`,
        fields: 'nextPageToken, files(id, name, mimeType, size, createdTime)',
        pageSize: 1000,
        pageToken: pageToken,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });
      
      files.push(...response.data.files);
      pageToken = response.data.nextPageToken;
      
      console.log(`📄 Fetched ${response.data.files.length} files (Total: ${files.length})`);
    } catch (error) {
      console.error('❌ Error fetching files:', error);
      throw error;
    }
  } while (pageToken);
  
  console.log(`✅ Total files found: ${files.length}`);
  return files;
}

// Process files in batches
async function processBatch() {
  if (isProcessing) return;
  
  isProcessing = true;
  
  try {
    // Fetch all files if not already done
    if (allFiles.length === 0) {
      allFiles = await fetchAllFiles();
      console.log(`🎯 Starting batch processing of ${allFiles.length} files`);
    }
    
    // Calculate batch boundaries
    const startIndex = currentBatchIndex * BATCH_SIZE;
    const endIndex = Math.min(startIndex + BATCH_SIZE, allFiles.length);
    
    if (startIndex >= allFiles.length) {
      console.log('🎉 All files have been processed!');
      if (process.env.AUTO_STOP_WHEN_COMPLETE === 'true') {
        console.log('🛑 Auto-stopping as all files are complete...');
        gracefulShutdown();
      }
      isProcessing = false;
      return;
    }
    
    const batchFiles = allFiles.slice(startIndex, endIndex);
    console.log(`\n🔄 Processing batch ${currentBatchIndex + 1}: files ${startIndex + 1}-${endIndex} of ${allFiles.length}`);
    
    // Process files in current batch concurrently
    const promises = batchFiles.map(file => processFileFromDrive(file));
    await Promise.all(promises);
    
    currentBatchIndex++;
    
    console.log(`✅ Batch ${currentBatchIndex} completed. Waiting ${PROCESSING_DELAY}ms before next batch...`);
    
    // Schedule next batch
    setTimeout(() => {
      isProcessing = false;
      processBatch();
    }, PROCESSING_DELAY);
    
  } catch (error) {
    console.error('❌ Error in batch processing:', error);
    isProcessing = false;
    
    // Retry after delay
    setTimeout(() => {
      processBatch();
    }, PROCESSING_DELAY * 2);
  }
}

// Process a single file from Drive
async function processFileFromDrive(file) {
  const releaseProcessing = await processingSemaphore.acquire();
  
  try {
    const { id: fileId, name: fileName } = file;
    
    // Skip if already processed
    if (processedFileSet.has(fileId) || processedFileNames.has(fileName.toLowerCase())) {
      console.log(`⏭️  Skipping already processed file: ${fileName}`);
      totalFilesSkipped++;
      return;
    }
    
    console.log(`🔍 Processing file: ${fileName} (${fileId})`);
    
    try {
      // Process the file
      await processFile(file, propertyData, drive, mainDestinationFolderId, GEMINI_API_KEY, geminiSemaphore);
      
      // Add to processed sets
      processedFileSet.add(fileId);
      processedFileNames.add(fileName.toLowerCase());
      
      // Add to ProcessedFiles sheet after successful processing
      try {
        await addProcessedFileToSheet(fileId, fileName);
        console.log(`📊 File added to ProcessedFiles sheet: ${fileName}`);
      } catch (sheetError) {
        console.error('⚠️  Warning: Failed to add to sheet (file still processed):', {
          error: sheetError.message,
          fileName: fileName
        });
      }
      
      console.log(`✅ File processed successfully: ${fileName}`);
      totalFilesProcessed++;
      
    } catch (processError) {
      console.error('❌ Error during file processing:', {
        fileName,
        fileId,
        error: processError.message
      });
      
      // For certain errors, mark as processed to avoid infinite retries
      const msg = String(processError?.message ?? '');
      if (msg.includes('PDF too large') || msg.includes('timeout') || msg.includes('400 Bad Request') || msg.includes('rate limit') || msg.includes('429')) {
        console.log('💀 Marking file as processed due to permanent/rate-limit error');
        processedFileSet.add(fileId);
        processedFileNames.add(fileName.toLowerCase());
        totalFilesProcessed++;
        
        try {
          await addProcessedFileToSheet(fileId, fileName + ' (ERROR: ' + msg + ')');
        } catch (sheetError) {
          console.error('⚠️  Failed to add error record to sheet:', sheetError.message);
        }
      } else {
        console.log('⚠️  Continuing with next file...');
      }
    }
  } finally {
    releaseProcessing();
  }
}

// Setup batch processing from Google Drive
function setupBatchProcessing() {
  console.log('🚀 Starting batch processing from Google Drive folder...');
  
  // Start batch processing
  processBatch();
  
  // Add graceful shutdown handling
  process.on('SIGINT', () => {
    console.log('\n🛑 Received SIGINT, shutting down gracefully...');
    gracefulShutdown();
  });
  
  process.on('SIGTERM', () => {
    console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
    gracefulShutdown();
  });
}

// Graceful shutdown function
async function gracefulShutdown() {
  console.log('📊 Final Statistics:');
  console.log(`   Total files found: ${allFiles.length}`);
  console.log(`   Current batch: ${currentBatchIndex + 1}`);
  console.log(`   Processed files count: ${processedFileSet.size}`);
  
  console.log('🔄 Waiting for current processing to complete...');
  
  // Wait for current processing to finish
  while (isProcessing) {
    await new Promise(resolve => setTimeout(resolve, 1000));
    console.log(`   Still processing batch ${currentBatchIndex + 1}...`);
  }
  
  console.log('✅ All processing completed.');
  console.log('👋 Shutdown complete.');
  process.exit(0);
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

  setupBatchProcessing();
  console.log('Batch processing is now running.');
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
      statistics: {
        totalFilesFound: allFiles.length,
        currentBatch: currentBatchIndex + 1,
        totalBatches: Math.ceil(allFiles.length / BATCH_SIZE),
        processedFilesCount: processedFileSet.size,
        progress: allFiles.length > 0 ? `${Math.round((processedFileSet.size / allFiles.length) * 100)}%` : '0%'
      },
      status: {
        isProcessing,
        currentBatchIndex
      },
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
      processingType: 'batch-drive-folder'
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
