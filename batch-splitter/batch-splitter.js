// Enhanced Google Drive API Batch File Splitter with Debug Features
// Run this in VS Code with Node.js

const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");

// Configuration - UPDATE THESE VALUES
const CONFIG = {
  SOURCE_FOLDER_ID: "1Boeiqa8SwEuiV5ybvN0Jm2jXlIFRkNIz", // Batch3 folder ID
  PARENT_FOLDER_ID: "10PPk-xirzKdwmzQkSOAyWTGhNewoaYLL", // Parent folder where new batch folders will be created
  FILES_PER_BATCH: 8000,
  PROCESS_CHUNK_SIZE: 100, // Reduced for better error handling
  CREDENTIALS_FILE: "./credentials.json", // Path to your service account key
  AUDIT_LOG_FILE: "./batch_split_audit.json",
  PROGRESS_FILE: "./batch_progress.json",
  AUDIT_SPREADSHEET_ID: "1-x2eRjV9Wv0-9T907Yxxd9bVk2iwhv0hYwC677QSgpY",
};

// Global state
let drive;
let sheets;
let auditSpreadsheetId;
let currentProgress = {
  currentBatch: 1,
  filesInCurrentBatch: 0,
  totalProcessed: 0,
  batchFolders: {},
};

/**
 * Test function to verify permissions and setup
 */
async function testSetup() {
  try {
    console.log("🧪 Testing setup and permissions...\n");
    
    // Initialize APIs
    await initializeAPIs();
    
    // Test source folder access
    console.log("1️⃣ Testing source folder access...");
    const sourceFolder = await drive.files.get({
      fileId: CONFIG.SOURCE_FOLDER_ID,
      fields: "id, name, permissions",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      supportsTeamDrives: true
    });
    console.log(`✅ Source folder: ${sourceFolder.data.name} (${sourceFolder.data.id})`);
    
    // Test parent folder access
    console.log("\n2️⃣ Testing parent folder access...");
    const parentFolder = await drive.files.get({
      fileId: CONFIG.PARENT_FOLDER_ID,
      fields: "id, name, permissions",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      supportsTeamDrives: true
    });
    console.log(`✅ Parent folder: ${parentFolder.data.name} (${parentFolder.data.id})`);
    
    // Test creating a temporary folder
    console.log("\n3️⃣ Testing folder creation permissions...");
    const testFolderMetadata = {
      name: "TEST_PERMISSIONS_DELETE_ME",
      parents: [CONFIG.PARENT_FOLDER_ID],
      mimeType: "application/vnd.google-apps.folder",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      supportsTeamDrives: true
    };
    
    const testFolder = await drive.files.create({
      resource: testFolderMetadata,
      fields: "id, name",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      supportsTeamDrives: true
    });
    console.log(`✅ Created test folder: ${testFolder.data.name} (${testFolder.data.id})`);
    
    // Test getting files from source folder
    console.log("\n4️⃣ Testing file listing from source folder...");
    const filesResponse = await drive.files.list({
      q: `parents='${CONFIG.SOURCE_FOLDER_ID}' and trashed=false`,
      fields: "files(id, name, size, parents, capabilities)",
      pageSize: 5,
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
      supportsTeamDrives: true
    });
    
    const files = filesResponse.data.files || [];
    console.log(`✅ Found ${files.length} files in source folder`);
    
    if (files.length > 0) {
      console.log("   Sample files:");
      files.forEach((file, index) => {
        console.log(`   ${index + 1}. ${file.name} (${file.id}) - Size: ${file.size || 'Unknown'}`);
        if (file.capabilities) {
          console.log(`      Can edit: ${file.capabilities.canEdit || 'unknown'}`);
          console.log(`      Can move: ${file.capabilities.canMoveItemIntoTeamDrive || 'unknown'}`);
        }
      });
      
      // Test moving one file
      console.log("\n5️⃣ Testing file move operation...");
      const testFile = files[0];
      
      try {
        // Move file to test folder
        await drive.files.update({
          fileId: testFile.id,
          addParents: testFolder.data.id,
          removeParents: testFile.parents.join(","),
          fields: "id, parents",
          supportsAllDrives: true,
          includeItemsFromAllDrives: true,
          supportsTeamDrives: true
        });
        
        console.log(`✅ Successfully moved test file: ${testFile.name}`);
        
        // Move it back
        await drive.files.update({
          fileId: testFile.id,
          addParents: CONFIG.SOURCE_FOLDER_ID,
          removeParents: testFolder.data.id,
          fields: "id, parents"
        });
        
        console.log(`✅ Successfully moved test file back to source`);
        
      } catch (moveError) {
        console.error(`❌ File move test failed: ${moveError.message}`);
        console.log("This indicates a permission issue with moving files");
      }
    } else {
      console.log("⚠️ No files found in source folder to test with");
    }
    
    // Clean up test folder
    console.log("\n6️⃣ Cleaning up test folder...");
    await drive.files.delete({
      fileId: testFolder.data.id
    });
    console.log("✅ Test folder deleted");
    
    console.log("\n🎉 All tests completed successfully! You can proceed with the main script.");
    
  } catch (error) {
    console.error("❌ Test failed:", error.message);
    console.log("\n🔍 Debug information:");
    console.log("- Check if service account has proper permissions");
    console.log("- Verify folder IDs are correct");
    console.log("- Ensure both folders are shared with service account email");
    console.log("- Check if files are in a Team Drive (requires different permissions)");
    throw error;
  }
}

/**
 * Main function to start the batch splitting process
 */
async function main() {
  try {
    console.log("🚀 Starting Google Drive Batch File Splitter...\n");

    // Initialize Google APIs
    await initializeAPIs();

    // Load or create progress
    loadProgress();

    // Create batch folders
    await createBatchFolders();

    // Setup audit spreadsheet
    await setupAuditSpreadsheet();

    // Start processing files
    await processFiles();

    console.log("\n✅ Batch splitting completed successfully!");
    console.log(`📊 Total files processed: ${currentProgress.totalProcessed}`);
    console.log(
      `📋 Audit spreadsheet: https://docs.google.com/spreadsheets/d/${auditSpreadsheetId}`
    );
  } catch (error) {
    console.error("❌ Error:", error.message);
    console.error("Stack trace:", error.stack);

    // Save progress before exiting
    saveProgress();
    process.exit(1);
  }
}

/**
 * Initialize Google Drive and Sheets APIs with better error handling
 */
async function initializeAPIs() {
  try {
    console.log("🔐 Initializing Google APIs...");

    // Check if credentials file exists
    if (!fs.existsSync(CONFIG.CREDENTIALS_FILE)) {
      throw new Error(`Credentials file not found: ${CONFIG.CREDENTIALS_FILE}`);
    }

    // Load service account credentials
    const credentials = JSON.parse(
      fs.readFileSync(CONFIG.CREDENTIALS_FILE, "utf8")
    );

    // Validate credentials structure
    if (!credentials.client_email || !credentials.private_key) {
      throw new Error("Invalid credentials file - missing client_email or private_key");
    }

    console.log(`Using service account: ${credentials.client_email}`);

    // Create GoogleAuth client
    const auth = new google.auth.GoogleAuth({
      credentials: credentials,
      scopes: [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
      ],
    });

    // Get authenticated client
    const authClient = await auth.getClient();

    // Initialize APIs
    drive = google.drive({ version: "v3", auth: authClient });
    sheets = google.sheets({ version: "v4", auth: authClient });

    console.log("✅ Google APIs initialized successfully\n");
  } catch (error) {
    throw new Error(`Failed to initialize APIs: ${error.message}`);
  }
}

/**
 * Load progress from file or initialize new progress
 */
function loadProgress() {
  try {
    if (fs.existsSync(CONFIG.PROGRESS_FILE)) {
      const savedProgress = JSON.parse(
        fs.readFileSync(CONFIG.PROGRESS_FILE, "utf8")
      );
      currentProgress = { ...currentProgress, ...savedProgress };
      console.log(
        `📂 Loaded progress: Batch ${currentProgress.currentBatch}, ${currentProgress.filesInCurrentBatch} files in current batch, ${currentProgress.totalProcessed} total processed\n`
      );
    } else {
      console.log("🆕 Starting fresh batch split process\n");
    }
  } catch (error) {
    console.log("⚠️  Could not load previous progress, starting fresh\n");
  }
}

/**
 * Save current progress to file
 */
function saveProgress() {
  try {
    fs.writeFileSync(
      CONFIG.PROGRESS_FILE,
      JSON.stringify(currentProgress, null, 2)
    );
    console.log("💾 Progress saved");
  } catch (error) {
    console.error("❌ Failed to save progress:", error.message);
  }
}

/**
 * Create batch folders in Google Drive with better error handling
 */
async function createBatchFolders() {
  console.log("📁 Creating/checking batch folders for Batch3 split...");

  for (let i = 1; i <= 9; i++) {
    const folderName = `Batch${i}`;

    try {
      // Check if folder already exists
      const existingFolders = await drive.files.list({
        q: `name='${folderName}' and parents='${CONFIG.PARENT_FOLDER_ID}' and mimeType='application/vnd.google-apps.folder' and trashed=false`,
        fields: "files(id, name)",
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      let folderId;

      if (existingFolders.data.files.length > 0) {
        folderId = existingFolders.data.files[0].id;
        console.log(`  ✅ Found existing ${folderName} (${folderId})`);
      } else {
        // Create new folder
        const folderMetadata = {
          name: folderName,
          parents: [CONFIG.PARENT_FOLDER_ID],
          mimeType: "application/vnd.google-apps.folder",
        };

        const folder = await drive.files.create({
          resource: folderMetadata,
          fields: "id",
        });

        folderId = folder.data.id;
        console.log(`  ✨ Created new ${folderName} (${folderId})`);
      }

      currentProgress.batchFolders[i] = folderId;
    } catch (error) {
      throw new Error(`Failed to create/check ${folderName}: ${error.message}`);
    }
  }

  console.log("");
}

/**
 * Setup audit spreadsheet
 */
async function setupAuditSpreadsheet() {
  console.log("📊 Setting up audit spreadsheet...");

  try {
    if (CONFIG.AUDIT_SPREADSHEET_ID) {
      auditSpreadsheetId = CONFIG.AUDIT_SPREADSHEET_ID;
      console.log(`✅ Using configured audit spreadsheet (${auditSpreadsheetId})\n`);
      return;
    }

    // Check if spreadsheet already exists
    const existingSheets = await drive.files.list({
      q: `name='Batch Split Audit Log' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false`,
      fields: "files(id, name)",
      supportsAllDrives: true,
      includeItemsFromAllDrives: true,
    });

    if (existingSheets.data.files.length > 0) {
      auditSpreadsheetId = existingSheets.data.files[0].id;
      console.log(
        `  ✅ Using existing audit spreadsheet (${auditSpreadsheetId})\n`
      );
    } else {
      // Create new spreadsheet
      const spreadsheet = await sheets.spreadsheets.create({
        resource: {
          properties: {
            title: "Batch Split Audit Log",
          },
          sheets: [
            {
              properties: {
                title: "File Movement Log",
              },
            },
          ],
        },
      });

      auditSpreadsheetId = spreadsheet.data.spreadsheetId;

      // Add headers
      await sheets.spreadsheets.values.update({
        spreadsheetId: auditSpreadsheetId,
        range: "A1:I1",
        valueInputOption: "RAW",
        resource: {
          values: [
            [
              "Timestamp",
              "File ID",
              "File Name",
              "Source Folder",
              "Destination Batch",
              "Destination Folder ID",
              "File Size (bytes)",
              "Status",
              "File ID After Move",
            ],
          ],
        },
      });

      console.log(
        `  ✨ Created new audit spreadsheet (${auditSpreadsheetId})\n`
      );
    }
  } catch (error) {
    throw new Error(`Failed to setup audit spreadsheet: ${error.message}`);
  }
}

/**
 * Main file processing function with enhanced debugging
 */
async function processFiles() {
  console.log("🔄 Starting file processing...\n");

  let pageToken = null;
  let filesToProcess = [];
  let skippedCount = 0;
  let totalFilesFound = 0;

  do {
    try {
      // Get files from source folder (Batch3)
      console.log(`📥 Fetching files from source folder... (Page token: ${pageToken || 'first page'})`);
      
      const response = await drive.files.list({
        q: `parents='${CONFIG.SOURCE_FOLDER_ID}' and trashed=false`,
        fields: "nextPageToken, files(id, name, size, parents, capabilities)",
        pageSize: 1000,
        pageToken: pageToken,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });

      const files = response.data.files || [];
      totalFilesFound += files.length;
      console.log(`📥 Retrieved ${files.length} files from API (Total so far: ${totalFilesFound})`);

      if (files.length === 0) {
        console.log("⚠️ No files found in this page");
      }

      for (const file of files) {
        // Skip files we've already processed
        if (skippedCount < currentProgress.totalProcessed) {
          skippedCount++;
          if (skippedCount % 1000 === 0) {
            console.log(`⏭️ Skipped ${skippedCount} already processed files...`);
          }
          continue;
        }

        filesToProcess.push(file);

        // Process when we have enough files or reached batch limit
        if (
          filesToProcess.length >= CONFIG.PROCESS_CHUNK_SIZE ||
          (currentProgress.currentBatch <= 8 &&
            currentProgress.filesInCurrentBatch + filesToProcess.length >=
              CONFIG.FILES_PER_BATCH)
        ) {
          await processFileChunk(filesToProcess);
          filesToProcess = [];

          // Save progress periodically
          saveProgress();
        }
      }

      pageToken = response.data.nextPageToken;
    } catch (error) {
      console.error(`❌ Error retrieving files: ${error.message}`);
      console.error("Stack trace:", error.stack);
      throw error;
    }
  } while (pageToken);

  // Process any remaining files
  if (filesToProcess.length > 0) {
    console.log(`📦 Processing final ${filesToProcess.length} files...`);
    await processFileChunk(filesToProcess);
  }

  // Final save
  saveProgress();
  console.log(`\n📊 Processing complete! Total files found: ${totalFilesFound}`);
}

/**
 * Process a chunk of files with enhanced error handling
 */
async function processFileChunk(files) {
  const targetFolderId =
    currentProgress.batchFolders[currentProgress.currentBatch];
  const auditData = [];
  let successCount = 0;

  console.log(
    `📦 Processing ${files.length} files for Batch${currentProgress.currentBatch}...`
  );
  console.log(`   Target folder ID: ${targetFolderId}`);

  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    
    try {
      console.log(`   📄 Processing file ${i + 1}/${files.length}: ${file.name} (${file.id})`);
      
      // Check file capabilities if available
      if (file.capabilities && file.capabilities.canEdit === false) {
        console.log(`   ⚠️ Warning: File ${file.name} may not be editable`);
      }

      // Move file to target folder
      const moveResult = await drive.files.update({
        fileId: file.id,
        addParents: targetFolderId,
        removeParents: file.parents.join(","),
        fields: "id, parents",
        supportsAllDrives: true,
      });

      console.log(`   ✅ Successfully moved ${file.name}`);
      
      successCount++;
      currentProgress.filesInCurrentBatch++;
      currentProgress.totalProcessed++;

      // Prepare audit data
      auditData.push([
        new Date().toISOString(),
        file.id,
        file.name,
        "Batch3",
        `Batch${currentProgress.currentBatch}`,
        targetFolderId,
        file.size || 0,
        "MOVED SUCCESSFULLY",
        file.id,
      ]);
    } catch (error) {
      console.error(`   ❌ Error moving ${file.name}: ${error.message}`);
      console.error(`   📋 File details: ID=${file.id}, Parents=${file.parents?.join(',') || 'none'}`);
      
      // Log detailed error information
      if (error.code) {
        console.error(`   🔍 Error code: ${error.code}`);
      }
      if (error.errors) {
        console.error(`   🔍 Error details:`, error.errors);
      }

      auditData.push([
        new Date().toISOString(),
        file.id,
        file.name,
        "Batch3",
        `Batch${currentProgress.currentBatch}`,
        targetFolderId,
        file.size || 0,
        `ERROR: ${error.message}`,
        "N/A",
      ]);
    }

    // Add small delay between files to avoid rate limiting
    if (i < files.length - 1) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  // Write audit data to spreadsheet
  if (auditData.length > 0) {
    try {
      await sheets.spreadsheets.values.append({
        spreadsheetId: auditSpreadsheetId,
        range: "A:I",
        valueInputOption: "RAW",
        resource: {
          values: auditData,
        },
      });
      console.log(`   📝 Wrote ${auditData.length} entries to audit log`);
    } catch (error) {
      console.error("❌ Failed to write audit data:", error.message);
      // Save audit data locally as backup
      const localAuditFile = `./audit_backup_${Date.now()}.json`;
      fs.writeFileSync(localAuditFile, JSON.stringify(auditData, null, 2));
      console.log(`💾 Saved audit data to local backup: ${localAuditFile}`);
    }
  }

  console.log(
    `  ✅ Processed ${successCount}/${files.length} files successfully`
  );
  console.log(
    `  📊 Batch${currentProgress.currentBatch}: ${currentProgress.filesInCurrentBatch} files | Total: ${currentProgress.totalProcessed}`
  );

  // Check if current batch is full (only for batches 1-8)
  if (
    currentProgress.currentBatch <= 8 &&
    currentProgress.filesInCurrentBatch >= CONFIG.FILES_PER_BATCH
  ) {
    console.log(
      `  🎉 Batch${currentProgress.currentBatch} completed with ${currentProgress.filesInCurrentBatch} files!\n`
    );
    currentProgress.currentBatch++;
    currentProgress.filesInCurrentBatch = 0;

    if (currentProgress.currentBatch > 9) {
      currentProgress.currentBatch = 9;
    }
  }

  // Add delay to respect rate limits
  await new Promise((resolve) => setTimeout(resolve, 2000));
}

/**
 * Resume from a specific batch (if needed)
 */
async function resumeFromBatch(batchNumber, filesInBatch = 0) {
  currentProgress.currentBatch = batchNumber;
  currentProgress.filesInCurrentBatch = filesInBatch;

  console.log(
    `🔄 Manually resuming from Batch ${batchNumber} with ${filesInBatch} files`
  );
  await main();
}

/**
 * Check current status
 */
function checkStatus() {
  loadProgress();
  console.log("📊 CURRENT STATUS:");
  console.log(`   Current Batch: ${currentProgress.currentBatch}`);
  console.log(
    `   Files in Current Batch: ${currentProgress.filesInCurrentBatch}`
  );
  console.log(`   Total Files Processed: ${currentProgress.totalProcessed}`);
  console.log(
    `   Batch Folders Created: ${
      Object.keys(currentProgress.batchFolders).length
    }`
  );
}

// Export functions for use
module.exports = {
  main,
  testSetup,
  resumeFromBatch,
  checkStatus,
  CONFIG,
};

// Run test function if this file is executed directly with 'test' argument
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.includes('test')) {
    testSetup().catch(console.error);
  } else {
    main().catch(console.error);
  }
}