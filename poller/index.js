require("dotenv").config();
const { google } = require("googleapis");
const { PubSub } = require("@google-cloud/pubsub");

const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const PUBSUB_TOPIC_NAME = process.env.PUBSUB_TOPIC_NAME;
const PROJECT_ID = process.env.PROJECT_ID;

// GOOGLE_APPLICATION_CREDENTIALS should point to the service account JSON file
// e.g. ./service-account.json (dotenv already set)

async function processFilesPageByPage(drive, folderId, pageProcessor) {
  let pageToken = null;
  let pageCount = 0;
  let totalFiles = 0;

  do {
    pageCount++;
    console.log(`Fetching page ${pageCount} of files...`);

    try {
      const res = await drive.files.list({
        q: `'${folderId}' in parents and trashed=false`,
        pageSize: 1000,
        fields: "nextPageToken, files(id, name, mimeType, parents)",
        pageToken,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });

      const pageFiles = res.data.files || [];
      console.log(`Page ${pageCount}: Found ${pageFiles.length} files`);
      
      totalFiles += pageFiles.length;
      pageToken = res.data.nextPageToken || null;

      if (pageFiles.length > 0) {
        console.log(
          `Sample files from page ${pageCount}:`,
          pageFiles.slice(0, 3).map((f) => f.name)
        );
        
        // Process this page of files immediately
        await pageProcessor(pageFiles, pageCount);
      }
    } catch (listError) {
      console.error(
        `Error listing files on page ${pageCount}:`,
        listError.message
      );
      if (listError.code === 403) {
        console.error(
          `Permission denied. Service account may not have access to folder contents.`
        );
      }
      throw listError;
    }
  } while (pageToken);

  console.log(`Total files processed across ${pageCount} pages: ${totalFiles}`);
  return totalFiles;
}

const express = require("express");
const app = express();
const PORT = process.env.PORT || 8080;

async function pollAndPublish() {
  // Drive auth via ADC with domain-wide delegation
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/drive"],
    keyFile: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    subject: process.env.IMPERSONATE_USER_EMAIL, // User to impersonate
  });
  const drive = google.drive({ version: "v3", auth });

  // Pub/Sub client
  const pubsub = new PubSub({ projectId: PROJECT_ID });
  const topic = pubsub.topic(PUBSUB_TOPIC_NAME);

  // Publish function with retry logic
  const publishOne = async (file) => {
    const data = Buffer.from(
      JSON.stringify({ fileId: file.id, fileName: file.name })
    );
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await topic.publishMessage({ data });
        console.log(`Published: ${file.name} (${file.id})`);
        return;
      } catch (e) {
        if (attempt === 3) throw e;
        const delay = 500 * attempt;
        console.warn(`Retrying ${file.name} in ${delay}ms...`, e.message);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  };

  // Page processor function - publishes all files in a page before moving to next
  const processPage = async (pageFiles, pageNumber) => {
    console.log(`Publishing ${pageFiles.length} files from page ${pageNumber}...`);
    
    // Publish all files in this page in parallel
    await Promise.all(pageFiles.map(publishOne));
    
    console.log(`Completed publishing page ${pageNumber}`);
  };

  try {
    const totalFiles = await processFilesPageByPage(drive, GOOGLE_DRIVE_FOLDER_ID, processPage);
    
    if (totalFiles === 0) {
      console.log("No files found in the specified folder.");
      return;
    }

    console.log(`Finished publishing ${totalFiles} file keys to Pub/Sub.`);
  } catch (error) {
    console.error("Error:", error);
  }
}

// Health check endpoint
app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

// Manual trigger endpoint
app.post("/poll", async (req, res) => {
  try {
    await pollAndPublish();
    res.status(200).send("Polling completed");
  } catch (error) {
    console.error("Polling error:", error);
    res.status(500).send("Polling failed");
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Poller service listening on port ${PORT}`);

  // Run initial poll
  pollAndPublish();

  // Set up periodic polling (every 5 minutes)
  setInterval(pollAndPublish, 5 * 60 * 1000);
});
