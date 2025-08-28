require("dotenv").config();
const { google } = require("googleapis");
const { PubSub } = require("@google-cloud/pubsub");

const GOOGLE_DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const PUBSUB_TOPIC_NAME = process.env.PUBSUB_TOPIC_NAME;
const PROJECT_ID = process.env.PROJECT_ID;

// GOOGLE_APPLICATION_CREDENTIALS should point to the service account JSON file
// e.g. ./service-account.json (dotenv already set)

async function listAllFiles(drive, folderId) {
  const files = [];
  let pageToken = null;
  do {
    const res = await drive.files.list({
      q: `'${folderId}' in parents and trashed=false`,
      pageSize: 1000,
      fields: "nextPageToken, files(id, name, mimeType, parents)",
      pageToken,
    });
    files.push(...(res.data.files || []));
    pageToken = res.data.nextPageToken || null;
  } while (pageToken);
  return files;
}

const express = require('express');
const app = express();
const PORT = process.env.PORT || 8080;

async function pollAndPublish() {
  // Drive auth via ADC (uses GOOGLE_APPLICATION_CREDENTIALS)
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/drive.readonly"],
  });
  const drive = google.drive({ version: "v3", auth });

  // Pub/Sub client
  const pubsub = new PubSub({ projectId: PROJECT_ID });
  const topic = pubsub.topic(PUBSUB_TOPIC_NAME);

  try {
    const files = await listAllFiles(drive, GOOGLE_DRIVE_FOLDER_ID);
    if (!files.length) {
      console.log("No files found in the specified folder.");
      return;
    }

    // Publish in parallel with basic backoff
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

    await Promise.all(files.map(publishOne));
    console.log("Finished publishing file keys to Pub/Sub.");
  } catch (error) {
    console.error("Error:", error);
  }
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Manual trigger endpoint
app.post('/poll', async (req, res) => {
  try {
    await pollAndPublish();
    res.status(200).send('Polling completed');
  } catch (error) {
    console.error('Polling error:', error);
    res.status(500).send('Polling failed');
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
