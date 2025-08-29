// sheetUtils.js (CommonJS)

const { GoogleSpreadsheet } = require('google-spreadsheet');

/**
 * Get or create the main destination folder in Drive
 * @param {google.drive_v3.Drive} drive - Google Drive client
 * @param {string} folderName - Name of the folder
 * @returns {Promise<string>} - Folder ID
 */
async function getOrCreateDestinationFolder(drive, folderName = 'Ordered Property Drawings') {
  // Check if folder exists
  const res = await drive.files.list({
    q: `name='${folderName}' and mimeType='application/vnd.google-apps.folder' and trashed=false`,
    fields: 'files(id, name)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  });

  if (res.data.files && res.data.files.length > 0) {
    console.log(`Destination folder exists: ${folderName}`);
    return res.data.files[0].id;
  }

  // Create folder
  const folder = await drive.files.create({
    requestBody: {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder'
    },
    fields: 'id',
    supportsAllDrives: true
  });

  console.log(`Created destination folder: ${folderName}`);
  return folder.data.id;
}

const propertyMap = new Map();

/**
 * Helper to safely read a header value from a row returned by google-spreadsheet
 * Supports both row.get('Header') and plain property access (row['Header'] or row.Header)
 */
function readRowField(row, headerName) {
  if (!row) return undefined;
  if (typeof row.get === 'function') {
    try {
      return row.get(headerName);
    } catch (e) {
      // fallthrough to other access methods
    }
  }
  return row[headerName] ?? row[headerName.toLowerCase()] ?? row[headerName.replace(/\s+/g, '')] ?? row[headerName.replace(/\s+/g, '').toLowerCase()];
}

/**
 * Fetch property data from Google Sheet
 * @param {string} spreadsheetId - Google Sheet ID
 * @param {string} sheetName - Sheet name
 * @param {object} creds - Service account credentials JSON (client_email, private_key)
 * @returns {Promise<Array>} - Array of properties [{name, address}]
 */
async function getPropertyData(spreadsheetId, sheetName, creds) {
  if (!spreadsheetId) throw new Error('spreadsheetId is required');
  if (!sheetName) throw new Error('sheetName is required');
  if (!creds || !creds.client_email || !creds.private_key) {
    throw new Error('Service account credentials (client_email and private_key) are required');
  }

  try {
    console.log('📊 Loading property data from sheet...');
    const doc = new GoogleSpreadsheet(spreadsheetId);

    // Authenticate with service account credentials
    await doc.useServiceAccountAuth({
      client_email: creds.client_email,
      private_key: creds.private_key,
    });

    await doc.loadInfo();

    const sheet = doc.sheetsByTitle[sheetName];
    if (!sheet) throw new Error(`Sheet "${sheetName}" not found`);

    const rows = await sheet.getRows();
    console.log(`✅ Loaded ${rows.length} properties from sheet`);

    // Return array of { name, address } and filter out incomplete rows
    const properties = rows
      .map(row => {
        const name = readRowField(row, 'Name');
        const address = readRowField(row, 'Address');
        return { name, address };
      })
      .filter(p => p.name && p.address);

    return properties;
  } catch (error) {
    console.error('❌ Error loading property data from sheet:', error?.message ?? error);
    throw error;
  }
}

module.exports = {
  getOrCreateDestinationFolder,
  propertyMap,
  getPropertyData
};
