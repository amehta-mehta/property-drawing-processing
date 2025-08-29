// processFile.js
const fetch = require('node-fetch');
const pdf = require('pdf-parse');
const { google } = require('googleapis');
const { propertyMap } = require('./sheetUtils.js');

// Normalize filename for matching
function normalizeFilename(filename) {
  return filename.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function toTitleCase(str) {
  return str.toLowerCase().replace(/\b[a-z]/g, c => c.toUpperCase());
}

// Identify property by substring
function identifyPropertyFromFilename(filename) {
  const clean = normalizeFilename(filename);
  for (const [lowName, canonical] of propertyMap.entries()) {
    if (clean.includes(lowName)) return canonical;
  }
  return null;
}

// Gemini RAG API to identify property if substring match fails
async function getPropertyNameFromFilenameRAG(filename, propertyData, geminiApiKey, geminiSemaphore = null) {
  if (!propertyData || propertyData.length === 0) return null;

  let contextPrompt = "You are an assistant that identifies property names from filenames.\n";
  propertyData.forEach(prop => {
    contextPrompt += `- Name: ${prop.name}, Address: ${prop.address}\n`;
  });
  contextPrompt += `\nFilename: ${filename}\nReturn ONLY the property name from the list or UNKNOWN.`;

  const payload = {
    contents: [{ parts: [{ text: contextPrompt }] }],
    generationConfig: { temperature: 0.0 }
  };

  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${geminiApiKey}`;

  // Use semaphore to limit concurrent Gemini API calls
  let releaseFunction = null;
  if (geminiSemaphore) {
    try {
      releaseFunction = await geminiSemaphore.acquire();
      console.log(`[Property ID] Acquired Gemini semaphore, current usage: ${await geminiSemaphore.getCurrentUsage()}`);
    } catch (error) {
      console.error('[Property ID] Failed to acquire Gemini semaphore:', error.message);
      throw new Error(`Gemini rate limit acquisition failed: ${error.message}`);
    }
  }

  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    
    if (!res.ok) {
      const errorText = await res.text();
      console.error(`[Property ID] Gemini API error ${res.status}:`, errorText);
      throw new Error(`Gemini API returned ${res.status}: ${errorText}`);
    }
  } catch (error) {
    console.error('[Property ID] Gemini API request failed:', error.message);
    throw error;
  } finally {
    if (releaseFunction) {
      releaseFunction();
      console.log(`[Property ID] Released Gemini semaphore`);
    }
  }

  const data = await res.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text?.trim();
  if (!text || text.toUpperCase() === 'UNKNOWN') return null;

  const normalized = toTitleCase(text.replace(/\s+/g, ' ').trim());
  propertyMap.set(normalized.toLowerCase(), normalized);
  return normalized;
}

// Extract year from PDF using Gemini
async function extractYearWithGemini(file, drive, geminiApiKey, geminiSemaphore = null) {
  if (!file.mimeType || !file.mimeType.includes('pdf')) return null;

  try {
    // Download PDF bytes using Drive API
    const pdfRes = await drive.files.get({
      fileId: file.id,
      alt: 'media',
      supportsAllDrives: true
    });

    // Convert response to Buffer
    let buffer;
    if (Buffer.isBuffer(pdfRes.data)) {
      buffer = pdfRes.data;
    } else if (pdfRes.data?.arrayBuffer) {
      const arrayBuffer = await pdfRes.data.arrayBuffer();
      buffer = Buffer.from(arrayBuffer);
    } else if (pdfRes.data) {
      // Fallback: if data is already a string/base64
      buffer = Buffer.from(pdfRes.data);
    } else {
      throw new Error('Unable to read PDF bytes from Drive response');
    }

    const base64Data = buffer.toString('base64');

    const maxBase64Size = 35 * 1024 * 1024; // 10MB in characters
    if (base64Data.length > maxBase64Size) {
      console.log(`⚠️  PDF too large for Gemini API, skipping year extraction`);
      return 'Unknown_Year';
    }

    const prompt = `
Look at this construction/landscape drawing PDF and find the YEAR it was created.
Respond ONLY with a 4-digit year (1950-current) or UNKNOWN.
`;

    const payload = {
      contents: [
        { parts: [{ text: prompt }, { inlineData: { mimeType: 'application/pdf', data: base64Data } }] }
      ],
      generationConfig: { temperature: 0.0, topK: 1, topP: 0.1, candidateCount: 1 }
    };

    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${geminiApiKey}`;

    // Use semaphore to limit concurrent Gemini API calls
    let releaseFunction = null;
    if (geminiSemaphore) {
      try {
        releaseFunction = await geminiSemaphore.acquire();
        console.log(`[Year Extract] Acquired Gemini semaphore, current usage: ${await geminiSemaphore.getCurrentUsage()}`);
      } catch (error) {
        console.error('[Year Extract] Failed to acquire Gemini semaphore:', error.message);
        return 'Unknown_Year';
      }
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000); // 15 second timeout

    let res;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (!res.ok) {
        const errorText = await res.text();
        console.error(`[Year Extract] Gemini API error ${res.status}:`, errorText);
        return 'Unknown_Year';
      }
    } catch (error) {
      clearTimeout(timeoutId);
      console.error('[Year Extract] Gemini API request failed:', error.message);
      throw error;
    } finally {
      if (releaseFunction) {
        releaseFunction();
        console.log(`[Year Extract] Released Gemini semaphore`);
      }
    }

    const data = await res.json();
    console.log('Gemini API response received');

    let yearText = data?.candidates?.[0]?.content?.parts?.[0]?.text?.trim();
    if (!yearText) {
      console.log('No year text found in Gemini response');
      return 'Unknown_Year';
    }

    console.log(`Gemini extracted text: ${yearText}`);
    const match = yearText.match(/\b(19|20)\d{2}\b/);
    const result = match ? match[0] : 'Unknown_Year';
    console.log(`Final year result: ${result}`);
    return result;

  } catch (error) {
    if (error.name === 'AbortError') {
      console.error('Gemini API call timed out after 15 seconds');
    } else {
      console.error('Error in extractYearWithGemini:', error.message);
    }
    return 'Unknown_Year';
  }
}

// Create / get folder in Drive
async function getOrCreateFolder(drive, parentId, folderName) {
  console.log(`getOrCreateFolder called with parentId: ${parentId}, folderName: ${folderName}`);

  if (!parentId) {
    throw new Error(`parentId is null or undefined when creating folder: ${folderName}`);
  }

  const res = await drive.files.list({
    q: `'${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and name='${folderName}' and trashed=false`,
    fields: 'files(id, name)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  });
  if (res.data.files && res.data.files.length > 0) return res.data.files[0].id;

  const folder = await drive.files.create({
    requestBody: {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId]
    },
    fields: 'id',
    supportsAllDrives: true
  });
  return folder.data.id;
}

// Move file into organized folder structure
async function moveFileToOrganizedFolder(drive, fileId, propertyName, year, mainFolderId) {
  console.log(`moveFileToOrganizedFolder called with mainFolderId: ${mainFolderId}`);
  console.log(`Creating/getting property folder: ${propertyName}`);
  const propertyFolderId = await getOrCreateFolder(drive, mainFolderId, propertyName);
  console.log(`Property folder ID: ${propertyFolderId}`);

  console.log(`Creating/getting year folder: ${year}`);
  const yearFolderId = await getOrCreateFolder(drive, propertyFolderId, year);
  console.log(`Year folder ID: ${yearFolderId}`);

  // Copy file to the organized folder
  const fileRes = await drive.files.get({ fileId, fields: 'name', supportsAllDrives: true });
  const fileName = fileRes.data.name;

  const copiedFile = await drive.files.copy({
    fileId,
    requestBody: {
      name: fileName,
      parents: [yearFolderId]
    },
    supportsAllDrives: true
  });

  console.log(`Copied file ${fileId} -> ${propertyName}/${year} (new file ID: ${copiedFile.data.id})`);
}

// Main processing function
async function processFile(file, propertyData, drive, mainFolderId, geminiApiKey, geminiSemaphore = null) {
  try {
    let propertyMatch = identifyPropertyFromFilename(file.name);

    if (!propertyMatch) {
      propertyMatch = await getPropertyNameFromFilenameRAG(file.name, propertyData, geminiApiKey, geminiSemaphore);
      console.log(`🏢 Property identified by RAG: ${propertyMatch}`);
    } else {
      console.log(`🏢 Property identified normally: ${propertyMatch}`);
    }

    if (!propertyMatch) {
      propertyMatch = 'Unidentified';
    }

    const year = await extractYearWithGemini(file, drive, geminiApiKey, geminiSemaphore) || 'Unknown_Year';
    console.log(`📅 Year extracted: ${year}`);

    await moveFileToOrganizedFolder(drive, file.id, propertyMatch, year, mainFolderId);
    console.log(`📁 File moved to: ${propertyMatch}/${year}`);
  } catch (error) {
    console.error(`Error processing file ${file.name}:`, error?.message ?? error);
    throw error; // Re-throw to be handled by caller
  }
}

module.exports = {
  processFile
};
