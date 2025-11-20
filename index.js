const path = require("path");

const functions = require("@google-cloud/functions-framework");

const { logVerbose, startDevMemoryLogger } = require("./lib/logging");
const {
  sanitizeGcsObjectName,
  buildGcsAudioResponse,
} = require("./lib/storageHelpers");
const {
  getDriveClientForUser,
  getStorageClient,
} = require("./lib/googleClients");
const { convertStreamAndUpload } = require("./lib/mediaPipeline");
const {
  acquireJobSlotWithMemoryGuard,
  releaseJobSlot,
  getMemoryUsageMb,
  hasAvailableJobSlot,
} = require("./lib/jobQueue");

const isRunningInCloud = Boolean(
  process.env.K_SERVICE || process.env.FUNCTION_TARGET
);

if (!isRunningInCloud) {
  try {
    const dotenvPath = path.resolve(__dirname, ".env");
    const result = require("dotenv").config({ path: dotenvPath });

    if (result.error && result.error.code !== "ENOENT") {
      console.warn("dotenv failed to load:", result.error.message);
    } else if (!result.error) {
      logVerbose("Loaded environment variables from", dotenvPath);
    }
  } catch (err) {
    if (err.code === "MODULE_NOT_FOUND") {
      console.warn("dotenv dependency missing; skipping local env loading");
    } else {
      console.warn("Unexpected dotenv error:", err.message);
    }
  }

  startDevMemoryLogger();
}

functions.http("processVideo", async (req, res) => {
  logVerbose("Received request:", {
    method: req.method,
    url: req.url,
    body: req.body,
  });

  console.info("[processVideo] Request received");
  let jobAcquired = false;

  try {
    if (req.method !== "POST") {
      console.warn("Rejected: non-POST method");
      return res.status(405).json({ error: "Only POST is allowed" });
    }

    const { fileId, userEmail } = req.body || {};
    logVerbose("Parsed body:", { fileId, userEmail });

    if (!fileId) {
      console.error("Missing fileId");
      return res.status(400).json({ error: "fileId is required" });
    }
    if (!userEmail) {
      console.error("Missing userEmail");
      return res.status(400).json({ error: "userEmail is required" });
    }

    console.info(
      `[processVideo] Validated request for file ${fileId} by ${userEmail}`
    );

    if (!hasAvailableJobSlot()) {
      console.warn(
        "[processVideo] Rejecting request: job slots full (max concurrent reached)"
      );
      return res.status(503).json({
        error: "Server is busy. Please retry shortly.",
        details: "Job slot unavailable",
      });
    }

    try {
      await acquireJobSlotWithMemoryGuard();
      jobAcquired = true;
    } catch (err) {
      console.error("Unable to acquire job slot:", err);
      return res.status(503).json({
        error: "Server is busy. Please retry shortly.",
        details: err.message,
      });
    }

    const drive = await getDriveClientForUser(userEmail);

    logVerbose("Fetching file metadata for:", fileId);
    let metaResp;
    try {
      metaResp = await drive.files.get({
        fileId,
        fields: "id,name,parents,mimeType",
      });
    } catch (err) {
      console.error("Drive API Error (getting metadata):", err);
      throw err;
    }

    console.info("[processVideo] Retrieved Drive metadata");
    logVerbose("File metadata:", metaResp.data);

    const sourceName = metaResp.data.name || fileId;
    const sourceParents = metaResp.data.parents || [];

    const mp3Name = sourceName.endsWith(".mp4")
      ? sourceName.replace(/\.mp4$/i, ".mp3")
      : `${sourceName}.mp3`;
    const sanitizedObjectName = sanitizeGcsObjectName(mp3Name);

    const bucketName = process.env.GCS_AUDIO_BUCKET;
    if (!bucketName) {
      throw new Error("GCS_AUDIO_BUCKET environment variable is required");
    }

    logVerbose("Target Cloud Storage bucket:", bucketName);
    const storage = await getStorageClient();
    const bucket = storage.bucket(bucketName);
    const objectName = sanitizedObjectName;
    logVerbose("Target Cloud Storage object:", objectName);

    const gcsFile = bucket.file(objectName);
    let gcsMetadata;
    const [alreadyExists] = await gcsFile.exists();
    if (alreadyExists) {
      [gcsMetadata] = await gcsFile.getMetadata();
      console.info("[processVideo] Existing MP3 found, skipping conversion");
      logVerbose(
        "Existing MP3 found in Cloud Storage bucket. Skipping conversion."
      );

      return res.status(200).json({
        status: "ok",
        actingUser: userEmail,
        originalFile: {
          id: fileId,
          name: sourceName,
          parents: sourceParents,
        },
        audioFile: buildGcsAudioResponse(bucketName, objectName, gcsMetadata),
        reused: true,
      });
    }

    console.info("[processVideo] Starting download and conversion pipeline");
    logVerbose("Downloading file bytes for:", fileId);
    let downloadResp;
    try {
      downloadResp = await drive.files.get(
        { fileId, alt: "media" },
        { responseType: "stream" }
      );
    } catch (err) {
      console.error("Drive API Error (downloading file):", err);
      throw err;
    }

    logVerbose("Starting streaming transcode + upload pipeline");
    try {
      await convertStreamAndUpload(downloadResp.data, gcsFile, {
        metadata: {
          sourceFileId: fileId,
          sourceFileName: sourceName,
        },
      });
      [gcsMetadata] = await gcsFile.getMetadata();
      logVerbose("Cloud Storage upload completed:", gcsMetadata?.name);
    } catch (err) {
      console.error("Cloud Storage Error (uploading MP3):", err);
      throw err;
    }

    console.info("[processVideo] Conversion complete, responding to client");
    logVerbose(
      "Returning success response. RSS after job:",
      `${getMemoryUsageMb().toFixed(1)}MB`
    );
    return res.status(200).json({
      status: "ok",
      actingUser: userEmail,
      originalFile: {
        id: fileId,
        name: sourceName,
        parents: sourceParents,
      },
      audioFile: buildGcsAudioResponse(bucketName, objectName, gcsMetadata),
    });
  } catch (err) {
    console.error("FATAL ERROR in processVideo():", err);
    return res.status(500).json({
      error: "Internal server error",
      details: err.message,
    });
  } finally {
    if (jobAcquired) {
      releaseJobSlot();
    }
  }
});
