const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const os = require("os");
const { spawn } = require("child_process");

const functions = require("@google-cloud/functions-framework");
const { google } = require("googleapis");
const { Storage } = require("@google-cloud/storage");
const ffmpegInstaller = require("@ffmpeg-installer/ffmpeg");

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
      console.log("Loaded environment variables from", dotenvPath);
    }
  } catch (err) {
    if (err.code === "MODULE_NOT_FOUND") {
      console.warn("dotenv dependency missing; skipping local env loading");
    } else {
      console.warn("Unexpected dotenv error:", err.message);
    }
  }
}

const TMP_DIR = os.tmpdir();
const FFMPEG_PATH = ffmpegInstaller.path;

let cachedServiceAccountKey = null;
let cachedStorageClient = null;

async function loadServiceAccountKey() {
  if (cachedServiceAccountKey) {
    return cachedServiceAccountKey;
  }

  if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
    console.log("Using GOOGLE_SERVICE_ACCOUNT_KEY env var.");
    const raw = process.env.GOOGLE_SERVICE_ACCOUNT_KEY.trim();
    const candidates = [];

    if (raw.includes("\\n")) {
      candidates.push(raw.replace(/\\n/g, "\n"));
    }

    candidates.push(raw);

    if (raw.includes("\n")) {
      candidates.push(raw.replace(/\n/g, "\\n"));
    }

    let parseError;
    for (const candidate of candidates) {
      try {
        cachedServiceAccountKey = JSON.parse(candidate);
        return cachedServiceAccountKey;
      } catch (err) {
        parseError = err;
      }
    }

    console.error("Invalid GOOGLE_SERVICE_ACCOUNT_KEY JSON:", parseError);
    throw parseError;
  }

  const keyFile = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_FILE;
  if (keyFile) {
    const resolvedPath = path.isAbsolute(keyFile)
      ? keyFile
      : path.resolve(process.cwd(), keyFile);
    console.log("Reading service account key from file:", resolvedPath);
    try {
      const fileContents = await fsp.readFile(resolvedPath, "utf8");
      cachedServiceAccountKey = JSON.parse(fileContents);
      return cachedServiceAccountKey;
    } catch (err) {
      console.error(
        "Failed to read or parse GOOGLE_SERVICE_ACCOUNT_KEY_FILE:",
        err
      );
      throw err;
    }
  }

  const message =
    "GOOGLE_SERVICE_ACCOUNT_KEY or GOOGLE_SERVICE_ACCOUNT_KEY_FILE must be provided";
  console.error(message);
  throw new Error(message);
}

async function getDriveClientForUser(userEmail) {
  console.log("getDriveClientForUser() called with userEmail:", userEmail);

  const key = await loadServiceAccountKey();

  console.log("Creating JWT client for SA:", key.client_email);

  const scopes = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly",
  ];
  const jwtClient = new google.auth.JWT(
    key.client_email,
    null,
    key.private_key,
    scopes,
    userEmail
  );

  console.log("Authorizing JWT client...");
  await jwtClient.authorize();
  console.log("JWT authorized successfully");

  const drive = google.drive({ version: "v3", auth: jwtClient });
  console.log("Drive client created");

  return drive;
}

async function getStorageClient() {
  if (cachedStorageClient) {
    return cachedStorageClient;
  }

  const key = await loadServiceAccountKey();
  cachedStorageClient = new Storage({
    projectId: key.project_id,
    credentials: {
      client_email: key.client_email,
      private_key: key.private_key,
    },
  });

  return cachedStorageClient;
}

async function convertToMp3(inputPath, outputPath) {
  console.log("convertToMp3() start");
  console.log("Input path:", inputPath);
  console.log("Output path:", outputPath);

  return new Promise((resolve, reject) => {
    console.log("Spawning ffmpeg:", FFMPEG_PATH);
    const ffmpeg = spawn(FFMPEG_PATH, [
      "-y",
      "-i",
      inputPath,
      "-vn",
      "-acodec",
      "libmp3lame",
      outputPath,
    ]);

    ffmpeg.stderr.on("data", (data) => {
      console.error("ffmpeg stderr:", data.toString());
    });

    ffmpeg.on("close", (code) => {
      console.log("ffmpeg exited with code:", code);
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exited with code ${code}`));
    });
  });
}

function encodeGcsObjectPath(objectName) {
  return objectName
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

function buildGcsAudioResponse(bucketName, objectName, metadata = {}) {
  const encodedPath = encodeGcsObjectPath(objectName);
  const publicUrl = `https://storage.googleapis.com/${bucketName}/${encodedPath}`;

  return {
    bucket: bucketName,
    object: objectName,
    gcsUri: `gs://${bucketName}/${objectName}`,
    publicUrl,
    size: metadata.size,
    contentType: metadata.contentType,
    mediaLink: metadata.mediaLink,
    selfLink: metadata.selfLink,
    updated: metadata.updated,
    metadata: metadata.metadata,
  };
}

function sanitizeGcsObjectName(name) {
  if (!name) {
    return `audio_${Date.now()}.mp3`;
  }

  const cleaned = name
    .trim()
    .replace(/[\\/]+/g, "_")
    .replace(/-/g, "_")
    .replace(/\s+/g, "_");

  if (!cleaned.toLowerCase().endsWith(".mp3")) {
    return `${cleaned}.mp3`;
  }

  return cleaned;
}

functions.http("processVideo", async (req, res) => {
  console.log("Received request:", {
    method: req.method,
    url: req.url,
    body: req.body,
  });

  try {
    if (req.method !== "POST") {
      console.warn("Rejected: non-POST method");
      return res.status(405).json({ error: "Only POST is allowed" });
    }

    const { fileId, userEmail } = req.body || {};
    console.log("Parsed body:", { fileId, userEmail });

    if (!fileId) {
      console.error("Missing fileId");
      return res.status(400).json({ error: "fileId is required" });
    }
    if (!userEmail) {
      console.error("Missing userEmail");
      return res.status(400).json({ error: "userEmail is required" });
    }

    const drive = await getDriveClientForUser(userEmail);

    console.log("Fetching file metadata for:", fileId);
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

    console.log("File metadata:", metaResp.data);

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

    console.log("Target Cloud Storage bucket:", bucketName);
    const storage = await getStorageClient();
    const bucket = storage.bucket(bucketName);
    const objectName = sanitizedObjectName;
    console.log("Target Cloud Storage object:", objectName);

    const gcsFile = bucket.file(objectName);
    let gcsMetadata;
    const [alreadyExists] = await gcsFile.exists();
    if (alreadyExists) {
      console.log(
        "Existing MP3 found in Cloud Storage bucket. Skipping conversion."
      );
      [gcsMetadata] = await gcsFile.getMetadata();

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

    console.log("Downloading file bytes for:", fileId);
    let downloadResp;
    try {
      downloadResp = await drive.files.get(
        { fileId, alt: "media" },
        { responseType: "arraybuffer" }
      );
    } catch (err) {
      console.error("Drive API Error (downloading file):", err);
      throw err;
    }

    console.log("Downloaded", downloadResp.data?.byteLength, "bytes");

    const videoBuffer = Buffer.from(downloadResp.data);

    const inputPath = path.join(TMP_DIR, `input-${fileId}.mp4`);
    const outputPath = path.join(TMP_DIR, `output-${fileId}.mp3`);
    console.log("Writing MP4 to:", inputPath);

    try {
      await fsp.writeFile(inputPath, videoBuffer);
      console.log("MP4 written successfully");
    } catch (err) {
      console.error("Error writing MP4 to /tmp:", err);
      throw err;
    }

    console.log("Calling convertToMp3...");
    await convertToMp3(inputPath, outputPath);
    console.log("MP3 created:", outputPath);

    console.log("Reading MP3 from:", outputPath);
    let audioBuffer;
    try {
      audioBuffer = await fsp.readFile(outputPath);
      console.log("MP3 read successfully:", audioBuffer.length, "bytes");
    } catch (err) {
      console.error("Error reading MP3:", err);
      throw err;
    }

    console.log("Uploading MP3 to Cloud Storage bucket:", bucketName);
    try {
      await gcsFile.save(audioBuffer, {
        resumable: false,
        contentType: "audio/mpeg",
        metadata: {
          metadata: {
            sourceFileId: fileId,
            sourceFileName: sourceName,
          },
        },
      });
      [gcsMetadata] = await gcsFile.getMetadata();
      console.log("Cloud Storage upload completed:", gcsMetadata?.name);
    } catch (err) {
      console.error("Cloud Storage Error (uploading MP3):", err);
      throw err;
    }

    console.log("Cleaning up temp files in:", TMP_DIR);

    try {
      await fsp.unlink(inputPath);
      await fsp.unlink(outputPath);
      console.log("Cleanup complete");
    } catch (e) {
      console.warn("Cleanup error:", e.message);
    }

    console.log("Returning success response");
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
  }
});
