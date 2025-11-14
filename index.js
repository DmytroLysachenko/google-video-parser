// index.js (with full console logging)
const functions = require("@google-cloud/functions-framework");
const { google } = require("googleapis");
const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
const { spawn } = require("child_process");
const { Readable } = require("stream");
const ffmpegInstaller = require("@ffmpeg-installer/ffmpeg");
const os = require("os");

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

const TMP_DIR = os.tmpdir(); // This works on Windows and Linux
console.log("Using temp dir:", TMP_DIR);

// Path to bundled ffmpeg binary (@ffmpeg-installer/ffmpeg)
const FFMPEG_PATH = ffmpegInstaller.path;
console.log("FFMPEG binary path:", FFMPEG_PATH);

// --- AUTH BUILDING ---------------------------------------------------------

async function loadServiceAccountKey() {
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
        return JSON.parse(candidate);
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
      return JSON.parse(fileContents);
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

  const scopes = ["https://www.googleapis.com/auth/drive.readonly"];
  const jwtClient = new google.auth.JWT(
    key.client_email,
    null,
    key.private_key,
    scopes,
    userEmail // impersonate user
  );

  console.log("Authorizing JWT client...");
  await jwtClient.authorize();
  console.log("JWT authorized successfully");

  const drive = google.drive({ version: "v3", auth: jwtClient });
  console.log("Drive client created");

  return drive;
}

// --- FFMPEG CONVERSION ---------------------------------------------------------

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

// --- UTIL ---------------------------------------------------------------------

function bufferToStream(buffer) {
  console.log("Converting Buffer to Readable stream...");
  const stream = new Readable();
  stream.push(buffer);
  stream.push(null);
  return stream;
}

// --- HTTP FUNCTION ------------------------------------------------------------

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

    // Initialize Drive client
    const drive = await getDriveClientForUser(userEmail);

    // 1) Get original file metadata
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

    // 2) Download video file
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

    // 3) Save MP4 to /tmp
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

    // 4) Convert via ffmpeg
    console.log("Calling convertToMp3...");
    await convertToMp3(inputPath, outputPath);
    console.log("MP3 created:", outputPath);

    // 5) Read MP3 back
    console.log("Reading MP3 from:", outputPath);
    let audioBuffer;
    try {
      audioBuffer = await fsp.readFile(outputPath);
      console.log("MP3 read successfully:", audioBuffer.length, "bytes");
    } catch (err) {
      console.error("Error reading MP3:", err);
      throw err;
    }

    // 6) Prepare metadata for upload
    const mp3Name = sourceName.endsWith(".mp4")
      ? sourceName.replace(/\.mp4$/i, ".mp3")
      : `${sourceName}.mp3`;

    console.log("MP3 upload name:", mp3Name);
    console.log("Uploading into parents:", sourceParents);

    const fileMetadata = {
      name: mp3Name,
      mimeType: "audio/mpeg",
      ...(sourceParents.length > 0 ? { parents: sourceParents } : {}),
    };

    const media = {
      mimeType: "audio/mpeg",
      body: bufferToStream(audioBuffer),
    };

    // 7) Upload MP3
    console.log("Uploading MP3 to Drive...");
    let uploadResp;
    try {
      uploadResp = await drive.files.create({
        requestBody: fileMetadata,
        media,
        fields: "id,name,parents,webViewLink,webContentLink",
      });
    } catch (err) {
      console.error("Drive API Error (uploading MP3):", err);
      throw err;
    }

    console.log("Upload completed:", uploadResp.data);

    // 8) Cleanup temp files
    console.log("Cleaning up temp files in:", TMP_DIR);

    try {
      await fsp.unlink(inputPath);
      await fsp.unlink(outputPath);
      console.log("Cleanup complete");
    } catch (e) {
      console.warn("Cleanup error:", e.message);
    }

    // Send success
    console.log("Returning success response");
    return res.status(200).json({
      status: "ok",
      actingUser: userEmail,
      originalFile: {
        id: fileId,
        name: sourceName,
        parents: sourceParents,
      },
      audioFile: uploadResp.data,
    });
  } catch (err) {
    console.error("FATAL ERROR in processVideo():", err);
    return res.status(500).json({
      error: "Internal server error",
      details: err.message,
    });
  }
});
