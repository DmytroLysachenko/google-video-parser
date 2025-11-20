const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const os = require("os");
const { pipeline } = require("stream/promises");
const { spawn } = require("child_process");

const { FFMPEG_PATH } = require("./config");
const { logVerbose } = require("./logging");

const TMP_DIR = os.tmpdir();

function makeTempPath(prefix, ext) {
  const unique = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return path.join(TMP_DIR, `${prefix}-${unique}${ext}`);
}

async function downloadStreamToFile(readableStream, targetPath) {
  logVerbose("Downloading stream to temp file:", targetPath);
  await pipeline(readableStream, fs.createWriteStream(targetPath));
}

async function convertFileToMp3(inputPath, outputPath) {
  logVerbose("Spawning ffmpeg for file transcode:", {
    inputPath,
    outputPath,
  });

  return new Promise((resolve, reject) => {
    const ffmpeg = spawn(FFMPEG_PATH, [
      "-y",
      "-i",
      inputPath,
      "-vn",
      "-ac",
      "1", // mono for speech
      "-ar",
      "22050", // lower sample rate; good for voice/STT and cheaper to encode
      "-b:a",
      "64k", // low bitrate, speech-appropriate
      "-acodec",
      "libmp3lame",
      "-compression_level",
      "9", // fastest, lowest CPU for LAME
      "-qscale:a",
      "9", // low quality is fine for STT; keeps encoding light
      outputPath,
    ]);

    ffmpeg.stderr.on("data", (data) => {
      const text = data.toString();
      if (text.trim()) {
        console.error("ffmpeg stderr:", text.trim());
      }
    });

    ffmpeg.on("close", (code) => {
      logVerbose("ffmpeg exited with code:", code);
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`ffmpeg exited with code ${code}`));
      }
    });
    ffmpeg.on("error", reject);
  });
}

async function uploadFileToGcs(localPath, gcsFile, metadata) {
  logVerbose("Uploading MP3 from temp file to GCS:", localPath);
  const readStream = fs.createReadStream(localPath);
  const writeStream = gcsFile.createWriteStream({
    resumable: false,
    contentType: "audio/mpeg",
    metadata,
  });

  await pipeline(readStream, writeStream);
}

async function cleanupFiles(...filePaths) {
  await Promise.all(
    filePaths.map(async (filePath) => {
      if (!filePath) return;
      try {
        await fsp.unlink(filePath);
        logVerbose("Cleaned temp file:", filePath);
      } catch (err) {
        if (err.code !== "ENOENT") {
          console.warn("Failed to remove temp file:", filePath, err.message);
        }
      }
    })
  );
}

async function convertStreamAndUpload(readableStream, gcsFile, metadata = {}) {
  const inputPath = makeTempPath("input", ".mp4");
  const outputPath = makeTempPath("output", ".mp3");

  try {
    await downloadStreamToFile(readableStream, inputPath);
    await convertFileToMp3(inputPath, outputPath);
    await uploadFileToGcs(outputPath, gcsFile, metadata);
  } finally {
    await cleanupFiles(inputPath, outputPath);
  }
}

module.exports = {
  convertStreamAndUpload,
};
