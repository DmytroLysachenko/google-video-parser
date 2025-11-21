const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const os = require("os");
const { spawn } = require("child_process");
const { pipeline } = require("stream/promises");

const { FFMPEG_PATH } = require("./config");
const { logVerbose } = require("./logging");

const TMP_DIR = os.tmpdir();

function makeTempPath(prefix, ext) {
  const unique = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return path.join(TMP_DIR, `${prefix}-${unique}${ext}`);
}

async function streamToFile(readableStream, targetPath) {
  logVerbose("Streaming data to temp file:", targetPath);
  await pipeline(readableStream, fs.createWriteStream(targetPath));
}

async function convertToMp3(inputPath, outputPath) {
  logVerbose("Spawning ffmpeg:", { inputPath, outputPath });

  return new Promise((resolve, reject) => {
    const ffmpeg = spawn(FFMPEG_PATH, [
      "-loglevel",
      process.env.FFMPEG_LOGLEVEL || "quiet",
      "-nostats",
      "-hide_banner",
      "-y",
      "-i",
      inputPath,
      "-vn",
      "-ac",
      "1", // mono for speech
      "-ar",
      "16000", // lower sample rate; good for STT
      "-b:a",
      "64k", // low bitrate, speech-appropriate
      "-acodec",
      "libmp3lame",
      "-compression_level",
      "9", // fastest encoding
      "-qscale:a",
      "9", // lowest quality is fine for STT
      "-threads",
      "1", // keep CPU bounded
      outputPath,
    ]);

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
  logVerbose("Uploading MP3 to GCS from temp file:", localPath);
  const writeStream = gcsFile.createWriteStream({
    resumable: false,
    contentType: "audio/mpeg",
    metadata,
  });

  await pipeline(fs.createReadStream(localPath), writeStream);
}

async function cleanupFiles(...filePaths) {
  await Promise.all(
    filePaths.map(async (filePath) => {
      if (!filePath) return;
      try {
        await fsp.unlink(filePath);
        logVerbose("Removed temp file:", filePath);
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
    await streamToFile(readableStream, inputPath);
    await convertToMp3(inputPath, outputPath);
    await uploadFileToGcs(outputPath, gcsFile, metadata);
  } finally {
    await cleanupFiles(inputPath, outputPath);
  }
}

module.exports = {
  convertStreamAndUpload,
};
