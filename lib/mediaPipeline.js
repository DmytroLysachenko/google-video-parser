const { spawn } = require("child_process");
const { PassThrough } = require("stream");
const { pipeline } = require("stream/promises");

const {
  FFMPEG_PATH,
  GCS_UPLOAD_HIGH_WATER_MARK,
  TRANSCODE_STREAM_HIGH_WATER_MARK,
} = require("./config");
const { logVerbose } = require("./logging");

function uploadStreamToGcs(readStream, gcsFile, metadata) {
  return new Promise((resolve, reject) => {
    const writeStream = gcsFile.createWriteStream({
      resumable: false,
      contentType: "audio/mpeg",
      metadata,
      validation: false, // reduce CPU/memory overhead; upstream already trusted
      highWaterMark: GCS_UPLOAD_HIGH_WATER_MARK,
    });

    let resolved = false;
    const cleanup = () => {
      readStream.removeListener("error", onReadError);
      writeStream.removeListener("error", onWriteError);
      writeStream.removeListener("finish", onFinish);
    };

    function onReadError(err) {
      if (!resolved) {
        resolved = true;
        cleanup();
        writeStream.destroy(err);
        reject(err);
      }
    }

    function onWriteError(err) {
      if (!resolved) {
        resolved = true;
        cleanup();
        readStream.destroy(err);
        reject(err);
      }
    }

    function onFinish() {
      if (!resolved) {
        resolved = true;
        cleanup();
        resolve();
      }
    }

    readStream.on("error", onReadError);
    writeStream.on("error", onWriteError);
    writeStream.on("finish", onFinish);

    readStream.pipe(writeStream);
  });
}

function spawnFfmpegTranscoder() {
  logVerbose("Spawning ffmpeg process for streaming transcode:", FFMPEG_PATH);
  const ffmpeg = spawn(FFMPEG_PATH, [
    "-loglevel",
    process.env.FFMPEG_LOGLEVEL || "error",
    "-i",
    "pipe:0",
    "-vn",
    "-acodec",
    "libmp3lame",
    "-f",
    "mp3",
    "pipe:1",
  ]);

  ffmpeg.stderr.on("data", (data) => {
    const text = data.toString();
    if (text.trim()) {
      console.error("ffmpeg stderr:", text.trim());
    }
  });

  return ffmpeg;
}

async function convertStreamAndUpload(readableStream, gcsFile, metadata = {}) {
  const ffmpeg = spawnFfmpegTranscoder();

  const ffmpegExitPromise = new Promise((resolve, reject) => {
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

  readableStream.on("error", (err) => {
    console.error("Download stream error:", err);
    ffmpeg.stdin.destroy(err);
  });

  const throttledTranscodeStream = new PassThrough({
    highWaterMark: TRANSCODE_STREAM_HIGH_WATER_MARK,
  });
  ffmpeg.stdout.pipe(throttledTranscodeStream);

  const uploadPromise = uploadStreamToGcs(
    throttledTranscodeStream,
    gcsFile,
    metadata
  );

  const inputPumpPromise = pipeline(readableStream, ffmpeg.stdin);

  try {
    await Promise.all([ffmpegExitPromise, uploadPromise, inputPumpPromise]);
  } catch (err) {
    ffmpeg.stdin.destroy(err);
    ffmpeg.stdout.destroy(err);
    throttledTranscodeStream.destroy(err);
    throw err;
  } finally {
    ffmpeg.stdin.destroy();
    ffmpeg.stdout.destroy();
    throttledTranscodeStream.destroy();
  }
}

module.exports = {
  convertStreamAndUpload,
};
