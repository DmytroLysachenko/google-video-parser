const ffmpegInstaller = require("@ffmpeg-installer/ffmpeg");

const FFMPEG_PATH = ffmpegInstaller.path;

const MAX_MEMORY_MB = (() => {
  const parsed = parseInt(process.env.MAX_MEMORY_MB || "450", 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    console.warn(
      "Invalid MAX_MEMORY_MB value provided; falling back to unlimited memory guard."
    );
    return Infinity;
  }
  return parsed;
})();

const JOB_SLOT_WAIT_INTERVAL_MS = (() => {
  const parsed = parseInt(process.env.JOB_SLOT_WAIT_INTERVAL_MS || "250", 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    console.warn("Invalid JOB_SLOT_WAIT_INTERVAL_MS; defaulting to 250ms.");
    return 250;
  }
  return parsed;
})();

const JOB_QUEUE_TIMEOUT_MS = (() => {
  const parsed = parseInt(process.env.JOB_QUEUE_TIMEOUT_MS || "600000", 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    console.warn("Invalid JOB_QUEUE_TIMEOUT_MS; defaulting to 10 minutes.");
    return 600000;
  }
  return parsed;
})();

const MAX_CONCURRENT_JOBS = parseInt(
  process.env.MAX_CONCURRENT_JOBS || "1",
  10
);

const GCS_UPLOAD_HIGH_WATER_MARK = (() => {
  const parsed = parseInt(process.env.GCS_UPLOAD_HIGH_WATER_MARK || "262144", 10); // 256KB
  if (Number.isNaN(parsed) || parsed <= 0) {
    console.warn("Invalid GCS_UPLOAD_HIGH_WATER_MARK; defaulting to 256KB.");
    return 262144;
  }
  return parsed;
})();

const TRANSCODE_STREAM_HIGH_WATER_MARK = (() => {
  const parsed = parseInt(
    process.env.TRANSCODE_STREAM_HIGH_WATER_MARK || "262144",
    10
  ); // 256KB
  if (Number.isNaN(parsed) || parsed <= 0) {
    console.warn(
      "Invalid TRANSCODE_STREAM_HIGH_WATER_MARK; defaulting to 256KB."
    );
    return 262144;
  }
  return parsed;
})();

module.exports = {
  FFMPEG_PATH,
  MAX_MEMORY_MB,
  JOB_SLOT_WAIT_INTERVAL_MS,
  JOB_QUEUE_TIMEOUT_MS,
  MAX_CONCURRENT_JOBS,
  GCS_UPLOAD_HIGH_WATER_MARK,
  TRANSCODE_STREAM_HIGH_WATER_MARK,
};
