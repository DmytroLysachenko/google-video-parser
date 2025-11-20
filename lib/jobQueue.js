const {
  MAX_MEMORY_MB,
  JOB_SLOT_WAIT_INTERVAL_MS,
  JOB_QUEUE_TIMEOUT_MS,
  MAX_CONCURRENT_JOBS,
} = require("./config");
const { logVerbose } = require("./logging");

let activeJobs = 0;

function hasAvailableJobSlot() {
  return activeJobs < MAX_CONCURRENT_JOBS;
}

function getMemoryUsageMb() {
  return process.memoryUsage().rss / 1024 / 1024;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryAcquireJobSlot() {
  if (activeJobs >= MAX_CONCURRENT_JOBS) {
    return false;
  }
  activeJobs++;
  logVerbose("Job slot acquired. Active jobs:", activeJobs);
  return true;
}

function releaseJobSlot() {
  if (activeJobs > 0) {
    activeJobs--;
  }
  logVerbose("Job slot released. Active jobs:", activeJobs);
}

async function acquireJobSlotWithMemoryGuard() {
  const start = Date.now();

  while (true) {
    const rssMb = getMemoryUsageMb();
    if (rssMb > MAX_MEMORY_MB) {
      console.warn(
        `Memory usage ${rssMb.toFixed(
          1
        )}MB exceeds limit ${MAX_MEMORY_MB}MB. Waiting before starting next job...`
      );
    } else if (tryAcquireJobSlot()) {
      logVerbose(
        `Job slot granted. RSS at start: ${rssMb.toFixed(1)}MB (limit ${
          MAX_MEMORY_MB === Infinity ? "unlimited" : MAX_MEMORY_MB
        }MB)`
      );
      return true;
    } else {
      console.warn(
        `Job slot unavailable (active: ${activeJobs}/${MAX_CONCURRENT_JOBS}). Waiting...`
      );
    }

    if (Date.now() - start > JOB_QUEUE_TIMEOUT_MS) {
      throw new Error(
        `Timed out after ${JOB_QUEUE_TIMEOUT_MS}ms waiting for free job slot and safe memory level`
      );
    }

    await delay(JOB_SLOT_WAIT_INTERVAL_MS);
  }
}

module.exports = {
  acquireJobSlotWithMemoryGuard,
  releaseJobSlot,
  getMemoryUsageMb,
  hasAvailableJobSlot,
};
