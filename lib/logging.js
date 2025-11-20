const isVerboseLoggingEnabled = /^true$/i.test(
  process.env.VERBOSE_LOGS || "false"
);

function logVerbose(...args) {
  if (isVerboseLoggingEnabled) {
    console.log(...args);
  }
}

function startDevMemoryLogger() {
  const intervalMs = parseInt(process.env.MEMORY_LOG_INTERVAL_MS || "5000", 10);
  if (Number.isNaN(intervalMs) || intervalMs <= 0) {
    console.warn("Skipping memory logger: invalid interval", intervalMs);
    return;
  }

  const log = () => {
    const usage = process.memoryUsage();
    console.log("DEV memory usage (MB):", {
      rss: (usage.rss / 1024 / 1024).toFixed(1),
      heapTotal: (usage.heapTotal / 1024 / 1024).toFixed(1),
      heapUsed: (usage.heapUsed / 1024 / 1024).toFixed(1),
      external: (usage.external / 1024 / 1024).toFixed(1),
    });
  };

  console.log(
    `Starting dev memory logger. Interval: ${intervalMs}ms (set MEMORY_LOG_INTERVAL_MS to adjust).`
  );
  const handle = setInterval(log, intervalMs);

  process.on("exit", () => clearInterval(handle));
}

module.exports = {
  isVerboseLoggingEnabled,
  logVerbose,
  startDevMemoryLogger,
};
