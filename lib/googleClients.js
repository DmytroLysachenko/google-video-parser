const path = require("path");
const fsp = require("fs").promises;

const { google } = require("googleapis");
const { Storage } = require("@google-cloud/storage");

const { logVerbose } = require("./logging");

let cachedServiceAccountKey = null;
let cachedStorageClient = null;

async function loadServiceAccountKey() {
  if (cachedServiceAccountKey) {
    return cachedServiceAccountKey;
  }

  if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
    logVerbose("Using GOOGLE_SERVICE_ACCOUNT_KEY env var.");
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
    logVerbose("Reading service account key from file:", resolvedPath);
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
  logVerbose("getDriveClientForUser() called with userEmail:", userEmail);

  const key = await loadServiceAccountKey();

  logVerbose("Creating JWT client for SA:", key.client_email);

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

  logVerbose("Authorizing JWT client...");
  await jwtClient.authorize();
  logVerbose("JWT authorized successfully");

  const drive = google.drive({ version: "v3", auth: jwtClient });
  logVerbose("Drive client created");

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

module.exports = {
  getDriveClientForUser,
  getStorageClient,
};
