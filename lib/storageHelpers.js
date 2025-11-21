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
    path: objectName,
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

  // Replace anything that is not alphanumeric, dot, or underscore with a single underscore
  const cleaned = name
    .trim()
    .replace(/[^a-zA-Z0-9._]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "") || `audio_${Date.now()}`;

  if (!cleaned.toLowerCase().endsWith(".mp3")) {
    return `${cleaned}.mp3`;
  }

  return cleaned;
}

module.exports = {
  sanitizeGcsObjectName,
  buildGcsAudioResponse,
  encodeGcsObjectPath,
};
