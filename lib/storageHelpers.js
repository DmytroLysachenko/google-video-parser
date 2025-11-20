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

module.exports = {
  sanitizeGcsObjectName,
  buildGcsAudioResponse,
  encodeGcsObjectPath,
};
