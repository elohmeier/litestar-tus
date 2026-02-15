import { createReadStream, createWriteStream, statSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as tus from "tus-js-client";

const TOTAL_SIZE = 5 * 1024 ** 3; // 5 GiB
const CHUNK_SIZE = 50 * 1024 * 1024; // 50 MiB
const PARALLEL_UPLOADS = 5;
const BUFFER_SIZE = 1024 * 1024; // 1 MiB
const ENDPOINT = process.env.TUS_ENDPOINT || "http://localhost:8000/files/";

const fillBuffer = Buffer.alloc(BUFFER_SIZE, 0xaa);

function formatBytes(bytes) {
  return (bytes / 1024 ** 3).toFixed(2);
}

function formatThroughput(bytes, seconds) {
  return (bytes / 1024 ** 2 / seconds).toFixed(1);
}

function formatTime(seconds) {
  const m = Math.floor(seconds / 60);
  const s = (seconds % 60).toFixed(1);
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// Write synthetic data to a temp file so tus-js-client can slice it for parallel uploads
const tempPath = join(tmpdir(), `tus-perf-${Date.now()}.bin`);
console.log(`Generating ${formatBytes(TOTAL_SIZE)} GiB temp file: ${tempPath}`);

const ws = createWriteStream(tempPath);
let written = 0;

function writeChunks() {
  let ok = true;
  while (ok && written < TOTAL_SIZE) {
    const toWrite = Math.min(BUFFER_SIZE, TOTAL_SIZE - written);
    const buf = toWrite === BUFFER_SIZE ? fillBuffer : fillBuffer.subarray(0, toWrite);
    ok = ws.write(buf);
    written += toWrite;
  }
  if (written < TOTAL_SIZE) {
    ws.once("drain", writeChunks);
  } else {
    ws.end();
  }
}

ws.on("finish", () => {
  const fileSize = statSync(tempPath).size;
  console.log(`Temp file ready (${formatBytes(fileSize)} GiB)\n`);

  const stream = createReadStream(tempPath);
  const startTime = Date.now();

  console.log(`Uploading to ${ENDPOINT}`);
  console.log(
    `Chunk size: ${CHUNK_SIZE / 1024 / 1024} MiB, parallel uploads: ${PARALLEL_UPLOADS}\n`,
  );

  const upload = new tus.Upload(stream, {
    endpoint: ENDPOINT,
    chunkSize: CHUNK_SIZE,
    parallelUploads: PARALLEL_UPLOADS,
    retryDelays: [0, 1000, 3000, 5000],

    onError(error) {
      console.error("\nUpload failed:", error.message || error);
      cleanup();
      process.exit(1);
    },

    onProgress(bytesUploaded, bytesTotal) {
      const elapsed = (Date.now() - startTime) / 1000;
      const pct = ((bytesUploaded / bytesTotal) * 100).toFixed(1);
      const throughput = formatThroughput(bytesUploaded, elapsed);
      process.stdout.write(
        `\r  ${pct}%  ${formatBytes(bytesUploaded)}/${formatBytes(bytesTotal)} GiB  ${throughput} MiB/s  ${
          formatTime(elapsed)
        }   `,
      );
    },

    onSuccess() {
      const elapsed = (Date.now() - startTime) / 1000;
      const avgThroughput = formatThroughput(TOTAL_SIZE, elapsed);
      console.log(
        `\n\nUpload complete: ${formatBytes(TOTAL_SIZE)} GiB in ${formatTime(elapsed)} (avg ${avgThroughput} MiB/s)`,
      );
      cleanup();
    },
  });

  upload.start();
});

function cleanup() {
  try {
    unlinkSync(tempPath);
  } catch {}
}

process.on("SIGINT", () => {
  cleanup();
  process.exit(1);
});

writeChunks();
