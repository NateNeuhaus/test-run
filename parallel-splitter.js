#!/usr/bin/env node

"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

// ============================================================
// CLI
// ============================================================

function usage() {
  console.log(`
Usage:

  node parallel-splitter.js \\
    --input input.json \\
    --output shards \\
    --workers 16

Options:

  --input       Input JSON file
  --output      Output directory
  --workers     Number of shards/workers
  --strategy    auto | hash-key | round-robin
                Default: auto

Supported roots:

  Object:
    {
      "id1": value,
      "id2": value
    }

  Array:
    [
      value,
      value
    ]

Outputs:

  shards/
    shard-00.json
    shard-01.json
    ...
    manifest.json
`);
}

function getArg(name, fallback = null) {
  const i = process.argv.indexOf(`--${name}`);

  if (i === -1) {
    return fallback;
  }

  if (i + 1 >= process.argv.length) {
    throw new Error(`Missing value for --${name}`);
  }

  return process.argv[i + 1];
}

const inputFile = getArg("input");
const outputDir = getArg("output");
const workerArg = getArg("workers", "16");
const requestedStrategy = getArg("strategy", "auto");

if (!inputFile || !outputDir) {
  usage();
  process.exit(1);
}

const workers = Number(workerArg);

if (
  !Number.isInteger(workers) ||
  workers < 1 ||
  workers > 999
) {
  throw new Error(
    `Invalid worker count: ${workerArg}`
  );
}

const allowedStrategies = new Set([
  "auto",
  "hash-key",
  "round-robin"
]);

if (!allowedStrategies.has(requestedStrategy)) {
  throw new Error(
    `Unknown strategy: ${requestedStrategy}`
  );
}

// ============================================================
// HELPERS
// ============================================================

function sha256Buffer(buffer) {
  return crypto
    .createHash("sha256")
    .update(buffer)
    .digest("hex");
}

function sha256File(filename) {
  const data = fs.readFileSync(filename);
  return sha256Buffer(data);
}

function hashToWorker(value, workerCount) {
  const digest = crypto
    .createHash("sha256")
    .update(String(value))
    .digest();

  // First 48 bits.
  //
  // Safely below Number.MAX_SAFE_INTEGER and gives vastly
  // more than enough distribution entropy for worker routing.

  const n =
    digest.readUIntBE(0, 6);

  return n % workerCount;
}

function padWorker(worker) {
  const width = Math.max(
    2,
    String(workers - 1).length
  );

  return String(worker).padStart(width, "0");
}

function isPlainObject(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function ensureEmptyDirectory(directory) {
  fs.mkdirSync(directory, {
    recursive: true
  });

  for (const name of fs.readdirSync(directory)) {
    const target = path.join(directory, name);

    fs.rmSync(target, {
      recursive: true,
      force: true
    });
  }
}

// ============================================================
// READ INPUT
// ============================================================

console.log("");
console.log("========================================");
console.log("PARALLEL SPLITTER");
console.log("========================================");
console.log("Input:   ", inputFile);
console.log("Output:  ", outputDir);
console.log("Workers: ", workers);

const sourceBytes =
  fs.readFileSync(inputFile);

const sourceSha256 =
  sha256Buffer(sourceBytes);

console.log("SHA256:  ", sourceSha256);
console.log("");

const data =
  JSON.parse(
    sourceBytes.toString("utf8")
  );

let rootType;

if (Array.isArray(data)) {
  rootType = "array";
} else if (isPlainObject(data)) {
  rootType = "object";
} else {
  throw new Error(
    "JSON root must be an object or array"
  );
}

let strategy = requestedStrategy;

if (strategy === "auto") {
  strategy =
    rootType === "object"
      ? "hash-key"
      : "round-robin";
}

if (
  rootType === "array" &&
  strategy === "hash-key"
) {
  throw new Error(
    "hash-key requires an object root"
  );
}

console.log("Root:    ", rootType);
console.log("Strategy:", strategy);
console.log("");

// ============================================================
// PREPARE SHARDS
// ============================================================

ensureEmptyDirectory(outputDir);

const shards =
  Array.from(
    { length: workers },
    () =>
      rootType === "object"
        ? {}
        : []
  );

const counts =
  Array(workers).fill(0);

const estimatedBytes =
  Array(workers).fill(0);

// ============================================================
// SPLIT OBJECT
// ============================================================

let sourceRecords = 0;

if (rootType === "object") {
  const entries =
    Object.entries(data);

  sourceRecords =
    entries.length;

  for (
    let index = 0;
    index < entries.length;
    index++
  ) {
    const [key, value] =
      entries[index];

    let worker;

    if (strategy === "hash-key") {
      worker =
        hashToWorker(
          key,
          workers
        );
    } else {
      worker =
        index % workers;
    }

    shards[worker][key] =
      value;

    counts[worker]++;

    estimatedBytes[worker] +=
      Buffer.byteLength(
        JSON.stringify(key)
      ) +
      Buffer.byteLength(
        JSON.stringify(value)
      );

    if (
      (index + 1) % 100000 === 0
    ) {
      console.log(
        "Assigned:",
        (index + 1).toLocaleString()
      );
    }
  }
}

// ============================================================
// SPLIT ARRAY
//
// Preserve original array indices in shard records:
//
// {
//   "__index": 123,
//   "value": ...
// }
//
// This lets the merger reconstruct exact original ordering.
// ============================================================

if (rootType === "array") {
  sourceRecords =
    data.length;

  for (
    let index = 0;
    index < data.length;
    index++
  ) {
    const worker =
      index % workers;

    const wrapped = {
      __index: index,
      value: data[index]
    };

    shards[worker].push(
      wrapped
    );

    counts[worker]++;

    estimatedBytes[worker] +=
      Buffer.byteLength(
        JSON.stringify(wrapped)
      );

    if (
      (index + 1) % 100000 === 0
    ) {
      console.log(
        "Assigned:",
        (index + 1).toLocaleString()
      );
    }
  }
}

// ============================================================
// WRITE SHARDS
// ============================================================

const shardManifest = {};

for (
  let worker = 0;
  worker < workers;
  worker++
) {
  const workerId =
    padWorker(worker);

  const filename =
    `shard-${workerId}.json`;

  const fullPath =
    path.join(
      outputDir,
      filename
    );

  const serialized =
    JSON.stringify(
      shards[worker]
    );

  fs.writeFileSync(
    fullPath,
    serialized
  );

  const stat =
    fs.statSync(fullPath);

  const shardSha256 =
    sha256File(fullPath);

  shardManifest[workerId] = {
    worker,
    filename,
    records: counts[worker],
    bytes: stat.size,
    estimated_source_bytes:
      estimatedBytes[worker],
    sha256: shardSha256
  };

  console.log(
    `Shard ${workerId}:`,
    counts[worker]
      .toLocaleString(),
    "records |",
    stat.size
      .toLocaleString(),
    "bytes"
  );
}

// ============================================================
// VERIFY SPLIT
// ============================================================

const shardRecordTotal =
  counts.reduce(
    (a, b) => a + b,
    0
  );

if (
  shardRecordTotal !==
  sourceRecords
) {
  throw new Error(
    `Split verification failed: ` +
    `${shardRecordTotal} != ${sourceRecords}`
  );
}

// ============================================================
// MANIFEST
// ============================================================

const manifest = {
  format:
    "parallel-splitter-v1",

  created_at:
    new Date().toISOString(),

  input_file:
    path.basename(inputFile),

  source_sha256:
    sourceSha256,

  root_type:
    rootType,

  strategy,

  workers,

  source_records:
    sourceRecords,

  shard_records:
    shardRecordTotal,

  verified:
    shardRecordTotal ===
    sourceRecords,

  shards:
    shardManifest
};

const manifestPath =
  path.join(
    outputDir,
    "manifest.json"
  );

fs.writeFileSync(
  manifestPath,
  JSON.stringify(
    manifest,
    null,
    2
  )
);

console.log("");
console.log("========================================");
console.log("SPLIT COMPLETE");
console.log("========================================");
console.log(
  "Source records:",
  sourceRecords.toLocaleString()
);
console.log(
  "Shard records: ",
  shardRecordTotal.toLocaleString()
);
console.log(
  "Workers:       ",
  workers
);
console.log(
  "Verified:      ",
  "YES"
);
console.log(
  "Manifest:      ",
  manifestPath
);