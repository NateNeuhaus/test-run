#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function getArg(name, fallback = null) {
  const i = process.argv.indexOf(`--${name}`);
  if (i === -1) return fallback;
  if (i + 1 >= process.argv.length) {
    throw new Error(`Missing value for --${name}`);
  }
  return process.argv[i + 1];
}

function sha256File(filename) {
  return crypto
    .createHash("sha256")
    .update(fs.readFileSync(filename))
    .digest("hex");
}

const inputDir = getArg("input");
const outputFile = getArg("output");
const manifestFile = getArg(
  "manifest",
  inputDir ? path.join(inputDir, "manifest.json") : null
);

if (!inputDir || !outputFile || !manifestFile) {
  console.error(
    "Usage: node parallel-merger.js --input SHARD_DIR --output OUTPUT.json [--manifest manifest.json]"
  );
  process.exit(1);
}

console.log("");
console.log("========================================");
console.log("PARALLEL MERGER");
console.log("========================================");
console.log("Input:   ", inputDir);
console.log("Output:  ", outputFile);
console.log("Manifest:", manifestFile);
console.log("");

if (!fs.existsSync(manifestFile)) {
  throw new Error(`Manifest not found: ${manifestFile}`);
}

const manifest = JSON.parse(
  fs.readFileSync(manifestFile, "utf8")
);

if (manifest.format !== "parallel-splitter-v1") {
  throw new Error(
    `Unsupported manifest format: ${manifest.format}`
  );
}

if (
  manifest.root_type !== "object" &&
  manifest.root_type !== "array"
) {
  throw new Error(
    `Unsupported root type: ${manifest.root_type}`
  );
}

const shardIds = Object.keys(manifest.shards).sort();

if (shardIds.length !== manifest.workers) {
  throw new Error(
    `Expected ${manifest.workers} shards, manifest contains ${shardIds.length}`
  );
}

let totalShardRecords = 0;

const loaded = [];

for (const shardId of shardIds) {
  const info = manifest.shards[shardId];

  const filename = path.join(
    inputDir,
    info.filename
  );

  if (!fs.existsSync(filename)) {
    throw new Error(
      `Missing shard ${shardId}: ${filename}`
    );
  }

  const actualHash = sha256File(filename);

  if (actualHash !== info.sha256) {
    throw new Error(
      `SHA256 mismatch for ${info.filename}`
    );
  }

  const shard = JSON.parse(
    fs.readFileSync(filename, "utf8")
  );

  let count;

  if (manifest.root_type === "object") {
    if (
      shard === null ||
      Array.isArray(shard) ||
      typeof shard !== "object"
    ) {
      throw new Error(
        `${info.filename} should contain an object`
      );
    }

    count = Object.keys(shard).length;
  } else {
    if (!Array.isArray(shard)) {
      throw new Error(
        `${info.filename} should contain an array`
      );
    }

    count = shard.length;
  }

  if (count !== info.records) {
    throw new Error(
      `${info.filename}: expected ${info.records} records, found ${count}`
    );
  }

  totalShardRecords += count;

  loaded.push({
    shardId,
    data: shard
  });

  console.log(
    `Verified shard ${shardId}: ${count.toLocaleString()} records`
  );
}

if (totalShardRecords !== manifest.source_records) {
  throw new Error(
    `Shard total ${totalShardRecords} != source total ${manifest.source_records}`
  );
}

let merged;

// ============================================================
// OBJECT MERGE
// ============================================================

if (manifest.root_type === "object") {
  merged = Object.create(null);

  let count = 0;

  for (const shard of loaded) {
    for (const [key, value] of Object.entries(shard.data)) {
      if (
        Object.prototype.hasOwnProperty.call(
          merged,
          key
        )
      ) {
        throw new Error(
          `Duplicate key detected: ${key}`
        );
      }

      merged[key] = value;
      count++;
    }
  }

  if (count !== manifest.source_records) {
    throw new Error(
      `Merged ${count} object records; expected ${manifest.source_records}`
    );
  }
}

// ============================================================
// ARRAY MERGE
// ============================================================

else {
  merged = new Array(manifest.source_records);

  const seen = new Uint8Array(
    manifest.source_records
  );

  let count = 0;

  for (const shard of loaded) {
    for (const record of shard.data) {
      if (
        record === null ||
        Array.isArray(record) ||
        typeof record !== "object" ||
        !Number.isInteger(record.__index)
      ) {
        throw new Error(
          `Invalid array wrapper in shard ${shard.shardId}`
        );
      }

      const index = record.__index;

      if (
        index < 0 ||
        index >= manifest.source_records
      ) {
        throw new Error(
          `Array index out of range: ${index}`
        );
      }

      if (seen[index]) {
        throw new Error(
          `Duplicate array index: ${index}`
        );
      }

      seen[index] = 1;
      merged[index] = record.value;

      count++;
    }
  }

  if (count !== manifest.source_records) {
    throw new Error(
      `Merged ${count} array records; expected ${manifest.source_records}`
    );
  }

  for (let i = 0; i < seen.length; i++) {
    if (!seen[i]) {
      throw new Error(
        `Missing array index: ${i}`
      );
    }
  }
}

// ============================================================
// WRITE + FINAL VERIFY
// ============================================================

fs.mkdirSync(
  path.dirname(path.resolve(outputFile)),
  { recursive: true }
);

fs.writeFileSync(
  outputFile,
  JSON.stringify(merged)
);

const finalData = JSON.parse(
  fs.readFileSync(outputFile, "utf8")
);

const finalCount =
  manifest.root_type === "object"
    ? Object.keys(finalData).length
    : finalData.length;

if (finalCount !== manifest.source_records) {
  throw new Error(
    `FINAL COUNT FAILURE: ${finalCount} != ${manifest.source_records}`
  );
}

console.log("");
console.log("========================================");
console.log("MERGE COMPLETE");
console.log("========================================");
console.log(
  "Source records:",
  manifest.source_records.toLocaleString()
);
console.log(
  "Merged records:",
  finalCount.toLocaleString()
);
console.log(
  "Output SHA256:",
  sha256File(outputFile)
);
console.log("Verified:      YES");
