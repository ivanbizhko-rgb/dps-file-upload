import fs from "fs";
import os from "os";
import path from "path";
import { pipeline } from "stream/promises";
import OpenAI from "openai";
import { fetch } from "undici";
import "dotenv/config";

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const MAX_BACKOFF_MS = 20 * 1000;

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function sanitizeFileName(name) {
  if (!name || typeof name !== "string") return "file";
  const cleaned = name
    .replace(/[^\w.\-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  return cleaned || "file";
}

async function downloadFile({ url, headers, destPath }) {
  const response = await fetch(url, { headers });
  if (!response.ok || !response.body) {
    throw new Error(`Download failed: ${response.status} ${response.statusText}`);
  }
  await pipeline(response.body, fs.createWriteStream(destPath));
}

function parseSqlColumns(columnsText) {
  return columnsText
    .split(",")
    .map((col) => col.trim().replace(/`/g, ""))
    .filter(Boolean);
}

function decodeSqlValue(value) {
  if (!value) return "";
  if (value.toUpperCase() === "NULL") return null;
  return value;
}

function decodeTextBuffer(buffer) {
  if (!Buffer.isBuffer(buffer)) return String(buffer ?? "");
  if (buffer.length >= 2) {
    const bom0 = buffer[0];
    const bom1 = buffer[1];
    if (bom0 === 0xff && bom1 === 0xfe) {
      return buffer.slice(2).toString("utf16le");
    }
    if (bom0 === 0xfe && bom1 === 0xff) {
      const sliced = buffer.slice(2);
      const swapped = Buffer.allocUnsafe(sliced.length);
      for (let i = 0; i + 1 < sliced.length; i += 2) {
        swapped[i] = sliced[i + 1];
        swapped[i + 1] = sliced[i];
      }
      return swapped.toString("utf16le");
    }
  }

  const sampleLen = Math.min(buffer.length, 1024);
  let nulEven = 0;
  let nulOdd = 0;
  for (let i = 0; i < sampleLen; i += 1) {
    if (buffer[i] === 0) {
      if (i % 2 === 0) nulEven += 1;
      else nulOdd += 1;
    }
  }
  const evenSlots = Math.ceil(sampleLen / 2);
  const oddSlots = Math.floor(sampleLen / 2);
  const evenRatio = evenSlots ? nulEven / evenSlots : 0;
  const oddRatio = oddSlots ? nulOdd / oddSlots : 0;

  if (oddRatio > 0.3 || evenRatio > 0.3) {
    if (oddRatio >= evenRatio) {
      return buffer.toString("utf16le");
    }
    const swapped = Buffer.allocUnsafe(buffer.length);
    for (let i = 0; i + 1 < buffer.length; i += 2) {
      swapped[i] = buffer[i + 1];
      swapped[i + 1] = buffer[i];
    }
    return swapped.toString("utf16le");
  }

  return buffer.toString("utf8");
}

function parseValuesSection(text, startIndex, columns, onRow) {
  const len = text.length;
  let i = startIndex;
  let inString = false;
  let inRow = false;
  let current = "";
  let values = [];

  while (i < len) {
    const ch = text[i];

    if (inString) {
      if (ch === "'" && text[i + 1] === "'") {
        current += "'";
        i += 2;
        continue;
      }
      if (ch === "'") {
        inString = false;
        i += 1;
        continue;
      }
      current += ch;
      i += 1;
      continue;
    }

    if (ch === "'") {
      inString = true;
      i += 1;
      continue;
    }

    if (ch === "(") {
      inRow = true;
      values = [];
      current = "";
      i += 1;
      continue;
    }

    if (ch === "," && inRow) {
      values.push(decodeSqlValue(current.trim()));
      current = "";
      i += 1;
      continue;
    }

    if (ch === ")" && inRow) {
      values.push(decodeSqlValue(current.trim()));
      current = "";
      inRow = false;
      i += 1;
      const row = {};
      for (let idx = 0; idx < columns.length; idx += 1) {
        row[columns[idx]] = values[idx] ?? null;
      }
      onRow(row);
      continue;
    }

    if (ch === ";" && !inRow) {
      return i + 1;
    }

    current += ch;
    i += 1;
  }

  return len;
}

function splitSqlByCategory(sqlText, log) {
  const categoryMap = new Map();
  const insertRegex =
    /INSERT\s+INTO\s+`?[^`(\s]*`?\s*\(([^)]+)\)\s*VALUES\s*/gi;
  let match;
  let insertCount = 0;
  let rowCount = 0;

  while ((match = insertRegex.exec(sqlText)) !== null) {
    insertCount += 1;
    const columns = parseSqlColumns(match[1]);
    const valuesStart = insertRegex.lastIndex;
    const endIndex = parseValuesSection(sqlText, valuesStart, columns, (row) => {
      rowCount += 1;
      const categoryRaw = row.category_id ?? row.cat_id;
      if (!categoryRaw) return;
      const categoryRoot = String(categoryRaw).split(".")[0].trim();
      if (!categoryRoot) return;

      const question = row.question ?? "";
      const shortText = row.answer ?? row.description ?? "";
      const fullText = row.description ?? row.answer ?? "";

      if (!categoryMap.has(categoryRoot)) categoryMap.set(categoryRoot, []);
      categoryMap.get(categoryRoot).push({
        question,
        short: shortText,
        full: fullText,
      });
    });
    insertRegex.lastIndex = endIndex;
  }

  if (typeof log === "function") {
    log(`SQL parse stats: inserts=${insertCount}, rows=${rowCount}, categories=${categoryMap.size}`);
  }

  return {
    categoryMap,
    stats: {
      inserts: insertCount,
      rows: rowCount,
      categories: categoryMap.size,
    },
  };
}

async function writeCategoryFiles({ categoryMap, outputDir, baseName }) {
  const categoriesDir = path.join(outputDir, `${baseName}_categories`);
  await fs.promises.mkdir(categoriesDir, { recursive: true });

  const filePaths = [];
  for (const [category, items] of categoryMap.entries()) {
    const filePath = path.join(categoriesDir, `${sanitizeFileName(category)}.json`);
    await fs.promises.writeFile(filePath, JSON.stringify(items, null, 2), "utf8");
    filePaths.push(filePath);
  }

  return { categoriesDir, filePaths };
}

async function safeRemovePath(targetPath) {
  if (!targetPath) return;
  await fs.promises.rm(targetPath, { recursive: true, force: true });
}

async function openaiFetch({ apiKey, pathName, method, body }) {
  const response = await fetch(`https://api.openai.com${pathName}`, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`OpenAI API error: ${response.status} ${response.statusText} ${text}`.trim());
  }
  return response.json();
}

async function pollFileBatch({ apiKey, vectorStoreId, batchId }) {
  const startedAt = Date.now();
  let backoffMs = 1000;

  while (Date.now() - startedAt < DEFAULT_TIMEOUT_MS) {
    const batch = await openaiFetch({
      apiKey,
      pathName: `/v1/vector_stores/${vectorStoreId}/file_batches/${batchId}`,
      method: "GET",
    });

    if (batch.status === "completed" || batch.status === "failed") {
      return batch;
    }

    await sleep(backoffMs);
    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
  }

  throw new Error("Vector store file batch polling timed out");
}

export default async function handler(input = {}) {
  const startedAt = new Date();
  const source = input?.data && typeof input.data === "object" ? input.data : input;
  const output = {
    result: {
      simulatorFileId: source.payload?.[0]?.id ?? null,
      localFilePath: null,
      openaiFileId: null,
      openaiFileIds: null,
      categoryFilePaths: null,
      vectorStoreId: null,
      batchId: null,
      status: "failed",
      error: null,
    },
    meta: {
      startedAt: startedAt.toISOString(),
      finishedAt: null,
      durationMs: null,
    },
  };

  let step = "init";
  let categoriesDir = null;

  try {
    const payloadItem = source.payload?.[0];
    const options = source.options ?? {};

    if (!payloadItem) throw new Error("Missing payload[0]");
    if (!source.gptToken) throw new Error("Missing gptToken");

    step = "download";
    const fileUrl =
      payloadItem.originFileUrl ||
      payloadItem.fileUrl ||
      source.fileUrl ||
      source.simulatorFileUrl ||
      (source.simulatorBaseUrl && payloadItem.fileName
        ? `${source.simulatorBaseUrl.replace(/\/+$/, "")}/${payloadItem.fileName.replace(/^\/+/, "")}`
        : null);
    if (!fileUrl) throw new Error("Missing fileUrl or simulatorBaseUrl + payload[0].fileName");

    const originalFileName =
      payloadItem.title ||
      payloadItem.fileName ||
      path.basename(new URL(fileUrl).pathname) ||
      "downloaded_file";
    const safeFileName = sanitizeFileName(originalFileName);
    const outputDir = options.outputDir || os.tmpdir();
    await fs.promises.mkdir(outputDir, { recursive: true });
    const localFilePath = path.join(outputDir, safeFileName);

    const headers = {};
    if (source.simulatorToken) {
      headers.Authorization = `Bearer ${source.simulatorToken}`;
    }

    console.log("Downloading file...");
    await downloadFile({ url: fileUrl, headers, destPath: localFilePath });
    output.result.localFilePath = localFilePath;

    step = "split_sql_by_category";
    console.log("Parsing SQL and splitting by categories...");
    const rawBuffer = await fs.promises.readFile(localFilePath);
    const sqlText = decodeTextBuffer(rawBuffer);
    output.result.parsePreview = sqlText.slice(0, 200);
    output.result.parseContainsInsert = /INSERT/i.test(sqlText);
    const parsed = splitSqlByCategory(sqlText, console.log);
    output.result.parseStats = parsed.stats;
    output.result.parseEncoding = rawBuffer.length ? "auto" : "empty";
    if (parsed.categoryMap.size === 0) {
      throw new Error("No categories found in SQL file");
    }

    const baseName = path.basename(localFilePath, path.extname(localFilePath));
    const categoryFiles = await writeCategoryFiles({
      categoryMap: parsed.categoryMap,
      outputDir,
      baseName,
    });
    categoriesDir = categoryFiles.categoriesDir;
    output.result.categoryFilePaths = categoryFiles.filePaths;

    step = "openai_file_upload";
    console.log("Uploading category files to OpenAI Files API...");
    const openai = new OpenAI({ apiKey: source.gptToken });
    const openaiFileIds = [];
    for (const filePath of categoryFiles.filePaths) {
      const upload = await openai.files.create({
        file: fs.createReadStream(filePath),
        purpose: "assistants",
      });
      openaiFileIds.push(upload.id);
    }
    output.result.openaiFileIds = openaiFileIds;
    output.result.openaiFileId = openaiFileIds[0] ?? null;

    step = "vector_store";
    let vectorStoreId = source.existingVectorStoreId;
    if (!vectorStoreId) {
      const namePrefix = source.vectorStoreNamePrefix || "vector-store";
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
      const vsName = `${namePrefix}-${sanitizeFileName(originalFileName)}-${timestamp}`;
      console.log("Creating vector store...");
      const vectorStore = await openaiFetch({
        apiKey: source.gptToken,
        pathName: "/v1/vector_stores",
        method: "POST",
        body: { name: vsName },
      });
      vectorStoreId = vectorStore.id;
    }
    output.result.vectorStoreId = vectorStoreId;

    console.log("Creating vector store file batch...");
    const batch = await openaiFetch({
      apiKey: source.gptToken,
      pathName: `/v1/vector_stores/${vectorStoreId}/file_batches`,
      method: "POST",
      body: { file_ids: output.result.openaiFileIds },
    });
    output.result.batchId = batch.id;

    console.log("Polling vector store file batch...");
    const finalBatch = await pollFileBatch({
      apiKey: source.gptToken,
      vectorStoreId,
      batchId: batch.id,
    });
    output.result.status = finalBatch.status;
    if (finalBatch.status !== "completed") {
      throw new Error(`Vector store file batch failed with status: ${finalBatch.status}`);
    }

    step = "cleanup";
    if (!options.dryRun && !options.keepLocalFile && output.result.localFilePath) {
      console.log("Cleaning up local file...");
      await safeRemovePath(output.result.localFilePath);
      output.result.localFilePath = null;
      await safeRemovePath(categoriesDir);
    }
  } catch (err) {
    output.result.status = "failed";
    output.result.error = {
      message: err?.message || "Unknown error",
      step,
    };
  } finally {
    const finishedAt = new Date();
    output.meta.finishedAt = finishedAt.toISOString();
    output.meta.durationMs = finishedAt.getTime() - startedAt.getTime();
  }

  return output;
}
