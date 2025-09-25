#!/usr/bin/env node

const fs = require('fs');

function countRows(content) {
  const normalized = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  const lines = normalized.split('\n');
  if (lines.length && lines[lines.length - 1] === '') {
    lines.pop();
  }
  return lines.length;
}

function normalizeContent(content) {
  let normalized = content.replace(/^\uFEFF/, '');
  normalized = normalized.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  return normalized;
}

function normalizeFile(path) {
  const originalBuffer = fs.readFileSync(path);
  const originalContent = originalBuffer.toString('utf8');
  const originalRows = countRows(originalContent);

  const normalizedContent = normalizeContent(originalContent);
  const normalizedRows = countRows(normalizedContent);

  if (originalRows !== normalizedRows) {
    throw new Error(
      `${path}: row count changed from ${originalRows} to ${normalizedRows}. Aborting.`
    );
  }

  const finalContent = normalizedContent.endsWith('\n')
    ? normalizedContent
    : `${normalizedContent}\n`;

  fs.writeFileSync(path, finalContent, { encoding: 'utf8' });
  console.log(
    `${path}: normalized line endings to LF. Rows: ${normalizedRows}. Byte length: ${finalContent.length}`
  );
}

function main() {
  const [, , ...files] = process.argv;
  if (!files.length) {
    console.error('Usage: normalize_csv_endings.js <file> [file ...]');
    process.exitCode = 1;
    return;
  }

  try {
    files.forEach(normalizeFile);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

main();
