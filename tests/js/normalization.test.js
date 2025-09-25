const test = require('node:test');
const assert = require('node:assert/strict');
const { normalizeInstruments } = require('../../tools/scripts/test-server');

const corruptedInstruments = [
  { instrumento: 'Good', id: '1' },
  { instrumento: 'Empty ID', id: '' },
  { instrumento: 'Invalid ID', id: 'abc' },
  { instrumento: 'Null ID', id: null },
  { instrumento: 'Missing ID' },
  { instrumento: 'Zero ID', id: 0 },
];

test('normalization function correctly handles corrupted data', () => {
  const normalized = normalizeInstruments(corruptedInstruments);

  // Expected IDs after normalization:
  // '1' -> 1
  // '' -> fallback to index 2
  // 'abc' -> fallback to index 3
  // null -> fallback to index 4
  // undefined -> fallback to index 5
  // 0 -> fallback to index 6
  const expectedIds = [1, 2, 3, 4, 5, 6];

  normalized.forEach((inst, idx) => {
    assert.strictEqual(inst.id, expectedIds[idx], `Test failed for: ${inst.instrumento}`);
    assert.ok(!Number.isNaN(inst.id), `Instrument ID should not be NaN for ${inst.instrumento}`);
    assert.ok(inst.id > 0, `Instrument ID should be a positive number for ${inst.instrumento}`);
  });
});
