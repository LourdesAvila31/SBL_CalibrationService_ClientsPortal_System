const test = require('node:test');
const assert = require('node:assert/strict');

const instruments = require('./data/instruments.json');

test('instrument dataset has 12 entries', () => {
  assert.equal(instruments.length, 12);
});

test('first instrument is Aerotest', () => {
  assert.equal(instruments[0].instrumento, 'Aerotest');
});

test('instruments include numeric id keys', () => {
  instruments.forEach((inst, idx) => {
    assert.equal(typeof inst.id, 'number');
    assert.equal(inst.id, idx + 1);
  });
});

test('includes 11 magnetic stirrers', () => {
  const count = instruments.filter(i => i.instrumento === 'Agitador magnético').length;
  assert.equal(count, 11);
});