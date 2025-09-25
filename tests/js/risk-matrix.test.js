const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateRisk } = require('../../app/Modules/Tenant/Risk/calculate_risk.js');

// Valid calculation example
test('calculateRisk returns risk and frequency for valid inputs', () => {
  const result = calculateRisk(1,2);
  assert.deepEqual(result, { risk: 2, frequency: 12 });
});

// Invalid value should throw error
test('calculateRisk rejects values outside 1-3', () => {
  assert.throws(() => calculateRisk(4,1));
});
