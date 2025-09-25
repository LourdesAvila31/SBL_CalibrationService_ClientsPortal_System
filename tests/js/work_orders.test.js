const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const dataPath = path.join(__dirname, 'data', 'work_orders.json');

const dataset = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

const requiredKeys = [
  'plan_id',
  'instrumento_id',
  'empresa_id',
  'tecnico_id',
  'estado_orden',
  'fecha_inicio',
  'fecha_cierre',
  'observaciones'
];

test('work order fixture structure', () => {
  assert.equal(Array.isArray(dataset), true, 'El dataset debe ser un arreglo');
  assert.equal(dataset.length > 0, true, 'El dataset debe contener elementos');
  dataset.forEach((item, index) => {
    requiredKeys.forEach((key) => {
      assert.equal(Object.prototype.hasOwnProperty.call(item, key), true, `Falta la clave ${key} en el registro ${index}`);
    });
    assert.equal(Number.isInteger(item.plan_id), true, 'plan_id debe ser entero');
    assert.equal(Number.isInteger(item.instrumento_id), true, 'instrumento_id debe ser entero');
    assert.equal(Number.isInteger(item.empresa_id), true, 'empresa_id debe ser entero');
    assert.equal(typeof item.estado_orden, 'string');
  });
});
