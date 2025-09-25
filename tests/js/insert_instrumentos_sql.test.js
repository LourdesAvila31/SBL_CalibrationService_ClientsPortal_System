const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SQL_PATH = path.join(__dirname, '..', '..', 'app', 'Modules', 'Internal', 'ArchivosSql', 'insert_instrumentos.sql');
const EXPECTED_COLUMNS = [
  'catalogo_id',
  'marca_id',
  'modelo_id',
  'serie',
  'codigo',
  'departamento_id',
  'ubicacion',
  'fecha_alta',
  'fecha_baja',
  'proxima_calibracion',
  'estado',
  'programado',
  'empresa_id'
];

const INSERT_REGEX = /INSERT INTO instrumentos \(([^)]+)\)\s+VALUES([\s\S]+?)ON DUPLICATE KEY UPDATE/g;

const countValues = (tupleLine) => {
  const trimmed = tupleLine.trim();
  if (!trimmed) {
    return 0;
  }

  let inner = trimmed;
  if (inner.endsWith(',')) {
    inner = inner.slice(0, -1);
  }
  if (inner.startsWith('(') && inner.endsWith(')')) {
    inner = inner.slice(1, -1);
  }

  let insideQuote = false;
  let values = 1;

  for (let i = 0; i < inner.length; i += 1) {
    const ch = inner[i];
    if (ch === "'") {
      if (insideQuote && inner[i + 1] === "'") {
        i += 1;
        continue;
      }
      insideQuote = !insideQuote;
    } else if (ch === ',' && !insideQuote) {
      values += 1;
    }
  }

  return values;
};

test('instrument inserts include extended columns and data', () => {
  const sql = fs.readFileSync(SQL_PATH, 'utf8');
  const matches = [...sql.matchAll(INSERT_REGEX)];

  assert.ok(matches.length > 0, 'No se encontró ninguna inserción de instrumentos');

  for (const match of matches) {
    const columns = match[1]
      .split(',')
      .map((col) => col.trim());

    assert.deepStrictEqual(
      columns,
      EXPECTED_COLUMNS,
      'Las columnas de instrumentos deben incluir los campos adicionales'
    );

    const valuesBlock = match[2]
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line.startsWith('('));

    assert.ok(valuesBlock.length > 0, 'La sección VALUES no contiene filas');

    for (const tupleLine of valuesBlock) {
      const count = countValues(tupleLine);
      assert.strictEqual(
        count,
        EXPECTED_COLUMNS.length,
        `La fila "${tupleLine}" no contiene ${EXPECTED_COLUMNS.length} valores`
      );
    }
  }
});

