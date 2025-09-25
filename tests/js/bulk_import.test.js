const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const scriptPath = path.join(__dirname, '..', 'php', 'bulk_import_cli.php');

test('bulk import harness validates business rules', () => {
  const execution = spawnSync('php', [scriptPath], { encoding: 'utf8' });
  assert.strictEqual(execution.status, 0, execution.stderr || 'La ejecución de PHP no finalizó correctamente');

  const output = execution.stdout.trim();
  assert.ok(output.length > 0, 'Se esperaba salida JSON del script de importación.');

  const payload = JSON.parse(output);
  const summary = payload.resultado && payload.resultado.summary;
  assert.ok(summary, 'La respuesta debe incluir un resumen.');
  assert.strictEqual(summary.total, 3, 'El resumen debe reportar las 3 filas del archivo de ejemplo.');
  assert.strictEqual(summary.inserted, 2, 'Deben importarse 2 instrumentos exitosamente.');
  assert.strictEqual(summary.failed, 1, 'Debe existir exactamente un registro fallido.');

  const insertados = payload.insertados || [];
  assert.strictEqual(insertados.length, 2, 'La colección de insertados debe tener dos elementos.');
  insertados.forEach(inst => {
    assert.strictEqual(inst.empresa_id, 7, 'Los instrumentos deben asociarse a la empresa de pruebas.');
    assert.strictEqual(inst.estado, 'activo', 'El estado debe derivarse como "activo" al importar.');
  });

  const errores = (payload.resultado.results || []).filter(row => row.status === 'error');
  assert.strictEqual(errores.length, 1, 'Debe registrarse un único error por fila.');
  const mensajeError = (errores[0].errors || []).join(' ').toLowerCase();
  assert.ok(mensajeError.includes('departamento'), 'El error debe indicar que el departamento es obligatorio.');
});
