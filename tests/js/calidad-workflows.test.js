const test = require('node:test');
const assert = require('node:assert/strict');

const workflows = require('../../public/assets/scripts/calidad/workflows.js');

test('documentStatusMetadata normalizes mixed case statuses', () => {
  const metadata = workflows.documentStatusMetadata('EN_REVISION');
  assert.equal(metadata.label, 'En revisión');
  assert.equal(metadata.badge, 'warning');
  assert.equal(metadata.order, 2);
});

test('sortTimeline orders events chronologically and enriches metadata', () => {
  const timeline = workflows.sortTimeline([
    { estado: 'publicado', timestamp: '2024-05-05T09:00:00Z' },
    { estado: 'borrador', timestamp: '2024-05-03T09:00:00Z' },
    { estado: 'en_revision', timestamp: '2024-05-04T09:00:00Z' }
  ]);

  assert.equal(timeline.length, 3);
  assert.equal(timeline[0].estado, 'Borrador');
  assert.equal(timeline[1].estado, 'En revisión');
  assert.equal(timeline[2].estado, 'Publicado');
});

test('summarizeAttendance aggregates different attendance states', () => {
  const summary = workflows.summarizeAttendance([
    { estatus: 'asistio' },
    { estatus: 'falta' },
    { estatus: 'justificado' },
    { status: 'attended' }
  ]);

  assert.deepEqual(summary, {
    total: 4,
    attended: 2,
    justified: 1,
    absences: 1,
    completion: 50
  });
});

test('nonConformityProgress returns completion percentage and pending list', () => {
  const progress = workflows.nonConformityProgress({
    acciones: [
      { estado: 'completada', descripcion: 'Actualizar POE' },
      { estado: 'pendiente', descripcion: 'Capacitar personal', responsable: 'Laura' }
    ]
  });

  assert.equal(progress.totalActions, 2);
  assert.equal(progress.closedActions, 1);
  assert.equal(progress.completion, 50);
  assert.equal(progress.pending.length, 1);
  assert.equal(progress.pending[0].descripcion, 'Capacitar personal');
});

test('canCloseNonConformity validates pending actions', () => {
  const record = {
    acciones: [
      { estado: 'completada' },
      { estado: 'pendiente' }
    ],
    estado: 'abierta'
  };

  assert.equal(workflows.canCloseNonConformity(record), false);

  const ready = {
    acciones: [
      { estado: 'completada' },
      { estado: 'completada' }
    ],
    estado: 'en_investigacion'
  };

  assert.equal(workflows.canCloseNonConformity(ready), true);
});
