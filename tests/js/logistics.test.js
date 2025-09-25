const path = require('path');
const test = require('node:test');
const assert = require('node:assert/strict');

const logisticsModulePath = path.join(__dirname, '../../public/assets/scripts/calibraciones/logistics.js');
const LogisticsTimeline = require(logisticsModulePath);

test('serializeFromApi returns defaults for empty payload', () => {
  const result = LogisticsTimeline.serializeFromApi({});
  assert.equal(result.estado, 'Pendiente');
  assert.ok(Array.isArray(result.timeline));
  assert.equal(result.timeline.length, LogisticsTimeline.STATES.length);
  result.timeline.forEach((step, index) => {
    assert.equal(step.estado, LogisticsTimeline.STATES[index]);
    if (index === 0) {
      assert.equal(step.completado, true);
    }
  });
});

test('serializeFromApi normalizes dates and state progression', () => {
  const row = {
    logistica_estado: 'en tránsito',
    logistica_fecha_envio: '2024-06-01',
    logistica_fecha_en_transito: '2024-06-03',
    log_fecha_en_transito: '2024-06-03',
    logistica_fecha_retorno: '',
    log_proveedor_externo: 'Metrología MX',
    log_transportista: 'DHL',
    log_numero_guia: 'ABC123'
  };
  const result = LogisticsTimeline.serializeFromApi(row);
  assert.equal(result.estado, 'En tránsito');
  assert.equal(result.proveedor, 'Metrología MX');
  assert.equal(result.transportista, 'DHL');
  assert.equal(result.numero_guia, 'ABC123');
  assert.equal(result.fecha_envio, '2024-06-01');
  assert.equal(result.fecha_en_transito, '2024-06-03');
  assert.equal(result.fecha_retorno, null);
  assert.ok(result.timeline.some(step => step.estado === 'En tránsito' && step.completado));
});

test('filterByState isolates external calibrations with matching logistics state', () => {
  const dataset = [
    { id: 1, tipo: 'Externa', logistica_estado: 'Pendiente' },
    { id: 2, tipo: 'Interna', logistica_estado: 'Enviado' },
    { id: 3, tipo: 'Externa', logistica: { estado: 'Enviado' } },
    { id: 4, tipo: 'Externa', logistica_estado: 'Recibido' }
  ];
  const enviados = LogisticsTimeline.filterByState(dataset, 'Enviado');
  assert.deepEqual(enviados.map(item => item.id), [3]);
  const pendientes = LogisticsTimeline.filterByState(dataset, 'Pendiente');
  assert.deepEqual(pendientes.map(item => item.id), [1]);
});

test('buildTimelineHtml renders timeline structure with placeholders', () => {
  const timeline = LogisticsTimeline.timelineFromData({ estado: 'Pendiente' });
  const html = LogisticsTimeline.buildTimelineHtml(timeline);
  assert.ok(html.includes('Pendiente'));
  assert.ok(html.includes('Enviado'));
  assert.ok(html.includes('En tránsito'));
  assert.ok(html.includes('Recibido'));
});
