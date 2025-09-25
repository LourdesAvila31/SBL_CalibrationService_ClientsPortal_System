const test = require('node:test');
const assert = require('node:assert/strict');

class MockElement {
  constructor(tagName = 'div') {
    this.tagName = tagName.toLowerCase();
    this.children = [];
    this.listeners = {};
    this._innerHTML = '';
    this.value = '';
    this.dataset = {};
    this._textContent = '';
  }

  set innerHTML(value) {
    this._innerHTML = value;
    if (this.tagName === 'select') {
      this.children = [];
    }
  }

  get innerHTML() {
    return this._innerHTML;
  }

  set textContent(value) {
    this._textContent = value;
  }

  get textContent() {
    return this._textContent;
  }

  appendChild(node) {
    this.children.push(node);
    return node;
  }

  addEventListener(event, handler) {
    this.listeners[event] = handler;
  }

  dispatch(event, payload) {
    if (this.listeners[event]) {
      this.listeners[event](payload);
    }
  }

  querySelector() {
    return null;
  }
}

test('calibration widget asigna mediciones en vivo', async () => {
  global.window = global;
  global.location = { origin: 'http://localhost' };
  global.resolveAppUrl = (path) => path;
  const fetchCalls = [];
  const responses = {
    calibradores: { success: true, data: [{ id: 1, nombre: 'Demo' }] },
    measurements: { success: true, data: [{
      id: 99,
      measurement_uuid: 'uuid-demo',
      fecha_lectura: '2024-05-01T12:00:00Z',
      valor: 23.5,
      unidad: '°C',
      payload_json: '{"valor":23.5}'
    }] }
  };

  global.fetch = async (input) => {
    const url = typeof input === 'string' ? input : input.toString();
    fetchCalls.push(url);
    if (url.includes('list_calibradores.php')) {
      return { ok: true, json: async () => responses.calibradores };
    }
    if (url.includes('list_measurements.php')) {
      return { ok: true, json: async () => responses.measurements };
    }
    throw new Error(`URL inesperada: ${url}`);
  };

  const selectElement = new MockElement('select');
  const statusElement = new MockElement('div');
  const hiddenMeasurement = new MockElement('input');
  const instrumentElement = new MockElement('select');

  const elements = {
    'calibrator-select': selectElement,
    'calibrator-status': statusElement,
    'measurement-id-field': hiddenMeasurement,
    'instrument': instrumentElement,
  };

  global.document = {
    addEventListener: () => {},
    getElementById: (id) => elements[id] || null,
    querySelector: () => null,
    createElement: (tag) => new MockElement(tag),
  };

  global.setInterval = () => 0;

  require('../../public/assets/scripts/calibradores.js');

  await window.CalibradoresUI.initCalibrationWidget();

  assert.equal(selectElement.children.length, 1, 'Debe cargar un calibrador en el selector');

  selectElement.value = '1';
  selectElement.dispatch('change');

  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.ok(statusElement.innerHTML.includes('Última medición'), 'El estado debe mostrar la medición más reciente');
  assert.equal(hiddenMeasurement.value, '', 'No debe seleccionar medición automáticamente');

  const fakeButton = {
    getAttribute: () => '99',
    textContent: 'Usar medición',
    classList: { remove() {}, add() {} }
  };

  statusElement.dispatch('click', { target: { closest: () => fakeButton } });

  assert.equal(hiddenMeasurement.value, '99', 'La medición seleccionada debe registrarse en el campo oculto');
  assert.equal(fakeButton.textContent, 'Medición seleccionada', 'El botón debe reflejar la selección');
  assert.ok(fetchCalls.some((url) => url.includes('list_measurements.php')), 'Debe consultar lecturas del calibrador');
});
