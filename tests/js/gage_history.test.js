const test = require('node:test');
const assert = require('node:assert/strict');
const { createServer } = require('../../tools/scripts/test-server');

const port = 3002;
const server = createServer();

test.before(() => {
  return new Promise(resolve => server.listen(port, resolve));
});

test.after(() => {
  return new Promise(resolve => server.close(resolve));
});

test('GET /api/gage-history?id=1 returns chronological history', async () => {
  const res = await fetch(`http://localhost:${port}/api/gage-history?id=1`);
  assert.equal(res.status, 200);
  const data = await res.json();
  assert.ok(Array.isArray(data.changes));
  assert.ok(Array.isArray(data.dates));
  assert.ok(Array.isArray(data.certificates));
  assert.equal(data.changes.length, data.dates.length);
  assert.equal(data.changes.length, data.certificates.length);
  const sortedDates = [...data.dates].sort((a, b) => new Date(a) - new Date(b));
  assert.deepEqual(data.dates, sortedDates);
});

test('History modal renders correctly', () => {
  const history = {
    changes: ['created', 'calibrated', 'repaired'],
    dates: ['2021-01-01', '2022-05-10', '2023-07-15'],
    certificates: ['certA.pdf', 'certB.pdf', 'certC.pdf']
  };

  const document = createDocument();
  const modal = document.createElement('div');
  document.register('modal', modal);
  const changesEl = document.createElement('ul');
  document.register('changes', changesEl);
  modal.appendChild(changesEl);
  const datesEl = document.createElement('ul');
  document.register('dates', datesEl);
  modal.appendChild(datesEl);
  const certsEl = document.createElement('ul');
  document.register('certificates', certsEl);
  modal.appendChild(certsEl);
  document.root.appendChild(modal);

  renderHistoryModal(document, history);

  const changes = document.querySelectorAll('#changes li').map(li => li.textContent);
  const dates = document.querySelectorAll('#dates li').map(li => li.textContent);
  const certs = document.querySelectorAll('#certificates li').map(li => li.textContent);

  assert.deepEqual(changes, history.changes);
  assert.deepEqual(dates, history.dates);
  assert.deepEqual(certs, history.certificates);
});

function renderHistoryModal(doc, history) {
  const fill = (selector, items) => {
    const ul = doc.querySelector(selector);
    items.forEach(item => {
      const li = doc.createElement('li');
      li.textContent = item;
      ul.appendChild(li);
    });
  };
  fill('#changes', history.changes);
  fill('#dates', history.dates);
  fill('#certificates', history.certificates);
}

function createDocument() {
  class Element {
    constructor(tag) {
      this.tagName = tag;
      this.children = [];
      this.textContent = '';
      this.id = null;
    }
    appendChild(child) {
      this.children.push(child);
    }
    findById(id) {
      if (this.id === id) return this;
      for (const child of this.children) {
        const found = child.findById(id);
        if (found) return found;
      }
      return null;
    }
    querySelectorAll(selector) {
      if (selector.startsWith('#')) {
        const [idPart, tag] = selector.slice(1).split(' ');
        const node = this.findById(idPart);
        if (!node) return [];
        if (tag) {
          return node.children.filter(ch => ch.tagName === tag);
        }
        return [node];
      }
      return [];
    }
  }

  const root = new Element('document');
  const doc = {
    root,
    createElement: tag => new Element(tag),
    register: (id, el) => { el.id = id; },
    querySelector: selector => root.querySelectorAll(selector)[0] || null,
    querySelectorAll: selector => root.querySelectorAll(selector)
  };
  return doc;
}
