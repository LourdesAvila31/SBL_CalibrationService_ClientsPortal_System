const test = require('node:test');
const assert = require('node:assert');

function createElementStub() {
    return {
        appendChild: () => {},
        remove: () => {},
        setAttribute: () => {},
        addEventListener: () => {},
        querySelector: () => null,
        querySelectorAll: () => [],
        classList: {
            add: () => {},
            remove: () => {},
            contains: () => false,
            toggle: () => {}
        },
        style: {},
        dataset: {}
    };
}

global.window = {
    fetch: async () => ({
        ok: true,
        json: async () => ({}),
        clone: () => ({ text: async () => '' })
    }),
    AppUrl: {
        normalizeAppUrl: value => value,
        resolveAppUrl: value => value
    },
    location: {
        pathname: '/',
        href: '/',
        assign: () => {}
    },
    addEventListener: () => {},
    removeEventListener: () => {}
};

global.document = {
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => true,
    createElement: () => createElementStub(),
    head: {
        appendChild: () => {}
    },
    body: {
        appendChild: () => {},
        removeChild: () => {},
        classList: {
            add: () => {},
            remove: () => {},
            contains: () => false,
            toggle: () => {}
        }
    },
    querySelector: () => null,
    querySelectorAll: () => [],
    getElementById: () => null,
    readyState: 'complete'
};

global.window.document = global.document;

global.CustomEvent = class {
    constructor(type, options = {}) {
        this.type = type;
        this.detail = options.detail;
    }
};

global.MutationObserver = class {
    constructor() {}
    observe() {}
    disconnect() {}
};

global.window.MutationObserver = global.MutationObserver;

global.localStorage = {
    getItem: () => null,
    setItem: () => {},
    removeItem: () => {}
};

global.window.localStorage = global.localStorage;

global.navigator = {
    userAgent: 'node'
};

global.window.navigator = global.navigator;

global.window.matchMedia = () => ({
    matches: false,
    addEventListener: () => {},
    removeEventListener: () => {}
});

const helpersPromise = import('../../public/assets/scripts/portal-base.js')
    .then(mod => mod.CalibrationStatusHelpers || global.CalibrationStatusHelpers);

function formatDateForHelper(offsetDays) {
    const date = new Date();
    date.setHours(0, 0, 0, 0);
    date.setDate(date.getDate() + offsetDays);
    const year = date.getFullYear();
    const month = `${date.getMonth() + 1}`.padStart(2, '0');
    const day = `${date.getDate()}`.padStart(2, '0');
    return `${year}-${month}-${day}T00:00:00`;
}

function calculateExpectedDays(targetString) {
    const target = new Date(targetString);
    const today = new Date();
    target.setHours(0, 0, 0, 0);
    today.setHours(0, 0, 0, 0);
    return Math.round((target.getTime() - today.getTime()) / 86400000);
}

test('getStatusInfo devuelve estado vigente para fechas futuras lejanas', async () => {
    const futureDate = formatDateForHelper(45);
    const helpers = await helpersPromise;
    const info = helpers.getStatusInfo({ fecha_proxima: futureDate });
    const expectedDays = calculateExpectedDays(futureDate);

    assert.strictEqual(info.estado, 'vigente');
    assert.strictEqual(info.dias_restantes, expectedDays);
});

test('getStatusInfo devuelve estado vencido para fechas pasadas', async () => {
    const pastDate = formatDateForHelper(-10);
    const helpers = await helpersPromise;
    const info = helpers.getStatusInfo({ fecha_proxima: pastDate });
    const expectedDays = calculateExpectedDays(pastDate);

    assert.strictEqual(info.estado, 'vencido');
    assert.strictEqual(info.dias_restantes, expectedDays);
});
