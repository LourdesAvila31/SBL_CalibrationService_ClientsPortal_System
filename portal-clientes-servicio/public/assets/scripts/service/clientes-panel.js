const appUrl = typeof window !== 'undefined' && window.AppUrl ? window.AppUrl : {};
const normalizeAppUrl = typeof appUrl.normalizeAppUrl === 'function'
    ? appUrl.normalizeAppUrl
    : value => value;
const resolveAppUrl = typeof appUrl.resolveAppUrl === 'function'
    ? appUrl.resolveAppUrl
    : normalizeAppUrl;

const ENDPOINTS = {
    empresas: '/backend/clientes/service_list.php',
    solicitudes: '/backend/clientes/service_requests.php',
    calibraciones: '/backend/clientes/service_calibrations.php',
    actualizar: '/backend/clientes/service_update_calibration.php',
    certificados: '/backend/clientes/service_attach_certificate.php'
};

const FRIENDLY_MESSAGES = {
    forbidden: 'Tu sesión no tiene permisos para consultar este módulo. Solicita a la administración que habilite tu acceso.'
};

const state = {
    empresas: [],
    solicitudes: [],
    calibraciones: [],
    empresaSolicitudes: '',
    empresaCalibraciones: '',
    calibracionSeleccionada: null
};

async function fetchJson(resource, options = {}) {
    const response = await fetch(resolveAppUrl(resource), options);
    let data;

    try {
        data = await response.json();
    } catch (error) {
        const parseError = new Error('Respuesta no válida del servidor');
        parseError.status = response.status;
        parseError.payload = null;
        throw parseError;
    }

    if (!response.ok || (data && typeof data === 'object' && data.error)) {
        const message = data && data.error ? data.error : `HTTP ${response.status}`;
        const requestError = new Error(message);
        requestError.status = response.status;
        requestError.payload = data;
        throw requestError;
    }

    return data;
}

function getFriendlyMessage(error, fallback) {
    if (error && error.status === 403) {
        return FRIENDLY_MESSAGES.forbidden;
    }
    return fallback;
}

function setTextContent(id, value) {
    const el = document.getElementById(id);
    if (el) {
        el.textContent = value;
    }
}

function updateSummary({ blocked = false } = {}) {
    if (blocked) {
        ['summary-empresas', 'summary-clientes', 'summary-instrumentos', 'summary-solicitudes']
            .forEach(id => setTextContent(id, '—'));
        return;
    }

    const totalEmpresas = state.empresas.length;
    const totalClientes = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.clientes || 0), 0);
    const totalInstrumentos = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.instrumentos || 0), 0);
    const totalSolicitudes = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.solicitudes_pendientes || 0), 0);

    setTextContent('summary-empresas', totalEmpresas.toString());
    setTextContent('summary-clientes', totalClientes.toString());
    setTextContent('summary-instrumentos', totalInstrumentos.toString());
    setTextContent('summary-solicitudes', totalSolicitudes.toString());
}

function createMessageRow(message, colspan) {
    const row = document.createElement('tr');
    row.className = 'empty-row';
    const cell = document.createElement('td');
    cell.colSpan = colspan;
    cell.textContent = message;
    row.appendChild(cell);
    return row;
}

function renderCompanies(message = '') {
    const tbody = document.getElementById('companies-body');
    if (!tbody) return;

    tbody.innerHTML = '';

    if (message) {
        tbody.appendChild(createMessageRow(message, 6));
        return;
    }

    if (!state.empresas.length) {
        tbody.appendChild(createMessageRow('No se encontraron empresas registradas.', 6));
        return;
    }

    state.empresas.forEach(empresa => {
        const row = document.createElement('tr');
        const contacto = empresa.contacto || 'Sin asignar';
        const clientes = Array.isArray(empresa.clientes) ? empresa.clientes : [];
        const clientesList = clientes.length
            ? clientes.map(c => c.nombre || c.correo).join(', ')
            : 'Sin clientes';

        const cells = [
            empresa.nombre || 'Empresa sin nombre',
            `${contacto}${empresa.telefono ? ` • ${empresa.telefono}` : ''}`,
            clientesList,
            (empresa.totales?.instrumentos ?? 0).toString(),
            (empresa.totales?.calibraciones ?? 0).toString(),
            (empresa.totales?.solicitudes_pendientes ?? 0).toString()
        ];

        cells.forEach(value => {
            const cell = document.createElement('td');
            cell.textContent = value;
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });
}

function setSelectPlaceholder(select, text) {
    if (!select) return;
    select.innerHTML = '';
    const option = document.createElement('option');
    option.value = '';
    option.textContent = text;
    option.disabled = true;
    option.selected = true;
    select.appendChild(option);
    select.disabled = true;
}

function populateSelect(select, items, { includeAll = false, placeholder = 'Selecciona una opción' } = {}) {
    if (!select) return;
    const current = select.value;
    select.innerHTML = '';

    if (includeAll) {
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = 'Todas';
        select.appendChild(opt);
    } else {
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = placeholder;
        select.appendChild(opt);
    }

    items.forEach(item => {
        const option = document.createElement('option');
        option.value = item.id;
        option.textContent = item.nombre || `Empresa #${item.id}`;
        select.appendChild(option);
    });

    if (current && select.querySelector(`option[value="${current}"]`)) {
        select.value = current;
    }

    select.disabled = !items.length && !includeAll;
}

function renderRequests(message = '') {
    const tbody = document.getElementById('requests-body');
    if (!tbody) return;

    tbody.innerHTML = '';

    if (message) {
        tbody.appendChild(createMessageRow(message, 7));
        return;
    }

    if (!state.solicitudes.length) {
        tbody.appendChild(createMessageRow('Sin solicitudes registradas.', 7));
        return;
    }

    state.solicitudes.forEach(item => {
        const row = document.createElement('tr');
        const instrucciones = typeof item.instrucciones_cliente === 'string'
            ? item.instrucciones_cliente.trim()
            : '';
        const cells = [
            item.empresa_nombre || 'NA',
            item.instrumento_nombre || item.instrumento_codigo || 'NA',
            item.tipo || 'Solicitud',
            item.solicitante || 'Cliente',
            item.fecha_deseada || 'Sin definir',
            instrucciones || 'Sin instrucciones',
            item.estado || 'Pendiente'
        ];

        cells.forEach((value, index) => {
            const cell = document.createElement('td');
            cell.textContent = value;
            if (index === 5 && instrucciones) {
                cell.title = instrucciones;
            }
            row.appendChild(cell);
        });

        tbody.appendChild(row);
    });
}

function renderCalibrations(message = '') {
    const select = document.getElementById('calibration-select');
    const certificates = document.getElementById('calibration-certificates');
    if (!select) return;

    select.innerHTML = '';

    if (message) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = message;
        option.disabled = true;
        option.selected = true;
        select.appendChild(option);
        select.disabled = true;

        if (certificates) {
            certificates.innerHTML = '';
            const li = document.createElement('li');
            li.className = 'text-muted';
            li.textContent = message;
            certificates.appendChild(li);
        }

        fillCalibrationForm(null);
        return;
    }

    if (!state.calibraciones.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'Selecciona una calibración';
        option.disabled = true;
        option.selected = true;
        select.appendChild(option);
        select.disabled = true;

        if (certificates) {
            certificates.innerHTML = '';
        }

        fillCalibrationForm(null);
        return;
    }

    select.disabled = false;
    const current = state.calibracionSeleccionada ? String(state.calibracionSeleccionada) : '';
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = 'Selecciona una calibración';
    placeholder.disabled = true;
    placeholder.selected = !current;
    select.appendChild(placeholder);

    state.calibraciones.forEach(cal => {
        const option = document.createElement('option');
        option.value = cal.id;
        const labelParts = [];
        if (cal.instrumento_nombre) labelParts.push(cal.instrumento_nombre);
        if (cal.instrumento_codigo) labelParts.push(`#${cal.instrumento_codigo}`);
        if (cal.fecha_calibracion) labelParts.push(cal.fecha_calibracion);
        option.textContent = labelParts.join(' • ') || `Calibración ${cal.id}`;
        select.appendChild(option);
    });

    if (certificates) {
        certificates.innerHTML = '';
    }

    if (current && select.querySelector(`option[value="${current}"]`)) {
        select.value = current;
        const match = state.calibraciones.find(item => String(item.id) === current);
        fillCalibrationForm(match || null);
    } else {
        select.value = '';
        fillCalibrationForm(null);
    }
}

function fillCalibrationForm(calibration) {
    const updateForm = document.getElementById('calibration-update-form');
    const certificateForm = document.getElementById('certificate-form');
    const certList = document.getElementById('calibration-certificates');

    state.calibracionSeleccionada = calibration ? calibration.id : null;

    const assignValue = (id, value) => {
        const el = document.getElementById(id);
        if (el) {
            el.value = value || '';
        }
    };

    assignValue('calibration-id', calibration ? calibration.id : '');
    assignValue('calibration-date', calibration ? calibration.fecha_calibracion : '');
    assignValue('calibration-next', calibration ? calibration.fecha_proxima : '');
    assignValue('calibration-result', calibration ? calibration.resultado : '');
    assignValue('calibration-type', calibration ? calibration.tipo : '');

    const notes = document.getElementById('calibration-notes');
    if (notes) {
        notes.value = calibration ? calibration.observaciones || '' : '';
    }

    assignValue('certificate-calibration-id', calibration ? calibration.id : '');

    const toggleForm = (form, disabled) => {
        if (!form) return;
        form.querySelectorAll('input, textarea, button, select').forEach(input => {
            if (input.name === 'calibracion_id') return;
            input.disabled = disabled;
        });
    };

    toggleForm(updateForm, !calibration);
    toggleForm(certificateForm, !calibration);

    if (certList) {
        certList.innerHTML = '';
        if (calibration && Array.isArray(calibration.certificados) && calibration.certificados.length) {
            calibration.certificados.forEach(cert => {
                const li = document.createElement('li');
                const link = document.createElement('a');
                const file = cert.archivo || '';
                link.textContent = file || `Certificado ${cert.id}`;
                link.href = resolveAppUrl(`/backend/calibraciones/certificates/${encodeURIComponent(file)}`);
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                li.appendChild(link);
                certList.appendChild(li);
            });
        }
    }
}

function showStatus(id, message, isError = false) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = message || '';
    el.classList.remove('success', 'error');
    if (message) {
        el.classList.add(isError ? 'error' : 'success');
    }
}

async function loadEmpresas() {
    try {
        const data = await fetchJson(ENDPOINTS.empresas);
        state.empresas = Array.isArray(data.empresas) ? data.empresas : [];
        updateSummary();
        renderCompanies();

        populateSelect(document.getElementById('requests-company'), state.empresas, { includeAll: true });
        populateSelect(document.getElementById('calibration-company'), state.empresas, { placeholder: 'Selecciona una empresa' });
    } catch (error) {
        console.error('Error al cargar empresas', error);
        state.empresas = [];
        const message = getFriendlyMessage(error, 'No fue posible recuperar las empresas registradas.');
        updateSummary({ blocked: error?.status === 403 });
        renderCompanies(message);

        const requestsSelect = document.getElementById('requests-company');
        const calibrationSelect = document.getElementById('calibration-company');
        if (error && error.status === 403) {
            setSelectPlaceholder(requestsSelect, message);
            setSelectPlaceholder(calibrationSelect, message);
        } else {
            populateSelect(requestsSelect, [], { includeAll: true });
            setSelectPlaceholder(calibrationSelect, 'Sin empresas disponibles');
        }
    }
}

async function loadSolicitudes() {
    const params = new URLSearchParams();
    if (state.empresaSolicitudes) {
        params.append('empresa_id', state.empresaSolicitudes);
    }

    const estado = document.getElementById('requests-status')?.value || '';
    const desde = document.getElementById('requests-from')?.value || '';
    const hasta = document.getElementById('requests-to')?.value || '';
    if (estado) params.append('estado', estado);
    if (desde) params.append('desde', desde);
    if (hasta) params.append('hasta', hasta);

    const resource = params.toString()
        ? `${ENDPOINTS.solicitudes}?${params.toString()}`
        : ENDPOINTS.solicitudes;

    try {
        const data = await fetchJson(resource);
        state.solicitudes = Array.isArray(data.solicitudes) ? data.solicitudes : [];
        renderRequests();
    } catch (error) {
        console.error('Error al cargar solicitudes', error);
        state.solicitudes = [];
        const message = getFriendlyMessage(error, 'No fue posible recuperar las solicitudes.');
        renderRequests(message);
    }
}

async function loadCalibraciones() {
    if (!state.empresaCalibraciones) {
        state.calibraciones = [];
        renderCalibrations('Selecciona una empresa para revisar las calibraciones.');
        return;
    }

    const params = new URLSearchParams({ empresa_id: state.empresaCalibraciones });

    try {
        const data = await fetchJson(`${ENDPOINTS.calibraciones}?${params.toString()}`);
        state.calibraciones = Array.isArray(data.calibraciones) ? data.calibraciones : [];
        renderCalibrations();
    } catch (error) {
        console.error('Error al cargar calibraciones', error);
        state.calibraciones = [];
        const message = getFriendlyMessage(error, 'No fue posible recuperar las calibraciones.');
        renderCalibrations(message);
        if (error && error.status === 403) {
            showStatus('calibration-update-status', message, true);
            showStatus('certificate-status', message, true);
        }
    }
}

function handleRequestsFilter(event) {
    event.preventDefault();
    const select = document.getElementById('requests-company');
    state.empresaSolicitudes = select ? select.value : '';
    loadSolicitudes();
}

function handleCalibrationCompanyChange(event) {
    state.empresaCalibraciones = event.target.value || '';
    state.calibracionSeleccionada = null;
    loadCalibraciones();
}

function handleCalibrationSelection(event) {
    const id = event.target.value;
    const calibration = state.calibraciones.find(item => String(item.id) === id) || null;
    fillCalibrationForm(calibration);
}

function ensurePermissions() {
    const flags = window.permissionFlags || {};
    if (!flags.canManageClientesServicio && !flags.canManageClientesService) {
        const message = 'No cuentas con permisos para gestionar clientes.';
        showStatus('calibration-update-status', message, true);
        showStatus('certificate-status', message, true);
    }
}

function readFormData(form) {
    const formData = new FormData(form);
    const payload = {};
    formData.forEach((value, key) => {
        if (value !== '' && key !== 'calibracion_id') {
            payload[key] = value;
        }
    });
    return payload;
}

function handleUpdateCalibration(event) {
    event.preventDefault();
    if (!state.calibracionSeleccionada) {
        showStatus('calibration-update-status', 'Selecciona una calibración antes de guardar.', true);
        return;
    }

    const form = event.currentTarget;
    const payload = readFormData(form);
    payload.calibracion_id = state.calibracionSeleccionada;

    fetch(resolveAppUrl(ENDPOINTS.actualizar), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
        .then(resp => resp.json().catch(() => ({})).then(body => ({ ok: resp.ok, status: resp.status, body })))
        .then(({ ok, status, body }) => {
            if (!ok || body?.error) {
                const errorMessage = status === 403
                    ? FRIENDLY_MESSAGES.forbidden
                    : (body?.error || 'No fue posible actualizar la calibración.');
                throw new Error(errorMessage);
            }
            showStatus('calibration-update-status', 'Cambios guardados correctamente.');
            return loadCalibraciones();
        })
        .catch(error => {
            console.error('Error al actualizar calibración', error);
            showStatus('calibration-update-status', error.message || 'Error al guardar.', true);
        });
}

function handleCertificateUpload(event) {
    event.preventDefault();
    if (!state.calibracionSeleccionada) {
        showStatus('certificate-status', 'Selecciona una calibración para adjuntar el certificado.', true);
        return;
    }

    const form = event.currentTarget;
    const formData = new FormData(form);
    formData.set('calibracion_id', state.calibracionSeleccionada);

    fetch(resolveAppUrl(ENDPOINTS.certificados), {
        method: 'POST',
        body: formData
    })
        .then(resp => resp.json().catch(() => ({})).then(body => ({ ok: resp.ok, status: resp.status, body })))
        .then(({ ok, status, body }) => {
            if (!ok || body?.error) {
                const errorMessage = status === 403
                    ? FRIENDLY_MESSAGES.forbidden
                    : (body?.error || 'No fue posible adjuntar el certificado.');
                throw new Error(errorMessage);
            }
            showStatus('certificate-status', 'Certificado adjuntado correctamente.');
            form.reset();
            return loadCalibraciones();
        })
        .catch(error => {
            console.error('Error al adjuntar certificado', error);
            showStatus('certificate-status', error.message || 'Error al adjuntar el certificado.', true);
        });
}

function initEvents() {
    const filterForm = document.getElementById('requests-filter');
    if (filterForm) {
        filterForm.addEventListener('submit', handleRequestsFilter);
    }

    const selectRequests = document.getElementById('requests-company');
    if (selectRequests) {
        selectRequests.addEventListener('change', event => {
            state.empresaSolicitudes = event.target.value || '';
            loadSolicitudes();
        });
    }

    const calibrationCompany = document.getElementById('calibration-company');
    if (calibrationCompany) {
        calibrationCompany.addEventListener('change', handleCalibrationCompanyChange);
    }

    const calibrationSelect = document.getElementById('calibration-select');
    if (calibrationSelect) {
        calibrationSelect.addEventListener('change', handleCalibrationSelection);
    }

    const updateForm = document.getElementById('calibration-update-form');
    if (updateForm) {
        updateForm.addEventListener('submit', handleUpdateCalibration);
    }

    const certificateForm = document.getElementById('certificate-form');
    if (certificateForm) {
        certificateForm.addEventListener('submit', handleCertificateUpload);
    }
}

function onReady() {
    initEvents();
    ensurePermissions();

    Promise.resolve()
        .then(() => (typeof window.fetchCurrentUser === 'function' ? window.fetchCurrentUser() : null))
        .catch(() => null)
        .then(() => loadEmpresas())
        .then(() => loadSolicitudes());

    document.addEventListener('permissionsready', ensurePermissions);
}

document.addEventListener('DOMContentLoaded', onReady);

export {};
