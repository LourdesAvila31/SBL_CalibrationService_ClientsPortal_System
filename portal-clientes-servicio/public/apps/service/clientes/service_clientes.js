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
    certificados: '/backend/clientes/service_attach_certificate.php',
};

const state = {
    empresas: [],
    solicitudes: [],
    calibraciones: [],
    empresaSolicitudes: '',
    empresaCalibraciones: '',
    calibracionSeleccionada: null,
};

function fetchJson(resource, options = {}) {
    return fetch(resolveAppUrl(resource), options).then(resp => {
        if (!resp.ok) {
            return resp.json().catch(() => ({})).then(body => {
                const message = body && body.error ? body.error : `HTTP ${resp.status}`;
                const error = new Error(message);
                error.status = resp.status;
                error.payload = body;
                throw error;
            });
        }
        return resp.json();
    });
}

function updateSummary() {
    const totalEmpresas = state.empresas.length;
    const totalClientes = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.clientes || 0), 0);
    const totalInstrumentos = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.instrumentos || 0), 0);
    const totalSolicitudes = state.empresas.reduce((acc, empresa) => acc + (empresa.totales?.solicitudes_pendientes || 0), 0);

    const setValue = (id, value) => {
        const el = document.getElementById(id);
        if (el) {
            el.textContent = value.toString();
        }
    };

    setValue('summary-empresas', totalEmpresas);
    setValue('summary-clientes', totalClientes);
    setValue('summary-instrumentos', totalInstrumentos);
    setValue('summary-solicitudes', totalSolicitudes);
}

function renderCompanies() {
    const tbody = document.getElementById('companies-body');
    if (!tbody) return;

    tbody.innerHTML = '';
    if (!state.empresas.length) {
        const row = document.createElement('tr');
        row.className = 'empty-row';
        const cell = document.createElement('td');
        cell.colSpan = 6;
        cell.textContent = 'No se encontraron empresas registradas.';
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }

    state.empresas.forEach(empresa => {
        const row = document.createElement('tr');
        const contacto = empresa.contacto || 'Sin asignar';
        const clientes = empresa.clientes || [];
        const clientesList = clientes.length
            ? clientes.map(c => c.nombre || c.correo).join(', ')
            : 'Sin clientes';

        const cells = [
            empresa.nombre || 'Empresa sin nombre',
            `${contacto}${empresa.telefono ? ` • ${empresa.telefono}` : ''}`,
            clientesList,
            (empresa.totales?.instrumentos ?? 0).toString(),
            (empresa.totales?.calibraciones ?? 0).toString(),
            (empresa.totales?.solicitudes_pendientes ?? 0).toString(),
        ];

        cells.forEach(value => {
            const cell = document.createElement('td');
            cell.textContent = value;
            row.appendChild(cell);
        });
        tbody.appendChild(row);
    });
}

function populateSelect(select, items, { includeAll = false } = {}) {
    if (!select) return;
    const current = select.value;
    select.innerHTML = '';
    if (includeAll) {
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = 'Todas';
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
}

function renderRequests() {
    const tbody = document.getElementById('requests-body');
    if (!tbody) return;

    tbody.innerHTML = '';
    if (!state.solicitudes.length) {
        const row = document.createElement('tr');
        row.className = 'empty-row';
        const cell = document.createElement('td');
        cell.colSpan = 7;
        cell.textContent = 'Sin solicitudes registradas.';
        row.appendChild(cell);
        tbody.appendChild(row);
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
            item.estado || 'Pendiente',
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

function renderCalibrations() {
    const select = document.getElementById('calibration-select');
    const certificates = document.getElementById('calibration-certificates');
    if (!select) return;

    const current = select.value;
    select.innerHTML = '';

    if (!state.calibraciones.length) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = 'Selecciona una calibración';
        option.disabled = true;
        option.selected = true;
        select.appendChild(option);
        select.disabled = true;
        if (certificates) certificates.innerHTML = '';
        fillCalibrationForm(null);
        return;
    }

    select.disabled = false;
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
        if (current && current === String(cal.id)) {
            option.selected = true;
        }
        select.appendChild(option);
    });

    if (current && select.querySelector(`option[value="${current}"]`)) {
        select.value = current;
        const match = state.calibraciones.find(item => String(item.id) === current);
        fillCalibrationForm(match || null);
    } else {
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

    if (updateForm) {
        updateForm.querySelectorAll('input, textarea, button').forEach(input => {
            if (input.name === 'calibracion_id') return;
            input.disabled = !calibration;
        });
    }
    if (certificateForm) {
        certificateForm.querySelectorAll('input, button').forEach(input => {
            if (input.name === 'calibracion_id') return;
            input.disabled = !calibration;
        });
    }

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

function loadEmpresas() {
    return fetchJson(ENDPOINTS.empresas)
        .then(data => {
            state.empresas = Array.isArray(data.empresas) ? data.empresas : [];
            updateSummary();
            renderCompanies();
            populateSelect(document.getElementById('requests-company'), state.empresas, { includeAll: true });
            populateSelect(document.getElementById('calibration-company'), state.empresas);
        })
        .catch(error => {
            console.error('Error al cargar empresas', error);
            renderCompanies();
        });
}

function loadSolicitudes() {
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

    return fetchJson(resource)
        .then(data => {
            state.solicitudes = Array.isArray(data.solicitudes) ? data.solicitudes : [];
            renderRequests();
        })
        .catch(error => {
            console.error('Error al cargar solicitudes', error);
            state.solicitudes = [];
            renderRequests();
        });
}

function loadCalibraciones() {
    if (!state.empresaCalibraciones) {
        state.calibraciones = [];
        renderCalibrations();
        return Promise.resolve();
    }
    const params = new URLSearchParams({ empresa_id: state.empresaCalibraciones });
    return fetchJson(`${ENDPOINTS.calibraciones}?${params.toString()}`)
        .then(data => {
            state.calibraciones = Array.isArray(data.calibraciones) ? data.calibraciones : [];
            renderCalibrations();
        })
        .catch(error => {
            console.error('Error al cargar calibraciones', error);
            state.calibraciones = [];
            renderCalibrations();
        });
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

function handleUpdateCalibration(event) {
    event.preventDefault();
    if (!state.calibracionSeleccionada) {
        showStatus('calibration-update-status', 'Selecciona una calibración antes de guardar.', true);
        return;
    }
    const form = event.currentTarget;
    const formData = new FormData(form);
    const payload = {};
    formData.forEach((value, key) => {
        if (value !== '' && key !== 'calibracion_id') {
            payload[key] = value;
        }
    });
    payload.calibracion_id = state.calibracionSeleccionada;

    fetch(resolveAppUrl(ENDPOINTS.actualizar), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    })
        .then(resp => resp.json().then(body => ({ ok: resp.ok, body })))
        .then(({ ok, body }) => {
            if (!ok || body?.error) {
                throw new Error(body?.error || 'No fue posible actualizar la calibración.');
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
        body: formData,
    })
        .then(resp => resp.json().then(body => ({ ok: resp.ok, body })))
        .then(({ ok, body }) => {
            if (!ok || body?.error) {
                throw new Error(body?.error || 'No fue posible adjuntar el certificado.');
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

function ensurePermissions() {
    const flags = window.permissionFlags || {};
    if (!flags.canManageClientesServicio && !flags.canManageClientesService) {
        const message = 'No cuentas con permisos para gestionar clientes.';
        showStatus('calibration-update-status', message, true);
        showStatus('certificate-status', message, true);
    }
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
        .then(() => {
            loadSolicitudes();
        });

    document.addEventListener('permissionsready', ensurePermissions);
}

document.addEventListener('DOMContentLoaded', onReady);
