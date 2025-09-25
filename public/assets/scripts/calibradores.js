(function (window, document) {
    'use strict';

    const resolveUrl = typeof window.resolveAppUrl === 'function'
        ? window.resolveAppUrl
        : (path) => path;

    async function apiGet(path, params = {}) {
        const url = new URL(resolveUrl(`/backend/integraciones/calibradores/${path}`), window.location.origin);
        Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined && value !== null && value !== '') {
                url.searchParams.set(key, value);
            }
        });
        const response = await fetch(url.toString(), { credentials: 'include' });
        const data = await response.json();
        if (!data || data.success === false) {
            const message = data && data.message ? data.message : 'Error al consultar calibradores';
            throw new Error(message);
        }
        return data.data || data;
    }

    async function apiPost(path, formData) {
        const response = await fetch(resolveUrl(`/backend/integraciones/calibradores/${path}`), {
            method: 'POST',
            body: formData,
            credentials: 'include',
        });
        const data = await response.json();
        if (!data || data.success === false) {
            const message = data && data.message ? data.message : 'Error al procesar la solicitud';
            throw new Error(message);
        }
        return data.data || data;
    }

    function formatDateTime(value) {
        if (!value) {
            return '—';
        }
        try {
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) {
                return value;
            }
            return date.toLocaleString();
        } catch (error) {
            return value;
        }
    }

    function renderCalibradoresTable(container, calibradores) {
        if (!container) {
            return;
        }
        if (!Array.isArray(calibradores) || calibradores.length === 0) {
            container.innerHTML = '<tr><td colspan="6" class="text-center">No se han registrado calibradores.</td></tr>';
            return;
        }
        container.innerHTML = calibradores.map((calibrador) => `
            <tr data-calibrador-id="${calibrador.id}">
                <td>${calibrador.nombre}</td>
                <td>${calibrador.numero_serie || '—'}</td>
                <td>${calibrador.tipo || '—'}</td>
                <td>${calibrador.instrumento_id_default || '—'}</td>
                <td>${formatDateTime(calibrador.ultimo_contacto)}</td>
                <td>
                    <span class="badge ${calibrador.activo ? 'bg-success' : 'bg-secondary'}">${calibrador.activo ? 'Activo' : 'Inactivo'}</span>
                </td>
            </tr>
        `).join('');
    }

    async function loadCalibradoresAdmin() {
        const tableBody = document.getElementById('calibradores-table-body');
        const counter = document.getElementById('calibradores-total');
        try {
            const data = await apiGet('list_calibradores.php');
            renderCalibradoresTable(tableBody, data);
            if (counter) {
                counter.textContent = Array.isArray(data) ? data.length : 0;
            }
        } catch (error) {
            console.error(error);
            if (tableBody) {
                tableBody.innerHTML = `<tr><td colspan="6" class="text-center text-danger">${error.message}</td></tr>`;
            }
        }
    }

    function bindCreateCalibradorForm() {
        const form = document.getElementById('crear-calibrador-form');
        if (!form) {
            return;
        }
        form.addEventListener('submit', async (event) => {
            event.preventDefault();
            const submitBtn = form.querySelector('button[type="submit"]');
            if (submitBtn) {
                submitBtn.disabled = true;
                submitBtn.innerText = 'Guardando…';
            }
            const formData = new FormData(form);
            try {
                await apiPost('create_calibrador.php', formData);
                form.reset();
                await loadCalibradoresAdmin();
            } catch (error) {
                alert(error.message);
            } finally {
                if (submitBtn) {
                    submitBtn.disabled = false;
                    submitBtn.innerText = 'Registrar calibrador';
                }
            }
        });
    }

    function bindLinkForm() {
        const form = document.getElementById('vincular-calibrador-form');
        if (!form) {
            return;
        }
        form.addEventListener('submit', async (event) => {
            event.preventDefault();
            const calibradorId = form.querySelector('[name="calibrador_id"]').value;
            const instrumentoId = form.querySelector('[name="instrumento_id"]').value;
            if (!calibradorId) {
                alert('Selecciona un calibrador.');
                return;
            }
            const formData = new FormData();
            formData.append('calibrador_id', calibradorId);
            if (instrumentoId) {
                formData.append('instrumento_id', instrumentoId);
            }
            try {
                await apiPost('link_instrument.php', formData);
                await loadCalibradoresAdmin();
                form.reset();
            } catch (error) {
                alert(error.message);
            }
        });
    }

    async function initAdminPage() {
        await loadCalibradoresAdmin();
        bindCreateCalibradorForm();
        bindLinkForm();
        const refreshBtn = document.getElementById('refresh-calibradores');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', loadCalibradoresAdmin);
        }
    }

    function populateCalibradorSelect(select, calibradores) {
        if (!select) {
            return;
        }
        select.innerHTML = '<option value="">— Selecciona —</option>';
        calibradores.forEach((calibrador) => {
            const option = document.createElement('option');
            option.value = calibrador.id;
            option.textContent = calibrador.nombre;
            select.appendChild(option);
        });
    }

    function renderMeasurementStatus(container, measurement) {
        if (!container) {
            return;
        }
        if (!measurement) {
            container.innerHTML = '<p class="text-muted mb-0">Sin lecturas recientes.</p>';
            return;
        }
        const payloadPreview = measurement.payload_json ? `<pre class="bg-light p-2 rounded small">${measurement.payload_json}</pre>` : '';
        container.innerHTML = `
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>Última medición:</strong> ${formatDateTime(measurement.fecha_lectura)}<br>
                    <span class="text-muted">UUID: ${measurement.measurement_uuid}</span><br>
                    <span>Valor: ${measurement.valor !== null ? measurement.valor : '—'} ${measurement.unidad || ''}</span>
                </div>
                <button type="button" class="btn btn-outline-primary btn-sm" data-assign-measurement="${measurement.id}">
                    Usar medición
                </button>
            </div>
            ${payloadPreview}
        `;
    }

    function bindMeasurementSelection(container, hiddenInput) {
        if (!container || !hiddenInput) {
            return;
        }
        container.addEventListener('click', (event) => {
            const target = event.target.closest('[data-assign-measurement]');
            if (!target) {
                return;
            }
            const measurementId = target.getAttribute('data-assign-measurement');
            hiddenInput.value = measurementId;
            target.textContent = 'Medición seleccionada';
            target.classList.remove('btn-outline-primary');
            target.classList.add('btn-success');
        });
    }

    async function initCalibrationWidget() {
        const select = document.getElementById('calibrator-select');
        const statusContainer = document.getElementById('calibrator-status');
        const hiddenMeasurement = document.getElementById('measurement-id-field');
        const instrumentSelect = document.getElementById('instrument');

        if (!select || !statusContainer || !hiddenMeasurement) {
            return;
        }

        let currentCalibrator = null;
        let lastMeasurementId = null;

        try {
            const calibradores = await apiGet('list_calibradores.php');
            populateCalibradorSelect(select, calibradores);
        } catch (error) {
            statusContainer.innerHTML = `<div class="text-danger">${error.message}</div>`;
        }

        async function refreshMeasurements() {
            if (!currentCalibrator) {
                return;
            }
            const params = { calibrador_id: currentCalibrator, limit: 1 };
            if (instrumentSelect && instrumentSelect.value) {
                params.instrumento_id = instrumentSelect.value;
            }
            try {
                const data = await apiGet('list_measurements.php', params);
                const measurement = Array.isArray(data) ? data[0] : null;
                renderMeasurementStatus(statusContainer, measurement);
                if (measurement && measurement.id !== lastMeasurementId) {
                    lastMeasurementId = measurement.id;
                    if (hiddenMeasurement.value && hiddenMeasurement.value !== String(measurement.id)) {
                        hiddenMeasurement.value = '';
                    }
                }
            } catch (error) {
                statusContainer.innerHTML = `<div class="text-danger">${error.message}</div>`;
            }
        }

        bindMeasurementSelection(statusContainer, hiddenMeasurement);

        select.addEventListener('change', () => {
            currentCalibrator = select.value || null;
            hiddenMeasurement.value = '';
            lastMeasurementId = null;
            if (!currentCalibrator) {
                statusContainer.innerHTML = '<p class="text-muted mb-0">Selecciona un calibrador para ver lecturas recientes.</p>';
                return;
            }
            refreshMeasurements();
        });

        if (instrumentSelect) {
            instrumentSelect.addEventListener('change', () => {
                if (currentCalibrator) {
                    refreshMeasurements();
                }
            });
        }

        setInterval(() => {
            if (currentCalibrator) {
                refreshMeasurements();
            }
        }, 5000);
    }

    document.addEventListener('DOMContentLoaded', () => {
        const adminRoot = document.querySelector('[data-calibradores-admin]');
        if (adminRoot) {
            initAdminPage();
        }
        const calibrationWidget = document.querySelector('[data-calibration-widget]');
        if (calibrationWidget) {
            initCalibrationWidget();
        }
    });

    window.CalibradoresUI = {
        initAdminPage,
        initCalibrationWidget,
    };
})(window, document);
