(function(){
    const appUrl = typeof window !== 'undefined' && window.AppUrl ? window.AppUrl : {};
    const normalizeAppUrl = typeof appUrl.normalizeAppUrl === 'function'
        ? appUrl.normalizeAppUrl
        : (value => value);
    const resolveAppUrl = typeof appUrl.resolveAppUrl === 'function'
        ? appUrl.resolveAppUrl
        : normalizeAppUrl;
    const formatDate = (dateStr) => {
        if (!dateStr) return 'NA';
        try {
            const date = new Date(dateStr);
            if (Number.isNaN(date.getTime())) return dateStr;
            if (typeof window.formatFechaCorta === 'function') {
                return window.formatFechaCorta(dateStr);
            }
            return date.toLocaleDateString('es-ES', { day: '2-digit', month: 'short', year: 'numeric' });
        } catch (e) {
            return dateStr;
        }
    };

    const normalizeRole = (role) => {
        if (!role) return '';
        return role.toString().trim().toLowerCase();
    };

    const hasClientRole = () => {
        const role = normalizeRole((window.currentUser && window.currentUser.role_name) || (window.currentUser && window.currentUser.role) || '');
        return role === 'cliente';
    };

    const showMessage = (element, message, isError = false) => {
        if (!element) return;
        element.textContent = message;
        element.classList.remove('success', 'error');
        if (message) {
            element.classList.add(isError ? 'error' : 'success');
        }
    };

    const toggleRequestAvailability = (hasInstruments) => {
        const helper = document.getElementById('request-helper');
        const submit = document.getElementById('request-submit');
        const select = document.getElementById('request-instrument');
        if (helper) {
            helper.hidden = hasInstruments;
        }
        if (submit) {
            submit.disabled = !hasInstruments;
        }
        if (select) {
            select.disabled = !hasInstruments;
        }
    };

    const populateSelect = (select, items) => {
        if (!select) return;
        select.innerHTML = '';
        if (!Array.isArray(items) || items.length === 0) {
            const option = document.createElement('option');
            option.value = '';
            option.disabled = true;
            option.selected = true;
            option.textContent = 'Sin instrumentos disponibles';
            select.appendChild(option);
            toggleRequestAvailability(false);
            return;
        }
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.disabled = true;
        placeholder.selected = true;
        placeholder.textContent = 'Selecciona un instrumento';
        select.appendChild(placeholder);
        items.forEach(item => {
            const option = document.createElement('option');
            option.value = item.id;
            const label = [item.nombre, item.codigo, item.serie].filter(Boolean).join(' • ');
            option.textContent = label || `Instrumento #${item.id}`;
            select.appendChild(option);
        });
        toggleRequestAvailability(true);
    };

    const renderCalibrations = (rows) => {
        const tbody = document.querySelector('#calibration-history tbody');
        const emptyRow = document.getElementById('calibration-empty');
        if (!tbody) return;
        tbody.querySelectorAll('tr').forEach(tr => {
            if (tr !== emptyRow) tr.remove();
        });
        if (!Array.isArray(rows) || rows.length === 0) {
            if (emptyRow) emptyRow.style.display = '';
            return;
        }
        if (emptyRow) emptyRow.style.display = 'none';
        rows.forEach(row => {
            const tr = document.createElement('tr');
            const cells = [
                row.instrumento || 'Sin nombre',
                row.codigo || 'NA',
                formatDate(row.fecha_calibracion),
                formatDate(row.fecha_proxima),
                row.resultado || 'NA'
            ];
            cells.forEach(value => {
                const td = document.createElement('td');
                td.textContent = value;
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
    };

    const renderCertificates = (rows) => {
        const tbody = document.querySelector('#certificate-table tbody');
        const emptyRow = document.getElementById('certificate-empty');
        if (!tbody) return;
        tbody.querySelectorAll('tr').forEach(tr => {
            if (tr !== emptyRow) tr.remove();
        });
        const certificates = [];
        (rows || []).forEach(row => {
            if (Array.isArray(row.certificates)) {
                row.certificates.forEach(cert => {
                    certificates.push({
                        instrument: row.instrumento || 'Instrumento',
                        date: row.fecha_calibracion,
                        file: cert.file,
                        id: cert.id
                    });
                });
            }
        });
        if (certificates.length === 0) {
            if (emptyRow) emptyRow.style.display = '';
            return;
        }
        if (emptyRow) emptyRow.style.display = 'none';
        certificates.forEach(cert => {
            const tr = document.createElement('tr');
            const tdInstrument = document.createElement('td');
            tdInstrument.textContent = cert.instrument;
            const tdDate = document.createElement('td');
            tdDate.textContent = formatDate(cert.date);
            const tdLink = document.createElement('td');
            const link = document.createElement('a');
            link.href = resolveAppUrl(`/backend/clientes/download_certificate.php?id=${encodeURIComponent(cert.id)}`);
            link.textContent = cert.file || 'Descargar certificado';
            link.target = '_blank';
            link.rel = 'noopener noreferrer';
            tdLink.appendChild(link);
            tr.appendChild(tdInstrument);
            tr.appendChild(tdDate);
            tr.appendChild(tdLink);
            tbody.appendChild(tr);
        });
    };

    const loadInstruments = () => {
        return fetch(resolveAppUrl('/backend/clientes/my_instruments.php'))
            .then(resp => {
                if (!resp.ok) throw new Error('No se pudieron obtener los instrumentos');
                return resp.json();
            })
            .then(data => {
                const select = document.getElementById('request-instrument');
                populateSelect(select, data.instruments || []);
            })
            .catch(err => {
                console.error(err);
                toggleRequestAvailability(false);
            });
    };

    const loadCalibrations = () => {
        return fetch(resolveAppUrl('/backend/clientes/get_calibrations.php'))
            .then(resp => {
                if (!resp.ok) throw new Error('No se pudo obtener el historial de calibraciones');
                return resp.json();
            })
            .then(data => {
                const rows = data.calibrations || [];
                renderCalibrations(rows);
                renderCertificates(rows);
            })
            .catch(err => {
                console.error(err);
            });
    };

    const setupForms = () => {
        const instrumentForm = document.getElementById('instrument-form');
        const requestForm = document.getElementById('calibration-request-form');
        const instrumentMsg = document.getElementById('instrument-message');
        const requestMsg = document.getElementById('request-message');

        if (instrumentForm) {
            instrumentForm.addEventListener('submit', (event) => {
                event.preventDefault();
                showMessage(instrumentMsg, 'Guardando...', false);
                const formData = new FormData(instrumentForm);
                fetch(resolveAppUrl('/backend/clientes/register_instrument.php'), {
                    method: 'POST',
                    body: formData
                }).then(resp => {
                    if (!resp.ok) {
                        throw new Error('Error al registrar el instrumento');
                    }
                    return resp.json();
                }).then(data => {
                    if (data.success) {
                        instrumentForm.reset();
                        showMessage(instrumentMsg, data.message || 'Instrumento registrado correctamente');
                        loadInstruments();
                    } else {
                        throw new Error(data.message || 'No fue posible guardar el instrumento');
                    }
                }).catch(err => {
                    showMessage(instrumentMsg, err.message || 'Error inesperado', true);
                });
            });
        }

        if (requestForm) {
            let instructionsField = requestForm.querySelector('[name="instrucciones_cliente"]');
            if (!instructionsField) {
                const wrapper = document.createElement('div');
                wrapper.className = 'form-group';
                const label = document.createElement('label');
                label.setAttribute('for', 'request-instructions');
                label.textContent = 'Instrucciones especiales';
                const textarea = document.createElement('textarea');
                textarea.id = 'request-instructions';
                textarea.name = 'instrucciones_cliente';
                textarea.rows = 3;
                textarea.placeholder = 'Comparte condiciones especiales para la calibración';
                wrapper.appendChild(label);
                wrapper.appendChild(textarea);
                const messageNode = requestForm.querySelector('#request-message');
                if (messageNode && messageNode.parentNode) {
                    messageNode.parentNode.insertBefore(wrapper, messageNode);
                } else {
                    requestForm.appendChild(wrapper);
                }
                instructionsField = textarea;
            }
            requestForm.addEventListener('submit', (event) => {
                event.preventDefault();
                if (requestForm.querySelector('#request-instrument').disabled) {
                    showMessage(requestMsg, 'Registre al menos un instrumento antes de solicitar calibraciones.', true);
                    return;
                }
                showMessage(requestMsg, 'Enviando solicitud...', false);
                const formData = new FormData(requestForm);
                if (instructionsField) {
                    const trimmed = instructionsField.value.trim();
                    instructionsField.value = trimmed;
                    formData.set('instrucciones_cliente', trimmed);
                }
                fetch(resolveAppUrl('/backend/clientes/request_calibration.php'), {
                    method: 'POST',
                    body: formData
                }).then(resp => {
                    if (!resp.ok) {
                        throw new Error('Error al enviar la solicitud');
                    }
                    return resp.json();
                }).then(data => {
                    if (data.success) {
                        requestForm.reset();
                        showMessage(requestMsg, data.message || 'Solicitud enviada correctamente');
                    } else {
                        throw new Error(data.message || 'No fue posible registrar la solicitud');
                    }
                }).catch(err => {
                    showMessage(requestMsg, err.message || 'Error inesperado', true);
                });
            });
        }
    };

    const init = () => {
        if (!hasClientRole()) {
            // El backend seguirá validando, pero la vista puede quedar en solo lectura.
            toggleRequestAvailability(false);
        }
        loadInstruments();
        loadCalibrations();
        setupForms();
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
