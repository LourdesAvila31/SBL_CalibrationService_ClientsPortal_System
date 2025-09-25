(function (global) {
    'use strict';

    function parseToken(search) {
        const params = new URLSearchParams((search || '').startsWith('?') ? search : `?${search || ''}`);
        const raw = params.get('token');
        return raw && raw.trim() !== '' ? raw.trim() : null;
    }

    function getBaseUrl(context) {
        const scope = context || globalThis;
        if (scope && typeof scope.PAGE_BASE_URL === 'string' && scope.PAGE_BASE_URL !== '') {
            return scope.PAGE_BASE_URL.replace(/\/$/, '');
        }
        if (scope && scope.location && typeof scope.location.pathname === 'string') {
            const marker = '/public/';
            const path = scope.location.pathname;
            const idx = path.indexOf(marker);
            if (idx !== -1) {
                return path.slice(0, idx + marker.length).replace(/\/$/, '');
            }
        }
        return '';
    }

    function formatDate(value) {
        if (!value) {
            return '';
        }
        try {
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) {
                return value;
            }
            return date.toLocaleDateString('es-ES', {
                year: 'numeric',
                month: 'short',
                day: '2-digit'
            }).replace('.', '');
        } catch (err) {
            return value;
        }
    }

    function setText(id, value) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById(id);
        if (el) {
            el.textContent = value || '';
        }
    }

    function setBadge(id, value, variant) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById(id);
        if (!el) {
            return;
        }
        el.textContent = value || '';
        el.className = variant ? `badge ${variant}` : 'badge bg-light text-dark';
    }

    function showMessage(type, message) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById('qrMessage');
        if (!el) {
            return;
        }
        if (!message) {
            el.classList.remove('show', 'alert-danger', 'alert-warning', 'alert-success', 'alert-info');
            el.textContent = '';
            return;
        }
        const baseClass = ['alert'];
        switch (type) {
            case 'warning':
                baseClass.push('alert-warning');
                break;
            case 'success':
                baseClass.push('alert-success');
                break;
            case 'info':
                baseClass.push('alert-info');
                break;
            default:
                baseClass.push('alert-danger');
                break;
        }
        el.className = `qr-alert show ${baseClass.join(' ')}`;
        el.textContent = message;
    }

    function buildIdentifier(instrument) {
        const code = (instrument.codigo || '').trim();
        if (code) {
            return code;
        }
        const ubicacion = (instrument.ubicacion || '').trim();
        if (ubicacion) {
            return ubicacion;
        }
        return instrument.id ? `ID ${instrument.id}` : 'Instrumento';
    }

    async function fetchDetails(baseUrl, token) {
        const endpoint = `${baseUrl}/backend/instrumentos/gages/share_details.php?token=${encodeURIComponent(token)}`;
        const response = await fetch(endpoint, { credentials: 'omit' });
        if (!response.ok) {
            let message = `Error ${response.status}`;
            try {
                const json = await response.clone().json();
                if (json && json.error) {
                    message = json.error;
                }
            } catch (err) {
                try {
                    const text = await response.text();
                    if (text) {
                        message = text;
                    }
                } catch (innerErr) {
                    // ignore
                }
            }
            const error = new Error(message);
            error.status = response.status;
            throw error;
        }
        return response.json();
    }

    function applyData(data) {
        const instrument = data.instrument || {};
        const calibration = data.calibration || {};
        const certificate = data.certificate || {};
        const tokenInfo = data.token || {};

        setText('qrInstrumentName', instrument.nombre || 'Instrumento');
        setText('qrInstrumentIdentifier', buildIdentifier(instrument));
        setText('qrInstrumentCode', instrument.codigo || 'Sin código');
        setText('qrInstrumentLocation', instrument.ubicacion || 'Sin ubicación');
        setText('qrInstrumentDepartment', instrument.departamento || 'Sin responsable');
        setText('qrInstrumentState', instrument.estado || 'Sin estado registrado');

        const estado = (instrument.estado_calibracion || instrument.estado || '').trim();
        let estadoVariant = 'bg-primary';
        if (estado) {
            const lowered = estado.toLowerCase();
            if (lowered.includes('venc')) {
                estadoVariant = 'bg-danger';
            } else if (lowered.includes('próx') || lowered.includes('prox')) {
                estadoVariant = 'bg-warning text-dark';
            } else if (lowered.includes('vigente') || lowered.includes('al día') || lowered.includes('ok')) {
                estadoVariant = 'bg-success';
            }
        }
        setBadge('qrCalibrationStatus', estado || 'Estado no disponible', estadoVariant);

        const dias = Number.isFinite(instrument.dias_restantes) ? instrument.dias_restantes : null;
        let vigenciaTexto = '';
        if (Number.isFinite(dias)) {
            if (dias > 0) {
                vigenciaTexto = `${dias} días restantes`;
            } else if (dias === 0) {
                vigenciaTexto = 'Vence hoy';
            } else {
                vigenciaTexto = `${Math.abs(dias)} días vencido`;
            }
        } else {
            const fechaNext = calibration.fecha_proxima || instrument.proxima_calibracion;
            vigenciaTexto = fechaNext ? formatDate(fechaNext) : 'Sin vigencia registrada';
        }
        setBadge('qrCalibrationDue', vigenciaTexto || 'Sin vigencia', 'bg-light text-dark');

        const resultado = (calibration.resultado || '').trim();
        setBadge('qrCalibrationResult', resultado || 'Sin resultado', 'bg-light text-dark');

        setText('qrCalibrationDate', formatDate(calibration.fecha_calibracion || certificate.issued_at));
        setText('qrCalibrationNext', formatDate(calibration.fecha_proxima || instrument.proxima_calibracion));
        setText('qrCalibrationOwner', calibration.responsable || 'Sin responsable');

        const link = global.document && global.document.getElementById('qrCertificateLink');
        if (link) {
            const href = certificate.share_url ? certificate.share_url : '#';
            link.href = certificate.share_url ? href : '#';
            link.classList.toggle('disabled', !certificate.share_url);
            if (!certificate.share_url) {
                link.setAttribute('aria-disabled', 'true');
            } else {
                link.removeAttribute('aria-disabled');
            }
        }

        const expiryEl = global.document && global.document.getElementById('qrTokenExpiry');
        if (expiryEl) {
            const expiresAt = tokenInfo.expires_at ? formatDate(tokenInfo.expires_at) : '';
            expiryEl.textContent = expiresAt ? `Válido hasta ${expiresAt}` : 'Vigencia no disponible';
        }

        showMessage('success', 'Información cargada correctamente.');
    }

    async function init() {
        if (!global.document) {
            return;
        }
        const token = parseToken(global.location ? global.location.search : '');
        if (!token) {
            showMessage('error', 'No se proporcionó el token del certificado.');
            return;
        }

        const baseUrl = getBaseUrl(global);
        try {
            showMessage('info', 'Validando información del instrumento...');
            const data = await fetchDetails(baseUrl, token);
            applyData(data);
        } catch (error) {
            console.error('qr_details', error);
            const status = error && error.status ? error.status : 0;
            let type = 'error';
            if (status === 404) {
                type = 'warning';
            } else if (status === 410) {
                type = 'warning';
            }
            showMessage(type, error && error.message ? error.message : 'No fue posible consultar la información.');
            const link = global.document.getElementById('qrCertificateLink');
            if (link) {
                link.classList.add('disabled');
                link.href = '#';
                link.setAttribute('aria-disabled', 'true');
            }
            const expiryEl = global.document.getElementById('qrTokenExpiry');
            if (expiryEl && status === 410) {
                expiryEl.textContent = 'El enlace ha expirado.';
            }
        }
    }

    if (global.document) {
        global.document.addEventListener('DOMContentLoaded', init);
    }
})(typeof window !== 'undefined' ? window : this);
