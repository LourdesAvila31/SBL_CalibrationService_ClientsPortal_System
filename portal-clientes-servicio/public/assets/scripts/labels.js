(function (global) {
    'use strict';

    const LabelUtils = {
        parseInstrumentId(search) {
            const query = typeof search === 'string' ? search : '';
            const params = new URLSearchParams(query.startsWith('?') ? query : `?${query}`);
            const raw = params.get('instrument_id') || params.get('id');
            if (!raw) {
                return null;
            }
            const parsed = Number.parseInt(raw, 10);
            return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
        },
        getBaseUrl(context) {
            const scope = context || globalThis;
            if (scope && typeof scope.BASE_URL === 'string' && scope.BASE_URL !== '') {
                return scope.BASE_URL.replace(/\/$/, '');
            }
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
        },
        joinBase(base, path) {
            const cleanBase = (base || '').replace(/\/$/, '');
            if (!path) {
                return cleanBase;
            }
            const normalized = path.startsWith('/') ? path : `/${path}`;
            return `${cleanBase}${normalized}`;
        },
        buildAbsoluteUrl(baseUrl, rawUrl) {
            const trimmed = (rawUrl || '').toString().trim();
            if (!trimmed) {
                return '';
            }
            try {
                if (baseUrl) {
                    const normalizedBase = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
                    const base = new URL(normalizedBase, typeof window !== 'undefined' && window.location ? window.location.origin : undefined);
                    if (trimmed.startsWith('/')) {
                        const basePath = base.pathname.replace(/\/$/, '');
                        const combinedPath = `${basePath}${trimmed}`.replace(/\/{2,}/g, '/');
                        return `${base.origin}${combinedPath.startsWith('/') ? combinedPath : `/${combinedPath}`}`;
                    }
                    return new URL(trimmed, base).toString();
                }
                return new URL(trimmed).toString();
            } catch (err) {
                const cleanBase = (baseUrl || '').replace(/\/$/, '');
                const cleanPath = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
                return cleanBase ? `${cleanBase}${cleanPath}` : cleanPath;
            }
        },
        normalizePayload(raw) {
            const source = raw && typeof raw === 'object' ? raw : {};
            const id = Number.parseInt(source.instrument_id, 10);
            const code = (source.code || '').toString().trim();
            const serial = (source.serial || '').toString().trim();
            const name = (source.name || '').toString().trim() || 'Instrumento';
            const location = (source.location || '').toString().trim();
            const department = (source.department || '').toString().trim();
            const brand = (source.brand || '').toString().trim();
            const model = (source.model || '').toString().trim();
            const status = (source.status || '').toString().trim();
            const certificateUrl = typeof source.certificate_url === 'string' && source.certificate_url.trim() !== ''
                ? source.certificate_url.trim()
                : null;
            const issuedAt = source.certificate_issued_at ? `${source.certificate_issued_at}` : null;
            const expiresAt = source.certificate_expires_at ? `${source.certificate_expires_at}` : null;
            const detailUrl = typeof source.detail_url === 'string' && source.detail_url.trim() !== ''
                ? source.detail_url.trim()
                : null;
            const detailToken = typeof source.detail_token === 'string' && source.detail_token.trim() !== ''
                ? source.detail_token.trim()
                : null;
            const detailExpiresAt = source.detail_expires_at ? `${source.detail_expires_at}` : null;
            const nextCalibration = source.next_calibration ? `${source.next_calibration}` : null;
            const identifier = code || serial || (Number.isFinite(id) ? `ID ${id}` : 'Instrumento');
            return {
                instrument_id: Number.isFinite(id) ? id : null,
                name,
                code,
                serial,
                brand,
                model,
                status,
                location,
                department,
                next_calibration: nextCalibration,
                identifier,
                certificate_url: certificateUrl,
                certificate_issued_at: issuedAt,
                certificate_expires_at: expiresAt,
                detail_url: detailUrl,
                detail_token: detailToken,
                detail_expires_at: detailExpiresAt,
                generated_at: source.generated_at ? `${source.generated_at}` : null,
                resource_url: typeof source.resource_url === 'string' && source.resource_url.trim() !== ''
                    ? source.resource_url
                    : (detailUrl || certificateUrl),
                has_certificate: Boolean(certificateUrl),
                has_detail: Boolean(detailUrl || detailToken)
            };
        },
        buildCertificateUrl(baseUrl, rawUrl) {
            return LabelUtils.buildAbsoluteUrl(baseUrl, rawUrl);
        },
        formatDate(value) {
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
    };

    function setText(id, value) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById(id);
        if (el) {
            el.textContent = value || '';
        }
    }

    function showStatus(message, type) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById('labelStatus');
        if (!el) {
            return;
        }
        el.textContent = message || '';
        el.dataset.type = type || '';
        if (!message) {
            el.classList.add('hidden');
        } else {
            el.classList.remove('hidden');
        }
    }

    async function fetchPayload(baseUrl, instrumentId) {
        const joined = LabelUtils.joinBase(baseUrl, `/backend/instrumentos/gages/get_label_payload.php?id=${instrumentId}`);
        const response = await fetch(joined, { credentials: 'same-origin' });
        if (!response.ok) {
            throw new Error(`Error ${response.status} al consultar la etiqueta`);
        }
        return response.json();
    }

    function renderLabel(baseUrl, normalized) {
        if (!global.document) {
            return;
        }
        setText('labelName', normalized.name);
        setText('labelIdentifier', normalized.identifier);
        setText('labelLocation', normalized.location || 'Sin ubicación');
        setText('labelDepartment', normalized.department || 'Sin departamento');
        setText('labelBrand', normalized.brand || 'Sin marca');
        setText('labelModel', normalized.model || 'Sin modelo');
        setText('labelInstrumentStatus', normalized.status || 'Sin estado');
        const formattedNextCalibration = LabelUtils.formatDate(normalized.next_calibration);
        setText('labelNextCalibration', formattedNextCalibration || '-');
        setText('labelIssuedAt', LabelUtils.formatDate(normalized.certificate_issued_at));
        setText('labelExpiresAt', LabelUtils.formatDate(normalized.certificate_expires_at));
        const linkEl = global.document.getElementById('labelCertificateLink');
        const detailLinkEl = global.document.getElementById('labelDetailLink');
        const qrContainer = global.document.getElementById('qrContainer');
        if (linkEl) {
            linkEl.textContent = '';
            linkEl.removeAttribute('href');
        }
        if (detailLinkEl) {
            detailLinkEl.textContent = '';
            detailLinkEl.removeAttribute('href');
        }
        if (qrContainer) {
            qrContainer.innerHTML = '';
        }
        const detailAbsoluteUrl = LabelUtils.buildAbsoluteUrl(baseUrl, normalized.detail_url);
        const certificateAbsoluteUrl = LabelUtils.buildAbsoluteUrl(baseUrl, normalized.certificate_url);
        if (detailLinkEl) {
            if (detailAbsoluteUrl) {
                detailLinkEl.textContent = detailAbsoluteUrl;
                detailLinkEl.href = detailAbsoluteUrl;
            } else {
                detailLinkEl.textContent = 'Sin ficha disponible';
            }
        }
        if (linkEl) {
            if (normalized.has_certificate && certificateAbsoluteUrl) {
                linkEl.textContent = certificateAbsoluteUrl;
                linkEl.href = certificateAbsoluteUrl;
            } else {
                linkEl.textContent = 'Sin certificado disponible';
            }
        }
        const qrTarget = detailAbsoluteUrl || certificateAbsoluteUrl;
        if (qrTarget && qrContainer) {
            if (typeof global.QRCode === 'function') {
                new global.QRCode(qrContainer, {
                    text: qrTarget,
                    width: 180,
                    height: 180,
                    correctLevel: global.QRCode.CorrectLevel ? global.QRCode.CorrectLevel.M : 0
                });
            } else {
                qrContainer.textContent = 'QR no disponible';
            }
        } else if (qrContainer) {
            qrContainer.textContent = 'QR no disponible';
        }
    }

    function init() {
        if (!global.document) {
            return;
        }
        const instrumentId = LabelUtils.parseInstrumentId(global.location ? global.location.search : '');
        const baseUrl = LabelUtils.getBaseUrl(global);
        const printButton = global.document.getElementById('printLabelButton');
        if (printButton) {
            printButton.addEventListener('click', () => {
                global.print();
            });
        }
        if (!instrumentId) {
            showStatus('Identificador de instrumento no válido.', 'error');
            return;
        }
        showStatus('Cargando etiqueta...', 'info');
        fetchPayload(baseUrl, instrumentId)
            .then((payload) => {
                const normalized = LabelUtils.normalizePayload(payload);
                renderLabel(baseUrl, normalized);
                showStatus('', '');
            })
            .catch((error) => {
                console.error('No se pudo cargar la etiqueta', error);
                showStatus('No se pudo cargar la información del instrumento.', 'error');
            });
    }

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = LabelUtils;
    } else {
        global.LabelUtils = LabelUtils;
    }

    if (global.document && typeof global.addEventListener === 'function') {
        global.addEventListener('DOMContentLoaded', init);
    }
})(typeof window !== 'undefined' ? window : globalThis);
