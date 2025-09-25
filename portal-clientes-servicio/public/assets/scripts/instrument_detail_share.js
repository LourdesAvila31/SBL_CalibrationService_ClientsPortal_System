(function (global) {
    'use strict';

    function getBaseUrl(scope) {
        const ctx = scope || globalThis;
        if (ctx && typeof ctx.BASE_URL === 'string' && ctx.BASE_URL !== '') {
            return ctx.BASE_URL.replace(/\/$/, '');
        }
        if (ctx && typeof ctx.PAGE_BASE_URL === 'string' && ctx.PAGE_BASE_URL !== '') {
            return ctx.PAGE_BASE_URL.replace(/\/$/, '');
        }
        if (ctx && ctx.location && typeof ctx.location.pathname === 'string') {
            const marker = '/public/';
            const path = ctx.location.pathname;
            const idx = path.indexOf(marker);
            if (idx !== -1) {
                return path.slice(0, idx + marker.length).replace(/\/$/, '');
            }
        }
        return '';
    }

    function joinBase(base, path) {
        const cleanBase = (base || '').replace(/\/$/, '');
        if (!path) {
            return cleanBase;
        }
        const normalized = path.startsWith('/') ? path : `/${path}`;
        return `${cleanBase}${normalized}`;
    }

    function buildAbsoluteUrl(baseUrl, rawUrl) {
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
                    const prefixed = combinedPath.startsWith('/') ? combinedPath : `/${combinedPath}`;
                    return `${base.origin}${prefixed}`;
                }
                return new URL(trimmed, base).toString();
            }
            return new URL(trimmed).toString();
        } catch (err) {
            const cleanBase = (baseUrl || '').replace(/\/$/, '');
            const cleanPath = trimmed.startsWith('/') ? trimmed : `/${trimmed}`;
            return cleanBase ? `${cleanBase}${cleanPath}` : cleanPath;
        }
    }

    function parseToken(search) {
        const query = typeof search === 'string' ? search : '';
        const params = new URLSearchParams(query.startsWith('?') ? query : `?${query}`);
        const token = params.get('token');
        return token ? token.trim() : '';
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
            return date.toLocaleString('es-ES', {
                year: 'numeric',
                month: 'short',
                day: '2-digit',
                hour: '2-digit',
                minute: '2-digit'
            }).replace('.', '');
        } catch (err) {
            return value;
        }
    }

    function formatDateOnly(value) {
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

    function setLink(id, url, fallbackText) {
        if (!global.document) {
            return;
        }
        const el = global.document.getElementById(id);
        if (!el) {
            return;
        }
        if (url) {
            el.textContent = url;
            el.href = url;
        } else {
            el.textContent = fallbackText || '';
            el.removeAttribute('href');
        }
    }

    function showStatus(message, type) {
        if (!global.document) {
            return;
        }
        const statusEl = global.document.getElementById('detailStatus');
        if (!statusEl) {
            return;
        }
        statusEl.textContent = message || '';
        statusEl.dataset.type = type || '';
        if (!message) {
            statusEl.classList.add('hidden');
        } else {
            statusEl.classList.remove('hidden');
        }
    }

    function toggleContent(visible) {
        if (!global.document) {
            return;
        }
        const contentEl = global.document.getElementById('detailContent');
        if (!contentEl) {
            return;
        }
        if (visible) {
            contentEl.classList.remove('hidden');
        } else {
            contentEl.classList.add('hidden');
        }
    }

    async function fetchDetail(baseUrl, token) {
        const url = joinBase(baseUrl, `/backend/instrumentos/share_detail.php?token=${encodeURIComponent(token)}`);
        const response = await fetch(url, { credentials: 'same-origin' });
        if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            const message = data && data.error ? data.error : `Error ${response.status}`;
            throw new Error(message);
        }
        return response.json();
    }

    function init() {
        if (!global.document) {
            return;
        }
        const token = parseToken(global.location ? global.location.search : '');
        const baseUrl = getBaseUrl(global);
        if (!token) {
            showStatus('Token no proporcionado.', 'error');
            toggleContent(false);
            return;
        }
        showStatus('Cargando ficha...', 'info');
        toggleContent(false);
        fetchDetail(baseUrl, token)
            .then((data) => {
                const name = (data.name || '').toString().trim() || 'Instrumento';
                const identifier = (data.identifier || '').toString().trim() || 'Instrumento';
                const company = (data.company || '').toString().trim();
                const status = (data.status || '').toString().trim();
                const nextCalibration = formatDateOnly(data.next_calibration);
                const issuedAt = formatDateOnly(data.certificate_issued_at);
                const tokenExpiresAt = formatDate(data.token_expires_at);
                const generatedAt = formatDate(data.generated_at);
                const detailPath = `/apps/tenant/instrumentos/qr_details.html?token=${encodeURIComponent(token)}`;
                const absoluteDetailUrl = buildAbsoluteUrl(baseUrl, detailPath);
                const absoluteCertificateUrl = data.certificate_url ? buildAbsoluteUrl(baseUrl, data.certificate_url) : '';

                setText('detailName', name);
                setText('detailIdentifier', identifier);
                setText('detailCompany', company);
                setText('detailStatusBadge', status || 'Sin estado');
                setText('detailLocation', (data.location || '').toString().trim() || 'Sin ubicación');
                setText('detailDepartment', (data.department || '').toString().trim() || 'Sin departamento');
                setText('detailBrand', (data.brand || '').toString().trim() || 'Sin marca');
                setText('detailModel', (data.model || '').toString().trim() || 'Sin modelo');
                setText('detailNextCalibration', nextCalibration || '-');
                setText('detailIssuedAt', issuedAt || '-');
                setText('detailExpiresAt', tokenExpiresAt || '-');
                setText('detailGeneratedAt', generatedAt || '-');
                setLink('detailShareLink', absoluteDetailUrl, '-');
                if (data.has_certificate && absoluteCertificateUrl) {
                    setLink('detailCertificateLink', absoluteCertificateUrl, '');
                } else {
                    setLink('detailCertificateLink', '', 'Sin certificado disponible');
                }

                toggleContent(true);
                showStatus('', '');
            })
            .catch((error) => {
                console.error('No se pudo cargar la ficha del instrumento', error);
                showStatus(error.message || 'No se pudo cargar la ficha.', 'error');
                toggleContent(false);
            });
    }

    if (global.document && typeof global.addEventListener === 'function') {
        global.addEventListener('DOMContentLoaded', init);
    }
})(typeof window !== 'undefined' ? window : globalThis);
