// portal-base.js - núcleo JS centralizado para el sistema de gestión de calibraciones
// Utiliza el helper común para determinar la ruta base del proyecto.
const appUrl = typeof window !== 'undefined' && window.AppUrl ? window.AppUrl : {};
const normalizeAppUrl = typeof appUrl.normalizeAppUrl === 'function'
    ? appUrl.normalizeAppUrl
    : (value => value);
const resolveAppUrl = typeof appUrl.resolveAppUrl === 'function'
    ? appUrl.resolveAppUrl
    : normalizeAppUrl;

const portalConfig = {
    portalId: 'unknown',
    serviceModuleEnabled: false
};

let portalScopeListenerBound = false;

function applyPortalScopeAttributes() {
    if (typeof document === 'undefined') {
        return;
    }

    const scope = portalConfig.portalId;
    if (!scope) {
        return;
    }

    const targets = [document.documentElement, document.body].filter(Boolean);
    targets.forEach(node => {
        if (node) {
            node.setAttribute('data-app-scope', scope);
        }
    });
}

function getDomPortalScope() {
    if (typeof document === 'undefined') {
        return '';
    }
    const scopeNode = document.querySelector('[data-app-scope]');
    if (!scopeNode) {
        return '';
    }
    return (scopeNode.getAttribute('data-app-scope') || '').trim().toLowerCase();
}

function shouldHandleServiceModule() {
    if (!portalConfig.serviceModuleEnabled) {
        return false;
    }

    const configured = (portalConfig.portalId || '').trim().toLowerCase();
    if (configured && configured !== 'service') {
        return false;
    }

    if (typeof window !== 'undefined' && typeof window.portalId === 'string') {
        const fromWindow = window.portalId.trim().toLowerCase();
        if (fromWindow && fromWindow !== 'service') {
            return false;
        }
    }

    const scopeFromDom = getDomPortalScope();
    if (scopeFromDom && scopeFromDom !== 'service') {
        return false;
    }

    return true;
}

const CUSTOM_REPORT_DATASETS = {
    calibraciones: {
        label: 'Calibraciones',
        dateLabel: 'Fecha de calibración',
        statusLabel: 'Estado de calibración',
        defaultColumns: ['id', 'instrumento', 'fecha_calibracion', 'resultado', 'estado'],
        statusSuggestions: ['Programada', 'En proceso', 'Completada', 'Reprogramada', 'Atrasada', 'Cancelada', 'Aprobado', 'Rechazado', 'En revisión'],
        columns: [
            { key: 'id', label: 'ID' },
            { key: 'instrumento', label: 'Instrumento' },
            { key: 'tipo', label: 'Tipo' },
            { key: 'fecha_calibracion', label: 'Fecha de calibración' },
            { key: 'fecha_proxima', label: 'Próxima calibración' },
            { key: 'resultado', label: 'Resultado' },
            { key: 'estado', label: 'Estado' },
            { key: 'estado_ejecucion', label: 'Estado de ejecución' },
            { key: 'duracion_horas', label: 'Duración (h)' },
            { key: 'costo_total', label: 'Costo total' },
            { key: 'periodo', label: 'Periodo' }
        ]
    },
    planes: {
        label: 'Planes de calibración',
        dateLabel: 'Fecha programada',
        statusLabel: 'Estado del plan',
        defaultColumns: ['id', 'instrumento', 'fecha_programada', 'estado', 'responsable'],
        statusSuggestions: ['Pendiente', 'Programado', 'En progreso', 'Completado', 'Cancelado'],
        columns: [
            { key: 'id', label: 'ID' },
            { key: 'instrumento', label: 'Instrumento' },
            { key: 'fecha_programada', label: 'Fecha programada' },
            { key: 'estado', label: 'Estado' },
            { key: 'responsable', label: 'Responsable' },
            { key: 'instrucciones_cliente', label: 'Instrucciones del cliente' }
        ]
    },
    instrumentos: {
        label: 'Instrumentos',
        dateLabel: 'Fecha de alta',
        statusLabel: 'Estado del instrumento',
        defaultColumns: ['codigo', 'serie', 'estado', 'ubicacion', 'proxima_calibracion'],
        statusSuggestions: ['Activo', 'Inactivo', 'Baja', 'Fuera de servicio', 'En calibración'],
        columns: [
            { key: 'codigo', label: 'Código' },
            { key: 'serie', label: 'Serie' },
            { key: 'estado', label: 'Estado' },
            { key: 'ubicacion', label: 'Ubicación' },
            { key: 'fecha_alta', label: 'Fecha de alta' },
            { key: 'proxima_calibracion', label: 'Próxima calibración' },
            { key: 'programado', label: 'Programado' }
        ]
    }
};

window.CUSTOM_REPORT_DATASETS = CUSTOM_REPORT_DATASETS;

// Asegura que todas las peticiones fetch incluyan las credenciales de sesión
const originalFetch = window.fetch.bind(window);
window.fetch = (input, init = {}) => {
    const config = { ...init, credentials: init.credentials || 'include' };

    if (typeof input === 'string') {
        input = normalizeAppUrl(input);
    } else if (typeof Request !== 'undefined' && input instanceof Request) {
        const normalizedUrl = normalizeAppUrl(input.url);
        if (normalizedUrl !== input.url) {
            input = new Request(normalizedUrl, input);
        }
    }

    return originalFetch(input, config);
};

async function fetchJson(resource, options = {}) {
    const response = await fetch(resource, options);
    const clone = response.clone();
    let data;

    try {
        data = await response.json();
    } catch (error) {
        const text = await clone.text();
        const parseError = new Error('Respuesta no válida del servidor');
        parseError.status = response.status;
        parseError.payload = text;
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

const ROLE_ALIASES = {
    'superadministrador': 'superadministrador',
    'super administrador': 'superadministrador',
    'administrador': 'administrador',
    'administrador general': 'administrador',
    'administrador general de sistema': 'administrador',
    'admin': 'administrador',
    'supervisor': 'supervisor',
    'operador': 'operador',
    'lector': 'lector',
    'cliente': 'cliente',
    'sistemas': 'sistemas'
};

function normalizeRoleName(role) {
    if (!role) return '';
    const normalized = role
        .toString()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .trim()
        .toLowerCase();

    if (ROLE_ALIASES[normalized]) {
        return ROLE_ALIASES[normalized];
    }

    if (normalized.includes('admin')) {
        return 'administrador';
    }
    if (normalized.includes('supervisor')) {
        return 'supervisor';
    }
    if (normalized.includes('operador')) {
        return 'operador';
    }
    if (normalized.includes('cliente')) {
        return 'cliente';
    }
    if (normalized.includes('sistem')) {
        return 'sistemas';
    }
    if (normalized.includes('lector') || normalized.includes('lectur')) {
        return 'lector';
    }

    return normalized;
}

const DEFAULT_PERMISSION_FLAGS = {
    canManageInstrumentos: false,
    canManagePlaneacion: false,
    canManageCalibraciones: false,
    canManageUsuarios: false,
    canReviewFeedback: false,
    canManageClientesService: false,
    canManageClientesServicio: false,
    canViewApiTokens: false,
    canManageApiTokens: false
};

window.permissionFlags = { ...DEFAULT_PERMISSION_FLAGS };

function coercePermissionFlag(value) {
    if (typeof value === 'boolean') {
        return value;
    }
    if (value === 1 || value === '1') {
        return true;
    }
    if (value === 0 || value === '0') {
        return false;
    }
    if (typeof value === 'string') {
        const trimmed = value.trim().toLowerCase();
        if (['true', 'yes', 'si', 'sí'].includes(trimmed)) {
            return true;
        }
        if (['false', 'no'].includes(trimmed)) {
            return false;
        }
    }
    return null;
}

let apiTokensAutoActivated = false;

function shouldAutoActivateApiTokens() {
    if (apiTokensAutoActivated) {
        return false;
    }

    if (document.body && document.body.dataset && document.body.dataset.openApiTokens === 'true') {
        return true;
    }

    const hash = (window.location.hash || '').replace(/^#/, '').toLowerCase();
    if (['apitokens', 'api-tokens', 'tokens'].includes(hash)) {
        return true;
    }

    let screen = '';
    try {
        const params = new URLSearchParams(window.location.search);
        screen = (params.get('screen')
            || params.get('tab')
            || params.get('view')
            || params.get('module')
            || '').toLowerCase();
    } catch (_) {
        screen = '';
    }

    if (screen && ['apitokens', 'api-tokens', 'tokens'].includes(screen)) {
        return true;
    }

    return false;
}

function activateApiTokensIfAvailable(flags = window.permissionFlags || DEFAULT_PERMISSION_FLAGS) {
    const hasAccess = !!(flags && (flags.canManageApiTokens || flags.canViewApiTokens));
    if (!hasAccess) {
        return false;
    }

    if (!shouldAutoActivateApiTokens()) {
        return false;
    }

    let handled = false;

    if (typeof window.activateApiTokensScreen === 'function') {
        try {
            const result = window.activateApiTokensScreen({ flags, reason: 'auto' });
            if (result !== false) {
                handled = true;
            }
        } catch (error) {
            console.error('No se pudo activar la pantalla de tokens API automáticamente.', error);
        }
    }

    if (!handled) {
        const trigger = document.querySelector('[data-api-tokens-activator]')
            || document.querySelector('[data-requires-api-tokens]');
        if (trigger && typeof trigger.click === 'function') {
            trigger.click();
            handled = true;
        }
    }

    if (handled) {
        apiTokensAutoActivated = true;
    }

    return handled;
}

function applyApiTokenVisibility(flags = window.permissionFlags || DEFAULT_PERMISSION_FLAGS) {
    const elements = document.querySelectorAll('[data-requires-api-tokens]');
    if (!elements.length) {
        return;
    }

    const canManage = !!(flags && flags.canManageApiTokens);
    const canView = !!(flags && (flags.canViewApiTokens || flags.canManageApiTokens));

    elements.forEach(element => {
        const requirement = (element.dataset.requiresApiTokens || '').trim().toLowerCase();
        const needsManage = requirement && requirement !== 'true' && requirement !== 'view';
        const visible = needsManage ? canManage : canView;

        element.dataset.apiTokensVisible = visible ? 'true' : 'false';

        if (visible) {
            element.classList.remove('is-hidden');
            if (element.style.display === 'none') {
                element.style.display = '';
            }
        } else {
            element.classList.add('is-hidden');
            element.style.display = 'none';
        }
    });
}
window.applyApiTokenVisibility = applyApiTokenVisibility;
window.activateApiTokensIfAvailable = (options = {}) => {
    const flags = options.flags || window.permissionFlags || DEFAULT_PERMISSION_FLAGS;
    return activateApiTokensIfAvailable(flags);
};

function applyPermissionFlags(roleOrUser) {
    const user = roleOrUser && typeof roleOrUser === 'object' ? roleOrUser : null;
    const normalizedRole = user
        ? normalizeRoleName(user.role_name || user.role || user.role_id)
        : normalizeRoleName(roleOrUser);

    const privileged = ['superadministrador', 'administrador', 'supervisor', 'cliente'];
    const calibrationManagers = ['superadministrador', 'administrador', 'supervisor'];
    const usuariosManagers = ['superadministrador', 'sistemas'];

    const resolvedFlags = {};
    Object.keys(DEFAULT_PERMISSION_FLAGS).forEach(key => {
        resolvedFlags[key] = null;
    });

    const serverFlags = user && typeof user.permission_flags === 'object'
        ? user.permission_flags
        : user && typeof user.permissionFlags === 'object'
            ? user.permissionFlags
            : null;

    const setFlag = (flagName, value, options = {}) => {
        if (value === null || value === undefined) {
            return;
        }
        const coerced = coercePermissionFlag(value);
        if (coerced === null) {
            return;
        }
        const preferTrue = options && options.preferTrue;
        if (preferTrue) {
            if (coerced) {
                resolvedFlags[flagName] = true;
                return;
            }
            if (typeof resolvedFlags[flagName] !== 'boolean') {
                resolvedFlags[flagName] = false;
            }
            return;
        }
        resolvedFlags[flagName] = coerced;
    };

    if (serverFlags) {
        const flagKeys = Object.keys(DEFAULT_PERMISSION_FLAGS);
        flagKeys.forEach(key => {
            const coerced = coercePermissionFlag(serverFlags[key]);
            if (coerced !== null) {
                resolvedFlags[key] = coerced;
            }
        });

        const tokenContainers = [];
        if (serverFlags.api_tokens && typeof serverFlags.api_tokens === 'object') {
            tokenContainers.push(serverFlags.api_tokens);
        }
        tokenContainers.push(serverFlags);

        const viewKeys = ['api_tokens_view', 'view_api_tokens', 'apiTokensView', 'canViewApiTokens', 'viewApiTokens'];
        const manageKeys = [
            'api_tokens_manage', 'manage_api_tokens', 'apiTokensManage', 'canManageApiTokens',
            'api_tokens_create', 'api_tokens_update', 'api_tokens_delete', 'api_tokens_write',
            'api_tokens_admin', 'admin_api_tokens', 'apiTokensAdmin', 'apiTokensWrite'
        ];

        tokenContainers.forEach(container => {
            if (!container || typeof container !== 'object') {
                return;
            }

            viewKeys.forEach(key => {
                if (key in container) {
                    setFlag('canViewApiTokens', container[key]);
                }
            });

            manageKeys.forEach(key => {
                if (key in container) {
                    setFlag('canManageApiTokens', container[key], { preferTrue: true });
                }
            });

            if ('api_tokens' in container) {
                setFlag('canViewApiTokens', container.api_tokens);
                setFlag('canManageApiTokens', container.api_tokens, { preferTrue: true });
            }
        });
    }

    if (typeof resolvedFlags.canManageInstrumentos !== 'boolean') {
        resolvedFlags.canManageInstrumentos = privileged.includes(normalizedRole);
    }
    if (typeof resolvedFlags.canManagePlaneacion !== 'boolean') {
        resolvedFlags.canManagePlaneacion = privileged.includes(normalizedRole);
    }
    if (typeof resolvedFlags.canManageCalibraciones !== 'boolean') {
        resolvedFlags.canManageCalibraciones = calibrationManagers.includes(normalizedRole);
    }
    if (typeof resolvedFlags.canManageUsuarios !== 'boolean') {
        resolvedFlags.canManageUsuarios = usuariosManagers.includes(normalizedRole);
    }
    if (typeof resolvedFlags.canReviewFeedback !== 'boolean') {
        resolvedFlags.canReviewFeedback = normalizedRole === 'superadministrador';
    }
    if (typeof resolvedFlags.canManageClientesService !== 'boolean'
        && typeof resolvedFlags.canManageClientesServicio === 'boolean') {
        resolvedFlags.canManageClientesService = resolvedFlags.canManageClientesServicio;
    }
    if (typeof resolvedFlags.canManageClientesService !== 'boolean') {
        const serviceRoles = ['superadministrador', 'administrador', 'supervisor', 'sistemas'];
        resolvedFlags.canManageClientesService = serviceRoles.includes(normalizedRole);
    }
    if (typeof resolvedFlags.canManageClientesServicio !== 'boolean') {
        resolvedFlags.canManageClientesServicio = resolvedFlags.canManageClientesService;
    }

    if (typeof resolvedFlags.canManageApiTokens !== 'boolean') {
        const apiTokenManagers = ['superadministrador', 'administrador', 'sistemas'];
        resolvedFlags.canManageApiTokens = apiTokenManagers.includes(normalizedRole);
    }

    if (typeof resolvedFlags.canViewApiTokens !== 'boolean') {
        const apiTokenViewers = ['superadministrador', 'administrador', 'sistemas', 'supervisor'];
        resolvedFlags.canViewApiTokens = resolvedFlags.canManageApiTokens || apiTokenViewers.includes(normalizedRole);
    }

    if (resolvedFlags.canManageApiTokens && resolvedFlags.canViewApiTokens === false) {
        resolvedFlags.canViewApiTokens = true;
    }

    window.permissionFlags = resolvedFlags;

    document.dispatchEvent(new CustomEvent('permissionsready', { detail: window.permissionFlags }));
    if (shouldHandleServiceModule()) {
        applyServiceModuleVisibility(window.permissionFlags);
    }
    applyApiTokenVisibility(window.permissionFlags);
    activateApiTokensIfAvailable(window.permissionFlags);
    enforceProtectedRoutes();
}

function enforceProtectedRoutes() {
    const flags = window.permissionFlags || DEFAULT_PERMISSION_FLAGS;
    const path = window.location.pathname;

    if (!flags.canManageInstrumentos) {
        if (path.includes('/apps/internal/instrumentos/add_gage.html')) {
            window.location.replace(resolveAppUrl('/apps/internal/instrumentos/list_gages.html'));
            return;
        }
    }

    if (!flags.canManagePlaneacion) {
        if (path.includes('/apps/internal/planeacion/add_planning.html')) {
            window.location.replace(resolveAppUrl('/apps/internal/planeacion/list_planning.html'));
            return;
        }
    }

    if (!flags.canManageCalibraciones) {
        if (path.includes('/apps/internal/calibraciones/add_calibration.html')) {
            window.location.replace(resolveAppUrl('/apps/internal/calibraciones/list_calibrations.html'));
            return;
        }
    }

    if (!flags.canManageUsuarios) {
        if (path.includes('/apps/internal/usuarios/add_user.html') || path.includes('/apps/internal/usuarios/edit_user.html')) {
            window.location.replace(resolveAppUrl('/apps/internal/usuarios/list_users.html'));
        }
    }

    if (!(flags.canManageClientesService || flags.canManageClientesServicio)) {
        if (path.includes('/apps/service/')) {
            window.location.replace(resolveAppUrl('/apps/internal/index.html'));
        }
    }
}

(function ensureRiskMatrix(){
    if(!window.RiskMatrix){
        const script=document.createElement('script');
        script.src = resolveAppUrl('/assets/scripts/risk-matrix.js');
        script.defer=true;
        document.head.appendChild(script);
    }
})();

function validateRiskInputs(...values){
    const allowed=(window.RiskMatrix && window.RiskMatrix.RISK_LEVELS)||[1,2,3];
    return values.every(v=>allowed.includes(Number(v)));
}
window.validateRiskInputs = validateRiskInputs;

// ======== AUTENTICACIÓN GLOBAL ========
function updateUserDisplay(u){
    const nameEl = document.getElementById('userName');
    const menuNombre = document.getElementById('menuNombre');
    const menuRol = document.getElementById('menuRol');
    if(!nameEl) return;
    const nombreCompleto = [u?.nombre, u?.apellidos].filter(Boolean).join(' ') || 'Invitado';
    const roleName = u?.role_name || 'Sin rol';
    nameEl.innerHTML = `${nombreCompleto} <span class="text-accent">(${roleName})</span>`;
    if(menuNombre) menuNombre.innerText = 'Nombre: ' + nombreCompleto;
    if(menuRol) menuRol.innerText = 'Rol: ' + roleName;
}
window.updateUserDisplay = updateUserDisplay;

function fetchCurrentUser(){
    return fetchJson(resolveAppUrl('/backend/usuarios/get_actual_user.php'))
        .then(u => {
            window.currentUser = u;
            updateUserDisplay(u);
            return u;
        });
}
window.fetchCurrentUser = fetchCurrentUser;

let feedbackShortcutTarget = null;

function navigateFeedbackShortcut(event) {
    if (event.type === 'keydown' && event.key !== 'Enter' && event.key !== ' ') {
        return;
    }
    event.preventDefault();
    const target = event.currentTarget.dataset.feedbackTarget
        || feedbackShortcutTarget
        || resolveAppUrl('/apps/internal/feedback/feedback_sbl.html');
    window.location.assign(target);
}

function ensureFeedbackShortcut() {
    const container = document.querySelector('.topbar-icons');
    if (!container) {
        return [];
    }

    const triggers = Array.from(container.querySelectorAll('[data-feedback-shortcut]'));
    if (triggers.length === 0) {
        const icon = document.createElement('i');
        icon.className = 'fa fa-comment-dots';
        icon.title = 'Enviar retroalimentación';
        icon.setAttribute('role', 'button');
        icon.setAttribute('tabindex', '0');
        icon.dataset.feedbackShortcut = 'true';
        container.insertBefore(icon, container.firstChild || null);
        triggers.push(icon);
    }

    triggers.forEach(trigger => {
        if (trigger.dataset.feedbackBound) {
            return;
        }
        trigger.addEventListener('click', navigateFeedbackShortcut);
        trigger.addEventListener('keydown', navigateFeedbackShortcut);
        trigger.dataset.feedbackBound = 'true';
    });

    return triggers;
}

function applyFeedbackShortcutTarget() {
    const triggers = ensureFeedbackShortcut();
    if (!triggers.length || !feedbackShortcutTarget) {
        return;
    }
    triggers.forEach(trigger => {
        trigger.dataset.feedbackTarget = feedbackShortcutTarget;
    });
}

function applyServiceModuleVisibility(flags = window.permissionFlags || DEFAULT_PERMISSION_FLAGS) {
    if (!shouldHandleServiceModule()) {
        return;
    }

    const links = document.querySelectorAll('[data-service-clientes]');
    if (!links.length) {
        return;
    }
    const visible = !!(flags && (flags.canManageClientesService || flags.canManageClientesServicio));
    links.forEach(link => {
        link.style.display = visible ? '' : 'none';
    });
}

function applyCalidadModuleVisibility(flags = window.permissionFlags || DEFAULT_PERMISSION_FLAGS) {
    const nodes = document.querySelectorAll('[data-calidad-menu], [data-calidad-section], [data-calidad-action]');
    if (!nodes.length) {
        return;
    }

    const canViewBase = !!(flags && (flags.canViewCalidad
        || flags.canManageCalidadDocumentos
        || flags.canManageCalidadCapacitaciones
        || flags.canManageCalidadNoConformidades));

    const canViewDocumentos = !!(flags && (flags.canManageCalidadDocumentos || flags.canViewCalidad));
    const canViewCapacitaciones = !!(flags && (flags.canManageCalidadCapacitaciones || flags.canViewCalidad));
    const canViewNoConformidades = !!(flags && (flags.canManageCalidadNoConformidades || flags.canViewCalidad));

    const sectionAccess = {
        overview: canViewBase,
        documentos: canViewDocumentos,
        capacitaciones: canViewCapacitaciones,
        'no-conformidades': canViewNoConformidades
    };

    document.querySelectorAll('[data-calidad-menu]').forEach(node => {
        const section = node.getAttribute('data-calidad-section') || 'overview';
        const visible = Object.prototype.hasOwnProperty.call(sectionAccess, section)
            ? sectionAccess[section]
            : canViewBase;
        node.style.display = visible ? '' : 'none';
        if (visible) {
            node.removeAttribute('aria-hidden');
        } else {
            node.setAttribute('aria-hidden', 'true');
        }
    });

    document.querySelectorAll('[data-calidad-section]').forEach(node => {
        const section = node.getAttribute('data-calidad-section') || 'overview';
        const visible = Object.prototype.hasOwnProperty.call(sectionAccess, section)
            ? sectionAccess[section]
            : canViewBase;
        if (visible) {
            node.style.display = '';
            if (node.hasAttribute('hidden')) {
                node.removeAttribute('hidden');
            }
        } else {
            node.style.display = 'none';
            node.setAttribute('hidden', 'hidden');
        }
    });

    document.querySelectorAll('[data-calidad-action]').forEach(node => {
        const action = node.getAttribute('data-calidad-action');
        let allowed = false;
        switch (action) {
            case 'documentos:create':
                allowed = !!(flags && flags.canManageCalidadDocumentos);
                break;
            case 'capacitaciones:create':
                allowed = !!(flags && flags.canManageCalidadCapacitaciones);
                break;
            case 'no-conformidades:create':
                allowed = !!(flags && flags.canManageCalidadNoConformidades);
                break;
            default:
                allowed = canViewBase;
                break;
        }

        if ('disabled' in node) {
            node.disabled = !allowed;
        }
        if (allowed) {
            node.classList.remove('locked');
            node.removeAttribute('aria-disabled');
        } else {
            node.classList.add('locked');
            node.setAttribute('aria-disabled', 'true');
        }
    });
}

document.addEventListener('permissionsready', event => {
    const flags = event && event.detail ? event.detail : window.permissionFlags || DEFAULT_PERMISSION_FLAGS;
    const user = window.currentUser || {};
    const normalizedRole = normalizeRoleName(user.role_name || user.role || user.role_id);

    if (shouldHandleServiceModule()) {
        applyServiceModuleVisibility(flags);
    } else {
        document.querySelectorAll('[data-service-clientes]').forEach(link => {
            link.style.display = 'none';
        });
    }
    applyApiTokenVisibility(flags);

    if (flags && typeof flags.canReviewFeedback === 'boolean' && flags.canReviewFeedback) {
        feedbackShortcutTarget = resolveAppUrl('/apps/internal/feedback/admin.html');
    } else if (normalizedRole === 'cliente') {
        feedbackShortcutTarget = resolveAppUrl('/apps/tenant/feedback/feedback_externos.html');
    } else {
        feedbackShortcutTarget = resolveAppUrl('/apps/internal/feedback/feedback_sbl.html');
    }

    applyFeedbackShortcutTarget();
    activateApiTokensIfAvailable(flags);
});

//===========SIDEBAR================
const ABSOLUTE_LINK_PATTERN = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#|javascript:)/i;

function deriveBaseUrl(){
    if (typeof window === 'undefined') {
        return '';
    }

    if (typeof window.BASE_URL === 'string' && window.BASE_URL) {
        return window.BASE_URL.replace(/\/$/, '');
    }

    if (window.AppUrl && typeof window.AppUrl.BASE_URL === 'string' && window.AppUrl.BASE_URL) {
        return window.AppUrl.BASE_URL.replace(/\/$/, '');
    }

    const marker = '/public/';
    const path = (window.location && window.location.pathname) ? window.location.pathname : '';
    const index = path.indexOf(marker);
    if (index !== -1) {
        return path.slice(0, index + marker.length).replace(/\/$/, '');
    }

    return '';
}

function resolveRelativeHref(rawHref){
    if (!rawHref) {
        return rawHref;
    }

    const href = rawHref.trim();
    if (!href || ABSOLUTE_LINK_PATTERN.test(href)) {
        return href;
    }

    const base = deriveBaseUrl();
    if (!base) {
        return normalizeAppUrl(href);
    }

    try {
        const origin = (typeof window !== 'undefined' && window.location && window.location.origin) ? window.location.origin : '';
        const baseHref = origin + base.replace(/\/$/, '') + '/';
        const composed = new URL(href, baseHref);
        const fullPath = composed.pathname + (composed.search || '') + (composed.hash || '');
        return normalizeAppUrl(fullPath);
    } catch (error) {
        console.warn('No se pudo ajustar la ruta relativa', href, error);
        const sanitized = href.replace(/^\.\//, '').replace(/^\/+/, '');
        return normalizeAppUrl(`${base}/${sanitized}`);
    }
}

function normalizePaths(root = document){
    const normalizeHref = (href) => resolveRelativeHref(normalizeAppUrl(href));

    root.querySelectorAll('a[href]').forEach(link => {
        const newHref = normalizeHref(link.getAttribute('href'));
        if(link.getAttribute('href') !== newHref) link.setAttribute('href', newHref);
    });

    root.querySelectorAll('form[action]').forEach(form => {
        const newAction = normalizeHref(form.getAttribute('action'));
        if(form.getAttribute('action') !== newAction) form.setAttribute('action', newAction);
    });

    const assetSelectors = [
        ['link[rel~="stylesheet"][href]', 'href'],
        ['link[rel~="icon"][href]', 'href'],
        ['img[src]', 'src'],
        ['source[src]', 'src'],
        ['video[src]', 'src'],
        ['audio[src]', 'src']
    ];

    assetSelectors.forEach(([selector, attr]) => {
        root.querySelectorAll(selector).forEach(el => {
            const currentValue = el.getAttribute(attr);
            const normalized = normalizeHref(currentValue);
            if (currentValue && normalized && currentValue !== normalized) {
                el.setAttribute(attr, normalized);
            }
        });
    });

    root.querySelectorAll('[onclick]').forEach(el => {
        const code = el.getAttribute('onclick');
        const newCode = code.replace(/window\.location\.href\s*=\s*['"]([^'"]+)['"]/g, (m, url) => {
            const final = normalizeHref(url);
            return `window.location.href='${final}'`;
        });
        if(code !== newCode) el.setAttribute('onclick', newCode);
    });
}

document.addEventListener("DOMContentLoaded", function() {
    normalizePaths();
    applyFeedbackShortcutTarget();
    if (shouldHandleServiceModule()) {
        applyServiceModuleVisibility();
    } else {
        document.querySelectorAll('[data-service-clientes]').forEach(link => {
            link.style.display = 'none';
        });
    }
    applyApiTokenVisibility();

    const obs = new MutationObserver(muts => {
        muts.forEach(m => m.addedNodes.forEach(n => { if(n.nodeType === 1) normalizePaths(n); }));
    });
    obs.observe(document.body, { childList:true, subtree:true });

    const SIDEBAR_BREAKPOINT = 900;
    let isDesktopCollapsed = document.body.classList.contains('sidebar-hidden');
    let sidebarStylesApplied = false;
    let sidebarBackdrop = null;

    const getSidebar = () => document.getElementById('sidebar');

    const ensureSidebarStyles = () => {
        if (sidebarStylesApplied) return;
        const sidebar = getSidebar();
        if (!sidebar) return;
        const styleId = 'sidebar-responsive-styles';
        if (!document.getElementById(styleId)) {
            const style = document.createElement('style');
            style.id = styleId;
            style.textContent = `
                #sidebar { transition: transform 0.3s ease; }
                .main-content { transition: margin-left 0.3s ease, width 0.3s ease; }
                body.sidebar-hidden #sidebar { transform: translateX(-100%) !important; }
                body.sidebar-hidden .main-content { margin-left: 0 !important; width: 100% !important; }
            `;
            document.head.appendChild(style);
        }
        sidebarStylesApplied = true;
    };

    const isMobileView = () => window.innerWidth <= SIDEBAR_BREAKPOINT;

    function ensureSidebarBackdrop() {
        if (sidebarBackdrop && sidebarBackdrop.isConnected) {
            return sidebarBackdrop;
        }

        sidebarBackdrop = document.getElementById('sidebarBackdrop');
        if (!sidebarBackdrop) {
            sidebarBackdrop = document.createElement('div');
            sidebarBackdrop.id = 'sidebarBackdrop';
            sidebarBackdrop.className = 'sidebar-backdrop';
            sidebarBackdrop.addEventListener('click', () => {
                if (isMobileView()) {
                    closeMobileSidebar();
                }
            });
            document.body.appendChild(sidebarBackdrop);
        }
        return sidebarBackdrop;
    }

    function showSidebarBackdrop() {
        if (!isMobileView()) return;
        const backdrop = ensureSidebarBackdrop();
        backdrop.classList.add('is-visible');
    }

    function hideSidebarBackdrop() {
        if (sidebarBackdrop) {
            sidebarBackdrop.classList.remove('is-visible');
        }
    }

    function collapseSidebarDesktop() {
        isDesktopCollapsed = true;
        document.body.classList.add('sidebar-hidden');
    }

    function expandSidebarDesktop() {
        isDesktopCollapsed = false;
        document.body.classList.remove('sidebar-hidden');
    }

    function openMobileSidebar() {
        const sidebar = getSidebar();
        if (!sidebar) return;
        sidebar.classList.add('open');
        document.body.classList.add('sidebar-mobile-open');
        showSidebarBackdrop();
    }

    function closeMobileSidebar() {
        const sidebar = getSidebar();
        if (!sidebar) return;
        sidebar.classList.remove('open');
        document.body.classList.remove('sidebar-mobile-open');
        hideSidebarBackdrop();
    }

    function applyResponsiveSidebarState() {
        const sidebar = getSidebar();
        if (!sidebar) return;

        if (isMobileView()) {
            expandSidebarDesktop();
            if (sidebar.classList.contains('open')) {
                document.body.classList.add('sidebar-mobile-open');
                showSidebarBackdrop();
            } else {
                document.body.classList.remove('sidebar-mobile-open');
                hideSidebarBackdrop();
            }
        } else {
            sidebar.classList.remove('open');
            document.body.classList.remove('sidebar-mobile-open');
            hideSidebarBackdrop();
            if (isDesktopCollapsed) {
                document.body.classList.add('sidebar-hidden');
            } else {
                document.body.classList.remove('sidebar-hidden');
            }
        }
    }

    function handleOpenButtonClick(event) {
        event.preventDefault();
        if (isMobileView()) {
            const sidebar = getSidebar();
            if (!sidebar) return;
            if (sidebar.classList.contains('open')) {
                closeMobileSidebar();
            } else {
                openMobileSidebar();
            }
        } else if (isDesktopCollapsed) {
            expandSidebarDesktop();
        } else {
            collapseSidebarDesktop();
        }
        applyResponsiveSidebarState();
    }

    function handleCloseButtonClick(event) {
        event.preventDefault();
        if (isMobileView()) {
            closeMobileSidebar();
        } else {
            collapseSidebarDesktop();
        }
        applyResponsiveSidebarState();
    }

    function bindSidebarControls() {
        const openButtons = document.querySelectorAll('.menu-toggle, #sidebarOpen');
        openButtons.forEach(button => {
            if (!button || button.dataset.sidebarBound) return;
            button.addEventListener('click', handleOpenButtonClick);
            button.dataset.sidebarBound = 'true';
        });

        const closeButtons = document.querySelectorAll('#sidebarToggle');
        closeButtons.forEach(button => {
            if (!button || button.dataset.sidebarBound) return;
            button.addEventListener('click', handleCloseButtonClick);
            button.dataset.sidebarBound = 'true';
        });

        const sidebar = getSidebar();
        if (sidebar && !sidebar.dataset.sidebarControlsBound) {
            sidebar.addEventListener('click', event => {
                if (!isMobileView()) return;
                if (event.target.closest('a.nav-link')) {
                    closeMobileSidebar();
                    applyResponsiveSidebarState();
                }
            });
            sidebar.dataset.sidebarControlsBound = 'true';
        }
    }

    function initializeSidebarControls() {
        ensureSidebarStyles();
        bindSidebarControls();
        applyResponsiveSidebarState();
    }

    initializeSidebarControls();
    window.initializeSidebarControls = initializeSidebarControls;

    const sidebarObserver = new MutationObserver(mutations => {
        let shouldReinitialize = false;
        for (const mutation of mutations) {
            if (mutation.type !== 'childList') continue;
            if (mutation.addedNodes.length || mutation.removedNodes.length) {
                shouldReinitialize = true;
                break;
            }
        }
        if (shouldReinitialize) {
            initializeSidebarControls();
            applyApiTokenVisibility();
            activateApiTokensIfAvailable(window.permissionFlags || DEFAULT_PERMISSION_FLAGS);
        }
    });
    sidebarObserver.observe(document.body, { childList: true, subtree: true });

    document.addEventListener('click', event => {
        if (!isMobileView()) return;
        const sidebar = getSidebar();
        if (!sidebar || !sidebar.classList.contains('open')) return;

        const openButtons = Array.from(document.querySelectorAll('.menu-toggle, #sidebarOpen'));
        const clickedInsideSidebar = sidebar.contains(event.target);
        const clickedOpenButton = openButtons.some(button => button.contains(event.target));

        if (!clickedInsideSidebar && !clickedOpenButton) {
            closeMobileSidebar();
            applyResponsiveSidebarState();
        }
    });

    document.addEventListener('keydown', event => {
        if (event.key !== 'Escape') return;
        const sidebar = getSidebar();
        if (!sidebar) return;

        if (isMobileView() && sidebar.classList.contains('open')) {
            closeMobileSidebar();
            applyResponsiveSidebarState();
        } else if (!isMobileView() && !isDesktopCollapsed) {
            collapseSidebarDesktop();
            applyResponsiveSidebarState();
        }
    });

    window.addEventListener('resize', applyResponsiveSidebarState);

    // Obtiene el usuario actual y aplica restricciones de secciones
    fetchCurrentUser()
        .then(u => {
            applyPermissionFlags(u);
            const rol = u && u.role_name ? u.role_name : undefined;
            lockSections(rol);
            applyUsuariosPermission();
        })
        .catch(error => {
            console.error('No se pudo obtener el usuario actual.', error);
            const loginPath = resolveLoginPathForPortal(detectPortalFromLocation());
            const needsRedirect = error
                && (error.status === 401
                    || error.status === 403
                    || (typeof error.message === 'string' && /sesión|session/i.test(error.message)));

            if (needsRedirect && window.location.pathname !== loginPath) {
                window.location.replace(loginPath);
            }
        });
});

function detectPortalFromLocation(){
    const pathname = (window.location && typeof window.location.pathname === 'string')
        ? window.location.pathname.toLowerCase()
        : '';

    if (pathname.includes('/apps/tenant/')){
        return 'tenant';
    }

    if (pathname.includes('/apps/service/')){
        return 'service';
    }

    return 'internal';
}

function resolveLoginPathForPortal(portal){
    const normalized = typeof portal === 'string' ? portal.toLowerCase().trim() : '';
    const map = {
        internal: '/apps/internal/usuarios/login.html',
        tenant: '/apps/tenant/usuarios/login.html',
        service: '/apps/service/usuarios/login.html'
    };

    const target = Object.prototype.hasOwnProperty.call(map, normalized) ? map[normalized] : map.internal;
    return normalizeAppUrl(target);
}

// Marca como bloqueadas las secciones a las que el rol no tiene acceso
function lockSections(rol){
    const normalized = normalizeRoleName(rol);
    const accessMap = {
        superadministrador: 'all',
        administrador: 'all',
        supervisor: ['instrumentos','calibradores','planeacion','calibraciones','reportes','tutorial'],
        operador: ['instrumentos','planeacion','calibraciones','reportes','tutorial'],
        lector: ['instrumentos','planeacion','calibraciones','reportes','tutorial'],
        cliente: ['instrumentos','planeacion','calibraciones','reportes','tutorial'],
        sistemas: 'all'
    };

    const sectionMap = {
        instrumentos : ['/instrumentos/'],
        calibradores : ['/calibradores'],
        planeacion   : ['/planeacion/'],
        calibraciones: ['/calibraciones/'],
        reportes     : ['/reportes/'],
        usuarios     : ['/usuarios/'],
        proveedores  : ['/proveedores/'],
        auditoria    : ['/auditoria/'],
        configuracion: ['/configuracion/'],
        tutorial     : ['/tutorial/']
    };

    // En condiciones normales, el backend expone el rol del usuario
    // (Superadministrador, Administrador, etc.) mediante
    // `/backend/usuarios/get_actual_user.php`.
    // Este bloque solo evita bloquear la interfaz si esa llamada falla
    // (por ejemplo, sesión expirada); las verificaciones reales de
    // permisos siempre se ejecutan en el servidor.
    const allowed = accessMap[normalized];
    if (allowed === undefined) return;

    document.querySelectorAll('.sidebar-nav .nav-link, a.dashboard-btn').forEach(link => {
        let section = link.dataset.section;
        if(!section){
            const href = link.getAttribute('href') || '';
            for (const [key, patterns] of Object.entries(sectionMap)){
                if (patterns.some(p => href.includes(p))){ section = key; break; }
            }
        }
        if(section === 'usuarios') return;
        if (section && allowed !== 'all' && !allowed.includes(section)){
            link.classList.add('locked');
        }
    });
}


function applyUsuariosPermission(){
    fetch(resolveAppUrl('/backend/check_permission.php?permiso=usuarios_view'))
        .then(r=>r.json())
        .then(d=>{
            document.querySelectorAll('[data-section="usuarios"]').forEach(link=>{
                if(!d.allowed) link.remove();
                else link.classList.remove('locked');
            });
        }).catch(()=>{});
}

// ========== UTILIDADES ==========

// Obtiene el filtro desde la URL (?filtro=activo etc.)
function getFiltroURL() {
    const params = new URLSearchParams(window.location.search);
    return params.get('filtro') || '';
}

// Devuelve "NA" cuando el valor es nulo, indefinido o cadena vacía
function formatNA(value) {
    return (value === null || value === undefined ||
            (typeof value === 'string' && value.trim() === ''))
        ? 'NA'
        : value;
}

function escapeHtml(value) {
    if (value === null || value === undefined) {
        return '';
    }
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Etiquetas legibles para cada estado normalizado de calibración.
const CALIBRATION_STATUS_LABELS = Object.freeze({
    vigente: 'Vigente',
    por_vencer: 'Por vencer',
    proximo: 'Próximo vencimiento',
    vencido: 'Vencido',
    sin_programar: 'Sin programar',
    desconocido: 'Sin información'
});

// Clases CSS para representar visualmente cada estado de calibración.
const CALIBRATION_STATUS_BADGES = Object.freeze({
    vigente: 'bg-success',
    por_vencer: 'bg-warning text-dark',
    proximo: 'bg-warning text-dark',
    vencido: 'bg-danger',
    sin_programar: 'bg-secondary',
    desconocido: 'bg-secondary'
});

// Normaliza un valor de estado a su representación en minúsculas sin espacios extra.
function normalizeCalibrationStatus(value) {
    if (value === null || value === undefined) {
        return '';
    }
    const raw = value.toString().trim().toLowerCase();
    const normalized = typeof raw.normalize === 'function'
        ? raw.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        : raw;
    return normalized.replace(/\s+/g, '_');
}

function calcularDiasRestantesDesdeFecha(fecha, referencia) {
    if (!fecha) return null;
    const objetivo = new Date(fecha);
    if (Number.isNaN(objetivo.getTime())) return null;
    const base = referencia ? new Date(referencia) : new Date();
    objetivo.setHours(0, 0, 0, 0);
    base.setHours(0, 0, 0, 0);
    const diffMs = objetivo.getTime() - base.getTime();
    return Math.round(diffMs / 86400000);
}

// Obtiene los días restantes desde un registro de calibración o los calcula desde fechas.
function obtenerDiasRestantesRegistro(registro) {
    if (!registro || typeof registro !== 'object') {
        return null;
    }
    if (typeof registro.dias_restantes === 'number' && Number.isFinite(registro.dias_restantes)) {
        return registro.dias_restantes;
    }
    if (typeof registro.dias_restantes === 'string' && registro.dias_restantes.trim() !== '') {
        const parsed = Number(registro.dias_restantes);
        if (Number.isFinite(parsed)) {
            return parsed;
        }
    }
    return calcularDiasRestantesDesdeFecha(registro.proxima_calibracion || registro.fecha_proxima || registro.fecha_programada);
}

// Devuelve una descripción legible del tiempo restante antes o después de la calibración.
function describirDiasRestantes(dias) {
    if (dias === null || dias === undefined) {
        return 'Sin fecha programada';
    }
    if (dias === 0) {
        return 'Vence hoy';
    }
    if (dias > 0) {
        return dias === 1 ? 'Falta 1 día para la calibración' : `Faltan ${dias} días para la calibración`;
    }
    const diasPositivos = Math.abs(dias);
    return diasPositivos === 1 ? 'Vencido hace 1 día' : `Vencido hace ${diasPositivos} días`;
}

// Construye la información completa del estado de calibración para un registro dado.
function obtenerInfoEstadoCalibracion(registro) {
    const diasRestantes = obtenerDiasRestantesRegistro(registro);
    let estado = normalizeCalibrationStatus(
        registro && (registro.calibration_status !== undefined && registro.calibration_status !== null
            ? registro.calibration_status
            : registro.estado_calibracion)
    );

    if (estado === 'porvencer') {
        estado = 'por_vencer';
    }
    if (estado === 'proximo') {
        estado = 'por_vencer';
    }

    if (!estado) {
        if (diasRestantes === null) {
            estado = 'sin_programar';
        } else if (diasRestantes < 0) {
            estado = 'vencido';
        } else if (diasRestantes <= 30) {
            estado = 'por_vencer';
        } else {
            estado = 'vigente';
        }
    }

    const label = CALIBRATION_STATUS_LABELS[estado] || (estado ? estado.charAt(0).toUpperCase() + estado.slice(1) : CALIBRATION_STATUS_LABELS.desconocido);
    const badgeClass = CALIBRATION_STATUS_BADGES[estado] || CALIBRATION_STATUS_BADGES.desconocido;
    const tooltip = describirDiasRestantes(diasRestantes);

    return {
        estado,
        calibration_status: estado,
        label,
        badgeClass,
        tooltip,
        dias_restantes: diasRestantes
    };
}

// Renderiza un badge HTML para mostrar el estado de calibración en tablas y listados.
function renderCalibrationStatusBadge(infoOrRegistro, options) {
    const info = infoOrRegistro && typeof infoOrRegistro === 'object' && infoOrRegistro.estado && infoOrRegistro.label
        ? infoOrRegistro
        : obtenerInfoEstadoCalibracion(infoOrRegistro);
    const settings = Object.assign({ includeTitle: true }, options || {});
    const titleAttr = settings.includeTitle ? ` title="${escapeHtml(info.tooltip)}"` : '';
    const diasAttr = info.dias_restantes !== null && info.dias_restantes !== undefined
        ? ` data-dias-restantes="${info.dias_restantes}"`
        : '';
    const statusCode = info.calibration_status || info.estado;
    return `<span class="badge ${info.badgeClass}" data-calibration-status="${escapeHtml(statusCode)}"${diasAttr}${titleAttr}>${escapeHtml(info.label)}</span>`;
}

// Determina si un registro coincide con un filtro de periodo de calibración dado.
function calibrationMatchesPeriodFilter(infoOrRegistro, filtro) {
    if (!filtro || filtro === 'all') {
        return true;
    }
    const info = infoOrRegistro && typeof infoOrRegistro === 'object' && infoOrRegistro.estado && infoOrRegistro.label
        ? infoOrRegistro
        : obtenerInfoEstadoCalibracion(infoOrRegistro);
    const dias = info.dias_restantes;
    if (filtro === 'expired') {
        return typeof dias === 'number' ? dias < 0 : false;
    }
    const normalizedFilter = normalizeCalibrationStatus(filtro);
    if (normalizedFilter === 'vigente' || normalizedFilter === 'por_vencer' || normalizedFilter === 'vencido') {
        return info.estado === normalizedFilter;
    }
    if (normalizedFilter === 'proximo') {
        return info.estado === 'por_vencer';
    }
    const limite = parseInt(normalizedFilter, 10);
    if (!Number.isNaN(limite)) {
        if (dias === null) {
            return false;
        }
        if (limite < 0) {
            return dias < 0;
        }
        return dias >= 0 && dias <= limite;
    }
    return true;
}

const calibrationStatusHelpers = {
    LABELS: CALIBRATION_STATUS_LABELS,
    BADGES: CALIBRATION_STATUS_BADGES,
    normalizeStatus: normalizeCalibrationStatus,
    getStatusInfo: obtenerInfoEstadoCalibracion,
    renderBadge: renderCalibrationStatusBadge,
    matchesPeriodFilter: calibrationMatchesPeriodFilter,
    describeDiasRestantes: describirDiasRestantes,
    getDiasRestantes: obtenerDiasRestantesRegistro
};

export const CalibrationStatusHelpers = calibrationStatusHelpers;

if (typeof window !== 'undefined') {
    window.CalibrationStatusHelpers = calibrationStatusHelpers;
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = calibrationStatusHelpers;
}

// Devuelve la fecha formateada como DD/Mmm/AAAA (e.g., 16/Abr/2019)
function formatFechaCorta(fecha) {
    return new Date(fecha)
        .toLocaleDateString('es-ES', {
            day: '2-digit',
            month: 'short',
            year: 'numeric'
        })
        .replace('.', '')
        .replace(/\b[a-z]/, m => m.toUpperCase());
}

function formatNumber(value, options) {
    var numeric = typeof value === 'number' ? value : Number(value);
    if (!isFinite(numeric)) {
        numeric = 0;
    }
    var config = Object.assign({ minimumFractionDigits: 0, maximumFractionDigits: 0 }, options || {});
    return numeric.toLocaleString('es-MX', config);
}

function formatPercentage(value, options) {
    var numeric = typeof value === 'number' ? value : Number(value);
    if (!isFinite(numeric)) {
        numeric = 0;
    }
    var config = Object.assign({ minimumFractionDigits: 0, maximumFractionDigits: 1 }, options || {});
    return numeric.toLocaleString('es-MX', config) + '%';
}

function formatKpiValue(metric, options) {
    if (!metric || metric.value === null || metric.value === undefined) {
        return 'NA';
    }
    var overrides = options || {};
    var decimals = typeof metric.decimals === 'number' ? metric.decimals : undefined;
    if (metric.type === 'percentage') {
        var percentConfig = Object.assign({ minimumFractionDigits: 0, maximumFractionDigits: 1 }, overrides);
        if (typeof decimals === 'number') {
            percentConfig.minimumFractionDigits = decimals;
            percentConfig.maximumFractionDigits = decimals;
        }
        return formatPercentage(metric.value, percentConfig);
    }
    var numberConfig = Object.assign({ minimumFractionDigits: 0, maximumFractionDigits: 0 }, overrides);
    if (typeof decimals === 'number') {
        numberConfig.minimumFractionDigits = decimals;
        numberConfig.maximumFractionDigits = decimals;
    }
    return formatNumber(metric.value, numberConfig);
}

function formatKpiSupport(metric) {
    if (!metric || !metric.support) {
        return '';
    }
    var support = metric.support;
    if (typeof support === 'string') {
        return support;
    }
    if (support.text) {
        return support.text;
    }
    var segments = [];
    if (support.label) {
        segments.push(support.label + ':');
    }
    if (support.total !== undefined && support.total !== null) {
        var numeratorDecimals = typeof support.decimals === 'number' ? support.decimals : 0;
        var numerator = formatNumber(support.value || 0, {
            minimumFractionDigits: numeratorDecimals,
            maximumFractionDigits: numeratorDecimals
        });
        var denominator = formatNumber(support.total || 0, {
            minimumFractionDigits: numeratorDecimals,
            maximumFractionDigits: numeratorDecimals
        });
        segments.push(numerator + ' / ' + denominator);
    } else if (support.value !== undefined && support.value !== null) {
        var decimals = typeof support.decimals === 'number' ? support.decimals : 0;
        segments.push(formatNumber(support.value, {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals
        }));
    }
    if (support.extra) {
        segments.push(support.extra);
    }
    return segments.join(' ').trim();
}

function describeKpiType(metric) {
    if (!metric || !metric.type) {
        return 'Conteo';
    }
    switch (metric.type) {
        case 'percentage':
            return 'Porcentaje';
        case 'decimal':
            return 'Decimal';
        default:
            return 'Conteo';
    }
}

// ========== INSTRUMENTOS ==========

// Carga los instrumentos desde el backend, opcionalmente filtrando por tipo
function cargarInstrumentos(filtro = "") {
    let url = resolveAppUrl('/backend/instrumentos/gages/list_gages.php');
    if (filtro) url += "?filtro=" + filtro;
    fetch(url)
        .then(r => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        })
        .then(data => {
             if (data.error) {
                console.error('API error:', data.error);
                showSaveModal('Error: ' + data.error);
                window.instrumentos = [];
            } else {
                window.instrumentos = data;
            }
            renderTablaInstrumentos();
        })
        .catch(err => {
            console.error('Fetch error:', err);
            showSaveModal('Error al cargar instrumentos');
            window.instrumentos = [];
            renderTablaInstrumentos();
        });
}

// Renderiza la tabla de instrumentos con paginación
function renderTablaInstrumentos() {
    // Variables globales para paginación
    window.currentPage = window.currentPage || 1;
    window.perPage = window.perPage || 100;
    const instrumentos = window.instrumentos || [];
    const tbody = document.querySelector('#tablaInstrumentos tbody');
    if (!tbody) return;

    tbody.innerHTML = "";
    const totalPages = Math.max(1, Math.ceil(instrumentos.length / window.perPage));
    const currentPage = window.currentPage;
    // Actualiza paginación
    if (document.getElementById('totalPages')) document.getElementById('totalPages').textContent = totalPages;
    if (document.getElementById('currentPage')) document.getElementById('currentPage').value = currentPage;
    // Botones de paginación
    if (document.getElementById('btnFirst')) document.getElementById('btnFirst').disabled = currentPage === 1;
    if (document.getElementById('btnPrev')) document.getElementById('btnPrev').disabled = currentPage === 1;
    if (document.getElementById('btnNext')) document.getElementById('btnNext').disabled = currentPage === totalPages;
    if (document.getElementById('btnLast')) document.getElementById('btnLast').disabled = currentPage === totalPages;

    if (!instrumentos.length) {
        const row = document.createElement('tr');
        const td = document.createElement('td');
        // Ajusta automáticamente al número de columnas en la cabecera
        const columnCount = document.querySelectorAll('#tablaInstrumentos thead th').length;
        td.colSpan = columnCount;
        td.className = "empty-row";
        td.textContent = "No hay instrumentos registrados.";
        row.appendChild(td);
        tbody.appendChild(row);
        return;
    }
    // Paginación
    const start = (currentPage - 1) * window.perPage;
    const end = Math.min(start + window.perPage, instrumentos.length);
    for (let i = start; i < end; i++) {
        const inst = instrumentos[i];
        const row = document.createElement('tr');
        row.innerHTML = `
            <td><input type="checkbox" /></td>
             <td>${formatNA(inst.id)}</td>
            <td>${formatNA(inst.nombre)}</td>
            <td>${formatNA(inst.marca)}</td>
            <td>${formatNA(inst.modelo)}</td>
            <td>${formatNA(inst.serie)}</td>
            <td>${formatNA(inst.codigo)}</td>
            <td>${formatNA(inst.departamento)}</td>
            <td>${formatNA(inst.ubicacion)}</td>
            <td class="date-col">${inst.fecha_alta ? formatFechaCorta(inst.fecha_alta) : 'NA'}</td>
            <td class="date-col">${inst.fecha_baja ? formatFechaCorta(inst.fecha_baja) : 'NA'}</td>
            <td class="date-col">${inst.proxima_calibracion ? formatFechaCorta(inst.proxima_calibracion) : 'NA'}</td>
        `;
        tbody.appendChild(row);
    }
}

// Carga el historial de un instrumento y llena el contenido del modal
function cargarHistorial(id) {
    const cambiosEl = document.getElementById('cambios');
    const fechasEl = document.getElementById('fechas');
    const certsEl = document.getElementById('certificados');
    if (cambiosEl) cambiosEl.innerHTML = '<p>Cargando...</p>';
    if (fechasEl) fechasEl.innerHTML = '<p>Cargando...</p>';
    if (certsEl) certsEl.innerHTML = '<p>Cargando...</p>';

    fetch(resolveAppUrl(`/backend/instrumentos/gages/get_gage_history.php?instrument_id=${id}`))
        .then(r => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        })
        .then(data => {
            // Historial de cambios
            if (cambiosEl) {
                if (data.changes && data.changes.length) {
                    let html = '<ul class="list-group">';
                    data.changes.forEach(ch => {
                        const tipo =
                            ch.type === 'department' ? 'Departamento' :
                            ch.type === 'location' ? 'Ubicación' :
                            ch.type === 'state' ? 'Estado' : '';
                        html += `<li class="list-group-item"><strong>${tipo}:</strong> ${formatNA(ch.value)} <span class="text-muted float-end">${ch.date ? formatFechaCorta(ch.date) : 'NA'}</span></li>`;
                    });
                    html += '</ul>';
                    cambiosEl.innerHTML = html;
                } else {
                    cambiosEl.innerHTML = '<p>No hay datos de cambios.</p>';
                }
            }

            // Fechas relevantes
            const calendarData = (data && typeof data.calendar === 'object' && data.calendar !== null)
                ? data.calendar
                : {};
            const periodsOrder = Array.isArray(data.periods) && data.periods.length
                ? data.periods
                : ['P1', 'P2', 'EXTRA'];
            const periodLabels = { P1: 'Periodo 1', P2: 'Periodo 2', EXTRA: 'Extraordinario' };

            const sortedYears = Object.keys(calendarData)
                .sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));

            if (fechasEl) {
                if (sortedYears.length) {
                    let html = '<div class="table-responsive"><table class="table table-sm align-middle mb-0">';
                    html += '<thead><tr><th>Año</th><th>Periodo</th><th>Fecha</th><th>Próxima</th><th>Resultado</th><th>Certificado</th></tr></thead><tbody>';
                    sortedYears.forEach(year => {
                        const yearData = calendarData[year] || {};
                        periodsOrder.forEach(period => {
                            const entry = yearData[period] || null;
                            const fecha = entry && entry.fecha ? formatFechaCorta(entry.fecha) : 'Sin registro';
                            const proxima = entry && entry.fecha_proxima ? formatFechaCorta(entry.fecha_proxima) : 'NA';
                            const resultado = entry ? formatNA(entry.resultado) : 'NA';
                            let certificado = 'Sin certificado';
                            if (entry && entry.certificado_url) {
                                const url = normalizeAppUrl(entry.certificado_url);
                                certificado = `<a href="${url}" target="_blank" rel="noopener">Ver certificado</a>`;
                            }
                            const periodoNombre = periodLabels[period] || period;
                            html += `<tr><td>${year}</td><td>${periodoNombre}</td><td>${fecha}</td><td>${proxima}</td><td>${resultado}</td><td>${certificado}</td></tr>`;
                        });
                    });
                    html += '</tbody></table></div>';
                    fechasEl.innerHTML = html;
                } else {
                    fechasEl.innerHTML = '<p>No hay fechas relevantes.</p>';
                }
            }

            if (certsEl) {
                const certificateItems = [];
                sortedYears.forEach(year => {
                    const yearData = calendarData[year] || {};
                    periodsOrder.forEach(period => {
                        const entry = yearData[period];
                        if (entry && entry.certificado_url) {
                            certificateItems.push({
                                year,
                                period,
                                url: normalizeAppUrl(entry.certificado_url),
                                date: entry.fecha || null
                            });
                        }
                    });
                });

                if (certificateItems.length) {
                    let html = '<ul class="list-group">';
                    certificateItems.forEach(item => {
                        const fecha = item.date ? formatFechaCorta(item.date) : 'Sin fecha';
                        const periodoNombre = periodLabels[item.period] || item.period;
                        html += `<li class="list-group-item d-flex justify-content-between align-items-center"><span>${fecha} (${item.year} - ${periodoNombre})</span><a class="btn btn-sm btn-primary" href="${item.url}" target="_blank" rel="noopener" download>Descargar</a></li>`;
                    });
                    html += '</ul>';
                    certsEl.innerHTML = html;
                } else {
                    certsEl.innerHTML = '<p>No hay certificados.</p>';
                }
            }
        })
        .catch(err => {
            console.error('Error al cargar historial', err);
            if (cambiosEl) cambiosEl.innerHTML = '<p>Error al cargar historial.</p>';
            if (fechasEl) fechasEl.innerHTML = '<p>Error al cargar historial.</p>';
            if (certsEl) certsEl.innerHTML = '<p>Error al cargar historial.</p>';
        });
}
window.cargarHistorial = cargarHistorial;

// ========== EVENTOS DE PAGINACIÓN INSTRUMENTOS ==========

function setPaginacionInstrumentos() {
    window.currentPage = 1;
    window.perPage = 100;
    // Select de paginación
    if (document.getElementById('selectPerPage')) {
        document.getElementById('selectPerPage').addEventListener('change', function() {
            window.perPage = parseInt(this.value);
            renderTablaInstrumentos();
        });
    }
    // Botones paginación
    if (document.getElementById('btnFirst')) document.getElementById('btnFirst').onclick = function() {
        window.currentPage = 1; renderTablaInstrumentos();
    };
    if (document.getElementById('btnPrev')) document.getElementById('btnPrev').onclick = function() {
        if (window.currentPage > 1) window.currentPage--; renderTablaInstrumentos();
    };
    if (document.getElementById('btnNext')) document.getElementById('btnNext').onclick = function() {
        const totalPages = Math.ceil((window.instrumentos || []).length / window.perPage);
        if (window.currentPage < totalPages) window.currentPage++; renderTablaInstrumentos();
    };
    if (document.getElementById('btnLast')) document.getElementById('btnLast').onclick = function() {
        window.currentPage = Math.max(1, Math.ceil((window.instrumentos || []).length / window.perPage));
        renderTablaInstrumentos();
    };
    if (document.getElementById('currentPage')) document.getElementById('currentPage').addEventListener('change', function() {
        let val = parseInt(this.value);
        const totalPages = Math.ceil((window.instrumentos || []).length / window.perPage);
        if (val < 1) val = 1;
        if (val > totalPages) val = totalPages;
        window.currentPage = val;
        renderTablaInstrumentos();
    });
}

// ========== FILTROS DE INSTRUMENTOS ==========

// Evento para el select de filtros en la lista general
function setFiltroInstrumentos() {
    // Cambia el filtro y recarga
    const filtroSelect = document.getElementById('filtroInstrumentos');
    if (filtroSelect) {
        filtroSelect.addEventListener('change', function() {
            cargarInstrumentos(this.value);
            // Cambia el título de la cabecera
            let txt = "Instrumentos &gt; Lista General";
            switch (this.value) {
                case "activo": txt = "Instrumentos &gt;  Activos"; break;
                case "inactivo": txt = "Instrumentos &gt;  Inactivos"; break;
                case "programado": txt = "Instrumentos &gt;  Programados"; break;
                case "no_programado": txt = "Instrumentos &gt;  No Programados"; break;
            }
            if (document.getElementById('instrumentosHeaderTitle')) {
                document.getElementById('instrumentosHeaderTitle').innerHTML = txt;
            }
        });
    }
}

// ========== BOTONES DEL DASHBOARD PRINCIPAL ==========

// Ejemplo: funciones para ir al filtro correcto desde el dashboard
function mostrarActivos() {
    window.location.href = resolveAppUrl('/apps/internal/instrumentos/list_gages.html?filtro=activo');
}
function mostrarInactivos() {
    window.location.href = resolveAppUrl('/apps/internal/instrumentos/list_gages.html?filtro=inactivo');
}
function mostrarProgramados() {
    window.location.href = resolveAppUrl('/apps/internal/instrumentos/list_gages.html?filtro=programado');
}
function mostrarNoProgramados() {
    window.location.href = resolveAppUrl('/apps/internal/instrumentos/list_gages.html?filtro=no_programado');
}
function mostrarTodosInstrumentos() {
    window.location.href = resolveAppUrl('/apps/internal/instrumentos/list_gages.html');
}

// ========== REPORTES PERSONALIZADOS ==========

const customReportSelections = {};

function getCustomDataset(datasetKey) {
    if (!datasetKey) return null;
    return Object.prototype.hasOwnProperty.call(CUSTOM_REPORT_DATASETS, datasetKey)
        ? CUSTOM_REPORT_DATASETS[datasetKey]
        : null;
}

function getSelectedCustomReportColumns() {
    const container = document.getElementById('datasetColumns');
    if (!container) return [];
    return Array.from(container.querySelectorAll('input[type="checkbox"]:checked')).map(input => input.value);
}

function populateCustomReportDatasets() {
    const select = document.getElementById('datasetType');
    if (!select) return;
    const previous = select.value;
    select.innerHTML = '<option value="">Selecciona un dataset…</option>';
    Object.entries(CUSTOM_REPORT_DATASETS).forEach(([key, meta]) => {
        const option = document.createElement('option');
        option.value = key;
        option.textContent = meta.label;
        select.appendChild(option);
    });
    if (previous && CUSTOM_REPORT_DATASETS[previous]) {
        select.value = previous;
    }
}

function renderCustomReportColumns(datasetKey) {
    const container = document.getElementById('datasetColumns');
    const selectAllBtn = document.getElementById('selectAllColumns');
    const clearBtn = document.getElementById('clearAllColumns');
    if (!container) return;

    const dataset = getCustomDataset(datasetKey);
    container.innerHTML = '';

    const buttonsDisabled = !dataset;
    if (selectAllBtn) selectAllBtn.disabled = buttonsDisabled;
    if (clearBtn) clearBtn.disabled = buttonsDisabled;

    if (!dataset) {
        container.innerHTML = '<div class="hint-text">Selecciona un dataset para elegir las columnas disponibles.</div>';
        updateCustomReportPreview();
        return;
    }

    if (!Array.isArray(customReportSelections[datasetKey]) || !customReportSelections[datasetKey].length) {
        customReportSelections[datasetKey] = Array.from(dataset.defaultColumns || []);
    }

    dataset.columns.forEach(column => {
        const label = document.createElement('label');
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = column.key;
        checkbox.checked = customReportSelections[datasetKey].includes(column.key);
        checkbox.addEventListener('change', () => {
            customReportSelections[datasetKey] = getSelectedCustomReportColumns();
            updateCustomReportPreview();
        });
        const span = document.createElement('span');
        span.textContent = column.label;
        label.appendChild(checkbox);
        label.appendChild(span);
        container.appendChild(label);
    });

    updateCustomReportPreview();
}

function updateCustomReportFilters(datasetKey) {
    const dataset = getCustomDataset(datasetKey);
    const dateLabelFrom = document.getElementById('dateFilterLabel');
    const dateLabelTo = document.getElementById('dateFilterLabelTo');
    const statusLabel = document.getElementById('statusFilterLabel');
    const statusInput = document.getElementById('filterStatus');
    const datalist = document.getElementById('statusSuggestionsList');

    if (dateLabelFrom) {
        dateLabelFrom.textContent = dataset && dataset.dateLabel ? dataset.dateLabel + ' (desde)' : 'Fecha desde';
    }
    if (dateLabelTo) {
        dateLabelTo.textContent = dataset && dataset.dateLabel ? dataset.dateLabel + ' (hasta)' : 'Fecha hasta';
    }
    if (statusLabel) {
        statusLabel.textContent = dataset && dataset.statusLabel ? dataset.statusLabel : 'Estado';
    }

    if (datalist) {
        datalist.innerHTML = '';
        if (dataset && Array.isArray(dataset.statusSuggestions)) {
            dataset.statusSuggestions.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                datalist.appendChild(option);
            });
        }
    }

    if (statusInput) {
        statusInput.placeholder = dataset ? 'Escribe o selecciona un estado' : 'Selecciona primero un dataset';
    }
}

function buildCustomReportPayload() {
    const datasetSelect = document.getElementById('datasetType');
    const formatSelect = document.getElementById('defaultFormat');
    const dateFrom = document.getElementById('filterDateFrom');
    const dateTo = document.getElementById('filterDateTo');
    const statusInput = document.getElementById('filterStatus');

    const datasetKey = datasetSelect ? datasetSelect.value : '';
    const dataset = getCustomDataset(datasetKey);
    const errors = [];

    if (!dataset) {
        errors.push('Selecciona un dataset válido.');
    }

    const selectedColumns = dataset ? getSelectedCustomReportColumns() : [];
    if (!selectedColumns.length) {
        errors.push('Selecciona al menos una columna para el reporte.');
    }

    const filtros = {};
    const desde = dateFrom && dateFrom.value ? dateFrom.value : '';
    const hasta = dateTo && dateTo.value ? dateTo.value : '';
    if (desde && hasta && desde > hasta) {
        errors.push('El rango de fechas seleccionado no es válido.');
    }
    if (desde) filtros.fecha_desde = desde;
    if (hasta) filtros.fecha_hasta = hasta;
    const estado = statusInput && statusInput.value ? statusInput.value.trim() : '';
    if (estado) filtros.estado = estado.slice(0, 100);

    const allowedFormats = ['excel', 'csv', 'pdf'];
    let formato = formatSelect ? formatSelect.value : 'excel';
    if (!allowedFormats.includes(formato)) {
        formato = 'excel';
    }

    return {
        config: { dataset: datasetKey, columnas: selectedColumns },
        filtros,
        formato,
        errors
    };
}

function updateCustomReportPreview() {
    const preview = document.getElementById('configPreview');
    const configInput = document.getElementById('configuracion');
    const filtrosInput = document.getElementById('filtros');
    if (!preview) return;

    const payload = buildCustomReportPayload();
    if (!payload.config.dataset) {
        preview.textContent = 'Selecciona un dataset y columnas para generar la vista previa…';
        if (configInput) configInput.value = '';
        if (filtrosInput) filtrosInput.value = '';
        return;
    }

    if (payload.errors.length) {
        preview.textContent = payload.errors[0];
        if (configInput) configInput.value = '';
        if (filtrosInput) filtrosInput.value = '';
        return;
    }

    const dataset = getCustomDataset(payload.config.dataset);
    const columnLabels = payload.config.columnas.map(columnKey => {
        const columnDef = dataset ? dataset.columns.find(col => col.key === columnKey) : null;
        return columnDef ? columnDef.label : columnKey;
    });

    const summary = {
        dataset: dataset ? dataset.label : payload.config.dataset,
        columnas: columnLabels,
        filtros: payload.filtros,
        formato: payload.formato
    };

    preview.textContent = JSON.stringify(summary, null, 2);
    if (configInput) configInput.value = JSON.stringify(payload.config);
    if (filtrosInput) filtrosInput.value = JSON.stringify(payload.filtros);
}

function cargarReportesPersonalizados() {
    fetch(resolveAppUrl('/backend/reportes/list_reports.php'))
        .then(r => r.json())
        .then(reportes => {
            const tbody = document.getElementById('reportBuilderBody');
            if (!tbody) return;
            tbody.innerHTML = '';
            window.reportesPersonalizados = Array.isArray(reportes) ? reportes : [];
            if (!window.reportesPersonalizados.length) {
                tbody.innerHTML = '<tr><td colspan="4" class="empty-row">Sin filas para mostrar.</td></tr>';
            } else {
                const columns = [
                    {
                        key: 'dataset_label',
                        label: 'Instrumento',
                        getValue: rep => rep.dataset_label || rep.instrumento || 'NA'
                    },
                    {
                        key: 'nombre',
                        label: 'Nombre del Reporte',
                        getValue: rep => rep.nombre || 'NA'
                    },
                    {
                        key: 'fecha_creacion',
                        label: 'Fecha de Creación',
                        className: 'date-col',
                        getValue: rep => rep.fecha_creacion ? formatFechaCorta(rep.fecha_creacion) : 'NA'
                    },
                    {
                        key: 'creado_por',
                        label: 'Creado Por',
                        getValue: rep => rep.creado_por || 'NA'
                    }
                ];

                window.reportesPersonalizados.forEach(rep => {
                    const tr = document.createElement('tr');
                    columns.forEach(col => {
                        const td = document.createElement('td');
                        if (col.className) {
                            td.classList.add(col.className);
                        }
                        td.dataset.label = col.label;
                        td.textContent = col.getValue ? col.getValue(rep) : (rep[col.key] || 'NA');
                        tr.appendChild(td);
                    });
                    tbody.appendChild(tr);
                });
            }
            document.dispatchEvent(new CustomEvent('reportesPersonalizadosActualizados', {
                detail: window.reportesPersonalizados
            }));
        })
        .catch(() => {
            window.reportesPersonalizados = [];
            const tbody = document.getElementById('reportBuilderBody');
            if (tbody && !tbody.innerHTML.trim()) {
                tbody.innerHTML = '<tr><td colspan="4" class="empty-row">Sin filas para mostrar.</td></tr>';
            }
            document.dispatchEvent(new CustomEvent('reportesPersonalizadosActualizados', {
                detail: window.reportesPersonalizados
            }));
        });
}

function ejecutarReportePersonalizado(id, format) {
    const allowed = ['excel', 'csv', 'pdf'];
    const selectedFormat = allowed.includes(format) ? format : 'excel';
    const url = `${resolveAppUrl('/backend/reportes/run_custom_report.php')}?id=${encodeURIComponent(id)}&format=${encodeURIComponent(selectedFormat)}`;
    window.open(url, '_blank');
}

function enviarReportePersonalizado() {
    const nombre = document.getElementById('reportName') ? document.getElementById('reportName').value.trim() : '';
    const msgBox = document.getElementById('addReportMsg');
    if (msgBox) {
        msgBox.innerHTML = '';
    }

    if (!nombre) {
        if (msgBox) {
            msgBox.innerHTML = '<div class="msg-error">El nombre del reporte es obligatorio.</div>';
        }
        return;
    }

    const payload = buildCustomReportPayload();
    if (payload.errors.length) {
        if (msgBox) {
            msgBox.innerHTML = `<div class="msg-error">${payload.errors.join('<br>')}</div>`;
        }
        return;
    }

    const formData = new FormData();
    formData.append('nombre', nombre);
    formData.append('configuracion', JSON.stringify(payload.config));
    formData.append('filtros', JSON.stringify(payload.filtros));
    formData.append('formato_preferido', payload.formato);

    fetch(resolveAppUrl('/backend/reportes/add_report.php'), {
        method: 'POST',
        body: formData
    })
        .then(r => r.json())
        .then(res => {
            if (msgBox) {
                if (res.success) {
                    msgBox.innerHTML = `<div class="msg-success">${res.msg}</div>`;
                    setTimeout(() => {
                        window.location.href = resolveAppUrl('/apps/internal/reportes/reports.html');
                    }, 1400);
                } else {
                    msgBox.innerHTML = `<div class="msg-error">${res.msg}</div>`;
                }
            }
        })
        .catch(() => {
            if (msgBox) {
                msgBox.innerHTML = '<div class="msg-error">Error de conexión.</div>';
            }
        });
}

function initializeCustomReportForm() {
    const form = document.getElementById('formAddReport');
    if (!form) return;

    populateCustomReportDatasets();

    const datasetSelect = document.getElementById('datasetType');
    if (datasetSelect) {
        datasetSelect.addEventListener('change', () => {
            const datasetKey = datasetSelect.value;
            renderCustomReportColumns(datasetKey);
            updateCustomReportFilters(datasetKey);
        });
    }

    const selectAllBtn = document.getElementById('selectAllColumns');
    if (selectAllBtn) {
        selectAllBtn.addEventListener('click', () => {
            if (!datasetSelect || !datasetSelect.value) return;
            const dataset = getCustomDataset(datasetSelect.value);
            if (!dataset) return;
            customReportSelections[datasetSelect.value] = dataset.columns.map(col => col.key);
            renderCustomReportColumns(datasetSelect.value);
        });
    }

    const clearBtn = document.getElementById('clearAllColumns');
    if (clearBtn) {
        clearBtn.addEventListener('click', () => {
            if (!datasetSelect || !datasetSelect.value) return;
            customReportSelections[datasetSelect.value] = [];
            renderCustomReportColumns(datasetSelect.value);
        });
    }

    ['filterDateFrom', 'filterDateTo', 'filterStatus', 'defaultFormat'].forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.addEventListener('change', updateCustomReportPreview);
            element.addEventListener('input', updateCustomReportPreview);
        }
    });

    form.addEventListener('submit', function (event) {
        event.preventDefault();
        enviarReportePersonalizado();
    });

    const initialDataset = datasetSelect ? datasetSelect.value : '';
    updateCustomReportFilters(initialDataset);
    renderCustomReportColumns(initialDataset);
}

// ========== INICIALIZACIÓN SEGÚN PÁGINA ==========

window.addEventListener('DOMContentLoaded', function() {
    // INSTRUMENTOS: detecta si existe la tabla para inicializar
    if (document.getElementById('tablaInstrumentos') && !document.getElementById('btnEdit')) {
        setPaginacionInstrumentos();
        setFiltroInstrumentos();
        // Si la página tiene filtro en la URL, úsalo
        const filtro = getFiltroURL();
        cargarInstrumentos(filtro);
    }
    // REPORTES PERSONALIZADOS
    if (document.getElementById('reportBuilderBody')) {
        cargarReportesPersonalizados();
    }
    // FORMULARIO DE REPORTE PERSONALIZADO
    if (document.getElementById('formAddReport')) {
        initializeCustomReportForm();
    }
});
document.querySelectorAll('.card.action button').forEach(btn => {
    btn.addEventListener('click', () => {
        showSaveModal('Función aún no implementada.');
    });
});

// ========== SIDEBAR: ACTIVAR SECCIÓN ACTUAL ==========
function initSidebar() {
    try {
        var sidebarEl = document.querySelector('.sidebar');
        if (!sidebarEl) return;
        var links = sidebarEl.querySelectorAll('.sidebar-nav a.nav-link');
        if (!links || !links.length) return;

        var basePath = (typeof window.BASE_URL === 'string' && window.BASE_URL)
            ? window.BASE_URL
            : '';

        function normalizePath(path) {
            if (!path) return '';
            try {
                var resolvedUrl = new URL(path, window.location.href);
                path = resolvedUrl.pathname || path;
            } catch (_) {}
            if (basePath && path.indexOf(basePath) === 0) {
                path = path.slice(basePath.length);
            }
            path = '/' + String(path || '').replace(/^\/+/, '');
            path = path.replace(/index\.html?$/i, '');
            if (path.length > 1 && path.endsWith('/')) {
                path = path.slice(0, -1);
            }
            return path;
        }

        function scoreMatch(target, current) {
            if (!target || !current) return -1;
            if (target === current) return target.length + 100;
            if (current.startsWith(target + '/')) return target.length + 50;
            var folder = target.replace(/\/[^/]*$/, '');
            if (folder && folder !== target && current.startsWith(folder + '/')) {
                return folder.length;
            }
            return -1;
        }

        var currentPath = normalizePath(window.location.pathname || '');
        var best = null;
        var bestScore = -1;

        links.forEach(function(a){
            var candidate = a.pathname || a.getAttribute('href') || '';
            var normalized = normalizePath(candidate);
            var score = scoreMatch(normalized, currentPath);
            if (score > bestScore) {
                bestScore = score;
                best = a;
            }
        });

        links.forEach(function(a){ a.classList.remove('active'); });
        if (best) best.classList.add('active');

        links.forEach(function(a){
            a.addEventListener('click', function(){
                links.forEach(function(x){ x.classList.remove('active'); });
                a.classList.add('active');
            });
        });
    } catch (e) {}
}

// Ejecutar al cargar (sirve para sidebars embebidos)
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initSidebar);
} else {
    initSidebar();
}
// Muestra un modal centrado para mensajes de guardado o error
function showSaveModal(message) {
    const hide = element => {
        element.classList.add('is-hidden');
        element.classList.remove('is-visible');
    };
    let bg = document.getElementById('saveModalBg');
    let msg = document.getElementById('saveModalMsg');
    if (!bg || !msg) {
        bg = document.createElement('div');
        bg.id = 'saveModalBg';
        bg.className = 'modal-save-bg is-hidden';
        const modal = document.createElement('div');
        modal.className = 'modal-save';
        const close = document.createElement('span');
        close.id = 'saveModalClose';
        close.className = 'modal-close';
        close.innerHTML = '&times;';
        msg = document.createElement('div');
        msg.id = 'saveModalMsg';
        msg.className = 'modal-message';
        modal.appendChild(close);
        modal.appendChild(msg);
        bg.appendChild(modal);
        document.body.appendChild(bg);
        close.addEventListener('click', () => hide(bg));
        bg.addEventListener('click', e => { if (e.target === bg) hide(bg); });
    }
    msg.textContent = message;
    bg.classList.remove('is-hidden');
    bg.classList.add('is-visible');
    setTimeout(() => hide(bg), 2000);
}

export function configurePortal(options = {}) {
    const requestedPortal = typeof options.portalId === 'string'
        ? options.portalId.trim()
        : '';
    portalConfig.portalId = requestedPortal || 'unknown';
    portalConfig.serviceModuleEnabled = !!options.serviceModuleEnabled;

    if (typeof window !== 'undefined') {
        window.portalId = portalConfig.portalId;
    }

    if (typeof document !== 'undefined' && !portalScopeListenerBound) {
        portalScopeListenerBound = true;
        document.addEventListener('DOMContentLoaded', applyPortalScopeAttributes, { once: true });
    }
    applyPortalScopeAttributes();

    if (shouldHandleServiceModule()) {
        const flags = options.flags || window.permissionFlags || DEFAULT_PERMISSION_FLAGS;
        applyServiceModuleVisibility(flags);
    } else if (typeof document !== 'undefined') {
        document.querySelectorAll('[data-service-clientes]').forEach(link => {
            link.style.display = 'none';
        });
    }
}
