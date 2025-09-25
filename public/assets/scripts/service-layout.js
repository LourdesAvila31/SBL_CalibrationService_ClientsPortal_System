(function () {
    'use strict';

    var ABSOLUTE_URL_PATTERN = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i;
    var SERVICE_ROOT_PATH = '/apps/service/';

    function resolve(path) {
        if (!path) {
            return path;
        }
        if (typeof window.resolveAppUrl === 'function') {
            return window.resolveAppUrl(path);
        }
        var base = (window.BASE_URL || '');
        if (path.charAt(0) !== '/') {
            path = '/' + path.replace(/^\/+/, '');
        }
        if (base && base !== '/' && path.startsWith('/')) {
            return base.replace(/\/$/, '') + path;
        }
        return path;
    }

    function normalizeServiceHref(value) {
        if (!value || ABSOLUTE_URL_PATTERN.test(value)) {
            return value;
        }

        var basePrefix = '';
        if (typeof window.resolveAppUrl === 'function') {
            try {
                basePrefix = window.resolveAppUrl('/') || '';
            } catch (error) {
                console.warn('No se pudo leer la ruta base declarada', error);
            }
        }
        if (!basePrefix && typeof window.BASE_URL === 'string') {
            basePrefix = window.BASE_URL || '';
        }

        if (basePrefix) {
            basePrefix = basePrefix.replace(/\/$/, '');
            var normalizedBase = basePrefix + SERVICE_ROOT_PATH.replace(/\/$/, '') + '/';
            if (value.indexOf(normalizedBase) === 0) {
                return value;
            }
        } else if (value.indexOf(SERVICE_ROOT_PATH) === 0) {
            return resolve(value);
        }

        try {
            var base = (window.location && window.location.origin ? window.location.origin : '') + SERVICE_ROOT_PATH;
            var normalizedUrl = new URL(value, base);
            var normalizedPath = normalizedUrl.pathname + (normalizedUrl.search || '') + (normalizedUrl.hash || '');
            return resolve(normalizedPath);
        } catch (error) {
            console.warn('No se pudo normalizar la ruta', value, error);
            return value;
        }
    }

    function normalizeLinks(scope) {
        if (!scope || typeof scope.querySelectorAll !== 'function') {
            return;
        }

        var links = scope.querySelectorAll('a[href]');
        Array.prototype.forEach.call(links, function (link) {
            var href = link.getAttribute('href');
            var normalized = normalizeServiceHref(href);
            if (normalized && normalized !== href) {
                link.setAttribute('href', normalized);
            }
        });
    }

    function loadFragment(targetId, fragmentPath, afterRender) {
        var target = document.getElementById(targetId);
        if (!target) {
            return;
        }
        fetch(resolve(fragmentPath))
            .then(function (response) {
                if (!response.ok) {
                    throw new Error('No se pudo cargar ' + fragmentPath);
                }
                return response.text();
            })
            .then(function (html) {
                target.innerHTML = html;
                normalizeLinks(target);
                if (typeof afterRender === 'function') {
                    afterRender(target);
                }
            })
            .catch(function (error) {
                console.error(error);
            });
    }

    function setActiveSection(activeSection, container) {
        if (!activeSection) {
            return;
        }
        var scope = container || document;
        var selector = '[data-section="' + activeSection + '"]';
        var activeLink = scope.querySelector(selector);
        if (activeLink) {
            activeLink.classList.add('active');
        }
    }

    function ensureBackdrop() {
        var backdrop = document.getElementById('sidebarBackdrop');
        if (!backdrop) {
            backdrop = document.createElement('div');
            backdrop.id = 'sidebarBackdrop';
            backdrop.className = 'sidebar-backdrop';
            document.body.appendChild(backdrop);
        }
        return backdrop;
    }

    document.addEventListener('DOMContentLoaded', function () {
        var activeSection = document.body ? (document.body.getAttribute('data-active-section') || '') : '';

        ensureBackdrop();
        normalizeLinks(document);

        loadFragment('sidebar', '/apps/service/sidebar.html', function (sidebarContainer) {
            setActiveSection(activeSection, sidebarContainer);
        });

        loadFragment('topbarContainer', '/apps/service/topbar.html', function () {
            if (typeof window.fetchCurrentUser === 'function') {
                window.fetchCurrentUser().catch(function (error) {
                    console.error('No se pudo recuperar el usuario actual', error);
                });
            }
        });
    });
})();
