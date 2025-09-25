(function () {
    'use strict';

    function resolve(path) {
        if (!path) {
            return path;
        }
        if (typeof window.resolveAppUrl === 'function') {
            return window.resolveAppUrl(path);
        }
        var base = (window.BASE_URL || '');
        var normalized = String(path);
        if (normalized.charAt(0) !== '/') {
            normalized = '/' + normalized.replace(/^\/+/, '');
        }
        if (base && base !== '/' && normalized.startsWith('/')) {
            return base.replace(/\/+$/, '') + normalized;
        }
        return normalized;
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
                if (typeof afterRender === 'function') {
                    afterRender(target);
                }
            })
            .catch(function (error) {
                console.error(error);
            });
    }

    function setActiveSection(container) {
        if (!container) {
            return;
        }
        var active = '';
        if (document.body) {
            active = document.body.getAttribute('data-active-section') || '';
        }
        if (!active) {
            return;
        }
        var items = container.querySelectorAll('[data-section]');
        items.forEach(function (item) {
            item.classList.remove('active');
            item.removeAttribute('aria-current');
        });
        var current = container.querySelector('[data-section="' + active + '"]');
        if (current) {
            current.classList.add('active');
            current.setAttribute('aria-current', 'page');
        }
    }

    function enhanceSidebar(container) {
        setActiveSection(container);
        if (typeof window.initializeSidebarControls === 'function') {
            window.initializeSidebarControls();
        }
        if (typeof window.applyApiTokenVisibility === 'function') {
            window.applyApiTokenVisibility();
        }
        if (typeof window.activateApiTokensIfAvailable === 'function') {
            window.activateApiTokensIfAvailable(window.permissionFlags || {});
        }
        if (typeof window.applyServiceModuleVisibility === 'function') {
            window.applyServiceModuleVisibility(window.permissionFlags || {});
        }
        if (typeof window.applyCalidadModuleVisibility === 'function') {
            window.applyCalidadModuleVisibility(window.permissionFlags || {});
        }
    }

    function handleTopbarLoaded() {
        if (typeof window.fetchCurrentUser === 'function') {
            window.fetchCurrentUser().catch(function (error) {
                console.error('No se pudo recuperar el usuario actual', error);
            });
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        loadFragment('sidebar', '/apps/tenant/sidebar.html', enhanceSidebar);
        loadFragment('topbarContainer', '/apps/tenant/topbar.html', handleTopbarLoaded);
    });
})();
