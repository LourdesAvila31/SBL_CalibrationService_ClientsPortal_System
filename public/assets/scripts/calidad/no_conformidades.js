(function (window, document) {
  'use strict';

  var appUrl = window.AppUrl || {};
  var resolveAppUrl = typeof appUrl.resolveAppUrl === 'function'
    ? appUrl.resolveAppUrl
    : function (path) {
        var base = window.BASE_URL || '';
        if (!path) { return base; }
        if (path.startsWith('http')) { return path; }
        if (base && path.startsWith(base)) { return path; }
        if (!path.startsWith('/')) { path = '/' + path; }
        if (base) { return base + path; }
        return path;
      };

  var fetchJson = typeof window.fetchJson === 'function'
    ? window.fetchJson
    : function (resource, options) {
        return window.fetch(resource, options).then(function (response) {
          if (!response.ok) {
            throw new Error('HTTP ' + response.status);
          }
          return response.json();
        });
      };

  var selectors = {
    sidebar: 'sidebar',
    userName: 'userName',
    guard: 'noConformidadesGuard',
    error: 'noConformidadesError',
    tableBody: 'noConformidadesTableBody',
    emptyState: 'noConformidadesEmptyState',
    search: 'noConformidadesSearch',
    estado: 'noConformidadesFilterEstado',
    criticidad: 'noConformidadesFilterCriticidad',
    formPanel: 'noConformidadFormPanel',
    form: 'noConformidadForm',
    formTitle: 'noConformidadFormTitle',
    formId: 'noConformidadId',
    openForm: 'openNoConformidadForm',
    cancelForm: 'cancelNoConformidadForm'
  };

  var endpoints = {
    list: '/backend/calidad/no_conformidades/list.php',
    save: '/backend/calidad/no_conformidades/save.php'
  };

  var state = {
    registros: [],
    canView: false,
    canManage: false,
    filters: {
      search: '',
      estado: 'todas',
      criticidad: 'todas'
    }
  };

  var bootstrapOffcanvas = window.bootstrap ? window.bootstrap.Offcanvas : null;
  var formPanelInstance = null;

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

  function formatDate(value) {
    if (!value) { return ''; }
    var date = new Date(value);
    if (isNaN(date.getTime())) {
      return escapeHtml(value);
    }
    return date.toLocaleDateString('es-MX', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function getRowId(item) {
    if (!item) { return null; }
    return item.no_conformidad_id || item.id || null;
  }

  function getEstadoBadge(estado) {
    var normalized = (estado || '').toString().toLowerCase();
    var badge = 'bg-secondary';
    if (normalized === 'abierta') badge = 'bg-danger';
    else if (normalized === 'en-progreso' || normalized === 'en progreso') badge = 'bg-warning text-dark';
    else if (normalized === 'cerrada') badge = 'bg-success';
    return '<span class="badge ' + badge + '">' + escapeHtml(estado || '—') + '</span>';
  }

  function getCriticidadBadge(criticidad) {
    var normalized = (criticidad || '').toString().toLowerCase();
    var badge = 'bg-secondary';
    if (normalized === 'alta') badge = 'bg-danger';
    else if (normalized === 'media') badge = 'bg-warning text-dark';
    else if (normalized === 'baja') badge = 'bg-info text-dark';
    return '<span class="badge ' + badge + '">' + escapeHtml(criticidad || '—') + '</span>';
  }

  function getElement(id) {
    return document.getElementById(id);
  }

  function showError(message) {
    var el = getElement(selectors.error);
    if (!el) { return; }
    if (message) {
      el.textContent = message;
      el.hidden = false;
    } else {
      el.textContent = '';
      el.hidden = true;
    }
  }

  function showGuard(show) {
    var el = getElement(selectors.guard);
    if (!el) { return; }
    el.hidden = !show;
  }

  function setEmptyState(visible) {
    var el = getElement(selectors.emptyState);
    if (!el) { return; }
    el.hidden = !visible;
  }

  function renderRegistros() {
    var tbody = getElement(selectors.tableBody);
    if (!tbody) { return; }
    if (!state.canView) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }

    var search = state.filters.search;
    var estado = state.filters.estado;
    var criticidad = state.filters.criticidad;

    var filtered = state.registros.filter(function (item) {
      var matchesSearch = true;
      if (search) {
        var haystack = [item.codigo, item.descripcion, item.proceso, item.responsable]
          .map(function (value) { return (value || '').toString().toLowerCase(); })
          .join(' ');
        matchesSearch = haystack.indexOf(search) !== -1;
      }
      var matchesEstado = estado === 'todas';
      if (!matchesEstado) {
        var current = (item.estado || '').toString().toLowerCase();
        matchesEstado = current === estado;
      }
      var matchesCriticidad = criticidad === 'todas';
      if (!matchesCriticidad) {
        var currentCrit = (item.criticidad || '').toString().toLowerCase();
        matchesCriticidad = currentCrit === criticidad;
      }
      return matchesSearch && matchesEstado && matchesCriticidad;
    });

    if (!filtered.length) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }

    tbody.innerHTML = filtered.map(function (item) {
      var id = getRowId(item);
      var acciones = state.canManage
        ? '<button class="btn btn-sm btn-outline-primary" data-action="edit" data-id="' + (id || '') + '"><i class="fa fa-pen"></i> Editar</button>'
        : '<span class="badge bg-secondary">Solo lectura</span>';
      return '<tr data-id="' + (id || '') + '">' +
        '<td>' + escapeHtml(item.codigo || '—') + '</td>' +
        '<td>' + escapeHtml(item.descripcion || '—') + '</td>' +
        '<td>' + escapeHtml(item.proceso || '—') + '</td>' +
        '<td>' + escapeHtml(item.responsable || '—') + '</td>' +
        '<td>' + getEstadoBadge(item.estado) + '</td>' +
        '<td>' + (formatDate(item.fecha_apertura || item.fecha || '') || '—') + '</td>' +
        '<td>' + (formatDate(item.fecha_compromiso || item.fecha_vencimiento || '') || '—') + '</td>' +
        '<td class="text-end">' + acciones + '</td>' +
        '</tr>';
    }).join('');
    setEmptyState(false);
  }

  function resetForm() {
    var form = getElement(selectors.form);
    if (!form) { return; }
    form.reset();
    var idField = getElement(selectors.formId);
    if (idField) { idField.value = ''; }
    var title = getElement(selectors.formTitle);
    if (title) { title.textContent = 'Registrar no conformidad'; }
  }

  function fillForm(item) {
    if (!item) { return; }
    var setValue = function (id, value) {
      var el = getElement(id);
      if (el) { el.value = value != null ? value : ''; }
    };
    var idField = getElement(selectors.formId);
    if (idField) { idField.value = getRowId(item) || ''; }
    setValue('noConformidadCodigo', item.codigo || '');
    setValue('noConformidadDescripcion', item.descripcion || '');
    setValue('noConformidadProceso', item.proceso || '');
    setValue('noConformidadResponsable', item.responsable || '');
    setValue('noConformidadCriticidad', (item.criticidad || '').toString().toLowerCase());
    setValue('noConformidadEstado', (item.estado || '').toString().toLowerCase());
    setValue('noConformidadApertura', item.fecha_apertura || item.fecha || '');
    setValue('noConformidadVencimiento', item.fecha_compromiso || item.fecha_vencimiento || '');
    setValue('noConformidadAcciones', item.acciones || item.plan_accion || '');
    var title = getElement(selectors.formTitle);
    if (title) { title.textContent = 'Actualizar no conformidad'; }
  }

  function ensureOffcanvas() {
    var panel = getElement(selectors.formPanel);
    if (!panel) { return null; }
    if (!formPanelInstance && bootstrapOffcanvas) {
      formPanelInstance = new bootstrapOffcanvas(panel);
    }
    return formPanelInstance;
  }

  function openForm(item) {
    if (!state.canManage) {
      showGuard(true);
      return;
    }
    resetForm();
    if (item) {
      fillForm(item);
    }
    var instance = ensureOffcanvas();
    if (instance && typeof instance.show === 'function') {
      instance.show();
    }
  }

  function closeForm() {
    if (formPanelInstance && typeof formPanelInstance.hide === 'function') {
      formPanelInstance.hide();
    }
  }

  function handleRowAction(event) {
    var target = event.target.closest('[data-action]');
    if (!target) { return; }
    if (target.getAttribute('data-action') !== 'edit') { return; }
    var id = target.getAttribute('data-id');
    if (!id) { return; }
    var item = state.registros.find(function (row) { return String(getRowId(row)) === String(id); });
    if (!item) { return; }
    openForm(item);
  }

  function serializeForm(form) {
    var formData = new FormData(form);
    var payload = {};
    formData.forEach(function (value, key) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) {
        if (!Array.isArray(payload[key])) {
          payload[key] = [payload[key]];
        }
        payload[key].push(value);
      } else {
        payload[key] = value;
      }
    });
    var idValue = payload.no_conformidad_id || payload.noConformidadId || payload.id;
    if (idValue !== undefined && idValue !== null && String(idValue).trim() !== '') {
      var parsedId = parseInt(idValue, 10);
      payload.id = isNaN(parsedId) ? idValue : parsedId;
      delete payload.no_conformidad_id;
      delete payload.noConformidadId;
    }
    return payload;
  }

  function handleFormSubmit(event) {
    event.preventDefault();
    if (!state.canManage) { return; }
    var form = event.target;
    if (!form || !form.checkValidity || !form.checkValidity()) {
      if (form && typeof form.reportValidity === 'function') {
        form.reportValidity();
      }
      return;
    }
    showError('');
    var payload = serializeForm(form);
    window.fetch(resolveAppUrl(endpoints.save), {
      method: 'POST',
      body: JSON.stringify(payload),
      headers: {
        'Content-Type': 'application/json'
      },
      credentials: 'include'
    }).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (data) {
        var isSuccess = response.ok && (!data.status || data.status === 'success');
        if (!isSuccess || (data && data.error)) {
          throw new Error((data && (data.message || data.error)) || 'No se pudo guardar la no conformidad.');
        }
        return data;
      });
    }).then(function () {
      closeForm();
      loadRegistros();
    }).catch(function (error) {
      showError(error && error.message ? error.message : 'No se pudo guardar la no conformidad.');
    });
  }

  function loadRegistros() {
    showError('');
    fetchJson(resolveAppUrl(endpoints.list), { credentials: 'include' })
      .then(function (data) {
        var registros = [];
        if (Array.isArray(data)) {
          registros = data;
        } else if (data && Array.isArray(data.no_conformidades)) {
          registros = data.no_conformidades;
        } else if (data && Array.isArray(data.data)) {
          registros = data.data;
        }
        state.registros = registros;
        renderRegistros();
      })
      .catch(function (error) {
        state.registros = [];
        renderRegistros();
        showError(error && error.message ? error.message : 'No se pudieron cargar las no conformidades.');
      });
  }

  function attachFilterEvents() {
    var searchEl = getElement(selectors.search);
    if (searchEl) {
      searchEl.addEventListener('input', function () {
        state.filters.search = searchEl.value.trim().toLowerCase();
        renderRegistros();
      });
    }
    var estadoEl = getElement(selectors.estado);
    if (estadoEl) {
      estadoEl.addEventListener('change', function () {
        state.filters.estado = estadoEl.value;
        renderRegistros();
      });
    }
    var criticidadEl = getElement(selectors.criticidad);
    if (criticidadEl) {
      criticidadEl.addEventListener('change', function () {
        state.filters.criticidad = criticidadEl.value;
        renderRegistros();
      });
    }
  }

  function enforcePermissions() {
    var searchEl = getElement(selectors.search);
    var estadoEl = getElement(selectors.estado);
    var criticidadEl = getElement(selectors.criticidad);
    var openBtn = getElement(selectors.openForm);
    var form = getElement(selectors.form);

    if (!state.canView) {
      showGuard(true);
      if (searchEl) searchEl.disabled = true;
      if (estadoEl) estadoEl.disabled = true;
      if (criticidadEl) criticidadEl.disabled = true;
      if (openBtn) openBtn.disabled = true;
      setEmptyState(true);
      return;
    }

    showGuard(false);
    if (searchEl) searchEl.disabled = false;
    if (estadoEl) estadoEl.disabled = false;
    if (criticidadEl) criticidadEl.disabled = false;

    if (openBtn) {
      openBtn.disabled = !state.canManage;
      if (state.canManage) {
        openBtn.classList.remove('locked');
      } else {
        openBtn.classList.add('locked');
      }
    }

    if (form) {
      Array.from(form.elements).forEach(function (el) {
        if (el && el.name) {
          el.disabled = !state.canManage && el.type !== 'hidden';
        }
      });
    }
  }

  function bindEvents() {
    var openBtn = getElement(selectors.openForm);
    if (openBtn) {
      openBtn.addEventListener('click', function () {
        openForm();
      });
    }
    var cancelBtn = getElement(selectors.cancelForm);
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function () {
        closeForm();
      });
    }
    var form = getElement(selectors.form);
    if (form) {
      form.addEventListener('submit', handleFormSubmit);
    }
    var table = getElement(selectors.tableBody);
    if (table) {
      table.addEventListener('click', handleRowAction);
    }
  }

  function loadSidebar() {
    var sidebarEl = getElement(selectors.sidebar);
    if (!sidebarEl) { return; }
    fetch(resolveAppUrl('/apps/internal/sidebar.html'))
      .then(function (response) { return response.text(); })
      .then(function (html) {
        sidebarEl.innerHTML = html;
        if (typeof window.initSidebar === 'function') {
          window.initSidebar();
        }
        if (typeof window.initializeSidebarControls === 'function') {
          window.initializeSidebarControls();
        }
      })
      .catch(function () { /* noop */ });
  }

  function loadUser() {
    var userEl = getElement(selectors.userName);
    if (!userEl) { return; }
    fetch(resolveAppUrl('/backend/usuarios/get_actual_user.php'), { credentials: 'include' })
      .then(function (response) { return response.json(); })
      .then(function (user) {
        if (!user) { return; }
        var nombre = user.nombre || user.nombre_completo || user.username;
        var rol = user.role_id || user.role_name || '';
        userEl.innerHTML = nombre ? nombre + (rol ? ' <span class="text-accent">(' + rol + ')</span>' : '') : '';
      })
      .catch(function () { /* noop */ });
  }

  document.addEventListener('permissionsready', function (event) {
    var flags = event && event.detail ? event.detail : window.permissionFlags || {};
    state.canManage = !!flags.canManageCalidadNoConformidades;
    state.canView = !!(flags.canManageCalidadNoConformidades || flags.canViewCalidad);
    enforcePermissions();
    renderRegistros();
  });

  document.addEventListener('DOMContentLoaded', function () {
    loadSidebar();
    loadUser();
    attachFilterEvents();
    bindEvents();
    enforcePermissions();
    renderRegistros();
    loadRegistros();
  });
})(typeof window !== 'undefined' ? window : this, typeof document !== 'undefined' ? document : undefined);
