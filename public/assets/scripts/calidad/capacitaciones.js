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
    guard: 'capacitacionesGuard',
    error: 'capacitacionesError',
    tableBody: 'capacitacionesTableBody',
    emptyState: 'capacitacionesEmptyState',
    search: 'capacitacionesSearch',
    estado: 'capacitacionesFilterEstado',
    periodo: 'capacitacionesFilterPeriodo',
    formPanel: 'capacitacionFormPanel',
    form: 'capacitacionForm',
    formTitle: 'capacitacionFormTitle',
    formId: 'capacitacionId',
    openForm: 'openCapacitacionForm',
    cancelForm: 'cancelCapacitacionForm'
  };

  var endpoints = {
    list: '/backend/calidad/capacitaciones/list.php',
    save: '/backend/calidad/capacitaciones/save.php'
  };

  var state = {
    capacitaciones: [],
    canView: false,
    canManage: false,
    filters: {
      search: '',
      estado: 'todas',
      periodo: '12'
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
    return item.capacitacion_id || item.id || null;
  }

  function getEstadoBadge(estado) {
    var normalized = (estado || '').toString().toLowerCase();
    var badgeClass = 'bg-secondary';
    if (normalized === 'programada') badgeClass = 'bg-info text-dark';
    else if (normalized === 'en-curso' || normalized === 'en curso') badgeClass = 'bg-warning text-dark';
    else if (normalized === 'completada') badgeClass = 'bg-success';
    return '<span class="badge ' + badgeClass + '">' + escapeHtml(estado || '—') + '</span>';
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

  function renderCapacitaciones() {
    var tbody = getElement(selectors.tableBody);
    if (!tbody) { return; }
    if (!state.canView) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }

    var search = state.filters.search;
    var estado = state.filters.estado;
    var periodo = parseInt(state.filters.periodo, 10) || 12;
    var now = new Date();
    var cutoff = new Date(now);
    cutoff.setMonth(cutoff.getMonth() - periodo);

    var filtered = state.capacitaciones.filter(function (item) {
      var matchesSearch = true;
      if (search) {
        var haystack = [item.tema, item.responsable, item.participantes]
          .map(function (value) { return (value || '').toString().toLowerCase(); })
          .join(' ');
        matchesSearch = haystack.indexOf(search) !== -1;
      }
      var matchesEstado = estado === 'todas';
      if (!matchesEstado) {
        var current = (item.estado || '').toString().toLowerCase();
        matchesEstado = current === estado;
      }
      var matchesPeriodo = true;
      if (item.fecha_programada) {
        var fecha = new Date(item.fecha_programada);
        if (!isNaN(fecha.getTime())) {
          matchesPeriodo = fecha >= cutoff;
        }
      }
      return matchesSearch && matchesEstado && matchesPeriodo;
    });

    if (!filtered.length) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }

    tbody.innerHTML = filtered.map(function (item) {
      var id = getRowId(item);
      var participantes = Array.isArray(item.participantes)
        ? item.participantes.join(', ')
        : (item.participantes || '—');
      var acciones = state.canManage
        ? '<button class="btn btn-sm btn-outline-primary" data-action="edit" data-id="' + (id || '') + '"><i class="fa fa-pen"></i> Editar</button>'
        : '<span class="badge bg-secondary">Solo lectura</span>';
      return '<tr data-id="' + (id || '') + '">' +
        '<td>' + escapeHtml(item.tema || '—') + '</td>' +
        '<td>' + (formatDate(item.fecha_programada) || '—') + '</td>' +
        '<td>' + escapeHtml(item.responsable || '—') + '</td>' +
        '<td>' + escapeHtml(participantes) + '</td>' +
        '<td>' + getEstadoBadge(item.estado) + '</td>' +
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
    if (title) { title.textContent = 'Nueva capacitación'; }
  }

  function fillForm(item) {
    if (!item) { return; }
    var setValue = function (id, value) {
      var el = getElement(id);
      if (el) { el.value = value != null ? value : ''; }
    };
    var idField = getElement(selectors.formId);
    if (idField) { idField.value = getRowId(item) || ''; }
    setValue('capacitacionTema', item.tema || '');
    setValue('capacitacionFecha', item.fecha_programada || item.fecha || '');
    setValue('capacitacionResponsable', item.responsable || '');
    if (Array.isArray(item.participantes)) {
      setValue('capacitacionParticipantes', item.participantes.join(', '));
    } else {
      setValue('capacitacionParticipantes', item.participantes || '');
    }
    setValue('capacitacionEstado', (item.estado || '').toString().toLowerCase());
    setValue('capacitacionEvaluacion', item.evaluacion || item.resultado || '');
    var title = getElement(selectors.formTitle);
    if (title) { title.textContent = 'Editar capacitación'; }
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
    var item = state.capacitaciones.find(function (row) { return String(getRowId(row)) === String(id); });
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
    if (typeof payload.participantes === 'string') {
      payload.participantes = payload.participantes
        .split(',')
        .map(function (p) { return p.trim(); })
        .filter(Boolean)
        .join(',');
    }
    var idValue = payload.capacitacion_id || payload.capacitacionId || payload.id;
    if (idValue !== undefined && idValue !== null && String(idValue).trim() !== '') {
      var parsedId = parseInt(idValue, 10);
      payload.id = isNaN(parsedId) ? idValue : parsedId;
      delete payload.capacitacion_id;
      delete payload.capacitacionId;
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
          throw new Error((data && (data.message || data.error)) || 'No se pudo guardar la capacitación.');
        }
        return data;
      });
    }).then(function () {
      closeForm();
      loadCapacitaciones();
    }).catch(function (error) {
      showError(error && error.message ? error.message : 'No se pudo guardar la capacitación.');
    });
  }

  function loadCapacitaciones() {
    showError('');
    fetchJson(resolveAppUrl(endpoints.list), { credentials: 'include' })
      .then(function (data) {
        var registros = [];
        if (Array.isArray(data)) {
          registros = data;
        } else if (data && Array.isArray(data.capacitaciones)) {
          registros = data.capacitaciones;
        } else if (data && Array.isArray(data.data)) {
          registros = data.data;
        }
        state.capacitaciones = registros;
        renderCapacitaciones();
      })
      .catch(function (error) {
        state.capacitaciones = [];
        renderCapacitaciones();
        showError(error && error.message ? error.message : 'No se pudieron cargar las capacitaciones.');
      });
  }

  function attachFilterEvents() {
    var searchEl = getElement(selectors.search);
    if (searchEl) {
      searchEl.addEventListener('input', function () {
        state.filters.search = searchEl.value.trim().toLowerCase();
        renderCapacitaciones();
      });
    }
    var estadoEl = getElement(selectors.estado);
    if (estadoEl) {
      estadoEl.addEventListener('change', function () {
        state.filters.estado = estadoEl.value;
        renderCapacitaciones();
      });
    }
    var periodoEl = getElement(selectors.periodo);
    if (periodoEl) {
      periodoEl.addEventListener('change', function () {
        state.filters.periodo = periodoEl.value;
        renderCapacitaciones();
      });
    }
  }

  function enforcePermissions() {
    var searchEl = getElement(selectors.search);
    var estadoEl = getElement(selectors.estado);
    var periodoEl = getElement(selectors.periodo);
    var openBtn = getElement(selectors.openForm);
    var form = getElement(selectors.form);

    if (!state.canView) {
      showGuard(true);
      if (searchEl) searchEl.disabled = true;
      if (estadoEl) estadoEl.disabled = true;
      if (periodoEl) periodoEl.disabled = true;
      if (openBtn) openBtn.disabled = true;
      setEmptyState(true);
      return;
    }

    showGuard(false);
    if (searchEl) searchEl.disabled = false;
    if (estadoEl) estadoEl.disabled = false;
    if (periodoEl) periodoEl.disabled = false;

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
    state.canManage = !!flags.canManageCalidadCapacitaciones;
    state.canView = !!(flags.canManageCalidadCapacitaciones || flags.canViewCalidad);
    enforcePermissions();
    renderCapacitaciones();
  });

  document.addEventListener('DOMContentLoaded', function () {
    loadSidebar();
    loadUser();
    attachFilterEvents();
    bindEvents();
    enforcePermissions();
    renderCapacitaciones();
    loadCapacitaciones();
  });
})(typeof window !== 'undefined' ? window : this, typeof document !== 'undefined' ? document : undefined);
