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
    guard: 'documentosGuard',
    error: 'documentosError',
    tableBody: 'documentosTableBody',
    emptyState: 'documentosEmptyState',
    search: 'documentosSearch',
    estado: 'documentosFilterEstado',
    categoria: 'documentosFilterCategoria',
    formPanel: 'documentoFormPanel',
    form: 'documentoForm',
    formTitle: 'documentoFormTitle',
    formId: 'documentoId',
    openForm: 'openDocumentoForm',
    cancelForm: 'cancelDocumentoForm'
  };

  var endpoints = {
    list: '/backend/calidad/documentos/list.php',
    save: '/backend/calidad/documentos/save.php'
  };

  var state = {
    documentos: [],
    canView: false,
    canManage: false,
    filters: {
      search: '',
      estado: 'todos',
      categoria: 'todas'
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

  function getRowId(documento) {
    if (!documento) { return null; }
    return documento.documento_id || documento.id || null;
  }

  function getEstadoBadge(estado) {
    var normalized = (estado || '').toString().toLowerCase();
    var badgeClass = 'bg-secondary';
    if (normalized === 'vigente') badgeClass = 'bg-success';
    else if (normalized === 'revision' || normalized === 'en revisión') badgeClass = 'bg-warning text-dark';
    else if (normalized === 'obsoleto') badgeClass = 'bg-danger';
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

  function populateCategorias() {
    var select = getElement(selectors.categoria);
    if (!select) { return; }
    var categorias = state.documentos
      .map(function (doc) { return doc.categoria || doc.area || ''; })
      .filter(Boolean)
      .map(function (value) { return value.trim(); });
    var unique = Array.from(new Set(categorias)).sort();
    var value = state.filters.categoria;
    select.innerHTML = '<option value="todas">Todas</option>' + unique.map(function (cat) {
      var escaped = escapeHtml(cat);
      var selected = value === cat ? ' selected' : '';
      return '<option value="' + escaped + '"' + selected + '>' + escaped + '</option>';
    }).join('');
  }

  function renderDocumentos() {
    var tbody = getElement(selectors.tableBody);
    if (!tbody) { return; }
    if (!state.canView) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }
    var search = state.filters.search;
    var estado = state.filters.estado;
    var categoria = state.filters.categoria;

    var filtered = state.documentos.filter(function (doc) {
      var matchesSearch = true;
      if (search) {
        var haystack = [doc.codigo, doc.titulo, doc.nombre, doc.categoria, doc.area]
          .map(function (value) { return (value || '').toString().toLowerCase(); })
          .join(' ');
        matchesSearch = haystack.indexOf(search) !== -1;
      }
      var matchesEstado = estado === 'todos';
      if (!matchesEstado) {
        var docEstado = (doc.estado || doc.status || '').toString().toLowerCase();
        matchesEstado = docEstado === estado;
      }
      var matchesCategoria = categoria === 'todas';
      if (!matchesCategoria) {
        var docCategoria = (doc.categoria || doc.area || '').toString().trim();
        matchesCategoria = docCategoria === categoria;
      }
      return matchesSearch && matchesEstado && matchesCategoria;
    });

    if (!filtered.length) {
      tbody.innerHTML = '';
      setEmptyState(true);
      return;
    }

    tbody.innerHTML = filtered.map(function (doc) {
      var id = getRowId(doc);
      var codigo = escapeHtml(doc.codigo || doc.clave || '—');
      var titulo = escapeHtml(doc.titulo || doc.nombre || '—');
      var categoriaTexto = escapeHtml(doc.categoria || doc.area || '—');
      var version = escapeHtml(doc.version || doc.revision || '1');
      var actualizado = formatDate(doc.actualizado || doc.fecha_actualizacion || doc.fecha_vigencia);
      var acciones;
      if (state.canManage) {
        acciones = '<button class="btn btn-sm btn-outline-primary" data-action="edit" data-id="' + (id || '') + '">' +
          '<i class="fa fa-pen"></i> Editar</button>';
      } else {
        acciones = '<span class="badge bg-secondary">Solo lectura</span>';
      }
      return '<tr data-id="' + (id || '') + '">' +
        '<td>' + codigo + '</td>' +
        '<td>' + titulo + '</td>' +
        '<td>' + categoriaTexto + '</td>' +
        '<td>' + version + '</td>' +
        '<td>' + getEstadoBadge(doc.estado || doc.status) + '</td>' +
        '<td>' + (actualizado || '—') + '</td>' +
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
    if (idField) {
      idField.value = '';
    }
    var title = getElement(selectors.formTitle);
    if (title) {
      title.textContent = 'Nuevo documento';
    }
  }

  function fillForm(documento) {
    if (!documento) { return; }
    getElement(selectors.formId).value = getRowId(documento) || '';
    var setValue = function (id, value) {
      var el = getElement(id);
      if (el) { el.value = value != null ? value : ''; }
    };
    setValue('documentoCodigo', documento.codigo || documento.clave || '');
    setValue('documentoTitulo', documento.titulo || documento.nombre || '');
    setValue('documentoCategoria', documento.categoria || documento.area || '');
    setValue('documentoVersion', documento.version || documento.revision || '1');
    setValue('documentoEstado', (documento.estado || '').toString().toLowerCase());
    setValue('documentoVigencia', documento.fecha_vigencia || documento.vigencia || '');
    setValue('documentoResponsable', documento.responsable || documento.dueno || '');
    setValue('documentoNotas', documento.notas || documento.descripcion || '');
    var title = getElement(selectors.formTitle);
    if (title) {
      title.textContent = 'Editar documento';
    }
  }

  function ensureOffcanvas() {
    var panel = getElement(selectors.formPanel);
    if (!panel) { return null; }
    if (!formPanelInstance && bootstrapOffcanvas) {
      formPanelInstance = new bootstrapOffcanvas(panel);
    }
    return formPanelInstance;
  }

  function openForm(documento) {
    if (!state.canManage) {
      showGuard(true);
      return;
    }
    resetForm();
    if (documento) {
      fillForm(documento);
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
    var action = target.getAttribute('data-action');
    if (action !== 'edit') { return; }
    var id = target.getAttribute('data-id');
    if (!id) { return; }
    var documento = state.documentos.find(function (doc) { return String(getRowId(doc)) === String(id); });
    if (!documento) { return; }
    openForm(documento);
  }

  function serializeForm(form) {
    var formData = new FormData(form);
    if (!formData.get('estado')) {
      formData.set('estado', 'vigente');
    }
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
    var idValue = payload.documento_id || payload.documentoId || payload.id;
    if (idValue !== undefined && idValue !== null && String(idValue).trim() !== '') {
      var parsedId = parseInt(idValue, 10);
      payload.id = isNaN(parsedId) ? idValue : parsedId;
      delete payload.documento_id;
      delete payload.documentoId;
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
    var url = resolveAppUrl(endpoints.save);
    window.fetch(url, {
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
          var message = (data && (data.message || data.error)) || 'No se pudo guardar el documento.';
          throw new Error(message);
        }
        return data;
      });
    }).then(function () {
      closeForm();
      loadDocumentos();
    }).catch(function (error) {
      showError(error && error.message ? error.message : 'No se pudo guardar el documento.');
    });
  }

  function loadDocumentos() {
    var url = resolveAppUrl(endpoints.list);
    setEmptyState(false);
    showError('');
    fetchJson(url, { credentials: 'include' })
      .then(function (data) {
        var registros = [];
        if (Array.isArray(data)) {
          registros = data;
        } else if (data && Array.isArray(data.documentos)) {
          registros = data.documentos;
        } else if (data && Array.isArray(data.data)) {
          registros = data.data;
        }
        state.documentos = registros;
        populateCategorias();
        renderDocumentos();
      })
      .catch(function (error) {
        state.documentos = [];
        renderDocumentos();
        showError(error && error.message ? error.message : 'No se pudieron cargar los documentos.');
      });
  }

  function attachFilterEvents() {
    var searchEl = getElement(selectors.search);
    if (searchEl) {
      searchEl.addEventListener('input', function () {
        state.filters.search = searchEl.value.trim().toLowerCase();
        renderDocumentos();
      });
    }
    var estadoEl = getElement(selectors.estado);
    if (estadoEl) {
      estadoEl.addEventListener('change', function () {
        state.filters.estado = estadoEl.value;
        renderDocumentos();
      });
    }
    var categoriaEl = getElement(selectors.categoria);
    if (categoriaEl) {
      categoriaEl.addEventListener('change', function () {
        state.filters.categoria = categoriaEl.value;
        renderDocumentos();
      });
    }
  }

  function enforcePermissions() {
    var searchEl = getElement(selectors.search);
    var estadoEl = getElement(selectors.estado);
    var categoriaEl = getElement(selectors.categoria);
    var openBtn = getElement(selectors.openForm);
    var form = getElement(selectors.form);

    if (!state.canView) {
      showGuard(true);
      if (searchEl) searchEl.disabled = true;
      if (estadoEl) estadoEl.disabled = true;
      if (categoriaEl) categoriaEl.disabled = true;
      if (openBtn) openBtn.disabled = true;
      setEmptyState(true);
      return;
    }

    showGuard(false);
    if (searchEl) searchEl.disabled = false;
    if (estadoEl) estadoEl.disabled = false;
    if (categoriaEl) categoriaEl.disabled = false;

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
    state.canManage = !!flags.canManageCalidadDocumentos;
    state.canView = !!(flags.canManageCalidadDocumentos || flags.canViewCalidad);
    enforcePermissions();
    renderDocumentos();
  });

  document.addEventListener('DOMContentLoaded', function () {
    loadSidebar();
    loadUser();
    attachFilterEvents();
    bindEvents();
    enforcePermissions();
    renderDocumentos();
    loadDocumentos();
  });
})(typeof window !== 'undefined' ? window : this, typeof document !== 'undefined' ? document : undefined);
