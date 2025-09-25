(function (global, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    global.LogisticsTimeline = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const STATES = ['Pendiente', 'Enviado', 'En tránsito', 'Recibido'];
  const STATE_ICONS = {
    Pendiente: 'fa-clock',
    Enviado: 'fa-paper-plane',
    'En tránsito': 'fa-truck-fast',
    Recibido: 'fa-box-open'
  };
  const STATE_BADGES = {
    Pendiente: 'bg-secondary',
    Enviado: 'bg-info',
    'En tránsito': 'bg-warning',
    Recibido: 'bg-success'
  };

  function normalizeState(state) {
    if (!state || typeof state !== 'string') {
      return 'Pendiente';
    }
    const clean = state.trim().toLowerCase();
    switch (clean) {
      case 'enviado':
        return 'Enviado';
      case 'en transito':
      case 'en tránsito':
      case 'en-transito':
      case 'en-tránsito':
        return 'En tránsito';
      case 'recibido':
      case 'retornado':
      case 'devuelto':
        return 'Recibido';
      case 'pendiente':
      default:
        return 'Pendiente';
    }
  }

  function parseDate(value) {
    if (!value || typeof value !== 'string') {
      return null;
    }
    const trimmed = value.trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
      return null;
    }
    return trimmed;
  }

  function timelineFromData(logistics) {
    const current = normalizeState(logistics && logistics.estado);
    const index = Math.max(STATES.indexOf(current), 0);
    const map = {
      Pendiente: null,
      Enviado: logistics ? parseDate(logistics.fecha_envio) : null,
      'En tránsito': logistics ? parseDate(logistics.fecha_en_transito) : null,
      Recibido: logistics ? parseDate(logistics.fecha_retorno || logistics.fecha_recibido) : null
    };

    return STATES.map((state, idx) => ({
      estado: state,
      fecha: map[state],
      completado: idx <= index
    }));
  }

  function serializeFromApi(row) {
    if (!row || typeof row !== 'object') {
      return {
        estado: 'Pendiente',
        proveedor: null,
        transportista: null,
        numero_guia: null,
        orden_servicio: null,
        comentarios: null,
        fecha_envio: null,
        fecha_en_transito: null,
        fecha_recibido: null,
        fecha_retorno: null,
        timeline: timelineFromData({ estado: 'Pendiente' })
      };
    }

    const stateSources = [
      row.logistica_estado,
      row.log_estado,
      row.logistica && row.logistica.estado,
      row.estado
    ];
    const estado = normalizeState(stateSources.find(Boolean));
    const fechaEnvio = parseDate(row.logistica_fecha_envio || row.log_fecha_envio || (row.logistica && row.logistica.fecha_envio));
    const fechaTransito = parseDate(row.log_fecha_en_transito || (row.logistica && row.logistica.fecha_en_transito));
    const fechaRecibido = parseDate(row.log_fecha_recibido || (row.logistica && row.logistica.fecha_recibido));
    const fechaRetorno = parseDate(row.logistica_fecha_retorno || row.log_fecha_retorno || (row.logistica && row.logistica.fecha_retorno));

    const logistics = {
      estado,
      proveedor: row.log_proveedor_externo || (row.logistica && row.logistica.proveedor) || null,
      transportista: row.log_transportista || (row.logistica && row.logistica.transportista) || null,
      numero_guia: row.log_numero_guia || (row.logistica && row.logistica.numero_guia) || null,
      orden_servicio: row.log_orden_servicio || (row.logistica && row.logistica.orden_servicio) || null,
      comentarios: row.log_comentarios || (row.logistica && row.logistica.comentarios) || null,
      fecha_envio: fechaEnvio,
      fecha_en_transito: fechaTransito,
      fecha_recibido: fechaRecibido,
      fecha_retorno: fechaRetorno
    };
    logistics.timeline = timelineFromData(logistics);
    return logistics;
  }

  function buildTimelineHtml(timeline) {
    if (!Array.isArray(timeline)) {
      return '';
    }
    return timeline.map(step => {
      const icon = step.completado ? 'fa-check-circle text-success' : 'fa-circle-notch text-muted';
      const dateText = step.fecha || '—';
      return `<div class="logistics-step ${step.completado ? 'completed' : ''}">`
        + `<span class="step-icon"><i class="fa ${icon}"></i></span>`
        + `<div class="step-info"><span class="step-title">${step.estado}</span>`
        + `<span class="step-date">${dateText}</span></div>`
        + `</div>`;
    }).join('');
  }

  function getBadge(state) {
    const normalized = normalizeState(state);
    return {
      state: normalized,
      icon: STATE_ICONS[normalized] || 'fa-circle',
      badgeClass: STATE_BADGES[normalized] || 'bg-secondary'
    };
  }

  function filterByState(records, state) {
    const normalized = normalizeState(state);
    if (!state || !normalized) {
      return records.slice();
    }
    return records.filter(item => {
      if (!item || (item.tipo && item.tipo !== 'Externa')) {
        return false;
      }
      const candidate = item.logistica && item.logistica.estado
        ? item.logistica.estado
        : (item.logistica_estado || item.log_estado || '');
      return normalizeState(candidate) === normalized;
    });
  }

  return {
    STATES,
    normalizeState,
    parseDate,
    timelineFromData,
    serializeFromApi,
    buildTimelineHtml,
    getBadge,
    filterByState
  };
});
