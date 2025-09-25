(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.CalidadWorkflows = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const DOCUMENT_STATES = {
    borrador: { label: 'Borrador', badge: 'secondary', order: 1 },
    en_revision: { label: 'En revisión', badge: 'warning', order: 2 },
    publicado: { label: 'Publicado', badge: 'success', order: 3 },
    obsoleto: { label: 'Obsoleto', badge: 'dark', order: 4 }
  };

  function documentStatusMetadata(status) {
    const normalized = String(status || '').toLowerCase();
    return DOCUMENT_STATES[normalized] || { label: 'Desconocido', badge: 'secondary', order: 99 };
  }

  function sortTimeline(history) {
    if (!Array.isArray(history)) {
      return [];
    }

    return history
      .map((entry) => {
        const timestamp = entry.timestamp || entry.fecha || entry.date || null;
        if (!timestamp) {
          return null;
        }
        const metadata = documentStatusMetadata(entry.estado || entry.status);
        return {
          ...entry,
          estado: metadata.label,
          badge: metadata.badge,
          timestamp
        };
      })
      .filter(Boolean)
      .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
  }

  function summarizeAttendance(attendees) {
    if (!Array.isArray(attendees) || attendees.length === 0) {
      return { total: 0, attended: 0, justified: 0, absences: 0, completion: 0 };
    }

    let attended = 0;
    let justified = 0;
    let absences = 0;

    attendees.forEach((attendee) => {
      const status = String(attendee.status || attendee.estatus || '').toLowerCase();
      if (status === 'asistio' || status === 'attended') {
        attended += 1;
      } else if (status === 'justificado' || status === 'excused') {
        justified += 1;
      } else if (status === 'falta' || status === 'absent') {
        absences += 1;
      }
    });

    const total = attendees.length;
    const completion = total === 0 ? 0 : Math.round((attended / total) * 100);

    return { total, attended, justified, absences, completion };
  }

  function nonConformityProgress(record) {
    const actions = Array.isArray(record && record.acciones)
      ? record.acciones
      : Array.isArray(record && record.actions)
        ? record.actions
        : [];

    const total = actions.length;
    let closed = 0;
    const pending = [];

    actions.forEach((action, index) => {
      const status = String(action.estado || action.status || '').toLowerCase();
      if (status === 'completada' || status === 'closed') {
        closed += 1;
      } else {
        pending.push({
          index,
          descripcion: action.descripcion || action.description || '',
          responsable: action.responsable || action.owner || null
        });
      }
    });

    const completion = total === 0 ? 100 : Math.round((closed / total) * 100);

    return {
      totalActions: total,
      closedActions: closed,
      completion,
      pending
    };
  }

  function canCloseNonConformity(record) {
    const status = String(record && (record.estado || record.status) || '').toLowerCase();
    if (status === 'cerrada' || status === 'closed') {
      return true;
    }

    const progress = nonConformityProgress(record);
    return progress.totalActions > 0 && progress.totalActions === progress.closedActions;
  }

  return {
    DOCUMENT_STATES,
    documentStatusMetadata,
    sortTimeline,
    summarizeAttendance,
    nonConformityProgress,
    canCloseNonConformity
  };
});
