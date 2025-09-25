(function (global) {
  'use strict';

  function normalizeRole(entry) {
    if (!entry || typeof entry === 'number') {
      return null;
    }
    if (typeof entry === 'string') {
      return {
        id: null,
        nombre: entry,
        empresa_id: null,
        delegated: false,
        requires_permission: null,
      };
    }
    const nombre = typeof entry.nombre === 'string' ? entry.nombre : '';
    if (nombre === '') {
      return null;
    }
    const empresa = entry.empresa_id === null || entry.empresa_id === undefined
      ? null
      : Number.parseInt(entry.empresa_id, 10);
    return {
      id: typeof entry.id === 'number' ? entry.id : null,
      nombre,
      empresa_id: Number.isNaN(empresa) ? null : empresa,
      delegated: Boolean(entry.delegated),
      requires_permission: typeof entry.requires_permission === 'string'
        ? entry.requires_permission
        : null,
    };
  }

  function filterRolesByEmpresa(roles, empresaId, canManageDelegated = true) {
    const targetEmpresa = Number.isFinite(empresaId) ? Number(empresaId) : null;
    return (Array.isArray(roles) ? roles : [])
      .map(normalizeRole)
      .filter((role) => role !== null)
      .filter((role) => {
        if (role.empresa_id === null) {
          return true;
        }
        if (targetEmpresa === null) {
          return false;
        }
        return role.empresa_id === targetEmpresa;
      })
      .filter((role) => canManageDelegated || role.delegated === false);
  }

  function buildRoleOptions(roles) {
    const seen = new Set();
    const options = [];
    roles.forEach((role) => {
      if (role && typeof role.nombre === 'string') {
        const key = role.nombre.toLowerCase();
        if (!seen.has(key)) {
          seen.add(key);
          options.push({
            value: role.nombre,
            label: role.nombre,
            delegated: Boolean(role.delegated),
            empresa_id: role.empresa_id,
          });
        }
      }
    });
    options.sort((a, b) => a.label.localeCompare(b.label, 'es'));
    return options;
  }

  const api = {
    normalizeRole,
    filterRolesByEmpresa,
    buildRoleOptions,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }
  if (global && typeof global === 'object') {
    global.RolesUtils = api;
  }
})(typeof window !== 'undefined' ? window : globalThis);

