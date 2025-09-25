const test = require('node:test');
const assert = require('node:assert/strict');
const RolesUtils = require('../../public/assets/scripts/roles-utils.js');

const dataset = [
  { nombre: 'Administrador', empresa_id: null, delegated: false },
  { nombre: 'Delegado Calidad', empresa_id: 1, delegated: true },
  { nombre: 'Supervisor', empresa_id: null, delegated: false },
  { nombre: 'Delegado Planta', empresa_id: 2, delegated: true }
];

test('RolesUtils filtra roles por empresa con y sin permisos delegados', () => {
  const empresaUno = RolesUtils.filterRolesByEmpresa(dataset, 1, true).map(r => r.nombre);
  assert.deepEqual(empresaUno, ['Administrador', 'Delegado Calidad', 'Supervisor']);

  const empresaUnoSinDelegados = RolesUtils.filterRolesByEmpresa(dataset, 1, false).map(r => r.nombre);
  assert.deepEqual(empresaUnoSinDelegados, ['Administrador', 'Supervisor']);

  const empresaDos = RolesUtils.filterRolesByEmpresa(dataset, 2, true).map(r => r.nombre);
  assert.deepEqual(empresaDos, ['Administrador', 'Supervisor', 'Delegado Planta']);
});

test('RolesUtils buildRoleOptions conserva etiquetas de delegados', () => {
  const roles = RolesUtils.filterRolesByEmpresa(dataset, 1, true);
  const opciones = RolesUtils.buildRoleOptions(roles);
  const etiquetas = opciones.map(opt => opt.delegated ? `${opt.label} (Delegado)` : opt.label);
  assert.ok(etiquetas.includes('Delegado Calidad (Delegado)'));
  assert.ok(etiquetas.includes('Administrador'));
});
