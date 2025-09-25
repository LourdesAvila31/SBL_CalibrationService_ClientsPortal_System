<?php

declare(strict_types=1);

if (!function_exists('company_provision_roles')) {
    /**
     * Provisiona roles delegados y roles tenant base para una empresa específica.
     */
    function company_provision_roles(mysqli $conn, int $empresaId): void
    {
        if ($empresaId <= 0) {
            return;
        }

        $delegatedRoleTemplates = [
            [
                'nombre' => 'Delegado Calidad',
                'delegated' => 1,
                'permissions' => [
                    'instrumentos_leer',
                    'planeacion_leer',
                    'auditoria_leer',
                ],
            ],
            [
                'nombre' => 'Delegado Planta',
                'delegated' => 1,
                'permissions' => [
                    'instrumentos_leer',
                    'calibraciones_leer',
                    'mensajeria_leer',
                ],
            ],
        ];

        $internalRoleTemplates = [
            [
                'nombre' => 'Developer',
                'delegated' => 0,
                'permissions' => [
                    'auditoria_leer',
                    'auditoria_crear',
                ],
            ],
        ];

        $tenantRoleTemplates = [
            [
                'nombre' => 'responsable',
                'nombre_visible' => 'Responsable',
                'permissions' => [
                    'instrumentos_crear',
                    'instrumentos_leer',
                    'instrumentos_actualizar',
                    'instrumentos_eliminar',
                    'calibraciones_crear',
                    'calibraciones_leer',
                    'calibraciones_actualizar',
                    'calibraciones_eliminar',
                    'reportes_crear',
                    'reportes_leer',
                    'reportes_actualizar',
                    'reportes_eliminar',
                    'planeacion_crear',
                    'planeacion_leer',
                    'planeacion_actualizar',
                    'planeacion_eliminar',
                    'adjuntos_crear',
                    'adjuntos_leer',
                    'adjuntos_actualizar',
                    'adjuntos_eliminar',
                    'proveedores_crear',
                    'proveedores_leer',
                    'proveedores_actualizar',
                    'proveedores_eliminar',
                    'clientes_crear',
                    'clientes_leer',
                    'clientes_actualizar',
                    'clientes_eliminar',
                    'auditoria_crear',
                    'auditoria_leer',
                    'auditoria_actualizar',
                    'auditoria_eliminar',
                    'configuracion_leer',
                    'configuracion_actualizar',
                    'mensajeria_leer',
                    'mensajeria_responder',
                    'usuarios_view',
                    'usuarios_add',
                    'usuarios_edit',
                    'usuarios_delete',
                ],
            ],
            [
                'nombre' => 'consulta',
                'nombre_visible' => 'Consulta',
                'permissions' => [
                    'instrumentos_leer',
                    'calibraciones_leer',
                    'reportes_leer',
                    'planeacion_leer',
                    'adjuntos_leer',
                    'proveedores_leer',
                    'clientes_leer',
                    'auditoria_leer',
                    'mensajeria_leer',
                    'usuarios_view',
                ],
            ],
        ];

        $permissionMap = [];
        $permissionsResult = $conn->query('SELECT id, nombre FROM permissions');
        if ($permissionsResult instanceof mysqli_result) {
            while ($permissionRow = $permissionsResult->fetch_assoc()) {
                if (!isset($permissionRow['id'], $permissionRow['nombre'])) {
                    continue;
                }
                $permissionMap[$permissionRow['nombre']] = (int) $permissionRow['id'];
            }
            $permissionsResult->close();
        }

        $portalMap = [];
        $portalsResult = $conn->query('SELECT id, slug FROM portals');
        if ($portalsResult instanceof mysqli_result) {
            while ($portalRow = $portalsResult->fetch_assoc()) {
                if (!isset($portalRow['id'], $portalRow['slug'])) {
                    continue;
                }
                $portalMap[(string) $portalRow['slug']] = (int) $portalRow['id'];
            }
            $portalsResult->close();
        }

        $tenantPortalId = $portalMap['tenant'] ?? null;
        $internalPortalId = $portalMap['internal'] ?? null;

        foreach ($internalRoleTemplates as $roleTemplate) {
            $portalId = $internalPortalId;
            if ($portalId === null) {
                continue;
            }

            $nombre = $roleTemplate['nombre'];
            $delegatedFlag = (int) ($roleTemplate['delegated'] ?? 0);

            $stmtRole = $conn->prepare(
                'INSERT INTO roles (nombre, empresa_id, delegated, portal_id) VALUES (?, ?, ?, ?) '
                . 'ON DUPLICATE KEY UPDATE delegated = VALUES(delegated), portal_id = VALUES(portal_id)'
            );

            if ($stmtRole) {
                $stmtRole->bind_param('siii', $nombre, $empresaId, $delegatedFlag, $portalId);
                $stmtRole->execute();
                $stmtRole->close();
            }

            $roleId = null;
            $stmtLookup = $conn->prepare('SELECT id FROM roles WHERE empresa_id = ? AND nombre = ? LIMIT 1');
            if ($stmtLookup) {
                $stmtLookup->bind_param('is', $empresaId, $nombre);
                if ($stmtLookup->execute()) {
                    $lookupResult = $stmtLookup->get_result();
                    if ($lookupResult instanceof mysqli_result) {
                        $roleRow = $lookupResult->fetch_assoc();
                        if ($roleRow && isset($roleRow['id'])) {
                            $roleValue = (int) $roleRow['id'];
                            $roleId = $roleValue > 0 ? $roleValue : null;
                        }
                        $lookupResult->close();
                    }
                }
                $stmtLookup->close();
            }

            if ($roleId === null) {
                continue;
            }

            foreach ($roleTemplate['permissions'] as $permissionName) {
                $permissionId = $permissionMap[$permissionName] ?? null;
                if ($permissionId === null) {
                    continue;
                }

                $conn->query(
                    "INSERT INTO role_permissions (role_id, permission_id) "
                    . "SELECT {$roleId}, {$permissionId} "
                    . "WHERE NOT EXISTS (SELECT 1 FROM role_permissions WHERE role_id = {$roleId} AND permission_id = {$permissionId})"
                );
            }

            $developerName = 'Developer';
            if (strcasecmp($nombre, $developerName) === 0) {
                $stmtUpdateDeveloper = $conn->prepare(
                    'UPDATE usuarios u '
                    . 'JOIN roles rg ON rg.id = u.role_id '
                    . 'SET u.role_id = ? '
                    . 'WHERE u.empresa_id = ? '
                    . 'AND rg.nombre = ? '
                    . 'AND (rg.empresa_id IS NULL OR rg.empresa_id = 0)'
                );

                if ($stmtUpdateDeveloper) {
                    $stmtUpdateDeveloper->bind_param('iis', $roleId, $empresaId, $developerName);
                    $stmtUpdateDeveloper->execute();
                    $stmtUpdateDeveloper->close();
                }
            }
        }

        foreach ($delegatedRoleTemplates as $roleTemplate) {
            $nombre = $roleTemplate['nombre'];
            $delegatedFlag = (int) ($roleTemplate['delegated'] ?? 0);
            $escapedNombre = $conn->real_escape_string($nombre);
            $portalValue = $tenantPortalId !== null ? (string) ((int) $tenantPortalId) : 'NULL';

            $conn->query(
                "INSERT INTO roles (nombre, empresa_id, delegated, portal_id) VALUES " .
                "('{$escapedNombre}', {$empresaId}, {$delegatedFlag}, {$portalValue}) " .
                "ON DUPLICATE KEY UPDATE delegated = VALUES(delegated), portal_id = VALUES(portal_id)"
            );

            $roleId = null;
            $roleLookup = $conn->query(
                "SELECT id FROM roles WHERE empresa_id = {$empresaId} AND nombre = '{$escapedNombre}' LIMIT 1"
            );
            if ($roleLookup instanceof mysqli_result) {
                $roleRow = $roleLookup->fetch_assoc();
                if ($roleRow) {
                    $roleId = (int) ($roleRow['id'] ?? 0);
                    $roleId = $roleId > 0 ? $roleId : null;
                }
                $roleLookup->close();
            }

            if ($roleId === null) {
                continue;
            }

            foreach ($roleTemplate['permissions'] as $permissionName) {
                $permissionId = $permissionMap[$permissionName] ?? null;
                if ($permissionId === null) {
                    continue;
                }

                $conn->query(
                    "INSERT INTO role_permissions (role_id, permission_id) " .
                    "SELECT {$roleId}, {$permissionId} " .
                    "WHERE NOT EXISTS (SELECT 1 FROM role_permissions WHERE role_id = {$roleId} AND permission_id = {$permissionId})"
                );
            }
        }

        foreach ($tenantRoleTemplates as $tenantRole) {
            $nombre = $tenantRole['nombre'];
            $nombreVisible = $tenantRole['nombre_visible'];
            $escapedNombre = $conn->real_escape_string($nombre);
            $escapedVisible = $conn->real_escape_string($nombreVisible);

            $conn->query(
                "INSERT INTO tenant_roles (empresa_id, nombre, nombre_visible) VALUES " .
                "({$empresaId}, '{$escapedNombre}', '{$escapedVisible}') " .
                "ON DUPLICATE KEY UPDATE nombre_visible = VALUES(nombre_visible)"
            );

            $tenantRoleId = null;
            $roleLookup = $conn->query(
                "SELECT id FROM tenant_roles WHERE empresa_id = {$empresaId} AND nombre = '{$escapedNombre}' LIMIT 1"
            );
            if ($roleLookup instanceof mysqli_result) {
                $roleRow = $roleLookup->fetch_assoc();
                if ($roleRow) {
                    $tenantRoleValue = (int) ($roleRow['id'] ?? 0);
                    $tenantRoleId = $tenantRoleValue > 0 ? $tenantRoleValue : null;
                }
                $roleLookup->close();
            }

            if ($tenantRoleId === null) {
                continue;
            }

            foreach ($tenantRole['permissions'] as $permissionName) {
                $permissionId = $permissionMap[$permissionName] ?? null;
                if ($permissionId === null) {
                    continue;
                }

                $conn->query(
                    "INSERT INTO tenant_role_permissions (tenant_role_id, permission_id) " .
                    "SELECT {$tenantRoleId}, {$permissionId} " .
                    "WHERE NOT EXISTS (SELECT 1 FROM tenant_role_permissions WHERE tenant_role_id = {$tenantRoleId} AND permission_id = {$permissionId})"
                );
            }
        }
    }
}
