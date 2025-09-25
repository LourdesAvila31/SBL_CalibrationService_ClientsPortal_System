<?php

declare(strict_types=1);

require_once dirname(__DIR__, 4) . '/Core/SessionGuard.php';
require_once dirname(__DIR__, 4) . '/Core/permissions.php';
require_once dirname(__DIR__, 1) . '/helpers.php';

if (!check_permission('calidad_documentos_leer')
    && !check_permission('calidad_capacitaciones_leer')
    && !check_permission('calidad_no_conformidades_leer')
) {
    calidadRespond(403, 'error', 'Acceso denegado');
}

global $conn;

if (!calidadEnsureTables($conn)) {
    calidadRespond(500, 'error', 'No se pudo preparar el esquema de datos de calidad');
}

$empresaId = calidadEmpresaId($conn);

$puedeDocumentos = check_permission('calidad_documentos_leer');
$puedeCapacitaciones = check_permission('calidad_capacitaciones_leer');
$puedeNoConformidades = check_permission('calidad_no_conformidades_leer');

$documentosVigentes = 0;
if ($puedeDocumentos) {
    $stmtDocumentos = $conn->prepare(
        "SELECT COUNT(*) AS total FROM calidad_documentos WHERE empresa_id = ? AND estado = 'publicado'"
    );
    if (!$stmtDocumentos) {
        calidadRespond(500, 'error', 'No se pudo preparar la consulta de documentos');
    }
    $stmtDocumentos->bind_param('i', $empresaId);
    if (!$stmtDocumentos->execute()) {
        $stmtDocumentos->close();
        calidadRespond(500, 'error', 'No se pudo obtener el total de documentos vigentes');
    }
    $stmtDocumentos->bind_result($documentosVigentes);
    $stmtDocumentos->fetch();
    $stmtDocumentos->close();
}

$capacitacionesActivas = 0;
if ($puedeCapacitaciones) {
    $stmtCapacitaciones = $conn->prepare(
        "SELECT COUNT(*) AS total FROM calidad_capacitaciones WHERE empresa_id = ? AND estado = 'publicado'"
    );
    if (!$stmtCapacitaciones) {
        calidadRespond(500, 'error', 'No se pudo preparar la consulta de capacitaciones');
    }
    $stmtCapacitaciones->bind_param('i', $empresaId);
    if (!$stmtCapacitaciones->execute()) {
        $stmtCapacitaciones->close();
        calidadRespond(500, 'error', 'No se pudo obtener el total de capacitaciones activas');
    }
    $stmtCapacitaciones->bind_result($capacitacionesActivas);
    $stmtCapacitaciones->fetch();
    $stmtCapacitaciones->close();
}

$noConformidadesAbiertas = 0;
if ($puedeNoConformidades) {
    $stmtNoConformidades = $conn->prepare(
        "SELECT COUNT(*) AS total FROM calidad_no_conformidades"
        . " WHERE empresa_id = ? AND estado IN ('abierta','en_proceso')"
    );
    if (!$stmtNoConformidades) {
        calidadRespond(500, 'error', 'No se pudo preparar la consulta de no conformidades');
    }
    $stmtNoConformidades->bind_param('i', $empresaId);
    if (!$stmtNoConformidades->execute()) {
        $stmtNoConformidades->close();
        calidadRespond(500, 'error', 'No se pudo obtener el total de no conformidades abiertas');
    }
    $stmtNoConformidades->bind_result($noConformidadesAbiertas);
    $stmtNoConformidades->fetch();
    $stmtNoConformidades->close();
}

$actualizaciones = [];

if ($puedeDocumentos) {
    $stmtUltimosDocumentos = $conn->prepare(
        "SELECT titulo, descripcion, actualizado_en"
        . " FROM calidad_documentos WHERE empresa_id = ?"
        . " ORDER BY actualizado_en DESC, id DESC LIMIT 5"
    );
    if ($stmtUltimosDocumentos) {
        $stmtUltimosDocumentos->bind_param('i', $empresaId);
        if ($stmtUltimosDocumentos->execute()) {
            $resultado = $stmtUltimosDocumentos->get_result();
            while ($fila = $resultado->fetch_assoc()) {
                $actualizaciones[] = [
                    'tipo' => 'documento',
                    'titulo' => $fila['titulo'],
                    'descripcion' => $fila['descripcion'] ?? '',
                    'actualizado_en' => $fila['actualizado_en'],
                ];
            }
        } else {
            $stmtUltimosDocumentos->close();
            calidadRespond(500, 'error', 'No se pudo obtener la actividad reciente de documentos');
        }
        $stmtUltimosDocumentos->close();
    }
}

if ($puedeCapacitaciones) {
    $stmtUltimasCapacitaciones = $conn->prepare(
        "SELECT tema AS titulo, descripcion, actualizado_en"
        . " FROM calidad_capacitaciones WHERE empresa_id = ?"
        . " ORDER BY actualizado_en DESC, id DESC LIMIT 5"
    );
    if ($stmtUltimasCapacitaciones) {
        $stmtUltimasCapacitaciones->bind_param('i', $empresaId);
        if ($stmtUltimasCapacitaciones->execute()) {
            $resultado = $stmtUltimasCapacitaciones->get_result();
            while ($fila = $resultado->fetch_assoc()) {
                $actualizaciones[] = [
                    'tipo' => 'capacitacion',
                    'titulo' => $fila['titulo'],
                    'descripcion' => $fila['descripcion'] ?? '',
                    'actualizado_en' => $fila['actualizado_en'],
                ];
            }
        } else {
            $stmtUltimasCapacitaciones->close();
            calidadRespond(500, 'error', 'No se pudo obtener la actividad reciente de capacitaciones');
        }
        $stmtUltimasCapacitaciones->close();
    }
}

if ($puedeNoConformidades) {
    $stmtUltimasNoConformidades = $conn->prepare(
        "SELECT titulo, descripcion, actualizado_en"
        . " FROM calidad_no_conformidades WHERE empresa_id = ?"
        . " ORDER BY actualizado_en DESC, id DESC LIMIT 5"
    );
    if ($stmtUltimasNoConformidades) {
        $stmtUltimasNoConformidades->bind_param('i', $empresaId);
        if ($stmtUltimasNoConformidades->execute()) {
            $resultado = $stmtUltimasNoConformidades->get_result();
            while ($fila = $resultado->fetch_assoc()) {
                $actualizaciones[] = [
                    'tipo' => 'no_conformidad',
                    'titulo' => $fila['titulo'],
                    'descripcion' => $fila['descripcion'] ?? '',
                    'actualizado_en' => $fila['actualizado_en'],
                ];
            }
        } else {
            $stmtUltimasNoConformidades->close();
            calidadRespond(500, 'error', 'No se pudo obtener la actividad reciente de no conformidades');
        }
        $stmtUltimasNoConformidades->close();
    }
}

usort(
    $actualizaciones,
    static fn(array $a, array $b): int => strcmp((string) $b['actualizado_en'], (string) $a['actualizado_en'])
);

$actualizaciones = array_slice($actualizaciones, 0, 10);

$actualizacionesFormateadas = array_map(
    static function (array $item): array {
        $fecha = null;
        if (!empty($item['actualizado_en'])) {
            try {
                $fecha = (new DateTimeImmutable($item['actualizado_en']))->format('d/m/Y H:i');
            } catch (Exception $e) {
                $fecha = $item['actualizado_en'];
            }
        }

        return [
            'tipo' => $item['tipo'],
            'titulo' => $item['titulo'],
            'descripcion' => $item['descripcion'],
            'fecha' => $fecha,
        ];
    },
    $actualizaciones
);

http_response_code(200);
header('Content-Type: application/json');
echo json_encode(
    [
        'documentos_vigentes' => $documentosVigentes,
        'capacitaciones_activas' => $capacitacionesActivas,
        'no_conformidades_abiertas' => $noConformidadesAbiertas,
        'actualizaciones' => $actualizacionesFormateadas,
    ],
    JSON_UNESCAPED_UNICODE
);
exit;
