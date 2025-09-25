<?php

declare(strict_types=1);

/**
 * Crea o actualiza la vista SQL que centraliza la información de planeación.
 *
 * La vista consolida los datos del instrumento, su próximo vencimiento y la
 * última planeación registrada para evitar repetir la lógica de combinaciones
 * en cada endpoint.
 *
 * @return bool Verdadero si la vista existe o se pudo crear correctamente.
 */
function ensurePlaneacionView(\mysqli $conn): bool
{
    static $ensured = false;

    if ($ensured) {
        return true;
    }

    $viewSql = <<<'SQL'
CREATE OR REPLACE VIEW vista_planeacion_instrumentos AS
SELECT
    i.id AS instrumento_id,
    i.empresa_id AS empresa_id,
    ci.nombre AS instrumento,
    i.codigo,
    m.nombre AS marca,
    mo.nombre AS modelo,
    i.serie,
    d.nombre AS departamento,
    i.ubicacion,
    i.proxima_calibracion AS fecha_proxima,
    pr.observaciones AS plan_observaciones,
    ult.fecha_programada,
    ult.responsable_id,
    u.usuario AS responsable,
    CASE WHEN ult.instrumento_id IS NULL THEN 0 ELSE 1 END AS tiene_plan,
    ult.estado_etiqueta AS estado_plan,
    ult.estado_etiqueta AS estado_descriptivo,
    ult.estado_clave,
    DATEDIFF(i.proxima_calibracion, CURDATE()) AS dias_restantes,
    i.estado AS estado_instrumento,
    ult.instrucciones_cliente
FROM instrumentos i
LEFT JOIN catalogo_instrumentos ci ON i.catalogo_id = ci.id
LEFT JOIN marcas m ON i.marca_id = m.id
LEFT JOIN modelos mo ON i.modelo_id = mo.id
LEFT JOIN departamentos d ON i.departamento_id = d.id
LEFT JOIN plan_riesgos pr ON pr.instrumento_id = i.id AND pr.empresa_id = i.empresa_id
LEFT JOIN (
    SELECT
        datos.instrumento_id,
        datos.empresa_id,
        datos.fecha_programada,
        datos.responsable_id,
        datos.estado_clave,
        datos.estado_etiqueta,
        datos.instrucciones_cliente
    FROM (
        SELECT
            sub.instrumento_id,
            sub.empresa_id,
            sub.fecha_programada,
            sub.responsable_id,
            CASE
                WHEN sub.estado_normalizado IS NULL OR sub.estado_normalizado = '' THEN 'programada'
                WHEN sub.estado_normalizado IN ('en curso', 'en_curso', 'encurso', 'en ejecucion', 'enejecucion', 'ejecucion', 'en proceso', 'enproceso', 'proceso', 'curso') THEN 'en_curso'
                WHEN sub.estado_normalizado IN ('completada', 'completado', 'finalizada', 'finalizado', 'terminada', 'terminado', 'cerrada', 'cerrado', 'concluida', 'concluido', 'ejecutada', 'ejecutado') THEN 'completada'
                WHEN sub.estado_normalizado IN ('cancelada', 'cancelado', 'anulada', 'anulado', 'suspendida', 'suspendido', 'detenida', 'detenido') THEN 'cancelada'
                WHEN sub.estado_normalizado IN ('sin fecha', 'sin_fecha', 'sinfecha') THEN 'sin_fecha'
                ELSE 'programada'
            END AS estado_clave,
            CASE
                WHEN sub.estado_normalizado IS NULL OR sub.estado_normalizado = '' THEN 'Programada'
                WHEN sub.estado_normalizado IN ('en curso', 'en_curso', 'encurso', 'en ejecucion', 'enejecucion', 'ejecucion', 'en proceso', 'enproceso', 'proceso', 'curso') THEN 'En curso'
                WHEN sub.estado_normalizado IN ('completada', 'completado', 'finalizada', 'finalizado', 'terminada', 'terminado', 'cerrada', 'cerrado', 'concluida', 'concluido', 'ejecutada', 'ejecutado') THEN 'Completada'
                WHEN sub.estado_normalizado IN ('cancelada', 'cancelado', 'anulada', 'anulado', 'suspendida', 'suspendido', 'detenida', 'detenido') THEN 'Cancelada'
                WHEN sub.estado_normalizado IN ('sin fecha', 'sin_fecha', 'sinfecha') THEN 'Sin fecha'
                ELSE 'Programada'
            END AS estado_etiqueta,
            sub.instrucciones_cliente
        FROM (
            SELECT
                p.instrumento_id,
                p.empresa_id,
                p.fecha_programada,
                COALESCE(oc.tecnico_id, p.responsable_id) AS responsable_id,
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        LOWER(
                                            TRIM(
                                                REPLACE(
                                                    REPLACE(
                                                        REPLACE(COALESCE(oc.estado_ejecucion, p.estado, 'Programada'), CHAR(10), ' '),
                                                        CHAR(13), ' '
                                                    ),
                                                    CHAR(9), ' '
                                                )
                                            )
                                        ),
                                        '-', ' '
                                    ),
                                    'á', 'a'
                                ),
                                'é', 'e'
                            ),
                            'í', 'i'
                        ),
                        'ó', 'o'
                    ),
                    'ú', 'u'
                ) AS estado_normalizado,
                p.instrucciones_cliente,
                ROW_NUMBER() OVER (PARTITION BY p.instrumento_id ORDER BY p.fecha_programada DESC, p.id DESC) AS rn
            FROM planes p
            LEFT JOIN ordenes_calibracion oc ON oc.plan_id = p.id AND oc.empresa_id = p.empresa_id
        ) AS sub
        WHERE sub.rn = 1
    ) AS datos
) AS ult ON ult.instrumento_id = i.id AND ult.empresa_id = i.empresa_id
LEFT JOIN usuarios u ON ult.responsable_id = u.id
SQL;

    if (!$conn->query($viewSql)) {
        error_log('Error al crear la vista vista_planeacion_instrumentos: ' . $conn->error);
        return false;
    }

    $ensured = true;
    return true;
}
