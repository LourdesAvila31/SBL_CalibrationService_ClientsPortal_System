<?php

/**
 * Definiciones centralizadas para los reportes personalizados.
 *
 * @return array<string,array<string,mixed>>
 */
function custom_report_definitions(): array
{
    return [
        'calibraciones' => [
            'label' => 'Calibraciones',
            'table' => 'calibraciones c',
            'empresa_column' => 'c.empresa_id',
            'order_by' => 'c.fecha_calibracion DESC',
            'filters' => [
                'fecha_desde' => ['column' => 'c.fecha_calibracion', 'operator' => '>=', 'type' => 's'],
                'fecha_hasta' => ['column' => 'c.fecha_calibracion', 'operator' => '<=', 'type' => 's'],
                'estado' => ['column' => 'c.estado', 'operator' => '=', 'type' => 's'],
            ],
            'default_columns' => ['id', 'instrumento', 'fecha_calibracion', 'resultado', 'estado'],
            'status_suggestions' => [
                'Programada', 'En proceso', 'Completada', 'Reprogramada', 'Atrasada', 'Cancelada',
                'Aprobado', 'Rechazado', 'En revisión',
            ],
            'date_label' => 'Fecha de calibración',
            'columns' => [
                'id' => ['label' => 'ID', 'select' => 'c.id'],
                'instrumento' => ['label' => 'Instrumento', 'select' => "COALESCE(i.codigo, CONCAT('INST-', c.instrumento_id))"],
                'tipo' => ['label' => 'Tipo', 'select' => 'c.tipo'],
                'fecha_calibracion' => ['label' => 'Fecha de calibración', 'select' => 'c.fecha_calibracion'],
                'fecha_proxima' => ['label' => 'Próxima calibración', 'select' => 'c.fecha_proxima'],
                'resultado' => ['label' => 'Resultado', 'select' => 'c.resultado'],
                'estado' => ['label' => 'Estado', 'select' => 'c.estado'],
                'estado_ejecucion' => ['label' => 'Estado de ejecución', 'select' => 'c.estado_ejecucion'],
                'duracion_horas' => ['label' => 'Duración (h)', 'select' => 'c.duracion_horas'],
                'costo_total' => ['label' => 'Costo total', 'select' => 'c.costo_total'],
                'periodo' => ['label' => 'Periodo', 'select' => 'c.periodo'],
            ],
            'joins' => [
                'LEFT JOIN instrumentos i ON i.id = c.instrumento_id',
            ],
        ],
        'planes' => [
            'label' => 'Planes de calibración',
            'table' => 'planes p',
            'empresa_column' => 'p.empresa_id',
            'order_by' => 'p.fecha_programada DESC',
            'filters' => [
                'fecha_desde' => ['column' => 'p.fecha_programada', 'operator' => '>=', 'type' => 's'],
                'fecha_hasta' => ['column' => 'p.fecha_programada', 'operator' => '<=', 'type' => 's'],
                'estado' => ['column' => 'p.estado', 'operator' => '=', 'type' => 's'],
            ],
            'default_columns' => ['id', 'instrumento', 'fecha_programada', 'estado', 'responsable'],
            'status_suggestions' => ['Pendiente', 'Programado', 'En progreso', 'Completado', 'Cancelado'],
            'date_label' => 'Fecha programada',
            'columns' => [
                'id' => ['label' => 'ID', 'select' => 'p.id'],
                'instrumento' => ['label' => 'Instrumento', 'select' => "COALESCE(i.codigo, CONCAT('INST-', p.instrumento_id))"],
                'fecha_programada' => ['label' => 'Fecha programada', 'select' => 'p.fecha_programada'],
                'estado' => ['label' => 'Estado', 'select' => 'p.estado'],
                'responsable' => ['label' => 'Responsable', 'select' => "TRIM(CONCAT(COALESCE(u.nombre,''), ' ', COALESCE(u.apellidos,'')))"],
                'instrucciones_cliente' => ['label' => 'Instrucciones del cliente', 'select' => 'p.instrucciones_cliente'],
            ],
            'joins' => [
                'LEFT JOIN instrumentos i ON i.id = p.instrumento_id',
                'LEFT JOIN usuarios u ON u.id = p.responsable_id',
            ],
        ],
        'instrumentos' => [
            'label' => 'Instrumentos',
            'table' => 'instrumentos i',
            'empresa_column' => 'i.empresa_id',
            'order_by' => 'i.codigo ASC',
            'filters' => [
                'fecha_desde' => ['column' => 'i.fecha_alta', 'operator' => '>=', 'type' => 's'],
                'fecha_hasta' => ['column' => 'i.fecha_alta', 'operator' => '<=', 'type' => 's'],
                'estado' => ['column' => 'i.estado', 'operator' => '=', 'type' => 's'],
            ],
            'default_columns' => ['codigo', 'serie', 'estado', 'ubicacion', 'proxima_calibracion'],
            'status_suggestions' => ['Activo', 'Inactivo', 'Baja', 'Fuera de servicio', 'En calibración'],
            'date_label' => 'Fecha de alta',
            'columns' => [
                'codigo' => ['label' => 'Código', 'select' => 'i.codigo'],
                'serie' => ['label' => 'Serie', 'select' => 'i.serie'],
                'estado' => ['label' => 'Estado', 'select' => 'i.estado'],
                'ubicacion' => ['label' => 'Ubicación', 'select' => 'i.ubicacion'],
                'fecha_alta' => ['label' => 'Fecha de alta', 'select' => 'i.fecha_alta'],
                'proxima_calibracion' => ['label' => 'Próxima calibración', 'select' => 'i.proxima_calibracion'],
                'programado' => ['label' => 'Programado', 'select' => "IF(i.programado = 1, 'Sí', 'No')"],
            ],
            'joins' => [],
        ],
    ];
}

/**
 * Lista de formatos soportados por los reportes personalizados.
 *
 * @return string[]
 */
function custom_report_allowed_formats(): array
{
    return ['excel', 'csv', 'pdf'];
}

