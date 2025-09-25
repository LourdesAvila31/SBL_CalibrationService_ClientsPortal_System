<?php

declare(strict_types=1);

/**
 * Calcula el estado del instrumento con base en la información disponible.
 *
 * @param array{
 *     estadoActual?: string|null,
 *     fechaBaja?: string|null,
 *     resultado?: string|null,
 *     observaciones?: string|null,
 *     departamentoId?: int|null,
 *     enStock?: bool|null
 * } $contexto
 */
function derivarEstadoInstrumento(array $contexto): string
{
    $estadoActual = trim((string)($contexto['estadoActual'] ?? ''));
    $fechaBaja = trim((string)($contexto['fechaBaja'] ?? ''));
    $resultado = trim((string)($contexto['resultado'] ?? ''));
    $observaciones = trim((string)($contexto['observaciones'] ?? ''));
    $departamentoId = $contexto['departamentoId'] ?? null;
    $enStock = (bool)($contexto['enStock'] ?? false);

    if ($observaciones !== '') {
        if (esTextoDeBaja($observaciones) || esTextoRechazado($observaciones)) {
            return $observaciones;
        }
    }

    if ($fechaBaja !== '') {
        if ($observaciones !== '') {
            return $observaciones;
        }
        return 'inactivo';
    }

    if ($resultado !== '') {
        if (esTextoRechazado($resultado)) {
            return $observaciones !== '' ? $observaciones : 'Rechazado en calibración';
        }
        if (esTextoDeBaja($resultado)) {
            return 'inactivo';
        }
    }

    if ($enStock) {
        return 'stock';
    }

    if ($departamentoId === null || $departamentoId === 0) {
        return 'stock';
    }

    if ($estadoActual !== '') {
        if (esTextoRechazado($estadoActual) || esTextoDeBaja($estadoActual)) {
            return $estadoActual;
        }
        if (esEstadoStock($estadoActual)) {
            return ($departamentoId === null || $departamentoId === 0) ? 'stock' : 'activo';
        }
        if (esEstadoInactivo($estadoActual) && $fechaBaja !== '') {
            return 'inactivo';
        }
    }

    return 'activo';
}

function esTextoRechazado(?string $texto): bool
{
    if ($texto === null || $texto === '') {
        return false;
    }

    $texto = mb_strtolower($texto);
    $palabras = ['rechazad', 'no cumple', 'fuera de tolerancia', 'fail'];

    foreach ($palabras as $palabra) {
        if (mb_strpos($texto, $palabra) !== false) {
            return true;
        }
    }

    return false;
}

function esTextoDeBaja(?string $texto): bool
{
    if ($texto === null || $texto === '') {
        return false;
    }

    $texto = mb_strtolower($texto);

    if (mb_strpos($texto, 'dado de baja') !== false || mb_strpos($texto, 'da de baja') !== false) {
        return true;
    }

    return preg_match('/\bde\s+baja\b/u', $texto) === 1;
}

function esEstadoStock(?string $estado): bool
{
    if ($estado === null || $estado === '') {
        return false;
    }

    $estado = mb_strtolower($estado);
    return $estado === 'stock' || $estado === 'en stock';
}

function esEstadoInactivo(?string $estado): bool
{
    if ($estado === null || $estado === '') {
        return false;
    }

    return mb_strpos(mb_strtolower($estado), 'inactiv') !== false;
}

/**
 * Normaliza el estado operativo para agruparlo en cuatro categorías principales.
 */
function instrumento_normalizar_estado_operativo(?string $estado): string
{
    $texto = trim((string)($estado ?? ''));
    if ($texto === '') {
        return 'en_uso';
    }

    $valor = mb_strtolower($texto, 'UTF-8');

    if (esTextoDeBaja($texto) || str_contains($valor, 'baja') || str_contains($valor, 'descart')) {
        return 'baja';
    }

    if (esTextoRechazado($texto) || esEstadoInactivo($texto) || str_contains($valor, 'fuera') || str_contains($valor, 'servicio') || str_contains($valor, 'manten') || str_contains($valor, 'repar') || str_contains($valor, 'susp')) {
        return 'fuera_servicio';
    }

    if (esEstadoStock($texto) || str_contains($valor, 'almac') || str_contains($valor, 'bodega') || str_contains($valor, 'resguardo') || str_contains($valor, 'inventario') || str_contains($valor, 'stock')) {
        return 'en_stock';
    }

    if (str_contains($valor, 'activo') || str_contains($valor, 'operativo') || str_contains($valor, 'uso') || str_contains($valor, 'producc') || str_contains($valor, 'linea')) {
        return 'en_uso';
    }

    return 'en_uso';
}

/**
 * Devuelve la etiqueta visible asociada al estado operativo.
 */
function instrumento_estado_operativo_label(?string $estadoOriginal, ?string $estadoNormalizado = null): string
{
    if ($estadoNormalizado === null) {
        $estadoNormalizado = instrumento_normalizar_estado_operativo($estadoOriginal);
    }

    $map = [
        'en_uso' => 'En uso',
        'fuera_servicio' => 'Fuera de servicio',
        'en_stock' => 'En stock',
        'baja' => 'Baja',
    ];

    $label = $map[$estadoNormalizado] ?? 'En uso';

    return $label;
}

/**
 * Valida si el estado operativo normalizado es uno de los soportados.
 */
function instrumento_validar_estado_operativo(?string $estado): ?string
{
    if ($estado === null) {
        return null;
    }

    $valor = trim(mb_strtolower($estado, 'UTF-8'));
    $permitidos = ['en_uso', 'fuera_servicio', 'en_stock', 'baja'];

    return in_array($valor, $permitidos, true) ? $valor : null;
}

/**
 * Estructura auxiliar para anexar la etiqueta visible al estado operativo.
 *
 * @return array{estado_operativo: string, estado_operativo_label: string}
 */
function instrumento_estado_operativo_info(?string $estado): array
{
    $normalizado = instrumento_normalizar_estado_operativo($estado);

    return [
        'estado_operativo' => $normalizado,
        'estado_operativo_label' => instrumento_estado_operativo_label($estado, $normalizado),
    ];
}
