<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Core/helpers/calibration_nonconformity.php';
require_once __DIR__ . '/../../app/Core/helpers/instrument_status.php';

function assert_true(bool $condition, string $message): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

$registroLlamadas = [];
$enviosCorreo = [];

$contexto = [
    'calibracion_id' => 5,
    'instrumento_id' => 9,
    'empresa_id' => 3,
    'resultado' => 'Resultado fuera de tolerancia',
    'notes' => 'Se excede la tolerancia permitida.',
];

$repositorio = function (string $accion, array $payload = []) use (&$registroLlamadas) {
    $registroLlamadas[] = ['accion' => $accion, 'payload' => $payload];
    if ($accion === 'upsert') {
        assert_true(($payload['calibracion_id'] ?? null) === 5, 'El upsert debe recibir la calibración esperada.');
        return ['id' => 42];
    }
    if ($accion === 'mark_notified') {
        assert_true(($payload['id'] ?? null) === 42, 'El marcado de notificación debe apuntar al registro creado.');
    }
    return null;
};

$envioCorreo = function (string $correo, string $nombre, string $asunto, string $html, ?string $texto) use (&$enviosCorreo) {
    $enviosCorreo[] = compact('correo', 'nombre', 'asunto', 'html', 'texto');
};

$resolverResponsable = function (int $empresaId) {
    assert_true($empresaId === 3, 'El resolver de responsable recibe la empresa correcta.');
    return ['email' => 'qa@example.com', 'nombre' => 'Responsable QA'];
};

$resolverInstrumento = function (int $instrumentoId, int $empresaId) {
    assert_true($instrumentoId === 9, 'El resolver de instrumento recibe el id correcto.');
    return [
        'instrumento_nombre' => 'Balanza analítica',
        'codigo' => 'BAL-001',
        'empresa_nombre' => 'Laboratorios Demo',
        'serie' => 'SN-77',
    ];
};

$ahora = new DateTimeImmutable('2024-05-10 12:30:00');

calibration_nonconformity_process(
    $contexto,
    $repositorio,
    $envioCorreo,
    $resolverResponsable,
    $resolverInstrumento,
    static fn () => $ahora
);

assert_true(count($registroLlamadas) === 2, 'El repositorio debe ser invocado para upsert y para marcar notificación.');
assert_true($registroLlamadas[0]['accion'] === 'upsert', 'La primera acción del repositorio debe ser un upsert.');
assert_true($registroLlamadas[1]['accion'] === 'mark_notified', 'La segunda acción debe registrar la notificación.');
assert_true(count($enviosCorreo) === 1, 'Debe enviarse un correo al responsable.');
assert_true(stripos($enviosCorreo[0]['asunto'], 'No conformidad') !== false, 'El asunto del correo debe mencionar la no conformidad.');
assert_true(esTextoRechazado('El equipo quedó fuera de tolerancia'), 'La detección reconoce la frase "fuera de tolerancia".');

echo "✓ Pruebas de no conformidades superadas" . PHP_EOL;
