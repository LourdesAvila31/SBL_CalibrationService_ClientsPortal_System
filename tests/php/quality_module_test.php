<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Modules/Internal/Calidad/quality_module.php';

function assertEquals($expected, $actual, string $message): void
{
    if ($expected != $actual) {
        fwrite(STDERR, sprintf("Assertion failed: %s. Expected %s, got %s\n", $message, var_export($expected, true), var_export($actual, true)));
        exit(1);
    }
}

function assertSameValue($expected, $actual, string $message): void
{
    if ($expected !== $actual) {
        fwrite(STDERR, sprintf("Assertion failed: %s. Expected %s, got %s\n", $message, var_export($expected, true), var_export($actual, true)));
        exit(1);
    }
}

function assertTrue(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "Assertion failed: {$message}\n");
        exit(1);
    }
}

$timezone = new DateTimeZone('UTC');
$quality = new QualityModule($timezone);

// Documento: creación -> revisión -> publicación
$createdAt = new DateTimeImmutable('2024-04-01 09:00:00', $timezone);
$doc = $quality->createDocument('POE-001', 'Control de cambios', 'Ana García', 'Contenido inicial', $createdAt);
assertSameValue('borrador', $doc['estado'], 'Documento nuevo debe iniciar en borrador');
assertSameValue(1, $doc['version'], 'La versión inicial debe ser 1');
assertSameValue('Ana García', $doc['autor'], 'Se conserva el autor');
assertEquals($createdAt->format(DateTimeImmutable::ATOM), $doc['fecha_creacion'], 'Fecha de creación fija');
assertEquals(1, count($doc['historial']), 'Historial inicial');

$reviewedAt = new DateTimeImmutable('2024-04-03 10:30:00', $timezone);
$doc = $quality->submitDocumentForReview($doc['id'], 'Carlos Ruiz', $reviewedAt, 'Revisión inicial');
assertSameValue('en_revision', $doc['estado'], 'Estado cambia a revisión');
assertSameValue('Carlos Ruiz', $doc['revisor'], 'Revisor asignado');
assertEquals($reviewedAt->format(DateTimeImmutable::ATOM), $doc['fecha_revision'], 'Fecha de revisión guardada');
assertEquals(2, count($doc['historial']), 'Historial incluye revisión');

$publishedAt = new DateTimeImmutable('2024-04-04 08:15:00', $timezone);
$doc = $quality->publishDocument($doc['id'], 'Laura Méndez', $publishedAt, 'Publicación aprobada');
assertSameValue('publicado', $doc['estado'], 'Documento publicado');
assertSameValue(2, $doc['version'], 'La publicación genera nueva versión');
assertSameValue('Laura Méndez', $doc['publicado_por'], 'Publicador registrado');
assertEquals($publishedAt->format(DateTimeImmutable::ATOM), $doc['fecha_publicacion'], 'Fecha de publicación guardada');
assertEquals(3, count($doc['historial']), 'Historial registra publicación');

// Capacitación: programación y asistencia
$trainingDate = new DateTimeImmutable('2024-05-20 15:00:00', $timezone);
$training = $quality->scheduleTraining('Actualización ISO 17025', $trainingDate, 'María López', ['Luis Pérez', 'Sofía Ramos'], 'virtual');
assertSameValue('Actualización ISO 17025', $training['tema'], 'Tema registrado');
assertEquals($trainingDate->format(DateTimeImmutable::ATOM), $training['fecha_programada'], 'Fecha programada conservada');
assertSameValue(2, $training['resumen']['total'], 'Dos participantes programados');
assertSameValue(0, $training['resumen']['asistencias'], 'Sin asistencia inicial');

$training = $quality->recordAttendance($training['id'], 'Luis Pérez', true, 'Ingreso puntual');
$training = $quality->recordAttendance($training['id'], 'Sofía Ramos', false, 'Justificó ausencia');
assertSameValue(2, $training['resumen']['total'], 'Total asistentes no cambia');
assertSameValue(1, $training['resumen']['asistencias'], 'Una asistencia registrada');
assertSameValue(1, $training['resumen']['faltas'], 'Una falta registrada');
assertSameValue(50.0, $training['resumen']['porcentaje_asistencia'], 'Asistencia al 50%');

// No conformidades: cierre exitoso
$openedAt = new DateTimeImmutable('2024-06-01 09:00:00', $timezone);
$nc = $quality->createNonConformity(
    'NC-24-001',
    'Hallazgo en auditoría interna',
    'Juan Torres',
    'Auditoría interna',
    ['Actualizar instructivo', 'Capacitar al equipo'],
    $openedAt
);
assertSameValue('abierta', $nc['estado'], 'No conformidad inicia abierta');
assertSameValue(2, count($nc['acciones']), 'Dos acciones planificadas');

$quality->markActionCompleted($nc['id'], 1, 'Se adjunta instructivo actualizado', new DateTimeImmutable('2024-06-05 12:00:00', $timezone));
$quality->markActionCompleted($nc['id'], 2, 'Lista de asistencia de capacitación', new DateTimeImmutable('2024-06-06 15:30:00', $timezone));

$closed = $quality->closeNonConformity($nc['id'], 'Verónica Díaz', 'Evidencia cargada en repositorio', new DateTimeImmutable('2024-06-07 09:15:00', $timezone));
assertSameValue('cerrada', $closed['estado'], 'Estado final cerrado');
assertSameValue('Verónica Díaz', $closed['verificado_por'], 'Verificador documentado');
assertEquals('Evidencia cargada en repositorio', $closed['evidencia_cierre'], 'Evidencia de cierre guardada');
assertEquals(4, count($closed['seguimiento']), 'Seguimiento registra cada hito del cierre');

// Intento de cierre con acciones pendientes
$ncPendiente = $quality->createNonConformity(
    'NC-24-002',
    'Desviación en medición de pH',
    'Irene Silva',
    'Reporte de laboratorio',
    ['Recalibrar equipo'],
    new DateTimeImmutable('2024-06-10 10:00:00', $timezone)
);

$exceptionCaught = false;
try {
    $quality->closeNonConformity($ncPendiente['id'], 'Supervisor', 'Sin evidencia');
} catch (RuntimeException $exception) {
    $exceptionCaught = true;
    assertTrue(strpos($exception->getMessage(), 'acciones') !== false, 'El mensaje debe indicar acciones pendientes');
}
assertTrue($exceptionCaught, 'No se puede cerrar con acciones pendientes');

echo "Quality module tests completed successfully\n";
