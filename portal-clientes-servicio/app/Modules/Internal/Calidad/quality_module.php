<?php

declare(strict_types=1);

class QualityModule
{
    /** @var array<int, array<string, mixed>> */
    private array $documents = [];
    /** @var array<int, array<string, mixed>> */
    private array $trainings = [];
    /** @var array<int, array<string, mixed>> */
    private array $nonConformities = [];

    private int $documentSequence = 1;
    private int $trainingSequence = 1;
    private int $nonConformitySequence = 1;

    private DateTimeZone $timezone;

    public function __construct(?DateTimeZone $timezone = null)
    {
        $this->timezone = $timezone ?? new DateTimeZone('UTC');
    }

    public function createDocument(
        string $code,
        string $title,
        string $author,
        string $content,
        ?DateTimeImmutable $createdAt = null
    ): array {
        $id = $this->documentSequence++;
        $timestamp = $this->formatTimestamp($createdAt);

        $document = [
            'id' => $id,
            'codigo' => $code,
            'titulo' => $title,
            'autor' => $author,
            'contenido' => $content,
            'version' => 1,
            'estado' => 'borrador',
            'revisor' => null,
            'publicado_por' => null,
            'fecha_creacion' => $timestamp,
            'fecha_revision' => null,
            'fecha_publicacion' => null,
            'historial' => [
                [
                    'evento' => 'creado',
                    'usuario' => $author,
                    'estado' => 'borrador',
                    'timestamp' => $timestamp,
                    'comentario' => 'Documento registrado en el módulo de calidad'
                ],
            ],
        ];

        $this->documents[$id] = $document;

        return $document;
    }

    public function submitDocumentForReview(
        int $documentId,
        string $reviewer,
        ?DateTimeImmutable $reviewedAt = null,
        string $comment = 'Revisión iniciada'
    ): array {
        $document = $this->requireDocument($documentId);

        if ($document['estado'] !== 'borrador') {
            throw new RuntimeException('Solo los documentos en borrador pueden enviarse a revisión');
        }

        $document['estado'] = 'en_revision';
        $document['revisor'] = $reviewer;
        $document['fecha_revision'] = $this->formatTimestamp($reviewedAt);
        $document['historial'][] = [
            'evento' => 'en_revision',
            'usuario' => $reviewer,
            'estado' => 'en_revision',
            'timestamp' => $document['fecha_revision'],
            'comentario' => $comment,
        ];

        $this->documents[$documentId] = $document;

        return $document;
    }

    public function publishDocument(
        int $documentId,
        string $publisher,
        ?DateTimeImmutable $publishedAt = null,
        string $comment = 'Documento publicado'
    ): array {
        $document = $this->requireDocument($documentId);

        if ($document['estado'] !== 'en_revision') {
            throw new RuntimeException('Solo los documentos en revisión pueden publicarse');
        }

        $document['estado'] = 'publicado';
        $document['publicado_por'] = $publisher;
        $document['version'] += 1;
        $document['fecha_publicacion'] = $this->formatTimestamp($publishedAt);
        $document['historial'][] = [
            'evento' => 'publicado',
            'usuario' => $publisher,
            'estado' => 'publicado',
            'timestamp' => $document['fecha_publicacion'],
            'comentario' => $comment,
        ];

        $this->documents[$documentId] = $document;

        return $document;
    }

    public function getDocument(int $documentId): array
    {
        return $this->requireDocument($documentId);
    }

    public function scheduleTraining(
        string $topic,
        DateTimeImmutable $date,
        string $responsible,
        array $attendees,
        string $modality = 'presencial'
    ): array {
        $id = $this->trainingSequence++;
        $formattedDate = $this->formatTimestamp($date);
        $records = [];

        foreach ($attendees as $attendee) {
            $records[] = [
                'participante' => $attendee,
                'estatus' => 'pendiente',
                'comentarios' => null,
            ];
        }

        $training = [
            'id' => $id,
            'tema' => $topic,
            'fecha_programada' => $formattedDate,
            'responsable' => $responsible,
            'modalidad' => $modality,
            'asistentes' => $records,
            'resumen' => $this->summarizeAttendance($records),
        ];

        $this->trainings[$id] = $training;

        return $training;
    }

    public function recordAttendance(
        int $trainingId,
        string $participant,
        bool $attended,
        ?string $comments = null
    ): array {
        $training = $this->requireTraining($trainingId);
        $updated = false;

        foreach ($training['asistentes'] as &$record) {
            if ($record['participante'] === $participant) {
                $record['estatus'] = $attended ? 'asistio' : 'falta';
                $record['comentarios'] = $comments;
                $updated = true;
                break;
            }
        }
        unset($record);

        if (!$updated) {
            $training['asistentes'][] = [
                'participante' => $participant,
                'estatus' => $attended ? 'asistio' : 'falta',
                'comentarios' => $comments,
            ];
        }

        $training['resumen'] = $this->summarizeAttendance($training['asistentes']);
        $this->trainings[$trainingId] = $training;

        return $training;
    }

    public function getTraining(int $trainingId): array
    {
        return $this->requireTraining($trainingId);
    }

    public function createNonConformity(
        string $code,
        string $description,
        string $owner,
        string $source,
        array $actions,
        ?DateTimeImmutable $openedAt = null
    ): array {
        $id = $this->nonConformitySequence++;
        $timestamp = $this->formatTimestamp($openedAt);
        $records = [];
        $sequence = 1;

        foreach ($actions as $action) {
            $records[] = [
                'id' => $sequence++,
                'descripcion' => $action,
                'estado' => 'pendiente',
                'responsable' => $owner,
                'evidencia' => null,
                'fecha_cierre' => null,
            ];
        }

        $nonConformity = [
            'id' => $id,
            'codigo' => $code,
            'descripcion' => $description,
            'estado' => 'abierta',
            'responsable' => $owner,
            'origen' => $source,
            'verificado_por' => null,
            'fecha_apertura' => $timestamp,
            'fecha_cierre' => null,
            'evidencia_cierre' => null,
            'acciones' => $records,
            'seguimiento' => [
                [
                    'evento' => 'registrada',
                    'usuario' => $owner,
                    'estado' => 'abierta',
                    'timestamp' => $timestamp,
                    'comentario' => 'No conformidad documentada',
                ],
            ],
        ];

        $this->nonConformities[$id] = $nonConformity;

        return $nonConformity;
    }

    public function markActionCompleted(
        int $nonConformityId,
        int $actionId,
        string $evidence,
        ?DateTimeImmutable $completedAt = null
    ): array {
        $nonConformity = $this->requireNonConformity($nonConformityId);
        $timestamp = $this->formatTimestamp($completedAt);
        $found = false;

        foreach ($nonConformity['acciones'] as &$action) {
            if ($action['id'] === $actionId) {
                $action['estado'] = 'completada';
                $action['evidencia'] = $evidence;
                $action['fecha_cierre'] = $timestamp;
                $found = true;
                break;
            }
        }
        unset($action);

        if (!$found) {
            throw new RuntimeException('La acción especificada no existe');
        }

        $nonConformity['seguimiento'][] = [
            'evento' => 'accion_completada',
            'usuario' => $nonConformity['responsable'],
            'estado' => $nonConformity['estado'],
            'timestamp' => $timestamp,
            'comentario' => sprintf('Acción %d marcada como completada', $actionId),
        ];

        $this->nonConformities[$nonConformityId] = $nonConformity;

        return $nonConformity;
    }

    public function closeNonConformity(
        int $nonConformityId,
        string $verifier,
        string $closureEvidence,
        ?DateTimeImmutable $closedAt = null
    ): array {
        $nonConformity = $this->requireNonConformity($nonConformityId);

        foreach ($nonConformity['acciones'] as $action) {
            if ($action['estado'] !== 'completada') {
                throw new RuntimeException('Todas las acciones deben completarse antes de cerrar la no conformidad');
            }
        }

        $timestamp = $this->formatTimestamp($closedAt);
        $nonConformity['estado'] = 'cerrada';
        $nonConformity['verificado_por'] = $verifier;
        $nonConformity['evidencia_cierre'] = $closureEvidence;
        $nonConformity['fecha_cierre'] = $timestamp;
        $nonConformity['seguimiento'][] = [
            'evento' => 'cerrada',
            'usuario' => $verifier,
            'estado' => 'cerrada',
            'timestamp' => $timestamp,
            'comentario' => 'Cierre verificado y evidencia cargada',
        ];

        $this->nonConformities[$nonConformityId] = $nonConformity;

        return $nonConformity;
    }

    public function getNonConformity(int $nonConformityId): array
    {
        return $this->requireNonConformity($nonConformityId);
    }

    private function requireDocument(int $documentId): array
    {
        if (!isset($this->documents[$documentId])) {
            throw new RuntimeException('Documento de calidad no encontrado');
        }

        return $this->documents[$documentId];
    }

    private function requireTraining(int $trainingId): array
    {
        if (!isset($this->trainings[$trainingId])) {
            throw new RuntimeException('Capacitación no encontrada');
        }

        return $this->trainings[$trainingId];
    }

    private function requireNonConformity(int $nonConformityId): array
    {
        if (!isset($this->nonConformities[$nonConformityId])) {
            throw new RuntimeException('No conformidad inexistente');
        }

        return $this->nonConformities[$nonConformityId];
    }

    /**
     * @param array<int, array<string, mixed>> $records
     * @return array{total:int, asistencias:int, faltas:int, porcentaje_asistencia:float}
     */
    private function summarizeAttendance(array $records): array
    {
        $total = count($records);
        $attended = 0;
        $absent = 0;

        foreach ($records as $record) {
            if (($record['estatus'] ?? '') === 'asistio') {
                $attended++;
            }

            if (($record['estatus'] ?? '') === 'falta') {
                $absent++;
            }
        }

        $percentage = $total === 0 ? 0.0 : round(($attended / $total) * 100, 2);

        return [
            'total' => $total,
            'asistencias' => $attended,
            'faltas' => $absent,
            'porcentaje_asistencia' => $percentage,
        ];
    }

    private function formatTimestamp(?DateTimeImmutable $dateTime): string
    {
        $dateTime = $dateTime ? $dateTime->setTimezone($this->timezone) : new DateTimeImmutable('now', $this->timezone);

        return $dateTime->format(DateTimeImmutable::ATOM);
    }
}
