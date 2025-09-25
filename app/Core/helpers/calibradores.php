<?php

declare(strict_types=1);

/**
 * Calcula el mensaje canónico que debe firmarse para validar una medición de calibrador.
 */
function calibrator_signature_payload(string $measurementUuid, string $timestampIso, string $payloadJson): string
{
    return $measurementUuid . '|' . $timestampIso . '|' . $payloadJson;
}

/**
 * Genera la firma esperada (HMAC-SHA256) para una lectura de calibrador.
 */
function calibrator_compute_signature(string $measurementUuid, string $timestampIso, string $payloadJson, string $token): string
{
    $message = calibrator_signature_payload($measurementUuid, $timestampIso, $payloadJson);
    return hash_hmac('sha256', $message, $token);
}

/**
 * Verifica si la firma recibida coincide con la generada a partir del token configurado.
 */
function calibrator_verify_signature(string $measurementUuid, string $timestampIso, string $payloadJson, string $token, string $firma): bool
{
    $expected = calibrator_compute_signature($measurementUuid, $timestampIso, $payloadJson, $token);
    return hash_equals($expected, $firma);
}

/**
 * Determina si la selección manual de calibrador es compatible con la medición capturada.
 */
function calibrator_validate_selection(?int $measurementCalibratorId, ?int $selectedCalibratorId): bool
{
    if ($measurementCalibratorId === null || $measurementCalibratorId === 0) {
        return true;
    }
    if ($selectedCalibratorId === null || $selectedCalibratorId === 0) {
        return true;
    }
    return $measurementCalibratorId === $selectedCalibratorId;
}
