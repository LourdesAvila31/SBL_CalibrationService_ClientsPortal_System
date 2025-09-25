<?php

declare(strict_types=1);

require_once __DIR__ . '/bulk_import_harness.php';

try {
    $data = run_bulk_import_harness();
    echo json_encode($data, JSON_UNESCAPED_UNICODE);
} catch (Throwable $e) {
    $payload = ['error' => $e->getMessage()];
    fwrite(STDERR, json_encode($payload, JSON_UNESCAPED_UNICODE) . PHP_EOL);
    exit(1);
}
