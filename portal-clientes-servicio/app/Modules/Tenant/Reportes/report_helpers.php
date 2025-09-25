<?php
require_once dirname(__DIR__, 3) . '/Core/helpers/company.php';
require_once dirname(__DIR__, 3) . '/Core/db.php';

/**
 * Obtiene un valor de la solicitud HTTP en POST o GET.
 *
 * @param string $key
 * @param mixed $default
 * @return mixed
 */
function request_input(string $key, $default = null)
{
    if (array_key_exists($key, $_POST)) {
        return $_POST[$key];
    }
    if (array_key_exists($key, $_GET)) {
        return $_GET[$key];
    }
    return $default;
}

/**
 * Obtiene el identificador de la empresa asociado a la sesión o solicitud.
 * Si no existe, envía un error HTTP 400 y detiene la ejecución.
 */
function require_empresa_id(): int
{
    $empresaId = obtenerEmpresaId();
    if (!$empresaId) {
        http_response_code(400);
        header('Content-Type: text/plain; charset=UTF-8');
        echo "Empresa no especificada";
        exit;
    }
    return (int) $empresaId;
}

/**
 * Envía una respuesta de error genérica en caso de problemas con la consulta.
 */
function respond_database_error(mysqli $conn): void
{
    http_response_code(500);
    header('Content-Type: text/plain; charset=UTF-8');
    echo 'Error al generar el reporte: ' . $conn->error;
    exit;
}

/**
 * Normaliza valores para mostrarlos en los reportes.
 */
function value_or_dash($value): string
{
    if ($value === null || $value === '') {
        return '-';
    }
    if ($value instanceof DateTimeInterface) {
        return $value->format('Y-m-d');
    }
    return (string) $value;
}

/**
 * Envía un reporte en formato CSV.
 *
 * @param string $filename Nombre sugerido del archivo.
 * @param array<int,string> $headers Encabezados de columnas.
 * @param array<int,array<int,string>> $rows Filas de datos.
 */
function stream_csv_report(string $filename, array $headers, array $rows): void
{
    header('Content-Type: text/csv; charset=UTF-8');
    header('Content-Disposition: attachment; filename="' . $filename . '"');

    // BOM para una correcta apertura en Excel.
    echo "\xEF\xBB\xBF";

    $outputRow = function (array $row): void {
        $escaped = array_map(static function ($value): string {
            $value = (string) $value;
            $value = str_replace('"', '""', $value);
            if (preg_match('/[",\n\r]/', $value)) {
                return '"' . $value . '"';
            }
            return $value;
        }, $row);
        echo implode(',', $escaped) . "\r\n";
    };

    $outputRow($headers);
    foreach ($rows as $row) {
        $outputRow($row);
    }
    exit;
}

/**
 * Envía un reporte en formato Excel (tabla HTML).
 *
 * @param string $filename
 * @param array<int,string> $headers
 * @param array<int,array<int,string>> $rows
 */
function stream_excel_report(string $filename, array $headers, array $rows): void
{
    header('Content-Type: application/vnd.ms-excel; charset=UTF-8');
    header('Content-Disposition: attachment; filename="' . $filename . '"');

    echo "<table border='1'><thead><tr>";
    foreach ($headers as $header) {
        echo '<th>' . htmlspecialchars($header, ENT_QUOTES, 'UTF-8') . '</th>';
    }
    echo "</tr></thead><tbody>";
    foreach ($rows as $row) {
        echo '<tr>';
        foreach ($row as $cell) {
            echo '<td>' . htmlspecialchars((string) $cell, ENT_QUOTES, 'UTF-8') . '</td>';
        }
        echo '</tr>';
    }
    echo '</tbody></table>';
    exit;
}

/**
 * Envía un listado en texto plano.
 *
 * @param string $filename
 * @param array<int,string> $lines
 */
function stream_list_report(string $filename, array $lines): void
{
    header('Content-Type: text/plain; charset=UTF-8');
    header('Content-Disposition: attachment; filename="' . $filename . '"');
    echo implode("\n", $lines);
    exit;
}

/**
 * Genera un documento PDF sencillo para listar datos tabulares.
 *
 * @param string $title
 * @param array<int,string> $headers
 * @param array<int,array<int,string>> $rows
 */
function stream_pdf_report(string $filename, string $title, array $headers, array $rows): void
{
    header('Content-Type: application/pdf');
    header('Content-Disposition: attachment; filename="' . $filename . '"');

    $lines = [];
    $lines[] = $title;
    $lines[] = str_repeat('=', mb_strlen($title, 'UTF-8'));
    $lines[] = implode(' | ', $headers);
    foreach ($rows as $row) {
        $lines[] = implode(' | ', array_map(static function ($value): string {
            $clean = (string) $value;
            // Sustituye saltos de línea para mantener el formato en PDF.
            $clean = str_replace(["\r", "\n"], ' ', $clean);
            return $clean === '' ? '-' : $clean;
        }, $row));
    }

    $pdf = build_simple_pdf($lines);
    echo $pdf;
    exit;
}

/**
 * Construye un PDF básico con texto.
 *
 * @param array<int,string> $lines
 */
function build_simple_pdf(array $lines): string
{
    $pdfLines = [];
    foreach ($lines as $line) {
        $pdfLines[] = sanitize_pdf_text($line);
    }

    $content = "BT\n/F1 12 Tf\n";
    $y = 780.0;
    foreach ($pdfLines as $line) {
        $escaped = pdf_escape($line);
        $content .= sprintf("1 0 0 1 50 %.2F Tm (%s) Tj\n", $y, $escaped);
        $y -= 14.0;
        if ($y < 40) {
            $y = 780.0; // Reinicia la posición si se excede la página.
        }
    }
    $content .= "ET\n";

    $objects = [];
    $objects[] = "1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n";
    $objects[] = "2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n";
    $objects[] = "3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n";
    $objects[] = "4 0 obj<< /Length " . strlen($content) . " >>stream\n" . $content . "endstream\nendobj\n";
    $objects[] = "5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n";

    $pdf = "%PDF-1.4\n";
    $offsets = [0];
    foreach ($objects as $object) {
        $offsets[] = strlen($pdf);
        $pdf .= $object;
    }

    $xrefPos = strlen($pdf);
    $pdf .= "xref\n0 " . count($offsets) . "\n";
    $pdf .= "0000000000 65535 f \n";
    for ($i = 1, $count = count($offsets); $i < $count; $i++) {
        $pdf .= sprintf('%010d 00000 n ', $offsets[$i]) . "\n";
    }
    $pdf .= "trailer<< /Size " . count($offsets) . " /Root 1 0 R >>\n";
    $pdf .= "startxref\n" . $xrefPos . "\n%%EOF";

    return $pdf;
}

function sanitize_pdf_text(string $text): string
{
    // Convierte caracteres a ASCII básico para evitar incompatibilidades.
    $normalized = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $text);
    if ($normalized === false) {
        $normalized = preg_replace('/[^\x20-\x7E]/', '', $text);
    }
    return $normalized ?? '';
}

function pdf_escape(string $text): string
{
    $text = str_replace('\\', '\\\\', $text);
    $text = str_replace(['(', ')'], ['\\(', '\\)'], $text);
    return $text;
}
