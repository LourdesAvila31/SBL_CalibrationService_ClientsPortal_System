<?php
declare(strict_types=1);

use App\Modules\Api\V1\ApiResponse;
use App\Modules\Api\V1\CalibracionesController;
use App\Modules\Api\V1\HttpResponder;

require_once __DIR__ . '/../../../../app/Modules/Api/V1/ApiResponse.php';
require_once __DIR__ . '/../../../../app/Modules/Api/V1/HttpResponder.php';
require_once __DIR__ . '/../../../../app/Modules/Api/V1/CalibracionesController.php';

$allowedMethods = ['GET'];
$method = strtoupper($_SERVER['REQUEST_METHOD'] ?? 'GET');

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Headers: Content-Type, Authorization, X-Requested-With');
header('Access-Control-Allow-Methods: ' . implode(', ', array_merge($allowedMethods, ['OPTIONS'])));

if ($method === 'OPTIONS') {
    header('Allow: ' . implode(', ', array_merge($allowedMethods, ['OPTIONS'])));
    http_response_code(204);
    exit;
}

try {
    $controller = new CalibracionesController();
    /** @var ApiResponse $response */
    $response = $controller->handle($method, $_GET);
    HttpResponder::emit($response);
} catch (\Throwable $e) {
    HttpResponder::emitException($e);
}
