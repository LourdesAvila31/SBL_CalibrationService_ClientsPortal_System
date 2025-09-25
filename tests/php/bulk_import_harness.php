<?php

declare(strict_types=1);

require_once __DIR__ . '/../../app/Modules/Tenant/Instrumentos/gages/BulkGageImporter.php';

/**
 * Implementación simulada del importador para pruebas unitarias.
 */
class FakeBulkGageImporter extends BulkGageImporter
{
    /** @var array<string,int> */
    private array $catalogoMap = [];
    /** @var array<string,int> */
    private array $marcaMap = [];
    /** @var array<string,int> */
    private array $departamentoMap = [];
    /** @var array<string,int> */
    private array $modeloMap = [];

    private int $catalogoCounter = 100;
    private int $marcaCounter = 200;
    private int $departamentoCounter = 300;
    private int $modeloCounter = 400;

    /** @var array<int,array<string,mixed>> */
    private array $inserted = [];

    public function __construct()
    {
        parent::__construct(null, 7, 42);

        $this->catalogoMap = [
            self::normalizeText('Balanza analítica') => 101,
            self::normalizeText('Micrómetro digital') => 102,
        ];
        $this->marcaMap = [
            self::normalizeText('Acme Instruments') => 201,
            self::normalizeText('Precision Co.') => 202,
        ];
        $this->departamentoMap = [
            self::normalizeText('Metrología') => 301,
            self::normalizeText('Producción') => 302,
            self::normalizeText('Calidad') => 303,
        ];
        $this->modeloMap = [
            $this->modeloKey('AX-200', 201) => 401,
            $this->modeloKey('MD-25', 202) => 402,
        ];
        $this->catalogoCounter = 150;
        $this->marcaCounter = 250;
        $this->departamentoCounter = 350;
        $this->modeloCounter = 450;
    }

    /** @return array<int,array<string,mixed>> */
    public function getInserted(): array
    {
        return $this->inserted;
    }

    protected function ensureCatalogoId(string $nombre): int
    {
        return $this->ensureFromMap($nombre, $this->catalogoMap, $this->catalogoCounter);
    }

    protected function ensureMarcaId(string $nombre): int
    {
        return $this->ensureFromMap($nombre, $this->marcaMap, $this->marcaCounter);
    }

    protected function ensureDepartamentoId(string $nombre): int
    {
        return $this->ensureFromMap($nombre, $this->departamentoMap, $this->departamentoCounter);
    }

    protected function ensureModeloId(string $nombre, int $marcaId): int
    {
        $normalized = $this->modeloKey($nombre, $marcaId);
        if (!isset($this->modeloMap[$normalized])) {
            $this->modeloCounter++;
            $this->modeloMap[$normalized] = $this->modeloCounter;
        }
        return $this->modeloMap[$normalized];
    }

    protected function insertInstrumento(array $instrumento): int
    {
        $id = count($this->inserted) + 1;
        $instrumento['id'] = $id;
        $this->inserted[] = $instrumento;
        return $id;
    }

    protected function findExistingInstrumento(string $codigo): ?int
    {
        foreach ($this->inserted as $row) {
            if (strcasecmp((string) $row['codigo'], $codigo) === 0) {
                return (int) $row['id'];
            }
        }
        return null;
    }

    protected function logImport(string $filename, array $resultado): void
    {
        // No se registra nada durante las pruebas.
    }

    protected function shouldLogImports(): bool
    {
        return false;
    }

    /**
     * @param array<string,int> $map
     */
    private function ensureFromMap(string $nombre, array &$map, int &$counter): int
    {
        $normalized = self::normalizeText($nombre);
        if ($normalized === '') {
            throw new RuntimeException('El valor no puede estar vacío.');
        }
        if (!isset($map[$normalized])) {
            $counter++;
            $map[$normalized] = $counter;
        }
        return $map[$normalized];
    }

    private function modeloKey(string $nombre, int $marcaId): string
    {
        return $marcaId . '::' . self::normalizeText($nombre);
    }
}

function run_bulk_import_harness(): array
{
    $samplePath = __DIR__ . '/../data/bulk_import_sample.csv';
    if (!is_file($samplePath)) {
        throw new RuntimeException('No se encontró el archivo de muestra para las pruebas.');
    }

    $importer = new FakeBulkGageImporter();
    $resultado = $importer->importFromFilePath($samplePath, [
        'originalName' => 'bulk_import_sample.csv',
        'extension' => 'csv',
    ]);

    return [
        'resultado' => $resultado,
        'insertados' => $importer->getInserted(),
    ];
}
