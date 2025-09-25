<?php

declare(strict_types=1);

class FakeCalibrationAlertResult
{
    /** @var array<int,array<string,mixed>> */
    private array $rows;
    private int $index = 0;

    /**
     * @param array<int,array<string,mixed>> $rows
     */
    public function __construct(array $rows)
    {
        $this->rows = array_values($rows);
    }

    /**
     * @return array<string,mixed>|null
     */
    public function fetch_assoc(): ?array
    {
        if (!isset($this->rows[$this->index])) {
            return null;
        }

        return $this->rows[$this->index++];
    }

    public function free(): void
    {
        $this->rows = [];
        $this->index = 0;
    }

    public function close(): void
    {
        $this->free();
    }
}

abstract class FakeCalibrationAbstractStmt
{
    protected FakeCalibrationMysqli $conn;
    /** @var array<int,mixed> */
    protected array $params = [];
    public string $error = '';

    public function __construct(FakeCalibrationMysqli $conn)
    {
        $this->conn = $conn;
    }

    public function bind_param(string $types, &...$vars): bool
    {
        $this->params = [];
        foreach ($vars as &$var) {
            $this->params[] = &$var;
        }
        return true;
    }

    public function close(): bool
    {
        $this->params = [];
        return true;
    }
}

class FakeCalibrationQueryStmt extends FakeCalibrationAbstractStmt
{
    private string $type;

    public function __construct(FakeCalibrationMysqli $conn, string $type)
    {
        parent::__construct($conn);
        $this->type = $type;
    }

    public function execute(): bool
    {
        return true;
    }

    public function get_result(): FakeCalibrationAlertResult
    {
        return new FakeCalibrationAlertResult($this->conn->getRowsForType($this->type));
    }
}

class FakeCalibrationCheckStmt extends FakeCalibrationAbstractStmt
{
    private int $numRowsValue = 0;

    public function execute(): bool
    {
        $instrumentoId = (int) ($this->params[0] ?? 0);
        $empresaId = (int) ($this->params[1] ?? 0);
        $dueDate = (string) ($this->params[2] ?? '');

        $key = FakeCalibrationMysqli::makeKeyValues($instrumentoId, $empresaId, $dueDate);
        $exists = in_array($key, $this->conn->existingNotifications, true);
        $this->numRowsValue = $exists ? 1 : 0;

        return true;
    }

    public function store_result(): bool
    {
        return true;
    }

    public function free_result(): void
    {
        // No-op for fake statement
    }

    public function __get(string $name)
    {
        if ($name === 'num_rows') {
            return $this->numRowsValue;
        }

        trigger_error('Undefined property: ' . $name, E_USER_NOTICE);
        return null;
    }
}

class FakeCalibrationSchemaStmt extends FakeCalibrationAbstractStmt
{
    private ?FakeCalibrationAlertResult $result = null;

    public function execute(): bool
    {
        $this->result = new FakeCalibrationAlertResult([['total' => 1]]);
        return true;
    }

    public function get_result(): FakeCalibrationAlertResult
    {
        return $this->result ?? new FakeCalibrationAlertResult([]);
    }
}

class FakeCalibrationChannelsStmt extends FakeCalibrationAbstractStmt
{
    public function execute(): bool
    {
        return true;
    }

    public function get_result(): FakeCalibrationAlertResult
    {
        return new FakeCalibrationAlertResult([]);
    }
}

class FakeCalibrationInsertStmt extends FakeCalibrationAbstractStmt
{
    public function execute(): bool
    {
        $instrumentoId = (int) ($this->params[0] ?? 0);
        $empresaId = (int) ($this->params[1] ?? 0);
        $dueDate = (string) ($this->params[2] ?? '');
        $record = [
            'instrumento_id' => $instrumentoId,
            'empresa_id' => $empresaId,
            'due_date' => $dueDate,
            'alert_type' => (string) ($this->params[3] ?? 'upcoming'),
        ];

        $this->conn->insertedNotifications[] = $record;
        $this->conn->existingNotifications[] = FakeCalibrationMysqli::makeKeyValues($instrumentoId, $empresaId, $dueDate);

        return true;
    }
}

class FakeCalibrationMysqli extends mysqli
{
    /** @var array<int,array<string,mixed>> */
    public array $upcomingRows;
    /** @var array<int,array<string,mixed>> */
    public array $overdueRows;
    /** @var array<int,string> */
    public array $existingNotifications = [];
    /** @var array<int,array<string,mixed>> */
    public array $insertedNotifications = [];
    private string $fakeError = '';
    private int $fakeErrno = 0;
    /** @var array<int,string> */
    public array $queries = [];

    /**
     * @param array<int,array<string,mixed>> $upcomingRows
     * @param array<int,array<string,mixed>> $overdueRows
     */
    public function __construct(array $upcomingRows, array $overdueRows)
    {
        $this->upcomingRows = $upcomingRows;
        $this->overdueRows = $overdueRows;
    }

    public static function makeKeyValues(int $instrumentoId, int $empresaId, string $dueDate): string
    {
        return $instrumentoId . '|' . $empresaId . '|' . $dueDate;
    }

    /**
     * @return array<int,array<string,mixed>>
     */
    public function getRowsForType(string $type): array
    {
        if ($type === 'overdue') {
            $filtered = [];
            foreach ($this->overdueRows as $row) {
                $key = self::makeKeyValues((int) $row['instrumento_id'], (int) $row['empresa_id'], (string) ($row['fecha_proxima'] ?? ''));
                if (in_array($key, $this->existingNotifications, true)) {
                    continue;
                }
                $filtered[] = $row;
            }
            return $filtered;
        }

        return $this->upcomingRows;
    }

    #[\ReturnTypeWillChange]
    public function prepare($query)
    {
        $normalized = trim((string) $query);
        if (stripos($normalized, 'INSERT INTO calibration_alert_notifications') === 0) {
            return new FakeCalibrationInsertStmt($this);
        }

        if (stripos($normalized, 'SELECT 1 FROM calibration_alert_notifications') === 0) {
            return new FakeCalibrationCheckStmt($this);
        }

        if (stripos($normalized, 'FROM information_schema.COLUMNS') !== false) {
            return new FakeCalibrationSchemaStmt($this);
        }

        if (stripos($normalized, 'FROM tenant_notification_channels') !== false) {
            return new FakeCalibrationChannelsStmt($this);
        }

        if (stripos($normalized, 'FROM calibraciones') !== false) {
            if (stripos($normalized, 'BETWEEN ? AND ?') !== false) {
                return new FakeCalibrationQueryStmt($this, 'upcoming');
            }
            if (stripos($normalized, '< CURDATE()') !== false) {
                return new FakeCalibrationQueryStmt($this, 'overdue');
            }
        }

        $this->setError('Consulta inesperada: ' . $query);
        return false;
    }

    #[\ReturnTypeWillChange]
    public function query(string $query, int $result_mode = MYSQLI_STORE_RESULT)
    {
        $this->queries[] = $query;
        if (stripos(trim($query), 'SELECT DATABASE()') === 0) {
            return new FakeCalibrationAlertResult([['db' => 'fake_schema']]);
        }

        return true;
    }

    public function __get(string $name)
    {
        if ($name === 'error') {
            return $this->fakeError;
        }
        if ($name === 'errno') {
            return $this->fakeErrno;
        }

        trigger_error('Undefined property: ' . $name, E_USER_NOTICE);
        return null;
    }

    public function setError(string $message, int $code = 0): void
    {
        $this->fakeError = $message;
        $this->fakeErrno = $code;
    }
}
