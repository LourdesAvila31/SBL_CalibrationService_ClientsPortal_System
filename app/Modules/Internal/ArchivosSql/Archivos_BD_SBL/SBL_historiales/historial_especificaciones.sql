-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'DC Voltage 0.00 V Tol ± 0.50V, 10.0 V Tol.  ±0.6V, 100.0 V Tol ±1.5V, 600.0 V Tol ±6.5V\nAC Voltage @ 60 Hz 0.00 V Tol ± 0.50, 10.0 V Tol ± 0.7V, 100.0 V Tol ±2.0V, 600.0 V Tol ±9.5V /\nResistance 0.0 Ω Tol ± 0.5Ω, 100.0 Ω Tol ±1.5Ω, 400.0 Ω Tol ±4.5Ω, 1000 Ω Tol ±15Ω, 4000 Ω Tol ±45Ω /\nDC current 0.00A ± 0.05A, 10.00 A Tol ±0.25A, 40.00 A Tol ±0.85A, 100.0 A Tol ±2.5A, 400.0 A Tol ±8.5A /\nAC current @60 Hz 10.00A Tol±0.25A, 40.00A Tol±0.85A, 100.0A Tol±2.5A, 400.0A Tol ±8.5A.\nTemperatua de 15°C a 70°C (tolerancia asignada por el proveedor), Capacitancia de 50 a 500 microfararios (tolerancia establecida por el proveedor).', NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'Externa', 'Velocidad del aire: 0.500 m/s ± 20% 1.000 m/s, 5.000 m/s, 10.000 m/s, 20.000 m/s, 30.000 m/s Tol. ±0.298 m/s Temperatura: 25°C Tol ±0.80°C', NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'Externa', 'Velocidad del aire: 0.000 m/s Tol ±0.002 m/s, 5.000 m/s Tol. ±0.100 m/s, 10.000 m/s Tol. ±0.200 m/s, 20.000 m/s Tol. ±0.400 m/s, 30.000 m/s Tol ±0.600 m/s Temperatura: 25°C Tol ±0.80°C', NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.0 mg, 1.0 mg, 50.0 mg, 75.0 mg Tol. ±0.2 mg, 1.0000 g, 5.000 g, 10.0000g, 50.0000 g, 100.0000 g, 120.0000 g Tol. ±0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta', '1.0 mg, 50.0 mg, 75.0 mg Tol. ± 0.2 mg, 1.0000 g, 5.000 g, 10.0000g, 50.0000 g, 100.0000 g, 120.0000 g Tol. ± 0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '1.0 mg, 50.0 mg, 75.0 mg, 1.0000 g, 5.0000 g, 10.0000 g, 50.0000 g, 100.0000 g, 150.0000 g, 200.0000 g, 220.0000 g Tol ± 0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', '0 mg, 1.0 mg, 2.0 mg, 20.0 mg y 50.0 mg Tol ± 0.2 mg\n1 g, 2 g, 5 g, 10 g, 30 g, 50 g, 80 g, 110 g Tol ± 0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', '0 mg, 1 mg, 2 mg, 20 mg y 50 mg Tol ± 0.2 mg\n1 g, 2 g, 5 g, 10 g, 30 g, 50 g, 80 g, 110 g Tol ±0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '1.0 mg, 50.0 mg Tol ± 0.2 mg, 1.000 g, 10.000 g, 20.000 g, 100.000 g, 200.000 g, 220.000 g Tol ± 0.003 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.0 mg, 1.0 mg, 50.0 mg, 75.0 mg Tol. ± 0.2 mg,\n1.0000 g, 5.0000 g, 10.0000 g, 50.0000 g, 100.0000 g, 120.0000 g. Tol. ± 0.0001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '2.0 g, 5.0 g, 10.0 g, 50.0 g, 100.0 g, 300.0 g, 500.0 g, 600.0 g Tol. ±0.2 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '2.00 g, 10.00 g, 50.00 g, 100.00 g, 150.00 g, 200.00 g, 220.00 g Tol±0.02 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.5 g, 1.0 g, 5.0 g, 10.0 g, 15 g, 50.0 g, 100.0 g, 200.0 g Tolerancia: ± 0.1 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '50.0 cfm Tol ±6.5 cfm, 100 cfm Tol ±8.0 cfm, 250 cfm Tol ±12.5 cfm, 500 cfm Tol ±20.0 cfm', NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '43.0°C, 45.0°C, 50.0°C Tol ±4.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '30.00°C, 50.00°C, 70.00°C Tol.±5.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta', '0.00 kg, 0.20 kg, 1.00 kg, 4.00 kg, 20.00 kg, 40.00 kg, 60.00 kg Tol ±0.1 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', '0.00 kg, 0.01 kg, 0.03 kg, 0.20 kg, 5.00 kg,10.00 kg, 20.00 kg, 30.00 kg Tol ±0.001 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '0.030 kg, 0.500 kg, 1.000 kg, 20.000 kg, 40.000 kg, 60.000 kg, 80.000 kg Tol ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.001 kg, 0.500 kg, 1.000 kg, 5.000 kg, 10.000 kg, 20.000 kg, 30.000 kg Tol ±0.001 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.001 kg, 0.500 kg, 1.000 kg, 5.000 kg, 10.000 kg, 20.000 kg, 30.000 kg Tol±0.001 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.0 g, 0.5 g, 1.0 g, 2.0 g, 5.0 g, 10.0 g, 20.0 g, 50.0 g, 100.0 g, 200.0 g, 500.0 g, 1000.0 g, 2000.0 g, 4000.0 g, 5000.0 g, 6000.0 g Tol ±0.3 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 70.000 kg Tol. ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 100.000 kg, 150.000 kg Tol. ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta', '0.00 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 100.000 kg, 150.000 kg Tol. ±0.0100 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 100.000 kg, 150.000 kg Tol ± 0.01 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '0.020 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 100.000 kg, 150.000 kg Tol. ± 0.005 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '0.020 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 100.000 kg, 150.000 kg Tol. ± 0.005 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta', '0.020 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg Tol. ± 0.005 kg\n100.000 kg, 150.000 kg Tol. ± 0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 80.000 kg, 100.000 kg, 150.000 kg Tol. ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.050 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 80.000 kg, 100.000 kg, 150.000 kg Tol. ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.000 kg, 0.030 kg, 0.05000 kg, 0.100 kg, 0.300 kg, 20.000 kg, 40.000 kg, 60.000 kg, 80.000 kg, 100.000 kg, 150.000 kg Tol. ±0.010 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '2.00 rpm, 5.00 rpm, 10.00 rpm, 20.00 rpm, 50.00 rpm, 100.00 rpm, 200.00 rpm Tol. (±0.02, ±0.05, ±0.10, ±0.20, ±0.50, ±1.00, ±2.00).', NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.0 mg, 1.0 mg, 50.0 mg, 200.0 mg Tol. ± 0.2 mg,\n1.0000 g, 10.0000 g, 20.0000 g, 100.0000 g, 220.0000 g. Tol. ± 0.0002 g', NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '10.0 mg, 50.0 mg, 200.0 mg, 1.0000 g, 10.0000 g, 50.0000 g, 100.0000 g, 220.0000 g Tol. ± 0.2 mg', NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', 'Tres puntos a 40°C , Tres puntos a 75%HR Tol ±2°C y ±5%HR', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', 'Temperatura 30°C Humedad 65%HR Tol. ±2°C y 5%HR.', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'En planta', '50.0°C ± 2.0°C, 110.0°C ±5°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta', '37.0°C Tol. ± 2.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '37°C, 38°C Tol. ± 2.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '50.0°C, 250.0°C, 600.0°C, 1,100°C Tol ±30°C.', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta', 'Temperatura 30.00°C y 40.00°C, Humedad 65%HR y 75%HR Tol. ±2°C y ±5%HR.', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', 'Temperatura 30°C Humedad 65%HR Tol. ±2°C y 5%HR.', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Temperatura 32.5°C Tol ±2.5°C.', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', 'Temperatura 56°C Tol. ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Temperatura 22.70°C Tol ±2.50°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Temperatura 42.00°C, 43.00°C, 44.00°C Tol ±1°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Temperatura 35.00°C , 36.00°C, 37.00°C Tol ±1°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', 'Temperatura 50.0°C , 150.0°C, 200.0°C Tol ±2°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'Externa', 'Flujo: 28.30 LPM ± 1.42 LPM\nTiempo de muestreo: 60.00 s ± 0.60 s\nEficiencia de conteo: 0.3 μm, 0.5 μm, 1.0 μm, 3.0 μm, 5.0 μm,10.0μm Tol ± 10%', NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'En planta', '15.0°C, 20.0°C, 25.0°C, 30.0°C Tol ± 1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CRI-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '0.0 s, 59.0 s, 3540.0 s, 356,400.0 s Tol ±1.0s', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '60.00 s, 3,600.00 s, 18,000.00 s, 36,000.00 s, 72,000.00 s, 86,400.00 s Tol (±0.006, ±0.36, ±1.80, ±3.60, ±7.20, ±8.64).', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '1.00 s,  10.00 min, 30.00 min, 1.00 hr, 5.00 hr, 10.00 hr, 12.00 hr, 15.00 hr, 20.00 hr ± 1.00 s', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '1.00 s, 1,800.00 s, 3,600.00 s, 18,000.00 s, 36,000.00 s Tol ±1.00 s', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '1.00 s, 1,800.00 s, 3,600.00 s, 18,000.00 s, 36,000.00 s Tol ±1.00 s', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '60.00 s, 300.00 s, 900.00 s, 1800 s, 3600 s (Tol. de acuerdo a los procedimientos del proveedor).', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '60.00 s, 600.00 s, 1800.00 s, 3600.00 s, 7200.00 s Tol ±3.00', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '60.00 s, 300.00 s, 900.00 s, 1800 s, 3600 s (Tol. de acuerdo a los procedimientos del proveedor).', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', '1.00 s, 1,800.00 s, 3,600.00 s, 18,000.00 s, 36,000.00 s Tolerancia de acuerdo a proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '20.0°C, 50.0°C, 100.0°C, 150.0°C, 200.0°C Tol ±2.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CTF-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '20.0°C, 50.0°C, 100.0°C, 150.0°C, 200.0°C Tol ±2.5°C', NULL
FROM instrumentos i
WHERE i.codigo = 'CTS-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', 'Temperatura -40.00°C, -20.00°C, 0.00, 20.00°C, 30.00°C, 40.00°C, 45.00°C Tol ± 1.00°C. Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C , 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C , 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 5.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura 10.0°C, 20.0°C, 35.0°C, 40°C, 45°C, 50°C Tol±1.0°C, Humedad 1.0%, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C Tol±1.0°C, 35.0%, 45.0%, 65.0%,75.0%, 80.0% Tol±2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-53'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-54'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Humedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-55'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-56'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-57'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-58'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-59'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-60'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-61'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-62'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-63'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-64'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-65'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-66'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-67'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-68'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-69'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-70'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-71'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-72'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-73'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-74'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-75'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-76'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-77'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-78'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-79'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-80'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Temperatura: 0.0°C, 10.0°C, 20.0°C, 35.0°C, 40°C, 50°C ± 1.0°C\nHumedad relativa:  35.0%, 45.0%, 65.0%,75.0%, 80.0% ± 2.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-81'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', 'Fuerza (Push) 20.00N, 50.00N, 100.00N, 250.00N,500.00N Tol. ±1.00N,\nFuerza (Pull) 20.00N, 50.00N, 100.00N, 250.00N,500.00N Tol. ±1.', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Fuerza (Push) 20.00N, 50.00N, 100.00N, 250.00N,500.00N Tol. ±1.00N,\nFuerza (Pull) 20.00N,  50.00N, 100.00N, 250.00N,500.00N Tol. ±1.', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '1.68, 4.01, 7.01, 10.01 pH Tol ± 0.02 pH', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '1.68, 4.01, 7.01, 10.01 pH Tol ± 0.02 pH', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-09-01', '%Y-%m-%d'), 'Externa', '0.00 pH, 1.68 pH, 4.01 pH, 7.01 pH, 10.01 pH, Tol ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '1.68 pH, 4.00 pH, 7.00 pH, 10.0 pH Tol ± 0.02', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '0.00 pH, 4.01 pH, 7.01 pH, 10.01 pH Tol ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '0.00 pH, 4.00 pH, 7.00 pH, 10.00 pH Tol ± 0.01 pH', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad Filtros HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad de un Filtro HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad de un Filtro HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad de un Filtro HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad de un Filtro HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta', 'Pruebas de integridad de un Filtro HEPA', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Punto fijo 47%HR Tol. ± 10%HR', NULL
FROM instrumentos i
WHERE i.codigo = 'HUM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '1.00, 7000.0, 10 000.00, 20 000.00, 30 000.00 luxes Tol ± 10.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'LUX-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'Externa', '20, 50, 100, 200, 300, 500, 750, 1000, 2000, 3000 lx Tol. ± 5%', NULL
FROM instrumentos i
WHERE i.codigo = 'LUX-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-100 a 100 mmH2O (Alta) / -100 a 100 mmH2O (baja) Tol. ± 5 mmH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 InH2O, -0.150 InH2O, -0.050 InH2O, 0.000 InH2O, 0.050 InH2O, 0.150 InH2O, 0.250 InH2O Tol ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.500 inH2O, 1.000 inH2O, 1.500 inH2O, 2.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.500 inH2O, 1.000 inH2O, 1.500 inH2O, 2.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.500 inH2O, 1.000 inH2O, 2.000 inH2O, 3.000 inH2O Tol ± 0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.100 inH2O, 0.200 inH2O, 0.400 inH2O, 0.600 inH2O, 0.800 inH2O, 1.000 inH2O Tol: ± 0.020', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.200 inH2O, 0.400 inH2O, 0.600 inH2O, 0.800 inH2O, 1.000 inH2O Tol: ± 0.020', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 inH2O, 0.500 inH2O, 1.000 inH2O, 1.500 inH2O, 2.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 MMh2O, 300 mmH2O, 600 mmH2O, 900 mmH2O, 1000 mmH2O Tol ±20 mmH20', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 MMh2O, 300 mmH2O, 600 mmH2O, 900 mmH2O, 1000 mmH2O Tol ±20 mmH20', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inH2O, 0.4 inH2O, 1.0 inH2O, 1.4 inH2O, 2.0 inH2O, 2.4 inH2O, 3.0 inH2O, 4.0 inH2O, 5.0 inH2O ± 0.2 inH2O.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inH2O, 0.4 inH2O, 1.0 inH2O, 1.4 inH2O, 2.0 inH2O, 2.4 inH2O, 3.0 inH2O, 4.0 inH2O, 5.0 inH2O ± 0.2 inH2O.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inH2O, 0.4 inH2O, 1.0 inH2O, 1.4 inH2O, 2.0 inH2O, 2.4 inH2O, 3.0 inH2O, 4.0 inH2O, 5.0 inH2O ± 0.2 inH2O.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inH2O, 0.4 inH2O, 1.0 inH2O, 1.4 inH2O, 2.0 inH2O, 2.4 inH2O, 3.0 inH2O, 4.0 inH2O, 5.0 inH2O ± 0.2 inH2O.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-1.000 inH2O, -0.500 inH2O, -0.250 inH2O, 0.000 inH2O,  0.250 inH2O, 0.500 inH2O, 1.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-1.000 inH2O, -0.500 inH2O, -0.250 inH2O, 0.000 inH2O,  0.250 inH2O, 0.500 inH2O, 1.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-1.00, -0.750, -0.500, -0.250, 0.000, 0.250, 0.500,0.750, 1.000 InH2O Tol ± 0.050.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-1.000 inH2O, -0.750 inH2O, -0.500 inH2O, -0.250 inH2O, 0.000 inH2O, 0.250 inH2O, 0.500 inH2O, 0.750 inH2O, 1.000 inH2O (Tolerancia: ± 0.050)', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-1.000 inH2O, -0.500 inH2O, -0.250 inH2O, 0.000 inH2O,  0.250 inH2O, 0.500 inH2O, 1.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '-1.000 inH2O, -0.500 inH2O, -0.250 inH2O, 0.000 inH2O,  0.250 inH2O, 0.500 inH2O, 1.000 inH2O Tol: ±0.050', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 inH2O, -0.150 inH2O, -0.050 inH2O, 0.000 inH2O, 0.050 inH2O, 0.150 inH2O, 0.250 inH2O  Tol: ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-0.250 InH2O, -0.150 InH2O, -0.050 InH2O, 0.000, 0.050 InH2O, 0.150 InH2O, 0.250 InH2O Tol ± 0.01', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '-1.00 InH2O, -0.50 InH2O, -0.25 InH2O, 0.0 InH2O, 0.25 InH2O, 0.50 InH2O, 1.00 InH2O ± 0.05', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Negativo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O\nPositivo\n0.00, 0.05, 0.10, 0.15, 0.20, 0.25 InH2O ±0.01 InH2O', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 5.00, 10.00, 15.00, 20.00, 25.00, 30.00 PSI Tol ± 0.50', NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 30.00 PSI, 60.00 PSI, 90.00 PSI, 120.00 PSI, 150.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 20.00, 40.00, 60.00, 80.00, 100.00, 120.00, 140.00, 160.00 PSI Tol ± 5.00 PSI.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'En planta', '0.00 PSI, 20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI, 120.00 PSI, 140.00 PSI, 160.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 kg/cm2, 2.00 kg/cm2, 4.00 kg/cm2, 6.00 kg/cm2, 8.00 kg/cm2, 10.00 kg/cm2 Tol ±.0.20kg/cm2', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 kg/cm2, 2.00 kg/cm2, 4.00 kg/cm2, 6.00 kg/cm2, 8.00 kg/cm2, 10.00 kg/cm2 Tol ±.0.20kg/cm2', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 50.00 PSI, 100.00 PSI, 150.00 PSI Tol ±5.00', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 50.00 PSI, 100.00 PSI, 150.00 PSI Tol ±10.00', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 50.00 PSI, 100.00 PSI, 150.00 PSI Tol ±10.00', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'En planta', '0.00 MPa, 0.20 MPa, 0.40 Mpa, 0.80 MPa, 1.00 MPa Tol ±0.02MPa', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 2 000PSI, 4 000PSI, 6 000PSI, 8 000PSI,10 000PSI, Tol ± 200 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0000 MPa, 0.2000 MPa, 0.4000MPa, 0.8000MPa, 1.0000MPa, Tol ± 0.0200 MPa', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 100.00 PSI, 200.00 PSI, 300.00 PSI, 400.0 PSI De acuerdo a proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI, 120.00 PSI, 140.00 PSI, 150.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 5.00, 10.00, 20.00, 30.00 PSI Tol ± 1.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 5.00, 10.00, 20.00, 30.00 PSI Tol ± 5.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 10 PSI, 20 PSI, 50 PSI, 100 PSI, 150 PSI, 200 PSI Tol ±1.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI, 120.00 PSI, 140.00 PSI, 160.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.000 InH2O, 1.000 InH2O, 2.000 InH2O, 3.000 InH2O, 4.000 InH2O, 5.000 InH2O ± 0.200 InH2O.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI ± 2.00 PSI.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 20 PSI 40 PSI, 60 PSI, 80 PSI, 100 PSI Tol ±2.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 20 PSI 40 PSI, 60 PSI, 80 PSI, 100 PSI Tol ±2.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 20 PSI 40 PSI, 60 PSI, 80 PSI, 100 PSI Tol ±2.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0 PSI, 30 PSI, 70 PSI, 90 PSI, 110 PSI, 140 PSI, 110 PSI, 90 PSI, 70 PSI, 30 PSI, 0 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 50.00 PSI, 100.00 PSI, 150.00 PSI Tol ±10.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 30.00, 50.00, 100.00, 150.00 PSI, ± 10.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00, 30.00, 50.00, 100.00, 150.00 PSI, ± 10.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 40.00 PSI, 80.00 PSI, 120.00 PSI, 160.00 PSI, 200.00 PSI Tolerancia de acuerdo a especificación de proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 PSI, 20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI Tol. ± 2.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 MPa, 0.10 MPa, 0.20 Mpa, 0.30 MPa, 0.40 MPa ± 0.02 MPa', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0.00, 50.00, 100.00, 150.00 PSI Tol ± 5.00 PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI, 120.00 PSI, 140.00 PSI, 160.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '20.00 PSI, 40.00 PSI, 60.00 PSI, 80.00 PSI, 100.00 PSI, 120.00 PSI, 140.00 PSI, 160.00 PSI Tol ±5.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'En planta', '0.00 PSI, 50.00 PSI, 100.00 PSI, 150.00 PSI, 200.00 PSI, 230.00 PSI Tol ±10.00PSI', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'Externa', '1.0000 Kg Tol. ±0.0100 Kg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '1.00mg, 2.00mg, 5.00mg, 10.00mg, 20.00mg, 20.00mg, 50.00mg, 100.00mg, 200.00mg, 200.00mg, 500.00mg, 1.00g, 2.00g, 2.00g, 5.00g, 10.00g, 20.00g, 20.00g, 50.00g, 100.00g Tolerancia establecida por el proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '1.00 mg (Tol. ±0.20), 5.0 mg (Tol. ±0.17), 20.00 mg (Tol. ±0.26), 200.00 mg (Tol. ±0.54), 20.00000 g (Tol. ±0.00400)', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '10.00 mg (Tol. ±0.21), 20.00 mg (Tol. ±0.26), 50.00 mg (Tol. ±0.35), 100.00 mg (Tol. ±0.43), 200.00 mg (Tol. ±0.54), 500.00 mg (Tol± 0.72) / 1.000000 g (Tol. ±0.000900), 2.000000 g (Tol. ±0.001120),5.000000 g (Tol. ±0.001500),10.000000 g (Tol. ±00.002000),20.000000g (Tol±0.004000),50.000000 g (Tol. ±0.0.010000),100.000000 g (Tol. ±0.02000),200.000000 g (Tol. ±0.040000).', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '1.000000 g Tol ±0.000001 g', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '50.000 g Tol. 0.01 g', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '100.0000 g Tol. 0.0002 g', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '10.00000 g Tol ±0.00200 g', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 inHG, 5.00 inHG, 10.00 inHG, 20.00 inHG, 25.00 inHg Tol ±1.00inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 inHG, 5.00 inHG, 10.00 inHG, 20.00 inHG, 25.00 inHg Tol ±1.00inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '-30.000 InHg, -25.00 InHg, -20.00 InHg, -15.00 InHg, -10.00 InHg, -5.00 InHg, 0.00 InHg Tol ± 1.00', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00InHg, 5.00inHg, 10.00 inHg, 15.00inHg, 20.00inHg, 30.00inHg Tol ±1.00InHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inHg, -5.0 inHg, -10.0 inHg, -15.0 inHg, -20.5  inHg, -25.5 inHg, -30.0 inHg Tol. ±1.0inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 inHg, 5.00inHg, 10.00 inHg, 15.00inHg, 20.00inHg, 22.00inHg Tol. ±3 inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 Mpa, -0.02 Mpa, -0.04 Mpa, -0.08 Mpa, -0.1 Mpa Tolerancia de acuerdo a proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 MPa, -0.02 MPa, -0.04 MPa, -0.06 MPa Tolerancia establecida por el proveedor.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00 inHg, -5.00 inHg, -10.00 inHg, -15.00 inHg, -20.00 inHg, -30.00 inHg Tol ±1.00inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '5.00 inHg, 10.00 inHg, 20.00inHg, 25.00inHg Tol ±0.50inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.00inHg, 5.00inHg, 10.00 inHg, 15.00inHg, 20.00inHg, 30.00inHg Tol ±1.00InHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '0.0 inHg, 5.00inHg, 10.0inHg, 15.0inHg, 20.0inHg, 24.0InHg Tol ±1inHg', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-01-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.2500 inWC, -0.2000 inWC, -0.1500 inWC,    - 0.1000 inWC, -0.0500 inWC, 0.0000 inWC, 0.0500 inWC, 0.1000 inWC, 0.1500 inWC, 0.2000 inWC, 0.2500 inWC Tol ± 1.5% (0.0075 InWC)\n- 62.0 Pa, - 50.0 Pa, - 37.0 Pa, - 25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa,                                        37.0 Pa, 50.0 Pa, 62.0 Pa\nTol ± 1.5% (1.8 Pa)', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-01-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '- 0.250 inWC, -0.200 inWC, -0.150 inWC,    - 0.100 inWC, -0.050 inWC, 0.000 inWC, 0.050 inWC, 0.100 inWC, 0.150 inWC, 0.200 inWC, 0.250 inWC Tol ± 0.001 InWC -62.0 Pa, -50.0 Pa, -37.0 Pa, -25.0 Pa, 12.0 Pa,  0.0 Pa, 12.0 Pa, 25.0 Pa, 37.0 Pa, 50.0 Pa, 62.0 Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '0.000000 in, 0.003937 in, 0.250000 in, 0.500000 in, 0.750000 in, 1.000000 in Tol. ±0.00005 in', NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '0.000000 in, 0.003937 in, 0.250000 in, 0.500000 in, 0.750000 in, 1.000000 in Tol. ±0.00005 in', NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '10.00 μL (Tol. ±0.16 µL), 50.00 μL (Tol. ±0.40 µL), 100.00 μL (Tol. ± 0.80 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '100.00 µl, 500.00 µl, 1000.00 µl (Tol. ± 8.00 µl)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '0.50000 ml, 2.50000 ml y 5.00000 ml Tol: ±0.01 ml', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '0.5000 mL (Tol±0.0120 mL), 2.5000 mL (Tol ±0.0300 mL), 5.0000 mL (Tol ±0.0300 mL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '100.00 µl (Tol±3.00 µl), 500.00 µl ( Tol±5.00 µl), 1000.00 µl (Tol. ± 6.00 µl)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '100.00 µl (Tol±3.00 µl), 500.00 µl ( Tol±5.00 µl), 1000.00 µl (Tol. ± 6.00 µl)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '100.00 µl (Tol. ± 0.6 µl), 500.0 µl (Tol. ± 3.00 µl), 1000 µl (Tol. ± 6.00 µl)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '100.00 µl (Tol. ± 0.6 µl), 500.0 µl (Tol. ± 3.00 µl), 1000 µl (Tol. ± 6.00 µl)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '100.00 μL (Tol ±1.0 µL), 500.00 μL (Tol ±4.00 µL), 1000.00 μL (Tol ±6.00 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-01', '%Y-%m-%d'), 'Externa', '100.00 μL (Tol.± 1.60 µL), 500.00 μL (Tol±3.50 µL), 1000.00 μL (Tol. 6.00 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '1,000 µL (Tol. ± 6 µL), 2,500 µL (Tol. ±15 µL), 5,000 µL (Tol. ± 25 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-09-01', '%Y-%m-%d'), 'Externa', '100.00 μL (Tol ±3 µL) , 500.00 μL(Tol ±10 µL), 1000.00 μL (Tol ± 15 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '1.0000 mL tol ± 0.0060 mL, 2.5000 mL Tol ± 0.0150 mL, 5.0000 mL Tol ± 0.0250 mL', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '50 μL (Tol.± 0.3 µL), 100 μL (Tol.± 0.6 µL), 200 μL (Tol.± 1.2 µL), 400 μL (Tol.± 2.4 µL), 600 μL (Tol.± 3.6 µL), 800 μL (Tol.± 4.8 µL), 1000 μL (Tol.± 6.0 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '0.5000 ml Tol ± 0.0400 ml, 5.0000 ml Tol ± 0.0400 ml', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '100.00 μL, 1 000.00  μL Tol ± 8.00 µL', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '10.0000 μL, 100.0000 μL tol ± 0.8000 µL', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', '10.00 μL (Tol ±0.16 µL), 50.00 μL (Tol.±0.40 µL) 100.00 μL (Tol ±0.80 µL).', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '100.00  μL (Tol ±8 µL), 500.00  μL (Tol.±8 µL), 1000.00  μL (Tol.±8 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '100.00 μL (Tol. ±1 µL), 500.00 μL (Tol ±4.00 µL), 1000.00 μL (Tol.±6.0 µL)', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', 'Flujo de aire 100.0 LPM, 100.0 LPM, 100.0 LPM, 100.0 LPM Tol.±2.5 LPM', NULL
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '0.0 μS/cm tol ±0.1, 84.0 μS/cm tol ±8.0, 1413 μS/cm tol ±14.0 µS/cm', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-04-01', '%Y-%m-%d'), 'Externa', 'Voltaje DC 200 mV (Tol. ±2.0), 2 V (Tol±0.018), 20 V (Tol±0.20),200 V (Tol.±2.40), 1000 V (Tol.±15.0),\nVoltaje AC @60 Hz 2 V (Tol.± 0.027), 20 V (Tol ±0.27), 20 0 V (Tol. ± 2.7), 750 V (Tol ±17),\nCorriente DC 2 mA (Tol.±0.031), 20 mA (Tol.±0.31), 200 mA (Tol.±3.1),\nCorriente AC@60Hz 200 mA (Tol± 3.8),\nResistencia 200 Ohm (Tol ±2.08), 2 KOhm (Tol±0.028), 20 KOhm (Tol.±0.28), 200 KOhm (Tol±2.8), 2 MOhm (Tol. ±0.028), 20 MOhm (Tol. 0.34), 200 MOhm (Tol.±5.2),\nCapacitancia 20 nF (Tol±1.57), 20 mF (Tol.±1.57),\nFrecuencia @250V 2 kHz (Tol ±0.024),\nTemperatura -20°C (Tol±3.0), 0.0 (Tol±4.0), 100.0°C (Tol.±7.0), 300.0°C (Tol±13.0), 500.0°C (Tol±19.0), 800.0°C (Tol±28.0), 1000.0°C (Tol±34.0).', NULL
FROM instrumentos i
WHERE i.codigo = 'MUT-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-04-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-03-01', '%Y-%m-%d'), 'Externa', '10.0°, 30.0°C, 45.0°, 60.0°, 90.0° Tol (±0.1°)', NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '5.00000 kg Tol (0.00050 kg)', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '10.000 kg (Tol±0.012 kg)', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '25.000 kg Tol±0.012 kg', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta', '0.0 MΩ/cm (Tol±0.2), 0.9 MΩ/cm (Tol±0.2), 10.0 MΩ/cm (Tol±0.2), 18 MΩ/cm (Tol±0.2), Temperatura ambiente ±0.2°C.', NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-200.00°C, -100.00°C, 0.00°C, 100.00°C, 300.00°C, 600.00°C, 900.00°C, 1200.00°C, 1370.00°C Tol±2.10,1.05,1.00,1.05,1.15,1.30,1.45,1.60,1.69 (10 puntos)', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-01-01', '%Y-%m-%d'), 'Externa', '-20.00°C, 0.00°C, 10.00°C, 30.00°C, 60.00°C,125.00°C Tol±0.20°C', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-01-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', '-20.00°C, 0.00°C, 10.00°C, 30.00°C, 60.00°C,125.00°C Tol±0.20°C', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '950 mbar, 15 mbar Tol ± 2.00mbar', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0 a 80°C Tol. ± 4°C', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0 a 80°C Tol ±2°C', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'En planta', '0 a 80°C Tol ±1°C', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'En planta', 'Tres puntos 5.0°C Tol ± 3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Punto de rocio -63.67°C (Tol.±3.0°C),-40.00°C, -19.54°C (Tol.±1.00°C), 4.71°C (Tol.±1.00°C), Temperatura 27.42°C (Tol.±0.21°C), 18.05°C(Tol±0.21°C), 16.92 (Tol±0.21°C), Humedad 35.00%, 55.00%, 75.00% (Tolerancia establecida por el proveedor).', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta', '1.0s, 2.0s, 9.0s ± 0.2s', NULL
FROM instrumentos i
WHERE i.codigo = 'SMI-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '10.000 rpm (Tol±0.102 rpm), 100.00 rpm (Tol±1.02 rpm), 1000.0 rpm (Tol±10.2 rpm), 5000.0 rpm (Tol±50.2 rpm), 10000 rpm (Tol±102 rpm), 15000 rpm (Tol±152 rpm), 20000 rpm (Tol±202 rpm)', NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-03-01', '%Y-%m-%d'), 'Externa', '10.0 rpm, 20.0 rpm, 50.0 rpm, 100.0 rpm, 200.0 rpm, 300.0 rpm, 500.00 rpm, 900.0 rpm, 1800.0 rpm, 3600.0 rpm, 7200.0 rpm Tol±1.0 rpm', NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', '0.000 g, 1.000 g, 5.000 g, 10.000 g, 20.000 g, 30.000 g, 40.000 g Tol± 0.002 g, Temperatura: 100.0°C, 150.0°C, 190.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-200.0°C, -100.0°C, 0.0°C, 100.0°C, 200.0°C(Tol±0.4°C), 400.0°C (Tol±0.5°C), 800.0°C (Tol±0.7°C) 1200.0°C(Tol.±1.0°C), 1372.0°C (Tol±1.0°C)', NULL
FROM instrumentos i
WHERE i.codigo = 'TED-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C Tol. ± 0.1°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', '0°C, 10°C, 20°C, 40.0°C, 50.0°C Tol. ±1°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '50.0.0°C, 100.0°C, 120.0°C, 150.0°C, 200.0°C, 245.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 25.0°C, 37.0°C, 50.0°C, 100.0°C, 125.0°C, 150.0°C, 200.0°C, 245.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'Externa', '-20.0°C, 0.0°C, 50.0°C, 100.0°C, 150.0°C, 200.0°C, 250.0°C Tol±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'En planta + Mantenimiento', '18°C Tol±2.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Tres puntos 21.5°C Tol±3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Punto fijo 22.5°C Tol±3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Punto fijo 21.5°C Tol±3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-06-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Punto fijo 21.5°C Tol±3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-06-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'En planta + Mantenimiento', 'Punto fijo: 21.5°C Tol. ± 3.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'En planta + Mantenimiento'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%, 30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C ± 1.0°C, 15%, 30%, 60% Tol ± 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C ± 1.0°C, 15%, 30%, 60% Tol ± 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C ± 1.0°C, 15%, 30%, 60% Tol ± 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura: -8.0°C, 0.0°C, 18.0°C, 25.0°C, 40.0°C, 55.0°C Tol ±1.0°C\nHumedad relativa: 15.0%, 30.0%, 65.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura: -8.0°C, 0.0°C, 18.0°C, 25.0°C, 40.0°C, 55.0°C Tol ±1.0°C\nHumedad relativa: 15.0%, 30.0%, 65.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura: -8.0°C, 0.0°C, 18.0°C, 25.0°C, 40.0°C, 55.0°C Tol ±1.0°C\nHumedad relativa: 15.0%, 30.0%, 65.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Temperatura: -8.0°C, 0.0°C, 18.0°C, 25.0°C, 40.0°C, 55.0°C Tol ±1.0°C\nHumedad relativa: 15.0%, 30.0%, 65.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'Externa', '20.0°C, 40.0°C, 45.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C, Tol ± 1.0°C\n15%, 30%, 60%, 80% Tol 6.0%.', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C, Tol ± 1.0°C\n15%, 30%, 60%, 80% Tol 6.0%.', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '0.0°C, 20.0°C, 40.0°C, 55°C Tol ± 1.0°C\n15%, 30%, 60%, 80% HR ±Tol 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ± 1.0°C\n15%, 30%, 60%, 80% Tol ± 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', 'Temperatura In, 0.0°C, 20.0°C, 40.0°C, 50.0°C Tol ± 1.0°C.\nTemperatura Out  -25.0°C, -15.0°C, -10.0°C, -0.0°C Tol ± 1.0°C\nHumedad In 15%, 30%, 60%, 80% Tol ± 6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', 'Temperatura  0.0ºC, 20.0ºC, 40.0ºC, 55ºC Tol ± 1.0\nHumedad 15%, 30%, 60%, 80% Tol ±6%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-02-01', '%Y-%m-%d'), 'Externa', '20.0°C, 30.0°C, 40.0°C Tol ± 1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-02-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-10-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-10-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-11-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-11-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 0.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '-8.0°C, 20.0°C, 40.0°C, 55.0°C Tol ±1.0°C, 15.0%,30.0%, 60.0%, 80.0% Tol ±6.0%', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2027-01-01', '%Y-%m-%d'), 'Externa', 'Temperatura In, 0.0°C, 20.0°C, 40.0°C, 50.0°C Tol ± 1.0°C.\nTemperatura Out (sonda) -25.0°C, -15.0°C, -10.0°C, 0.0°C Tol ± 1.0°C.\nHumedad In 15%, 30%, 60%, 80% Tol ± 6%.', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2027-01-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-07-01', '%Y-%m-%d'), 'Externa', '0.00°C (Tol ±1.5°C), 25.00°C (Tol ±1.5°C), 50.00°C (Tol ±1.5°C), 75.00°C (Tol ±1.5°C), 100.00°C (Tol ±1.5°C), 250.0°C (Tol ±3.5°C), 500.00°C(Tol ±3.5°C)', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-07-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', '0.0°C, 25.0°C, 50.0°C, 75.0°C, 100.0°C Tol ± 1.5°C\n250°C, 500°C Tol ± 3.5°C.', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta', '25 rpm Tol. ± 1 rpm', NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'En planta', 'Velocidad @ 3.0 min 25 rpm', NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'En planta', '2 min, 3 min, 10min, 15 min, 20 min Toleancia ±0.0160 min', NULL
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'En planta'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '18.0°C (Tol±2.2°C), 50.0°C (Tol±2.2°C), 100.0°C(Tol±2.2°C), 150°C (Tol±2.2°C), 200.0°C (Tol±2.2°C), 300.00°C (Tol±2.5°C), 400.00°C (Tol.± 3.0°C), 500.00°C (Tol.±3.75°C)', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', '0.0°C, 1.0°C, 25.0°C, 50.0°C, 75.0°C, 90.0°C Tol ±0.3°C', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', 'Longitud OD 0.000 in, 0.1000 in, 0.2000 in, 0.3000 in, 0.4000 in, 0.5000 in, 0.6000 in, 0.7000 in, 0.8000 in, 0.9000 in, 1.0000 in, 2.0000 in, 3.0000 in, 4.0000 in, 5.0000 in, 6.0000 in (Tol ±0.0010)\nLongitud ID 0.0000 in, 2.0000 in (Tol±0.0010),\nProfundidad 1.0000 in (Tol±0.0010).', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-12-01', '%Y-%m-%d'), 'Externa', 'Dimensión interna 0.1000 in (Tol±0.00058), 0.20000 in (Tol±0.00058), 0.50000 in (Tol±0.00058), 1.00000 in (Tol±0.00058), 2.00000 in (Tol±0.00058), 4.00000 in (Tol±0.00058), 6.0000 in (Tol±0.00065), 8.0000 in (Tol±0.00065)\nProfundidad 1.40000 Tol ± 0.00058\nDimensión externa 2.00000 Tol ± 0.00058', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2025-12-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-09-01', '%Y-%m-%d'), 'Externa', 'Dimensión externa:\n0.00 mm,5.00 mm,80.00 mm,150.00 mm Tol ±0.02 mm\nProfundidad:\n0.00 mm,5.00 mm, 50.00,150.00 mm Tol ±0.02 mm\nDimensión interna:\n0.00 mm, 5.00 mm, 80.00 mm,150.00 mm Tol ±0.02 mm', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-09-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', 'Longitud OD 0.000 in, 0.1000 in, 0.2000 in, 0.3000 in, 0.4000 in, 0.5000 in, 0.6000 in, 0.7000 in, 0.8000 in, 0.9000 in, 1.0000 in, 2.0000 in, 3.0000 in, 4.0000 in, 5.0000 in, 6.0000 in Tol ± 0.00110 in\nLongitud ID 0.0000 in, 2.0000 in Tol ± 0.00110 in\nProfundidad 1.0000 in Tol ± 0.00110 in', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-05-01', '%Y-%m-%d'), 'Externa', 'Longitud exterior 3 mm, 4 mm, 5 mm, 6 mm, 7 mm, 8 mm, 10 mm, 12 mm, 14 mm, 16 mm, 18 mm, 20 mm, 22 mm, 120 mm. Longitud interna 3 mm, 4 mm, 5 mm, 6 mm, 7 mm, 8 mm, 10 mm, 12 mm. Tolerancia : ±0.02 mm', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-05-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-03-01', '%Y-%m-%d'), 'Externa', 'Longitud exterior 0.10000 in, 0.20000 in, 0.30000 in, 0.40000 in, 0.50000 in, 0.60000 in, 0.70000 in, 0.80000 in, 0.90000 in, 1.00000 in, 2.00000 in, 3.00000 in, 4.00000 in, 5.00000 in, 6.00000 in Tol±0.0001 in\nLongitud interna 2.0000 in Tol ± 0.0001 in\nProfundidad 2.0000 Tol ± 0.0001 in', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-03-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

INSERT INTO historial_especificaciones (instrumento_id, empresa_id, fecha_vigencia, version, descripcion, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2026-08-01', '%Y-%m-%d'), 'Externa', 'Puntos a calibrar: Los establecidos por el proveedor Tol ± 5%', NULL
FROM instrumentos i
WHERE i.codigo = 'VIS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_especificaciones he
      WHERE he.instrumento_id = i.id
        AND he.empresa_id = @empresa_id
        AND he.fecha_vigencia = STR_TO_DATE('2026-08-01', '%Y-%m-%d')
        AND he.version = 'Externa'
  );

COMMIT;
