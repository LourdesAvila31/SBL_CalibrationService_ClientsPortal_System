-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-12-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-12-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-08-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-08-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-27', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-19', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-02-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-02-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-29', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-29', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-08-30', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-08-30', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-20', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-20', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRI-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-07-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-07-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-16', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-16', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-09', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-26', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-06-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-06-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-08-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-08-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-20', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-20', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-27', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-29', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-29', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-27', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-29', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-29', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-28', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-29', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-29', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-28', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-28', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-28', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'HUM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'LUX-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-07-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-07-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-08-16', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-08-16', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-09-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-12-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-12-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-12-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-12-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-27', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-27', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-07-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-07-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-06-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-18', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-31', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-07-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-07-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-20', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-20', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-11-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-11-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-30', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-30', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-20', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-20', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-19', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-09-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-09-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-02-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-18', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-23', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-30', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-30', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-29', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-29', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-04-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-04-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-06-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-06-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-06-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-06-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-04-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-04-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-04-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-04-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-19', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-04-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'MUT-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-04-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-21', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-06-30', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-06-30', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-18', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-18', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-01-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-01-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-02-16', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-02-16', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-05-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-05-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-31', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-31', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TED-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-09', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-07', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-09', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-15', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-15', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-15', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-20', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-20', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-02-16', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-02-16', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-02-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-02-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-07-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-07-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-07-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-07-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-06-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-06-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-02-14', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-02-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-27', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-27', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-10-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-10-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-06-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-06-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-11-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-11-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-25', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-13', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-13', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-07-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-07-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-02', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-06-10', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-06-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-05-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-05-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-10', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-10', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-23', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-05-19', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-05-19', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-02-12', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-02-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-02-23', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-02-23', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-02-24', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-02-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-25', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-25', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-01-17', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-01-17', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-24', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-24', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-22', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-01-26', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-01-26', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-12-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-12-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-09-03', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-09-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-22', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-22', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-04', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-05-11', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-05-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-05-14', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-05-14', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-05-05', '%Y-%m-%d'), 'Calibración programada', 'Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-05-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración programada'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-04-12', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-04-12', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-04-11', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-04-11', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-04-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-04-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-01', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-01', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-01-09', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-01-09', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-01-07', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-01-07', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-01-05', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-01-05', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2018-03-08', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2018-03-08', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2019-03-21', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2019-03-21', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2020-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2020-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2021-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2021-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2022-03-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2022-03-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-05-06', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-05-06', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-05-03', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-05-03', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2023-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2023-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2024-03-02', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2024-03-02', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

INSERT INTO historial_calibraciones (instrumento_id, empresa_id, fecha_evento, tipo_evento, descripcion, certificado_codigo, usuario_id)
SELECT i.id, @empresa_id, STR_TO_DATE('2025-03-04', '%Y-%m-%d'), 'Calibración', 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original.csv', NULL, NULL
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_calibraciones hc
      WHERE hc.instrumento_id = i.id
        AND hc.empresa_id = @empresa_id
        AND hc.fecha_evento = STR_TO_DATE('2025-03-04', '%Y-%m-%d')
        AND hc.tipo_evento = 'Calibración'
  );

COMMIT;
