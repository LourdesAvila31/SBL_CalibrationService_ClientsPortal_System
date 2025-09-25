-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-04-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 14-Abr-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-04-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 08-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 08-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-04-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-04-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGP-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 26-Ago-23. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2027-01-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2027-01-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2027-01-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2027-01-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-24', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 24-Ago-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-24', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-06-19', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-06-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 10-Abr-23. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 07-Nov-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'Se asigna a Ambiental para el pesado de residuos, no requiere calibración dado que el uso no es "crítico".. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-23', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 23-Ago-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitada a 60.00 Kg kvazquez 02-Oct-24', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitada a 80.000 Kg kvazquez 02-Sep-25', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 05-Mar-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-AM-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-03-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 22-Mar-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-03-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 11-Abr-24. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se asigna a Ambiental para el pesado de residuos, no requiere calibración dado que el uso no es "crítico".. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 20-Feb-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-01-19', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 19-Ene-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-01-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 20-Feb-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-02-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'BOP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Ago-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Dic-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRI-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'DNC24-023', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'DNC24-023', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja por daño. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), CURDATE()), 'Stock', 'DNC24-023', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), CURDATE()), 'Stock', 'DNC24-023', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 12-Abr-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-04-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 05-Abr-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-04-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 13-Oct-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 13-Oct-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-23', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-03-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Mar-20. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-03-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja por extravío. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Dic-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CTF-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'CTS-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-05-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-05-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-03-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-03-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-04-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-05-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado en calibración vrodriguez 21-May-25\nBaja: 22-Jul-25 vrodriguez 22-Jul-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 26-Ago-25 vrodriguez 11-Sep-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-53'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-54'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-55'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-56'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-57'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-58'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-59'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-60'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-61'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-62'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-63'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-64'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-65'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-66'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-67'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-68'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-69'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-70'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-71'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-72'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-73'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-74'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-75'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-76'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-77'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-78'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-79'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-80'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-81'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DEN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 23-Dic-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración se realiza con la calificación de TIT-DA-01. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 16-Dic-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 16-Dic-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado en calibración. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Certificado disponible en la carpeta física de calificación del equipo', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Certificado disponible en la carpeta física de calificación del equipo', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Certificado disponible en la carpeta física de calificación del equipo', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Certificado disponible en la carpeta física de calificación del equipo', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Se realizará en el MVC del sistema de agua. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-03-26', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 26-Mar-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-03-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 27-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja por cambio de sistema. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja por cambio de sistema. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 27-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 27-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'FUS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'HUM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'LAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'LUV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'LUV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'LUX-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'LUX-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-09-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 04-Sep-20. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-09-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 03-Sep-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAB-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 28-Sep-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 28-Sep-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Abr-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado en calibración. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Abr-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Abr-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-03-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 11-Mar-24. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-03-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Abr-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Abr-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 03-Sep-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-09-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 03-Mar-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 03-Mar-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Abr-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 04-Mar-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAE-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-11-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-11-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 08-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Por requerimiento de usuario no se procede a calibración.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 05-Mar-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Por requerimiento de usuario no se procede a calibración.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 22-Jul-25 vrodriguez 22-Jul-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 12-Nov-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 20-Ago-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-05-31', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-05-31', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'Por requerimiento de usuario no se procede a calibración.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado en calibración. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja por extravío. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-07-25', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 25-Jul-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-07-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-07-25', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 25-Jul-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-07-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-07-24', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 24-Jul-20. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-07-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Sep-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 12-Nov-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 12-Nov-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 12-Nov-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Sep-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Sep-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-10-19', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 19-Oct-20. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-10-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 11-Nov-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Dic-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 11-Nov-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 06-Dic-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No hay calibración por instrumento fuera de uso. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-10', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 10-May-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Dic-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-08-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-08-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 08-Jun-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 11-Nov-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-01-18', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 18-Ene-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-01-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 04-Mar-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 04-Mar-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 06-Dic-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-12-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-03-09', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 09-Mar-20. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-03-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 14-Oct-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado en calibración. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-04-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se manda a calibrar solo la masa de 1 kg para uso del departamento de almacén, el resto de las masas queda al resguardo de Control en proceso.', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-04-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Falta una masa de 2.00mg del set', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Fuera de uso se mandará a calibrar solo si el usuario lo solicita. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 07-Sep-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 11-Oct-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), CURDATE()), 'Stock', 'Por mantener garantia se cancela calibración. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-21', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja:15-Ago-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja:16-Nov-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja:17-Dic-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Para repuesto, se mandará a calibrar una vez que se requiera.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Instrumento como repuesto.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se coloca equivalencia de unidades en Pa', NULL
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 02-Jul-25\nvrodriguez 03-Jul-25. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-09-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-27', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 05-Ago-24. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-05-06', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 06-May-25', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-05-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-05-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-05-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-05-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-09-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'LIMITADO: Solo usar hasta 2 MOhm', NULL
FROM instrumentos i
WHERE i.codigo = 'MUT-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2027-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2027-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-04-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-10-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-10-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PES-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-12-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PIR-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-05-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-05-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-01-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-01-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SEN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'SMI-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 09-Abr-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'SON-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 17-Ago-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'SON-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-08-18', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 18-Ago-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'SON-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-08-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'SON-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado: Usar solo el modo sin contacto.', NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2027-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2027-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TED-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-21', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-02-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-03-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-03-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-06', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-09-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-09-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se enviará a calibrar cuando se requiera. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-09-30', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 30-Sep-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-09-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-CP-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-FR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado para temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'CC-23-271. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-10', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'El instrumento se mandará a calibrar cuando se requiera su uso. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se calibrará cuando sea necesario. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LC-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-08', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-08', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-01-09', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 09-Ene-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-01-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 07-Sep-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 15-Abr-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado para temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 15-Abr-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-04-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2021-10-18', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 18-Oct-21. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2021-10-18', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-05-14', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 14-May-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-05-14', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-28', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-28', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Usar solo para medir temperatura. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-06', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja 06-Dic-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 07-Sep-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-10-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 07-Sep-18. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Ago-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Ago-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Ago-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 01-Ago-19. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2019-08-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se encuentra en validación, se calibrará cuando se requiera.', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado: Solo usar en temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-11-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado: Solo usar en temperatura.', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado para temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-11-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'Se encuentra en validación, se calibrará cuando se requiera.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Fallo de bateria.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-01-20', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Rechazado, se da de baja el 20-Ene-22.. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-01-20', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), CURDATE()), 'Stock', 'Solo usar para medir temperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Limitado para termperatura', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TER-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-01-10', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-01-10', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-06-15', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TIR-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-06-15', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-12-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-08-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 17-Ene-23. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 17-Ene-23. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja el 09-Feb-23. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRK-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-02-09', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE()), 'Inactivo', 'Baja: 16-Dic-22. No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TRT-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Inactivo'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), CURDATE()), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'TTH-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2026-03-01', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), CURDATE()), 'Stock', 'Calibración', NULL
FROM instrumentos i
WHERE i.codigo = 'VIS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), CURDATE())
  );

INSERT INTO historial_estado_instrumento (instrumento_id, empresa_id, fecha_evento, estado, motivo, usuario_id)
SELECT i.id, @empresa_id, CURDATE(), 'Stock', 'No se ha añadido su primer certificado', NULL
FROM instrumentos i
WHERE i.codigo = 'VOR-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_estado_instrumento hei
      WHERE hei.instrumento_id = i.id
        AND hei.empresa_id = @empresa_id
        AND hei.estado = 'Stock'
        AND hei.fecha_evento = CURDATE()
  );

COMMIT;
