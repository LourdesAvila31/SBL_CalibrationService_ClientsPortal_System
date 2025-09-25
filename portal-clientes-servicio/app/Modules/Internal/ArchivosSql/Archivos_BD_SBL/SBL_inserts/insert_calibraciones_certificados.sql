-- Calibraciones programadas generadas desde CERT_instrumentos_original_v2.csv
-- Empresa destino: 1
START TRANSACTION;
SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'AMP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-08', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'AMP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-07', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'AMP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-12-07', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-12-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'AMP-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-08-19', 'P1', '2023-08-19', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-08-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'AMP-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-08-02', 'P1', '2024-08-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-12', 'P1', '2019-09-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-11', 'P1', '2022-07-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-06', 'P1', '2024-01-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-12', 'P1', '2019-09-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-11', 'P1', '2022-07-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-06', 'P1', '2024-01-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ANE-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-21', 'P1', '2019-08-21', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-19', 'P1', '2020-08-19', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-04', 'P1', '2022-10-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2024-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-CP-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-07', 'P1', '2020-07-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-05', 'P1', '2021-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-07', 'P1', '2020-07-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-05', 'P1', '2021-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-03', 'P1', '2022-07-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-05-07', 'P1', '2018-11-07', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-22', 'P1', '2019-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-17', 'P1', '2020-07-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAA-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-12', 'P1', '2018-09-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-21', 'P1', '2019-09-21', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAE-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-14', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-22', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-31', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-17', 'P1', '2021-12-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-07', 'P1', '2022-11-07', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-05', 'P1', '2024-05-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAL-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-07', 'P1', '2025-11-07', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-08', 'P1', '2019-03-08', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-21', 'P1', '2020-03-21', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-01', 'P1', '2023-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2019-04-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2020-04-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2021-03-31', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-01', 'P1', '2023-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-06-07', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-06', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-04', 'P1', '2022-10-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2024-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-27', 'P1', '2023-07-27', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2024-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-08', 'P1', '2024-11-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AL-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AM-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-06-07', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AM-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-06', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AM-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-AM-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-07', 'P1', '2020-07-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-05', 'P1', '2021-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-03', 'P1', '2022-07-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-21', 'P1', '2024-08-21', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-21', 'P1', '2024-08-21', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-11', 'P1', '2024-10-11', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-06', 'P1', '2025-08-06', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-FR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-02-08', 'P1', '2018-08-08', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-21', 'P1', '2019-08-21', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-19', 'P1', '2020-08-19', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-18', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-22', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-06-07', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-06', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-02', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-22', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-19', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-13', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-05-09', 'P1', '2019-11-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-06', 'P1', '2020-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2021-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-09', 'P1', '2022-12-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-09', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-02', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-12', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-06-06', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BAS-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-01', 'P1', '2023-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-04-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-01', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BOP-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-02-11', 'P1', '2021-08-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-02-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'BSM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-08', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-06-06', 'P1', '2019-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-08', 'P1', '2020-12-08', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2021-11-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-19', 'P1', '2023-01-19', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-08-10', 'P1', '2024-08-10', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-05-09', 'P1', '2020-05-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-11', 'P1', '2021-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2022-05-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-02', 'P1', '2023-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-10', 'P1', '2024-05-10', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-23', 'P1', '2020-04-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-04-03', 'P1', '2021-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-01', 'P1', '2023-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-GC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-25', 'P1', '2019-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-25', 'P1', '2020-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-17', 'P1', '2021-01-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2022-01-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2023-01-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-04-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-01', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-25', 'P1', '2022-10-25', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2024-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-19', 'P1', '2020-08-19', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-06-03', 'P1', '2021-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-03', 'P1', '2022-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-12', 'P1', '2018-10-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-22', 'P1', '2019-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2020-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2021-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-02', 'P1', '2022-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-18', 'P1', '2018-07-18', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-22', 'P1', '2019-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-17', 'P1', '2020-07-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-06-07', 'P1', '2018-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-06-06', 'P1', '2019-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-08', 'P1', '2020-12-08', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2021-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-02', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2021-11-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2021-11-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CAL-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2023-01-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-15', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-29', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-29' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-08-30', 'P1', '2023-08-30', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-08-30' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-20', 'P1', '2024-09-20', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-20' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'COP-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-02', 'P1', '2026-04-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRI-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-08-03', 'P1', '2024-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-21', 'P1', '2023-01-21', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-07-01', 'P1', '2024-01-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-07-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2024-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-09', 'P1', '2019-07-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-16', 'P1', '2020-07-16', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-16' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-08', 'P1', '2021-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-05', 'P1', '2022-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-03', 'P1', '2024-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-09', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-08', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-13', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-05', 'P1', '2024-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2024-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-03', 'P1', '2025-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2024-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-05', 'P1', '2025-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-FR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-07', 'P1', '2023-09-07', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-05', 'P1', '2025-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-07-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-02-02', 'P1', '2019-02-02', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-23', 'P1', '2020-02-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-21', 'P1', '2021-02-21', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-21', 'P1', '2022-01-21', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-24', 'P1', '2023-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-10', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-10', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-06', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-07', 'P1', '2023-09-07', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-05', 'P1', '2025-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-08-02', 'P1', '2025-02-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2026-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'CRO-VA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-08-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-26', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-07', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-10', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-24', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-24', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-12', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-05', 'P1', '2023-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-06-11', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-12', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-12', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-06-11', 'P1', '2019-06-11', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-06-12', 'P1', '2020-06-12', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-11', 'P1', '2021-06-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-06-10', 'P1', '2022-06-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-06-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-LO-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-11', 'P1', '2023-06-11', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-10', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-32' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-32' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-33' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-33' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DAT-VA-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-06', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DUR-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DUR-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-21', 'P1', '2023-10-21', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DUR-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-09', 'P1', '2021-01-09', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DUR-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-12', 'P1', '2022-01-12', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'DUR-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-06', 'P1', '2023-01-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-08-04', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-08-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-14', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-14', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-12-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-12-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-09-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-03', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-20', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-20' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-05', 'P1', '2024-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-17', 'P1', '2025-05-17', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-17', 'P1', '2025-05-17', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-17', 'P1', '2025-05-17', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-17', 'P1', '2025-05-17', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-06', 'P1', '2024-08-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-19', 'P1', '2025-08-19', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'ELE-MI-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2026-02-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-04', 'P1', '2019-04-04', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-28', 'P1', '2020-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-27', 'P1', '2021-03-27', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-29', 'P1', '2022-03-29', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-29' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-28', 'P1', '2023-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-28', 'P1', '2024-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-04', 'P1', '2025-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-28', 'P1', '2020-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-27', 'P1', '2021-03-27', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-29', 'P1', '2022-03-29', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-29' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-28', 'P1', '2023-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-28', 'P1', '2024-03-28', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-04', 'P1', '2025-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-10', 'P1', '2026-03-10', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-29', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-29' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-28', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-28', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-28' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-23', 'P1', '2024-03-23', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-23', 'P1', '2024-03-23', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2026-06-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'FIL-MA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2026-06-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'HUM-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2026-06-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'LUX-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-22', 'P1', '2025-01-22', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-46' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-09', 'P1', '2023-04-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-46' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-46' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-46' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-47' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-47' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-48' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-48' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-49' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-49' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-50' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-50' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-51' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-51' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-52' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-FR-52' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-05', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-05', 'P1', '2018-10-05', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-09', 'P1', '2023-04-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-09', 'P1', '2023-04-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-07', 'P1', '2024-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-07', 'P1', '2024-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-07', 'P1', '2024-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-07', 'P1', '2024-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-MA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-22', 'P1', '2019-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-17', 'P1', '2020-07-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-22', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-15', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-06-02', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-22', 'P1', '2019-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-17', 'P1', '2020-07-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-15', 'P1', '2021-07-15', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2022-07-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-04-03', 'P1', '2020-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-02', 'P1', '2019-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-04-03', 'P1', '2020-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-13', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-13', 'P1', '2018-09-13', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-31' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-36' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-01', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-03-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-39' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-40' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-08', 'P1', '2020-12-08', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2021-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2022-11-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2023-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2024-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-41' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2025-11-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-42' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-42' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-42' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-42' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-42' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2020-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-43' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-02', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-44' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-02', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-45' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-45' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-45' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-PR-45' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-05', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-06-17', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-01', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-06-02', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-23', 'P1', '2025-07-23', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-23', 'P1', '2025-07-23', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAD-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-23', 'P1', '2025-07-23', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-22', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-21', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-04-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-04-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-11', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-04-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAE-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAN-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAN-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-09-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-04', 'P1', '2023-07-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-07-03', 'P1', '2024-07-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-07-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-08-16', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-08-16' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-09-03', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-09-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-12-07', 'P1', '2019-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-12-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-12-07', 'P1', '2020-06-07', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-12-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-12-05', 'P1', '2024-06-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-09', 'P1', '2023-04-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-09', 'P1', '2023-04-09', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-11', 'P1', '2024-01-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-11', 'P1', '2024-01-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-27', 'P1', '2024-07-27', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-19' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-07-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-01-27', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-01-08', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-14', 'P1', '2025-02-14', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2026-02-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2025-06-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-FR-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2026-06-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-04', 'P1', '2023-07-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-07-03', 'P1', '2024-07-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-07-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-09-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2022-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2023-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2024-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-15', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-09', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-06', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-05-15', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-09-02', 'P1', '2025-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-06-06', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-06-08', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-25' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-26' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-27' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-28' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-03', 'P1', '2023-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-29' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-06-06', 'P1', '2022-12-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-30' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2025-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-32' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-05', 'P1', '2024-04-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-32' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-32' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-04', 'P1', '2024-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-04', 'P1', '2024-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MA-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-18', 'P1', '2023-04-18', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-03', 'P1', '2024-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-01', 'P1', '2025-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2026-04-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-04', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2022-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2023-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2024-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2022-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2023-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2024-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-06', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-31', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-05-09', 'P1', '2020-05-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-06', 'P1', '2021-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-04', 'P1', '2023-07-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAP-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-07-03', 'P1', '2024-07-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-07-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-17', 'P1', '2019-01-17', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-24', 'P1', '2020-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-22', 'P1', '2021-01-22', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-24', 'P1', '2023-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-05', 'P1', '2024-04-05', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-02', 'P1', '2025-04-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-02', 'P1', '2026-04-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-CP-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-20', 'P1', '2023-07-20', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-20' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-14', 'P1', '2020-01-14', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-08', 'P1', '2021-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-07', 'P1', '2022-01-07', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-04', 'P1', '2023-01-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-11-24', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-11-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-30', 'P1', '2019-01-30', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-30' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-14', 'P1', '2020-01-14', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-20', 'P1', '2021-03-20', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-20' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-08', 'P1', '2023-03-08', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-05', 'P1', '2026-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-19', 'P1', '2019-01-19', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-04', 'P1', '2021-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-07', 'P1', '2023-03-07', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-03', 'P1', '2024-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-04', 'P1', '2025-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-19', 'P1', '2019-01-19', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-04', 'P1', '2021-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-07', 'P1', '2023-03-07', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-03', 'P1', '2024-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-19', 'P1', '2019-01-19', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-04', 'P1', '2021-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-07', 'P1', '2023-03-07', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-03', 'P1', '2024-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-19', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-25', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-04', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAS-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-05', 'P1', '2023-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-07-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-04', 'P1', '2023-11-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-08', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-05', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-08', 'P1', '2020-03-08', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-05', 'P1', '2021-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-03-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2023-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-DA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-12-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-11-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-FR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-11', 'P1', '2024-01-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-FR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-09-04', 'P1', '2024-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-09-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-09-02', 'P1', '2025-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-12-01', 'P1', '2023-12-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2022-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2023-05-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2024-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MAV-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIM-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-02', 'P1', '2024-08-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIM-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIM-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2022-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIM-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2023-05-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-18', 'P1', '2018-07-18', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-14', 'P1', '2019-07-14', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-07', 'P1', '2020-08-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-12', 'P1', '2021-07-12', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-05', 'P1', '2022-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-07', 'P1', '2023-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-18', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-12', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-06', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-18', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-12', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-07', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-06-07', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-06-04', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-06-03', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-25', 'P1', '2018-09-25', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-15', 'P1', '2019-09-15', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-11', 'P1', '2020-09-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-12', 'P1', '2021-11-12', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2022-11-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2023-11-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2024-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2025-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-25', 'P1', '2018-09-25', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-15', 'P1', '2019-09-15', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-11', 'P1', '2020-09-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-08', 'P1', '2021-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-04', 'P1', '2022-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2023-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2024-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-12', 'P1', '2025-09-12', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-02-03', 'P1', '2023-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2024-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-02-03', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-02-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-04', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-03-02', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-DA-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-18', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-24', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-23', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-30', 'P1', '2018-07-30', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-30' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-24', 'P1', '2019-07-24', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-22', 'P1', '2020-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-22', 'P1', '2021-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-13', 'P1', '2022-07-13', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-07', 'P1', '2023-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-14', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-11', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-05-14', 'P1', '2019-11-14', NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-07', 'P1', '2020-11-07', NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-11', 'P1', '2021-11-11', NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-04', 'P1', '2022-11-04', NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', '2023-11-11', NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-06', 'P1', '2024-11-06', NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-05-14', 'P1', '2019-11-14', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-07', 'P1', '2020-11-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2021-11-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2022-11-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2023-11-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-02', 'P1', '2024-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-24', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-29', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-29' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-25', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-08', 'P1', '2021-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-04', 'P1', '2022-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2023-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2024-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2025-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2021-11-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2022-11-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2023-11-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2024-11-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2025-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-04-08', 'P1', '2022-10-08', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-04-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-06-07', 'P1', '2023-12-07', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-06-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-06-04', 'P1', '2024-12-04', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-06-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2024-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-06', 'P1', '2025-08-06', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2024-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2024-08-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2023-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2024-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2025-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-04', 'P1', '2023-10-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-04-02', 'P1', '2024-10-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-04-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MIP-MI-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-04-01', 'P1', '2025-10-01', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-04-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUE-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-12', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-14', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-19', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-DA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-07-06', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2025-02-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUL-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2026-02-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'MUT-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-04-04', 'P1', '2024-10-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-04-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'NIV-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-12', 'P1', '2019-09-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'NIV-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-08', 'P1', '2022-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'NIV-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2024-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'NIV-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PES-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-21', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PES-VA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-24', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PES-VA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-21', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PES-VA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-24', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-26', 'P1', '2019-03-26', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-05', 'P1', '2020-04-05', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-06-30', 'P1', '2021-06-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-06-30' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2022-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-18', 'P1', '2023-05-18', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-18' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-04', 'P1', '2024-05-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-08', 'P1', '2025-05-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'PUR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'REG-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-05-12', 'P1', '2019-05-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'REG-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-24', 'P1', '2021-08-24', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'REG-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-01-21', 'P1', '2024-07-21', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-01-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'REG-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-02-16', 'P1', '2023-08-16', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-02-16' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'REG-VA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-05', 'P1', '2026-08-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-02', 'P1', '2024-08-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2025-08-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-13', 'P1', '2024-11-13', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-13', 'P1', '2024-11-13', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-13', 'P1', '2024-11-13', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-FR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'SEN-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-11', 'P1', '2023-01-11', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-08', 'P1', '2019-09-08', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-23', 'P1', '2020-09-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-04', 'P1', '2021-09-04', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2022-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2024-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TAC-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-05-15', 'P1', '2018-11-15', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-05-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-23', 'P1', '2019-10-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-31', 'P1', '2020-09-30', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-31' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-05', 'P1', '2021-09-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2022-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2023-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2024-09-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEB-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2025-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TED-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-05', 'P1', '2023-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-DA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-09', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-07', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-09', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-25', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-15', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-25', 'P1', '2019-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-15', 'P1', '2020-03-15', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-15' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-11', 'P1', '2021-03-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-02', 'P1', '2023-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-25', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-20', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-20' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-25', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-06', 'P1', '2019-03-06', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-02', 'P1', '2023-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-06', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-25', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-02', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-06', 'P1', '2019-03-06', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-02', 'P1', '2023-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-06', 'P1', '2019-03-06', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-25', 'P1', '2020-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-02', 'P1', '2023-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-04', 'P1', '2024-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-02-16', 'P1', '2023-02-16', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-02-16' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-02-04', 'P1', '2024-02-04', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2025-02-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-04', 'P1', '2026-02-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-23', 'P1', '2020-02-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-11', 'P1', '2021-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2022-05-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-06', 'P1', '2023-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2024-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2025-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEM-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-06', 'P1', '2026-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-02', 'P1', '2023-09-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-04', 'P1', '2023-07-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-07-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-06', 'P1', '2022-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-03', 'P1', '2023-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-02', 'P1', '2024-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2026-06-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-07' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-02', 'P1', '2026-06-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TEP-MA-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-10', 'P1', '2026-02-10', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-AL-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-AL-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2025-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-AL-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2025-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-AL-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2025-11-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-CP-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-05', 'P1', '2023-07-05', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-CP-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-CP-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-FR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2025-02-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-FR-37' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-05', 'P1', '2026-02-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-FR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2025-02-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-FR-38' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-05', 'P1', '2026-02-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-25', 'P1', '2019-03-25', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-02-12', 'P1', '2019-02-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-23', 'P1', '2020-02-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-24', 'P1', '2021-02-24', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-26', 'P1', '2023-01-26', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-10-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-08' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-11' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-02-12', 'P1', '2019-02-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-23', 'P1', '2020-02-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-24', 'P1', '2021-02-24', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-26', 'P1', '2023-01-26', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2022-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2023-05-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-02-03', 'P1', '2025-02-03', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-05', 'P1', '2026-02-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2022-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-04', 'P1', '2023-05-04', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-11', 'P1', '2024-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2025-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2026-05-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-02-03', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-02-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2025-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-20' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2026-05-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2025-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2026-05-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-07-24', 'P1', '2025-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-07-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-22' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-07-24', 'P1', '2025-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-07-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-23' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-06-03', 'P1', '2025-12-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-06-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LC-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-02-14', 'P1', '2025-08-14', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-02-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-27', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-27' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-23', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-25', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-25', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-LO-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-26', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-17', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-17', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-17', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-01-24', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-01-22', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-25', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-02-12', 'P1', '2019-02-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-02-23', 'P1', '2020-02-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-24', 'P1', '2021-02-24', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-06' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-26', 'P1', '2023-01-26', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-12' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-07-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-08', 'P1', '2023-11-08', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-01-08', 'P1', '2025-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-11-08', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-05-11', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2024-05-06', 'P1', NULL, NULL, 'Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2025-05-05', 'P1', NULL, NULL, 'Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-01-17', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-21' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-10-03', 'P1', '2023-10-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-10-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-24' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-33' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-34' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-PR-35' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-SC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-06-11', 'P1', '2019-06-11', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-06-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-SC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-11', 'P1', '2022-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-SC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-11-08', 'P1', '2023-11-08', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-11-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-17', 'P1', '2019-01-17', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-24', 'P1', '2020-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-22', 'P1', '2021-01-22', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-26', 'P1', '2023-01-26', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-05' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-09-03', 'P1', '2023-09-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-09' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-13', 'P1', '2023-01-13', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-25', 'P1', '2022-01-25', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-10' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-13', 'P1', '2023-01-13', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-08', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-13' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-13', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-17', 'P1', '2021-02-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-08', 'P1', '2022-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-14' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2023-01-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-08', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-15' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-17', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-08', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-16' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-17', 'P1', '2021-02-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-08', 'P1', '2022-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-17' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-13', 'P1', '2023-01-13', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-13' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-02-17', 'P1', '2021-02-17', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-08', 'P1', '2022-01-08', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TER-VA-18' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-10', 'P1', '2023-01-10', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-07-22', 'P1', '2023-07-22', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-07-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-03-08', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-03-25', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-03-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2023-03-02', 'P1', NULL, NULL, 'Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-04-05', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-04-03', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-03', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TIR-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-06-10', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-06-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-05-09', 'P1', '2020-05-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-05-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-05-11', 'P1', '2021-05-11', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-05-10', 'P1', '2022-05-10', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-05-02', 'P1', '2023-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-10', 'P1', '2024-05-10', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-10' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-08', 'P1', '2025-05-08', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-05', 'P1', '2026-05-05', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-23', 'P1', '2020-04-23', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-04-03', 'P1', '2021-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-01', 'P1', '2023-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-01', 'P1', '2025-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TMB-GC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-03', 'P1', '2026-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TPR-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-05-19', 'P1', '2024-05-19', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-05-19' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TPR-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-02', 'P1', '2025-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TPR-FR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-02', 'P1', '2026-05-02', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2018-02-12', 'P1', NULL, NULL, 'Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-02-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2019-02-23', 'P1', NULL, NULL, 'Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-02-23' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-02-24', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-02-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-01-22', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-01-25', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-25' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-01-17', 'P1', '2019-01-17', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-01-17' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-24', 'P1', '2020-01-24', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-24' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-22', 'P1', '2021-01-22', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-22', 'P1', '2022-01-22', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-01-26', 'P1', '2023-01-26', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-01-26' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-12-02', 'P1', '2023-12-02', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-12-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-LC-04' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-09-03', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-09-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-22', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-05', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-02' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-03-22', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-22' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRK-PR-03' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-03-04', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRT-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2020-05-11', 'P1', NULL, NULL, 'Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-05-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRT-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2021-05-14', 'P1', NULL, NULL, 'Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-05-14' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'TRT-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración programada', '2022-05-05', 'P1', NULL, NULL, 'Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-05-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-04-12', 'P1', '2019-04-12', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-04-12' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-04-11', 'P1', '2020-04-11', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-04-11' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-04-03', 'P1', '2021-04-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-04-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2023-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-01', 'P1', '2024-03-01', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-01' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-CP-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-01-09', 'P1', '2020-01-09', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-01-09' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-01-07', 'P1', '2021-01-07', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-01-07' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-DF-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-01-05', 'P1', '2022-01-05', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-01-05' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2018-03-08', 'P1', '2019-03-08', NULL, 'Requerimiento: Calibración. Periodo 1 2018. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2018-03-08' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2019-03-21', 'P1', '2020-03-21', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2019-03-21' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2020-03-03', 'P1', '2021-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2020. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2020-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2021-03-03', 'P1', '2022-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2021. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2021-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2022-03-03', 'P1', '2023-03-03', NULL, 'Requerimiento: Calibración. Periodo 1 2022. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2022-03-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-02', 'P1', '2024-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-LC-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-05-06', 'P1', '2025-05-06', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-05-06' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-03', 'P1', '2026-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2019. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-MA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-05-03', 'P1', '2026-05-03', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-05-03' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2023-03-02', 'P1', '2024-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2023. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2023-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2024-03-02', 'P1', '2025-03-02', NULL, 'Requerimiento: Calibración. Periodo 1 2024. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2024-03-02' AND periodo = 'P1');

SET @instrumento_id = (SELECT id FROM instrumentos WHERE codigo = 'VER-VA-01' AND empresa_id = 1 LIMIT 1);
INSERT INTO calibraciones (instrumento_id, empresa_id, tipo, fecha_calibracion, periodo, fecha_proxima, resultado, observaciones)
SELECT @instrumento_id, 1, 'Calibración', '2025-03-04', 'P1', '2026-03-04', NULL, 'Requerimiento: Calibración. Periodo 1 2025. Fuente: CERT_instrumentos_original_v2.csv' FROM DUAL WHERE @instrumento_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM calibraciones WHERE instrumento_id = @instrumento_id AND empresa_id = 1 AND fecha_calibracion = '2025-03-04' AND periodo = 'P1');

COMMIT;
