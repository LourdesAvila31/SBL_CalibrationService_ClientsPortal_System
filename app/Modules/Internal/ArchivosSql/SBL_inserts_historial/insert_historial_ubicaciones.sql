-- Archivo generado automáticamente por convert_historiales_csv.py

START TRANSACTION;
SET @now_historial := NOW();

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT
    datos.instrumento_id,
    datos.valor_texto,
    datos.empresa_id,
    COALESCE(datos.fecha_evento, @now_historial) AS fecha,
    COALESCE(datos.fecha_evento, @now_historial) AS `timestamp`
FROM (
    SELECT
        base.empresa_id,
        (SELECT id FROM instrumentos WHERE codigo = base.instrumento_codigo AND empresa_id = base.empresa_id LIMIT 1) AS instrumento_id,
        base.valor_texto,
        base.fecha_evento
    FROM (
        SELECT 1 AS empresa_id, 'VIS-DA-02' AS instrumento_codigo, 'Móvil' AS valor_texto, '2024-04-19 12:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, 'Mantenimiento' AS valor_texto, '2024-04-29 13:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, 'Almacén de validación' AS valor_texto, '2024-04-29 14:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, 'Area de balanzas' AS valor_texto, '2024-04-29 14:47:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, 'Recepcion de muestras' AS valor_texto, '2024-04-29 14:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, 'Recepción de muestras' AS valor_texto, '2024-04-29 14:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAE-MI-01' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2024-05-08 16:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-FR-23' AS instrumento_codigo, 'Tableteado 2, en TAB-FR-02' AS valor_texto, '2024-05-31 14:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAD-MA-09' AS instrumento_codigo, 'Piso técnico, UMA-MA-04' AS valor_texto, '2024-06-20 15:36:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-DA-02' AS instrumento_codigo, 'Móvil' AS valor_texto, '2024-06-25 13:25:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-DF-01' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2024-07-03 12:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-20' AS instrumento_codigo, 'Mezclado y tamizado 1' AS valor_texto, '2024-07-09 13:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-54' AS instrumento_codigo, 'Disolución, REF-LC-05' AS valor_texto, '2024-07-11 10:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-22' AS instrumento_codigo, 'Recepción de muestras' AS valor_texto, '2024-07-19 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-23' AS instrumento_codigo, 'Muestras de grupo I, II y III.' AS valor_texto, '2024-07-19 11:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-24' AS instrumento_codigo, 'Congelador, REF-LC-05. Disolución' AS valor_texto, '2024-07-19 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-61' AS instrumento_codigo, 'Almacén de validación' AS valor_texto, '2024-07-19 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-CP-05' AS instrumento_codigo, 'Control en proceso' AS valor_texto, '2024-07-26 12:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-03' AS instrumento_codigo, 'Análisis Fisicoquímicos' AS valor_texto, '2024-08-06 11:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-02' AS instrumento_codigo, 'Analisis fisicoquimicos' AS valor_texto, '2024-08-08 16:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-03' AS instrumento_codigo, 'Analisis fisicoquimicos' AS valor_texto, '2024-08-08 16:23:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-02' AS instrumento_codigo, 'Análisis Fisicoquímicos' AS valor_texto, '2024-08-12 08:44:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-06' AS instrumento_codigo, 'Preparación Microbiológica' AS valor_texto, '2024-08-29 09:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-07' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2024-08-29 09:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'HUM-MA-01' AS instrumento_codigo, 'Piso técnico, en UMA-MA-04' AS valor_texto, '2024-08-29 09:33:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'LUX-VA-01' AS instrumento_codigo, 'Almacén de Validación' AS valor_texto, '2024-09-26 08:39:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAA-AL-01' AS instrumento_codigo, 'Pesado' AS valor_texto, '2024-10-04 11:24:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-65' AS instrumento_codigo, 'Preparación microbiológica' AS valor_texto, '2024-10-09 07:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-70' AS instrumento_codigo, 'Microbiología' AS valor_texto, '2024-10-09 07:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2024-10-09 07:50:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-03' AS instrumento_codigo, 'Cuarentena de PT' AS valor_texto, '2024-11-08 15:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, 'Almacén de PT controlado' AS valor_texto, '2024-11-08 15:52:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, 'Almacén de Materia Prima Aprobada' AS valor_texto, '2024-11-08 15:55:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-06' AS instrumento_codigo, 'Almacén de Validación' AS valor_texto, '2024-11-08 15:57:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-39' AS instrumento_codigo, 'Tableteado 2' AS valor_texto, '2024-11-19 08:12:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-40' AS instrumento_codigo, 'Encapsulado y Abrillantado #5' AS valor_texto, '2024-11-19 08:12:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-41' AS instrumento_codigo, 'Encapsulado y Abrillantado 4' AS valor_texto, '2024-11-19 08:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-FR-42' AS instrumento_codigo, 'Recubrimiento' AS valor_texto, '2024-11-19 08:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'Almacén de validación' AS valor_texto, '2024-11-22 14:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'Almacén de validación' AS valor_texto, '2024-11-22 14:48:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BSM-LC-02' AS instrumento_codigo, 'Area de balanzas' AS valor_texto, '2024-11-27 09:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAP-MA-38' AS instrumento_codigo, 'Tanque hidroneumático TH 02' AS valor_texto, '2024-12-03 14:46:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-DF-01' AS instrumento_codigo, 'Desintegrador, DES-DF-01' AS valor_texto, '2024-12-05 14:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-54' AS instrumento_codigo, 'Friabilizador, FRI-DF-01' AS valor_texto, '2024-12-05 15:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TMB-GC-01' AS instrumento_codigo, 'Friabilizador, FRI-CP-01' AS valor_texto, '2024-12-11 12:53:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-GC-02' AS instrumento_codigo, 'Desintegrador, DES-CP-01' AS valor_texto, '2024-12-11 13:01:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-MA-01' AS instrumento_codigo, 'Almacén de validación' AS valor_texto, '2024-12-14 15:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-21' AS instrumento_codigo, 'Oficina de Fabricación' AS valor_texto, '2025-01-20 15:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-22' AS instrumento_codigo, 'Oficina de Fabricación' AS valor_texto, '2025-01-20 15:45:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-11' AS instrumento_codigo, 'Análisis Fisicoquímicos' AS valor_texto, '2025-01-22 09:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-DA-12' AS instrumento_codigo, 'Análisis Fisicoquímicos' AS valor_texto, '2025-01-22 09:21:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-03' AS instrumento_codigo, 'Preparación microbiológica, POT-LC-02' AS valor_texto, '2025-01-30 14:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, 'Preparación Microbiologica POT-LC-02' AS valor_texto, '2025-01-30 14:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-08' AS instrumento_codigo, 'Acondicionamiento Secundario, UPA-MA-01' AS valor_texto, '2025-02-11 10:36:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-21' AS instrumento_codigo, 'Tableteado 1' AS valor_texto, '2025-02-21 11:56:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-22' AS instrumento_codigo, 'Tableteado 2' AS valor_texto, '2025-02-21 11:57:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-20' AS instrumento_codigo, 'Acondicionamiento Primario Blíster' AS valor_texto, '2025-03-31 08:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-DF-01' AS instrumento_codigo, 'Estabilidades, en CAC-DA-02' AS valor_texto, '2025-03-31 08:33:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CAL-GC-02' AS instrumento_codigo, 'Análisis Instrumental II, en ESE-DA-01' AS valor_texto, '2025-03-31 08:34:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, 'Acondicionamiento Primario Blíster, en BLI-FR-02.' AS valor_texto, '2025-03-31 08:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, 'Acondicionamiento Primario Blíster, en BLI-FR-02' AS valor_texto, '2025-03-31 08:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'FIL-MA-12' AS instrumento_codigo, 'Análisis Físicoquímicos' AS valor_texto, '2025-03-31 08:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-MI-02' AS instrumento_codigo, 'Preparación Microbiológica, en POT-LC-02' AS valor_texto, '2025-03-31 08:49:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MAD-MA-11' AS instrumento_codigo, 'Control de Calidad' AS valor_texto, '2025-03-31 09:25:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, 'Preparación Microbiológica' AS valor_texto, '2025-03-31 10:21:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, 'Control en Proceso' AS valor_texto, '2025-03-31 10:21:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'PAA-LC-02' AS instrumento_codigo, 'Móvil' AS valor_texto, '2025-03-31 10:22:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TMB-GC-01' AS instrumento_codigo, 'Análisis Instrumental IV' AS valor_texto, '2025-03-31 10:27:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-04' AS instrumento_codigo, 'Móvil, en FRI-DF-01' AS valor_texto, '2025-03-31 10:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-05' AS instrumento_codigo, 'Control en Proceso, en FRI-CP-01' AS valor_texto, '2025-03-31 10:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-06' AS instrumento_codigo, 'Mezclado y Tamizado 1, en PAN-FR-03.' AS valor_texto, '2025-03-31 10:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-62' AS instrumento_codigo, 'Almacén de Material de Empaque y Envase Aprobado' AS valor_texto, '2025-03-31 10:29:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-64' AS instrumento_codigo, 'Cuarentena de Producto Terminado' AS valor_texto, '2025-03-31 10:30:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-65' AS instrumento_codigo, 'Almacén de Producto Terminado Controlado' AS valor_texto, '2025-03-31 10:30:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-69' AS instrumento_codigo, 'Microbiología, en REF-LC-04' AS valor_texto, '2025-03-31 10:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-70' AS instrumento_codigo, 'Muestreo de Materia Prima' AS valor_texto, '2025-03-31 10:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-73' AS instrumento_codigo, 'Órdenes Surtidas' AS valor_texto, '2025-03-31 10:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-75' AS instrumento_codigo, 'Análisis Instrumental II, en DES-LC-01' AS valor_texto, '2025-03-31 10:31:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-76' AS instrumento_codigo, 'Análisis Instrumental I, EN DES-LC-02' AS valor_texto, '2025-03-31 10:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-76' AS instrumento_codigo, 'Análisis Instrumental I, en DES-LC-02' AS valor_texto, '2025-03-31 10:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-77' AS instrumento_codigo, 'Preparación Microbiológica' AS valor_texto, '2025-03-31 10:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-78' AS instrumento_codigo, 'Disolución, en REF-LC-05' AS valor_texto, '2025-03-31 10:32:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-79' AS instrumento_codigo, 'Encapsulado y Abrillantando 1' AS valor_texto, '2025-03-31 10:33:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-VA-29' AS instrumento_codigo, 'Control en Proceso, en DES-CP-01' AS valor_texto, '2025-03-31 11:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-06' AS instrumento_codigo, 'Analisis Fisicoquímicos' AS valor_texto, '2025-03-31 11:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEP-MA-08' AS instrumento_codigo, 'Acondicionamiento Secundario, en UPA-MA-01' AS valor_texto, '2025-03-31 11:04:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'BAS-FR-23' AS instrumento_codigo, 'Mezclado y Tamizado 1' AS valor_texto, '2025-04-01 15:02:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-66' AS instrumento_codigo, 'Producto Terminado Controlado' AS valor_texto, '2025-04-02 14:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-77' AS instrumento_codigo, 'Área de balanzas, en DES-LC-02' AS valor_texto, '2025-04-02 14:38:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-77' AS instrumento_codigo, 'Área de Balanzas, en DES-LC-02' AS valor_texto, '2025-04-02 15:00:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MIP-LC-13' AS instrumento_codigo, 'Análisis Fisicoquímicos' AS valor_texto, '2025-04-02 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VIS-LC-02' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 14:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-26' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'CRO-DF-01' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:13:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-28' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-30' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-31' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-32' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:14:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-33' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-35' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:15:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-AL-06' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-04-03 15:16:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-65' AS instrumento_codigo, 'Almacén de Materia Prima Aprobada y Cuarentena' AS valor_texto, '2025-04-15 10:43:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-04' AS instrumento_codigo, 'Instrumental II, en TIT-DA-01' AS valor_texto, '2025-05-21 09:54:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-05' AS instrumento_codigo, 'Móvil (Análisis Instrumental I, Análisis Fisicoquímico)' AS valor_texto, '2025-07-03 11:17:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'ELE-DA-06' AS instrumento_codigo, 'Almacén de Validación' AS valor_texto, '2025-07-03 14:05:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-01' AS instrumento_codigo, 'Mezclado y Tamizado 1' AS valor_texto, '2025-07-07 13:03:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-02' AS instrumento_codigo, 'Pesado' AS valor_texto, '2025-07-07 13:06:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-03' AS instrumento_codigo, 'Utensilios y Aditamentos de Fabricación' AS valor_texto, '2025-07-07 13:07:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-04' AS instrumento_codigo, 'Lavado' AS valor_texto, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-05' AS instrumento_codigo, 'Órdenes Surtidas' AS valor_texto, '2025-07-07 13:08:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-06' AS instrumento_codigo, 'Esclusa 2' AS valor_texto, '2025-07-07 13:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-07' AS instrumento_codigo, 'Encapsulado y Abrillantado #5' AS valor_texto, '2025-07-07 13:10:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'MDI-MA-08' AS instrumento_codigo, 'Detección de Metales' AS valor_texto, '2025-07-07 13:11:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-VA-29' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-07-07 13:18:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LO-02' AS instrumento_codigo, 'Móvil, Transporte' AS valor_texto, '2025-07-07 13:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-52' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-07-21 11:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-26' AS instrumento_codigo, 'Área de Balanzas, Estándares secundarios para grupos I, II y III' AS valor_texto, '2025-07-29 11:42:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'SMI-FR-01' AS instrumento_codigo, 'Acondicionamiento Primario Llenado de Frascos' AS valor_texto, '2025-08-12 15:09:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TEM-DF-02' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2025-08-13 14:28:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'VER-DF-02' AS instrumento_codigo, 'Desarrollo Farmacéutico' AS valor_texto, '2025-08-14 10:59:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-34' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-08-26 10:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'DAT-VA-36' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-08-26 10:37:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-19' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-20' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
        UNION ALL
        SELECT 1 AS empresa_id, 'TER-LC-21' AS instrumento_codigo, 'Móvil, Almacén de Validación' AS valor_texto, '2025-08-26 10:40:00' AS fecha_evento
    ) AS base
) AS datos
WHERE datos.instrumento_id IS NOT NULL
  AND datos.valor_texto IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = datos.instrumento_id
        AND hu.ubicacion = datos.valor_texto
        AND hu.empresa_id = datos.empresa_id
        AND hu.fecha <=> COALESCE(datos.fecha_evento, @now_historial)
  );

COMMIT;
