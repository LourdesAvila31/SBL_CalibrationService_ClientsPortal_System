-- Archivo generado automáticamente por generate_historial_inserts.py
-- Empresa destino: 1
-- Antes de ejecutar en phpMyAdmin fija la empresa objetivo, por ejemplo:
-- SET @empresa_id = 1;

SET @empresa_id := IFNULL(@empresa_id, 1);

START TRANSACTION;

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-08-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AGM-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGP-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AGV-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Taller de Mantenimiento', @empresa_id, COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'AMP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'ANE-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pesado', @empresa_id, COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pesado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Muestreo de Materia Prima', @empresa_id, COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Muestreo de Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAA-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-06-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAL-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Residuos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Residuos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recepción Materia Prima', @empresa_id, COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recepción Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pesado', @empresa_id, COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pesado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pesado', @empresa_id, COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-02-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pesado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Surtido de Material de Acondicionamiento', @empresa_id, COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Surtido de Material de Acondicionamiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recepción Materia Prima', @empresa_id, COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-AL-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recepción Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster', @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4', @empresa_id, COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado #5', @empresa_id, COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado #5'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos', @empresa_id, COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 1', @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 2', @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 2'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Mezclado y Tamizado 1', @empresa_id, COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Mezclado y Tamizado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 1', @empresa_id, COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén Temporal de Residuos Peligrosos (Sólidos)', @empresa_id, COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén Temporal de Residuos Peligrosos (Sólidos)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento', @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Detección de Metales', @empresa_id, COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BAS-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Detección de Metales'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BOP-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-01-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'BSM-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Estabilidades, en CAC-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Estabilidades, en CAC-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Estabilidades, en CAC-DA-02', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Estabilidades, en CAC-DA-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental II, en ESE-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental II, en ESE-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, en DES-DF-01', @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, en DES-DF-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso, en DES-CP-01', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'CAL-GC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso, en DES-CP-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental IV, en MUF-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental IV, en MUF-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Estabilidades, en CAC-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Estabilidades, en CAC-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Estabilidades, en CAC-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Estabilidades, en CAC-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en INC-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en INC-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en INC-LC-04', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en INC-LC-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en INC-LC-05', @empresa_id, COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en INC-LC-05'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en INC-LC-06', @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en INC-LC-06'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en INC-LC-07', @empresa_id, COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en INC-LC-07'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental IV, en ESE-LC-03', @empresa_id, COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CAL-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental IV, en ESE-LC-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'COP-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II y Análisis Fisicoquímicos)', @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRI-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II y Análisis Fisicoquímicos)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-10-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Mezclado y Tamizado 1', @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Mezclado y Tamizado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-08-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-12-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental II', @empresa_id, COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental II'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CRO-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster, en BLI-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CTF-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster, en BLI-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster, en BLI-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'CTS-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster, en BLI-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-05-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-LO-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-53'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-54'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-55'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-56'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-57'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-58'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-59'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-60'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-61'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-62'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-63'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-64'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-65'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-66'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-67'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-68'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-69'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-70'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-71'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-72'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-73'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-74'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-75'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-76'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-77'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-78'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-79'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-80'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DAT-VA-81'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DEN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'DUR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-04-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Instrumental II, en TIT-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-05-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Instrumental II, en TIT-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Fisicoquímico)', @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Fisicoquímico)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en TIT-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en TIT-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Físicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Físicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en POT-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'ELE-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en POT-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-01 y UMA-MA-02', @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-01 y UMA-MA-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'FIL-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'FUS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'HUM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'LAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil', @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUV-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUV-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil', @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUX-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-09-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'LUX-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-46'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Esclusa de Muestreo de Materia Prima', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-47'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Esclusa de Muestreo de Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Surtido de Materia Prima para Fabricación', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-48'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Surtido de Materia Prima para Fabricación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pre esclusa 2', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-49'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pre esclusa 2'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pasillo A', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-50'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pasillo A'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-51'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Surtido de Material de Acondicionamiento', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-FR-52'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Surtido de Material de Acondicionamiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Esclusa a Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Esclusa a Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control de Calidad', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control de Calidad'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Prefiltro corrugado (UMA-MA-01)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Prefiltro corrugado (UMA-MA-01)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Filtro de bolsa (UMA-MA-01)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Filtro de bolsa (UMA-MA-01)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Filtro HEPA (UMA-MA-01)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Filtro HEPA (UMA-MA-01)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Prefiltro corrugado (UMA-MA-02)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Prefiltro corrugado (UMA-MA-02)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Filtro de bolsa (UMA-MA-02)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Filtro de bolsa (UMA-MA-02)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Filtro HEPA (UMA-MA-02)', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Filtro HEPA (UMA-MA-02)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer Piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer Piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer Piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer Piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer Piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer Piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tercer Piso, en UMA-MA-04', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-MA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tercer Piso, en UMA-MA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Secundario', @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Secundario'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Materia Prima Aprobada y Cuarentena', @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Materia Prima Aprobada y Cuarentena'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preesclusa de Material de Acondicionamiento', @empresa_id, COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preesclusa de Material de Acondicionamiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preesclusa de Muestreo de Materia Prima', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preesclusa de Muestreo de Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 1', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 1', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 2', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 2'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Muestreo de Materia Prima', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Muestreo de Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-31'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preesclusa 1', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preesclusa 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Esclusa 1', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Esclusa 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarentena de Graneles', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarentena de Graneles'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Material de Empaque', @empresa_id, COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-43'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Material de Empaque'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-44'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-09-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-PR-45'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAD-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAN-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso, en BOV-CP-01', @empresa_id, COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso, en BOV-CP-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-02', @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-03', @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster, en BLI-FR-02.', @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster, en BLI-FR-02.'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Abrillantadora, ABR-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Abrillantadora, ABR-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Abrillantadora, ABR-FR-03', @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Abrillantadora, ABR-FR-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 1, en TAB-PR-01', @empresa_id, COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 1, en TAB-PR-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Secundario Blíster, en ENA-FR-01', @empresa_id, COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Secundario Blíster, en ENA-FR-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 2, en TAB-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-05-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-FR-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 2, en TAB-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-07-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en EST-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en EST-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-31', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología, en BOV-LC-03', @empresa_id, COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología, en BOV-LC-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en MUE-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en MUE-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-04', @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Extractor de polvos', @empresa_id, COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Extractor de polvos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en Tanque Hidroneumático #1', @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en Tanque Hidroneumático #1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarto Técnico, en Ósmosis Inversa', @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarto Técnico, en Ósmosis Inversa'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarto Técnico, antes de filtros suavizadores.', @empresa_id, COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-11-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarto Técnico, antes de filtros suavizadores.'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4, en AC-03', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4, en AC-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Lavado, en AC-06', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Lavado, en AC-06'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en AC-11', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en AC-11'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en AC-12', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en AC-12'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmaceútico, en AC-10', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmaceútico, en AC-10'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster, en AC-13', @empresa_id, COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-05-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-30'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster, en AC-13'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso técnico, en Colector de polvos (AC-02)', @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-32'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso técnico, en Colector de polvos (AC-02)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado #5, en AC-04', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado #5, en AC-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 1, en AC-07', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 1, en AC-07'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarto Técnico, en salida del Tanque Pulmón de aire comprimido', @empresa_id, COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarto Técnico, en salida del Tanque Pulmón de aire comprimido'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarto Técnico, en Tanque hidroneumático (TH 02)', @empresa_id, COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-12-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MA-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarto Técnico, en Tanque hidroneumático (TH 02)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en EST-MI-01.', @empresa_id, COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-04-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en EST-MI-01.'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos, en TAP-AC-01', @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos, en TAP-AC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos, en CON-AC-02', @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-PR-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos, en CON-AC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-04-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAP-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recepción de Material de Envase y Empaque', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recepción de Material de Envase y Empaque'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de balanzas', @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-01-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-03-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-10-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAS-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-03', @empresa_id, COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-02', @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-DA-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental II, en ESE-DA-01', @empresa_id, COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-14', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-DA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental II, en ESE-DA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4, en ENC-PR-04 (bomba)', @empresa_id, COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-09-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4, en ENC-PR-04 (bomba)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster, en BLI-FR-02 (Bomba de vacío)', @empresa_id, COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster, en BLI-FR-02 (Bomba de vacío)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado #5, en ENC-FR-05', @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado #5, en ENC-FR-05'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4, en ENC-FR-06', @empresa_id, COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-FR-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4, en ENC-FR-06'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos, en BOV-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos, en BOV-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-08-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso, en BOV-CP-01', @empresa_id, COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-11-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso, en BOV-CP-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-04', @empresa_id, COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución), en BOV-LC-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-01-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MAV-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Mezclado y Tamizado 1', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Mezclado y Tamizado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pesado', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pesado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Utensilios y Aditamentos de Fabricación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Utensilios y Aditamentos de Fabricación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Lavado', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Lavado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Órdenes Surtidas', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Órdenes Surtidas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Esclusa 2', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Esclusa 2'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado #5', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado #5'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Detección de Metales', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MDI-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Detección de Metales'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIC-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIM-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Taller de Mantenimiento', @empresa_id, COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-05-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIM-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-01-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-DA-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-08-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Analisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-09-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Analisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Analisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Analisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Analisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Analisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Analisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Analisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-02-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MIP-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-08-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-09-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución)', @empresa_id, COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-02-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUL-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Taller de Mantenimiento', @empresa_id, COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-10-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'MUT-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Taller de Mantenimiento', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'NIV-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil', @empresa_id, COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-06-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil', @empresa_id, COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-08-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-25', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAA-MI-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-03-28', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PAE-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Utensilios y Aditamentos de Fabricación', @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Utensilios y Aditamentos de Fabricación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Utensilios y Aditamentos de Fabricación', @empresa_id, COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-12-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PES-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Utensilios y Aditamentos de Fabricación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-09', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'PUN-DF-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'PUR-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'REG-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'REG-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-07-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'REG-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental II, en BOV-DA-04', @empresa_id, COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental II, en BOV-DA-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso Técnico, en UMA-MA-03', @empresa_id, COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-03-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso Técnico, en UMA-MA-03'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-FR-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica, en REF-MI-01', @empresa_id, COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica, en REF-MI-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-12-13', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SEN-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos', @empresa_id, COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-12', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SMI-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental IV', @empresa_id, COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-07-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'SON-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental IV'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-10-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TAC-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TAC-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEB-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TED-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-24', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-02-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso, en DES-CP-01', @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso, en DES-CP-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología, en BAM-LC-01', @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología, en BAM-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Analisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-11-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Analisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2019-01-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEM-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Material impreso', @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Material impreso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de material de envase y empaque aprobado', @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de material de envase y empaque aprobado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de material de envase y empaque aprobado', @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de material de envase y empaque aprobado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarentena de material de envase y empaque', @empresa_id, COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarentena de material de envase y empaque'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Esclusa 1', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Esclusa 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Materia Prima Aprobada', @empresa_id, COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-07-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Materia Prima Aprobada'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pasillo A', @empresa_id, COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-03-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pasillo A'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-06-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Secundario, en UPA-MA-01', @empresa_id, COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-02-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Secundario, en UPA-MA-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Piso técnico, UMA-MA-05', @empresa_id, COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-09-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TEP-MA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Piso técnico, UMA-MA-05'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Materia Prima Aprobada y Cuarentena', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-AL-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Materia Prima Aprobada y Cuarentena'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Pesado', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-AL-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Pesado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarentena de Producto Terminado', @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarentena de Producto Terminado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Producto Terminado Controlado', @empresa_id, COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Producto Terminado Controlado'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Materia Prima Aprobada', @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Materia Prima Aprobada'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-AL-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología, en REF-LC-04', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología, en REF-LC-04'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Muestreo de Materia Prima', @empresa_id, COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-08-16', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-CP-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Muestreo de Materia Prima'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Museo de Muestras', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Museo de Muestras'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Órdenes Surtidas', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-CP-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Órdenes Surtidas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-03', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 1', @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-36'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Llenado de Frascos', @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-37'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Llenado de Frascos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Mezclado y Tamizado 1', @empresa_id, COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-01-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-38'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Mezclado y Tamizado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Tableteado 2', @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-39'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Tableteado 2'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado #5', @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-40'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado #5'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento', @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-41'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 4', @empresa_id, COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-11-19', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-FR-42'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 4'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental II, en DES-LC-01', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental II, en DES-LC-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, en DES-LC-02', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, en DES-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Preparación Microbiológica', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Preparación Microbiológica'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución, en REF-LC-05', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución, en REF-LC-05'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Encapsulado y Abrillantado 1', @empresa_id, COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-27', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Encapsulado y Abrillantado 1'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-08'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Reactivos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Reactivos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental I', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental I'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Estabilidades', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-LC-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Estabilidades'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-20', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-16'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2021-04-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas', @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Instrumental IV', @empresa_id, COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-29', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Instrumental IV'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos, en Recepción de Muestras', @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos, en Recepción de Muestras'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos, en Muestras de Grupo I, II y III', @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos, en Muestras de Grupo I, II y III'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución, en REF-LC-05 (Congelador)', @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución, en REF-LC-05 (Congelador)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-07-18', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Área de Balanzas, Estándares secundarios para grupos I, II y III', @empresa_id, COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LC-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Área de Balanzas, Estándares secundarios para grupos I, II y III'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Transporte', @empresa_id, COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-LO-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Transporte'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Almacén de Material de Empaque', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Almacén de Material de Empaque'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-12'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-12-15', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Transporte', @empresa_id, COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-06-08', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-15'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Transporte'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2017-11-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Secundario Frasco', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Secundario Frasco'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-PR-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-33'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-34'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-11-10', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-PR-35'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Primario Blíster', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-SC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Primario Blíster'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TER-VA-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-04'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-09-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-05'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-06'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-07'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-09'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-10'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-10-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-11'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-13'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Detección de Metales', @empresa_id, COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-22', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-14'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Detección de Metales'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Acondicionamiento Secundario Blíster', @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-17'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Acondicionamiento Secundario Blíster'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Cuarentena de Graneles', @empresa_id, COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-01-23', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-18'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Cuarentena de Graneles'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-19'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-20'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-21'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-22'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-23'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-24'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-25'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-26'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-27'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-06-01', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-28'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, Almacén de Validación', @empresa_id, COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TER-VA-29'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, Almacén de Validación'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2022-07-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento', @empresa_id, COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-08-02', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TIR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil, en FRI-DF-01', @empresa_id, COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-11-06', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TMB-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil, en FRI-DF-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso, en FRI-CP-01', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TMB-GC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso, en FRI-CP-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Mezclado y Tamizado 1, en PAN-FR-03.', @empresa_id, COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-05-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TPR-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Mezclado y Tamizado 1, en PAN-FR-03.'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Disolución', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Disolución'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología, en POT-LC-02', @empresa_id, COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2020-11-04', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-LC-03'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología, en POT-LC-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Microbiología, en POT-MI-01', @empresa_id, COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-09-21', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TRK-MI-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Microbiología, en POT-MI-01'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Recubrimiento, en REC-FR-02', @empresa_id, COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-03-30', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'TTH-FR-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Recubrimiento, en REC-FR-02'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Control en Proceso', @empresa_id, COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-04-05', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-CP-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Control en Proceso'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2018-12-17', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-DF-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Desarrollo Farmacéutico', @empresa_id, COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2025-08-11', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-DF-02'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Desarrollo Farmacéutico'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VER-LC-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Taller de Mantenimiento', @empresa_id, COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2024-04-26', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VER-MA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Taller de Mantenimiento', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VER-VA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Taller de Mantenimiento'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución)', @empresa_id, COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW()), COALESCE(STR_TO_DATE('2023-07-07', '%Y-%m-%d'), NOW())
FROM instrumentos i
WHERE i.codigo = 'VIS-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Móvil (Análisis Instrumental I, Análisis Instrumental II, Análisis Fisicoquímicos y Disolución)'
        AND hu.empresa_id = @empresa_id
  );

INSERT INTO historial_ubicaciones (instrumento_id, ubicacion, empresa_id, fecha, `timestamp`)
SELECT i.id, 'Análisis Fisicoquímicos', @empresa_id, NOW(), NOW()
FROM instrumentos i
WHERE i.codigo = 'VOR-DA-01'
  AND i.empresa_id = @empresa_id
  AND NOT EXISTS (
      SELECT 1
      FROM historial_ubicaciones hu
      WHERE hu.instrumento_id = i.id
        AND hu.ubicacion = 'Análisis Fisicoquímicos'
        AND hu.empresa_id = @empresa_id
  );

COMMIT;
