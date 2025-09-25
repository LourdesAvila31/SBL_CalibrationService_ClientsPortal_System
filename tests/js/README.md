# Pruebas de instrumentos

Este directorio contiene datos de ejemplo para realizar pruebas locales con la información de instrumentos.

## Ejecutar pruebas

1. Instala las dependencias si aún no lo has hecho:
   ```bash
   npm install
   ```
2. Ejecuta las pruebas automatizadas:
   ```bash
   npm test
   ```

   Para ejecutar únicamente la prueba de estado de calibración puedes usar el módulo integrado de Node.js:
   ```bash
   node --test tests/js/calibration-state.test.js
   ```

El archivo [`data/instruments.json`](data/instruments.json) incluye la información de 12 instrumentos extraída de la tabla proporcionada.

## Servidor local

Para consultar los datos a través de un endpoint HTTP se incluye un servidor ligero.

1. Inicia el servidor manualmente:
   ```bash
   npm run test:server
   ```
2. Abre en el navegador `http://localhost:3000/api/instruments` para ver el JSON.

Las pruebas de integración (`instrument_api.test.js`) levantan automáticamente un servidor en el puerto `3001`, por lo que no es necesario iniciarlo manualmente para ejecutarlas.
