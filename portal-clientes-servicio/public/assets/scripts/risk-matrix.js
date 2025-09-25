(function(root, factory){
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.RiskMatrix = factory();
  }
})(typeof self !== 'undefined' ? self : this, function(){
  const RISK_LEVELS = [1,2,3];
  // Matriz de impacto vs detección
  const IMPACT_DETECTION_MATRIX = {
    1: {1:1, 2:2, 3:3},
    2: {1:2, 2:2, 3:3},
    3: {1:3, 2:3, 3:3}
  };
  // Matriz de impacto vs probabilidad
  const IMPACT_PROBABILITY_MATRIX = {
    1: {1:1, 2:2, 3:3},
    2: {1:2, 2:2, 3:3},
    3: {1:3, 2:3, 3:3}
  };
  // Tabla de frecuencia en meses según el nivel de riesgo
  const FREQUENCY_TABLE = {
    1: 18,
    2: 12,
    3: 6
  };
  return { RISK_LEVELS, IMPACT_DETECTION_MATRIX, IMPACT_PROBABILITY_MATRIX, FREQUENCY_TABLE };
});
