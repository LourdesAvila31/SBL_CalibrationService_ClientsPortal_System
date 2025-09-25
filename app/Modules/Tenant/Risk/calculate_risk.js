const path = require('node:path');

const {
  IMPACT_DETECTION_MATRIX,
  FREQUENCY_TABLE,
  RISK_LEVELS
} = require(path.join(__dirname, '..', '..', '..', '..', 'public', 'assets', 'scripts', 'risk-matrix.js'));

function calculateRisk(impact, detection){
  if(!RISK_LEVELS.includes(impact) || !RISK_LEVELS.includes(detection)){
    throw new Error('Valores de riesgo fuera de rango');
  }
  const risk = IMPACT_DETECTION_MATRIX[impact][detection];
  const frequency = FREQUENCY_TABLE[risk];
  return { risk, frequency };
}

module.exports = { calculateRisk };
