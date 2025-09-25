const http = require('http');
const fs = require('fs');
const path = require('path');

const dataPath = path.join(__dirname, '..', '..', 'tests', 'js', 'data', 'instruments.json');
const historyPath = path.join(__dirname, '..', '..', 'tests', 'js', 'data', 'gage_history.json');
const labelPayloadPath = path.join(__dirname, '..', '..', 'tests', 'js', 'data', 'label_payload.json');
// Normalize dataset by ensuring each instrument has a numeric id
const normalizeInstruments = (data) => {
  return data.map((inst, idx) => {
    const parsedId = inst.id !== null && inst.id !== undefined ? parseInt(inst.id, 10) : NaN;
    return {
      ...inst,
      id: !isNaN(parsedId) && parsedId > 0 ? parsedId : idx + 1
    };
  });
};

const instruments = normalizeInstruments(JSON.parse(fs.readFileSync(dataPath, 'utf8')));
const gageHistory = JSON.parse(fs.readFileSync(historyPath, 'utf8'));
const labelPayloads = JSON.parse(fs.readFileSync(labelPayloadPath, 'utf8'));

const requestHandler = (req, res) => {
  if (req.method === 'GET' && req.url === '/api/instruments') {
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(instruments));
  } else if (req.method === 'GET' && req.url.startsWith('/api/gage-history')) {
    const urlObj = new URL(req.url, `http://${req.headers.host}`);
    const id = urlObj.searchParams.get('id');
    const history = gageHistory[id];
    if (history) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(history));
    } else {
      res.statusCode = 404;
      res.end('Not found');
    }
  } else if (req.method === 'GET' && req.url.startsWith('/api/label-payload')) {
    const urlObj = new URL(req.url, `http://${req.headers.host}`);
    const id = urlObj.searchParams.get('id');
    const payload = labelPayloads[id];
    if (payload) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(payload));
    } else {
      res.statusCode = 404;
      res.end('Not found');
    }
  } else {
    res.statusCode = 404;
    res.end('Not found');
  }
};

const createServer = () => http.createServer(requestHandler);
const server = createServer();

if (require.main === module) {
  const port = process.env.PORT || 3000;
  server.listen(port, () => {
    console.log(`API server listening at http://localhost:${port}`);
  });
}

module.exports = { server, normalizeInstruments, createServer };