const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { strict: assert } = require('node:assert');
const test = require('node:test');

const execFileAsync = promisify(execFile);

const PHP_BINARY = process.env.PHP_BINARY || 'php';
const SCRIPT_PATH = 'tools/scripts/run_validation.php';

async function runValidation(envOverrides = {}, { allowFailure = false } = {}) {
  try {
    const { stdout } = await execFileAsync(PHP_BINARY, [SCRIPT_PATH, '--format=json'], {
      env: { ...process.env, ...envOverrides },
      encoding: 'utf8',
      maxBuffer: 10 * 1024 * 1024,
    });
    return { report: JSON.parse(stdout), exitCode: 0 };
  } catch (error) {
    if (allowFailure && error && typeof error.stdout === 'string') {
      return { report: JSON.parse(error.stdout), exitCode: typeof error.code === 'number' ? error.code : 1 };
    }
    throw error;
  }
}

test('validation runner reports success using fixtures', async () => {
  const { report, exitCode } = await runValidation({ VALIDATION_FIXTURES: 'tests/fixtures/validation/success.json' });

  assert.equal(exitCode, 0);
  assert.equal(report.mode, 'fixtures');
  assert.equal(report.summary.overall, 'passed');

  assert.equal(report.iq.status, 'passed');
  assert.equal(report.oq.status, 'passed');
  assert.equal(report.pq.status, 'passed');

  assert.equal(report.oq.flows.login_page.status, 'passed');
  assert.equal(report.oq.flows.approval.status, 'passed');
  assert.equal(report.oq.flows.rejection.status, 'passed');
});

test('validation runner flags missing permissions and failed flows', async () => {
  const { report, exitCode } = await runValidation({ VALIDATION_FIXTURES: 'tests/fixtures/validation/permissions_failure.json' }, { allowFailure: true });

  assert.equal(exitCode, 1);
  assert.equal(report.summary.overall, 'failed');
  assert.equal(report.iq.permissions.status, 'failed');
  assert.equal(report.oq.reports.status, 'failed');
  assert.equal(report.pq.status, 'failed');

  assert.equal(report.oq.flows.calibration_capture.status, 'failed');
  assert.equal(report.oq.flows.approval.status, 'failed');
  assert.equal(report.oq.flows.rejection.status, 'passed');
});
