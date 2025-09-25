const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const CalibrationStatus = require('../../public/assets/scripts/calibration-status.js');

describe('CalibrationStatus helper', () => {
    it('normalizes statuses case-insensitively', () => {
        assert.equal(CalibrationStatus.normalizeStatus('programada'), 'Programada');
        assert.equal(CalibrationStatus.normalizeStatus(' REPROGRAMADA '), 'Reprogramada');
        assert.equal(CalibrationStatus.normalizeStatus('Desconocido'), 'Completada');
        assert.equal(CalibrationStatus.normalizeStatus(null), 'Completada');
    });

    it('provides filter options for all statuses', () => {
        const options = CalibrationStatus.getFilterOptions();
        assert.deepEqual(options, [
            'Programada',
            'En proceso',
            'Completada',
            'Reprogramada',
            'Atrasada',
            'Cancelada',
        ]);
        assert.notStrictEqual(options, CalibrationStatus.getFilterOptions(), 'returns copy');
    });

    it('maps statuses to bootstrap badge classes', () => {
        assert.equal(CalibrationStatus.getStatusBadgeClass('Programada'), 'bg-secondary');
        assert.equal(CalibrationStatus.getStatusBadgeClass('En proceso'), 'bg-info text-dark');
        assert.equal(CalibrationStatus.getStatusBadgeClass('Completada'), 'bg-success');
        assert.equal(CalibrationStatus.getStatusBadgeClass('Reprogramada'), 'bg-warning text-dark');
        assert.equal(CalibrationStatus.getStatusBadgeClass('Atrasada'), 'bg-danger');
        assert.equal(CalibrationStatus.getStatusBadgeClass('Cancelada'), 'bg-dark');
        assert.equal(CalibrationStatus.getStatusBadgeClass('otra'), 'bg-success');
    });
});
