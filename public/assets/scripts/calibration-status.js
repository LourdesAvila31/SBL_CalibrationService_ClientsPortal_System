(function (global) {
    const STATUSES = [
        'Programada',
        'En proceso',
        'Completada',
        'Reprogramada',
        'Atrasada',
        'Cancelada'
    ];

    const NORMALIZED = STATUSES.reduce((acc, status) => {
        acc[status.toLowerCase()] = status;
        return acc;
    }, {});

    function normalizeStatus(status) {
        if (typeof status !== 'string') {
            return 'Completada';
        }
        const trimmed = status.trim().toLowerCase();
        if (trimmed === '') {
            return 'Completada';
        }
        return NORMALIZED[trimmed] || 'Completada';
    }

    function getFilterOptions() {
        return STATUSES.slice();
    }

    function getStatusBadgeClass(status) {
        switch (normalizeStatus(status)) {
            case 'Programada':
                return 'bg-secondary';
            case 'En proceso':
                return 'bg-info text-dark';
            case 'Completada':
                return 'bg-success';
            case 'Reprogramada':
                return 'bg-warning text-dark';
            case 'Atrasada':
                return 'bg-danger';
            case 'Cancelada':
                return 'bg-dark';
            default:
                return 'bg-secondary';
        }
    }

    const api = {
        getFilterOptions,
        getStatusBadgeClass,
        normalizeStatus,
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }

    global.CalibrationStatus = api;
}(typeof window !== 'undefined' ? window : globalThis));
