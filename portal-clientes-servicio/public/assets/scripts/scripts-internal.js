import { configurePortal } from './portal-base.js';
import { initInternalTutorial } from './internal-tutorial.js';

configurePortal({
    portalId: 'internal',
    serviceModuleEnabled: false
});

initInternalTutorial();
