const STORAGE_PREFIX = 'internalTutorialSeen:';
const CHECK_INTERVAL = 150;
const WAIT_TIMEOUT = 800;

function readStorage(key) {
    if (typeof window === 'undefined' || !window.localStorage) {
        return null;
    }
    try {
        return window.localStorage.getItem(key);
    } catch (error) {
        console.warn('No fue posible leer la preferencia del tutorial.', error);
        return null;
    }
}

function writeStorage(key, value) {
    if (typeof window === 'undefined' || !window.localStorage) {
        return;
    }
    try {
        window.localStorage.setItem(key, value);
    } catch (error) {
        console.warn('No fue posible guardar la preferencia del tutorial.', error);
    }
}

function collectGuideSteps() {
    if (typeof document === 'undefined') {
        return [];
    }
    return Array.from(document.querySelectorAll('[data-guide-step]'))
        .map(element => ({
            element,
            index: Number(element.getAttribute('data-guide-step')) || 0,
            title: element.getAttribute('data-guide-title') || 'Paso',
            description: element.getAttribute('data-guide-description') || ''
        }))
        .filter(step => step.index > 0 && step.element instanceof HTMLElement)
        .sort((a, b) => a.index - b.index);
}

function ensureOverlay() {
    let overlay = document.getElementById('internalGuidedOverlay');
    if (overlay) {
        return overlay;
    }

    overlay = document.createElement('div');
    overlay.id = 'internalGuidedOverlay';
    overlay.className = 'guided-tour-overlay';
    overlay.setAttribute('role', 'dialog');
    overlay.setAttribute('aria-modal', 'true');
    overlay.setAttribute('aria-hidden', 'true');

    overlay.innerHTML = `
        <div class="guided-tour-dialog" role="document">
            <button type="button" class="guided-tour-close" data-guide-action="close" aria-label="Cerrar tutorial">&times;</button>
            <span class="guided-tour-step-label" data-guide-role="label"></span>
            <h2 class="guided-tour-title" data-guide-role="title"></h2>
            <p class="guided-tour-description" data-guide-role="description"></p>
            <div class="guided-tour-actions">
                <button type="button" data-guide-action="previous">Anterior</button>
                <button type="button" data-guide-action="next">Siguiente</button>
            </div>
        </div>
    `;

    document.body.appendChild(overlay);
    return overlay;
}

function applyHighlight(step) {
    document.querySelectorAll('.guided-tour-highlight').forEach(element => {
        element.classList.remove('guided-tour-highlight');
        element.removeAttribute('data-guide-active');
    });
    if (!step || !step.element) {
        return;
    }
    step.element.classList.add('guided-tour-highlight');
    step.element.setAttribute('data-guide-active', 'true');
    if (typeof step.element.scrollIntoView === 'function') {
        try {
            step.element.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'center' });
        } catch (error) {
            step.element.scrollIntoView();
        }
    }
}

function getUserStorageKey(user) {
    if (!user || typeof user !== 'object') {
        return STORAGE_PREFIX + 'anonimo';
    }
    const candidate = user.id || user.usuario_id || user.user_id || user.uuid || user.email || 'anonimo';
    return STORAGE_PREFIX + String(candidate);
}

function waitForExistingUser(timeoutMs) {
    return new Promise(resolve => {
        if (window.currentUser) {
            resolve(window.currentUser);
            return;
        }
        const start = Date.now();
        function check() {
            if (window.currentUser) {
                resolve(window.currentUser);
                return;
            }
            if (Date.now() - start >= timeoutMs) {
                resolve(null);
                return;
            }
            window.setTimeout(check, CHECK_INTERVAL);
        }
        check();
    });
}

function resolveCurrentUser() {
    return waitForExistingUser(WAIT_TIMEOUT).then(user => {
        if (user) {
            return user;
        }
        if (typeof window.fetchCurrentUser === 'function') {
            return window.fetchCurrentUser().catch(() => null);
        }
        return null;
    });
}

function startGuidedTour(steps, storageKey) {
    if (!steps.length) {
        return;
    }
    const overlay = ensureOverlay();
    const label = overlay.querySelector('[data-guide-role="label"]');
    const title = overlay.querySelector('[data-guide-role="title"]');
    const description = overlay.querySelector('[data-guide-role="description"]');
    const prevButton = overlay.querySelector('[data-guide-action="previous"]');
    const nextButton = overlay.querySelector('[data-guide-action="next"]');
    const closeButton = overlay.querySelector('[data-guide-action="close"]');

    let currentIndex = 0;

    function updateStep(index) {
        const step = steps[index];
        if (!step) {
            return;
        }
        applyHighlight(step);
        if (label) {
            label.textContent = `Paso ${index + 1} de ${steps.length}`;
        }
        if (title) {
            title.textContent = step.title;
        }
        if (description) {
            description.textContent = step.description;
        }
        if (prevButton) {
            prevButton.disabled = index === 0;
        }
        if (nextButton) {
            nextButton.textContent = index === steps.length - 1 ? 'Finalizar' : 'Siguiente';
        }
    }

    function closeTour() {
        overlay.classList.remove('is-visible');
        overlay.setAttribute('aria-hidden', 'true');
        applyHighlight(null);
        document.removeEventListener('keydown', handleKeydown);
    }

    function goPrevious() {
        if (currentIndex === 0) {
            return;
        }
        currentIndex -= 1;
        updateStep(currentIndex);
    }

    function goNext() {
        if (currentIndex < steps.length - 1) {
            currentIndex += 1;
            updateStep(currentIndex);
        } else {
            closeTour();
        }
    }

    function handleKeydown(event) {
        if (!overlay.classList.contains('is-visible')) {
            return;
        }
        if (event.key === 'Escape') {
            event.preventDefault();
            closeTour();
        } else if (event.key === 'ArrowRight') {
            event.preventDefault();
            goNext();
        } else if (event.key === 'ArrowLeft') {
            event.preventDefault();
            goPrevious();
        }
    }

    if (prevButton && !prevButton.dataset.bound) {
        prevButton.addEventListener('click', goPrevious);
        prevButton.dataset.bound = 'true';
    }
    if (nextButton && !nextButton.dataset.bound) {
        nextButton.addEventListener('click', goNext);
        nextButton.dataset.bound = 'true';
    }
    if (closeButton && !closeButton.dataset.bound) {
        closeButton.addEventListener('click', closeTour);
        closeButton.dataset.bound = 'true';
    }
    if (!overlay.dataset.bound) {
        overlay.addEventListener('click', event => {
            if (event.target === overlay) {
                closeTour();
            }
        });
        overlay.dataset.bound = 'true';
    }

    document.addEventListener('keydown', handleKeydown);

    overlay.classList.add('is-visible');
    overlay.setAttribute('aria-hidden', 'false');
    writeStorage(storageKey, '1');
    updateStep(currentIndex);

    const focusTarget = nextButton || closeButton;
    if (focusTarget && typeof focusTarget.focus === 'function') {
        focusTarget.focus();
    }
}

export function initInternalTutorial() {
    function initialize() {
        const steps = collectGuideSteps();
        if (!steps.length) {
            return;
        }
        resolveCurrentUser().then(user => {
            const storageKey = getUserStorageKey(user);
            const alreadySeen = readStorage(storageKey);
            if (alreadySeen) {
                return;
            }
            startGuidedTour(steps, storageKey);
        });
    }

    if (typeof document === 'undefined') {
        return;
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initialize, { once: true });
    } else {
        initialize();
    }
}

export default initInternalTutorial;
