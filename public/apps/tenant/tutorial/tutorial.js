const STORAGE_KEY = 'tenantTutorialChecklist';

function loadChecklistState() {
    try {
        const stored = localStorage.getItem(STORAGE_KEY);
        if (!stored) {
            return {};
        }
        return JSON.parse(stored) || {};
    } catch (error) {
        console.warn('No fue posible recuperar el avance del tutorial.', error);
        return {};
    }
}

function saveChecklistState(state) {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (error) {
        console.warn('No fue posible guardar el avance del tutorial.', error);
    }
}

function updateProgressDisplay(checkboxes) {
    const progressFill = document.querySelector('.progress-bar-fill');
    const progressBadge = document.getElementById('tutorialProgressBadge');
    const total = checkboxes.length;
    const completed = checkboxes.filter(cb => cb.checked).length;
    const percentage = total > 0 ? Math.round((completed / total) * 100) : 0;

    if (progressFill) {
        progressFill.style.width = `${percentage}%`;
        progressFill.setAttribute('aria-valuenow', String(percentage));
    }
    if (progressBadge) {
        progressBadge.textContent = `${completed}/${total} completados (${percentage}%)`;
    }
}

function initChecklist() {
    const checkboxes = Array.from(document.querySelectorAll('.tutorial-checklist input[type="checkbox"]'));
    const state = loadChecklistState();

    checkboxes.forEach(cb => {
        const key = cb.value || cb.id;
        if (state[key]) {
            cb.checked = true;
        }
        cb.addEventListener('change', () => {
            const updatedState = loadChecklistState();
            const itemKey = cb.value || cb.id;
            if (cb.checked) {
                updatedState[itemKey] = true;
            } else {
                delete updatedState[itemKey];
            }
            saveChecklistState(updatedState);
            updateProgressDisplay(checkboxes);
        });
    });

    const resetButton = document.getElementById('resetChecklist');
    if (resetButton) {
        resetButton.addEventListener('click', () => {
            const confirmed = window.confirm('¿Deseas reiniciar el avance del tutorial?');
            if (!confirmed) return;
            saveChecklistState({});
            checkboxes.forEach(cb => { cb.checked = false; });
            updateProgressDisplay(checkboxes);
        });
    }

    updateProgressDisplay(checkboxes);
}

function highlightElement(element) {
    document.querySelectorAll('.tutorial-step-card').forEach(card => {
        card.classList.remove('is-highlighted');
    });
    if (element) {
        element.classList.add('is-highlighted');
        element.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
}

function initGuidedTour() {
    const trigger = document.getElementById('startGuidedTour');
    const overlay = document.getElementById('guideOverlay');
    const dialog = document.querySelector('.guide-dialog');
    if (!trigger || !overlay || !dialog) {
        return;
    }

    const steps = Array.from(document.querySelectorAll('[data-guide-step]'))
        .map(element => ({
            element,
            index: Number(element.getAttribute('data-guide-step')) || 0,
            title: element.getAttribute('data-guide-title') || 'Paso',
            description: element.getAttribute('data-guide-description') || ''
        }))
        .filter(step => step.index > 0)
        .sort((a, b) => a.index - b.index);

    if (!steps.length) {
        trigger.addEventListener('click', () => window.alert('Los pasos del recorrido no están configurados.'));
        return;
    }

    const label = overlay.querySelector('.guide-step-label');
    const title = overlay.querySelector('.guide-step-title');
    const description = overlay.querySelector('.guide-step-description');
    const nextButton = overlay.querySelector('[data-guide-action="next"]');
    const prevButton = overlay.querySelector('[data-guide-action="previous"]');
    const closeButton = overlay.querySelector('[data-guide-action="close"]');

    let currentIndex = 0;

    function renderStep(index) {
        const step = steps[index];
        if (!step) return;
        highlightElement(step.element);
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

    function openOverlay() {
        overlay.classList.add('is-visible');
        currentIndex = 0;
        renderStep(currentIndex);
    }

    function closeOverlay() {
        overlay.classList.remove('is-visible');
        highlightElement(null);
    }

    trigger.addEventListener('click', openOverlay);

    if (closeButton) {
        closeButton.addEventListener('click', closeOverlay);
    }

    if (prevButton) {
        prevButton.addEventListener('click', () => {
            if (currentIndex === 0) return;
            currentIndex -= 1;
            renderStep(currentIndex);
        });
    }

    if (nextButton) {
        nextButton.addEventListener('click', () => {
            if (currentIndex < steps.length - 1) {
                currentIndex += 1;
                renderStep(currentIndex);
            } else {
                closeOverlay();
            }
        });
    }

    overlay.addEventListener('click', (event) => {
        if (event.target === overlay) {
            closeOverlay();
        }
    });

    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && overlay.classList.contains('is-visible')) {
            closeOverlay();
        }
    });
}

function initHeroActions() {
    const checklistCta = document.getElementById('focusChecklist');
    if (checklistCta) {
        checklistCta.addEventListener('click', () => {
            const checklist = document.getElementById('tutorialChecklist');
            if (checklist) {
                checklist.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    initChecklist();
    initGuidedTour();
    initHeroActions();
});
