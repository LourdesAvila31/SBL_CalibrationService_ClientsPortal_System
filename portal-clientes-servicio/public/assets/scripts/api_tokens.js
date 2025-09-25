(function(window, document){
  'use strict';

  const appRoot = document.getElementById('apiTokensApp');
  if(!appRoot){
    return;
  }

  const state = {
    tokens: [],
    rotateTokenId: null,
    revokeTokenId: null
  };

  const elements = {
    messageContainer: document.getElementById('apiTokenMessages'),
    loadingRow: document.getElementById('apiTokensLoading'),
    tableBody: document.getElementById('apiTokensTableBody'),
    counter: document.getElementById('apiTokensCounter'),
    refreshBtn: document.getElementById('refreshTokensBtn'),
    openCreateBtn: document.getElementById('openCreateTokenModal'),
    createModal: document.getElementById('createTokenModal'),
    createForm: document.getElementById('createTokenForm'),
    createSubmit: document.getElementById('createTokenSubmit'),
    createFeedback: document.getElementById('createTokenFeedback'),
    createResult: document.getElementById('createTokenResult'),
    createTokenValue: document.getElementById('createTokenValue'),
    rotateModal: document.getElementById('rotateTokenModal'),
    rotateForm: document.getElementById('rotateTokenForm'),
    rotateSubmit: document.getElementById('rotateTokenSubmit'),
    rotateTokenName: document.getElementById('rotateTokenName'),
    rotateFeedback: document.getElementById('rotateTokenFeedback'),
    rotateResult: document.getElementById('rotateTokenResult'),
    rotateTokenValue: document.getElementById('rotateTokenValue'),
    revokeModal: document.getElementById('revokeTokenModal'),
    revokeForm: document.getElementById('revokeTokenForm'),
    revokeSubmit: document.getElementById('revokeTokenSubmit'),
    revokeTokenName: document.getElementById('revokeTokenName'),
    revokeFeedback: document.getElementById('revokeTokenFeedback')
  };

  const endpointRoot = normalizeRoot(appRoot.getAttribute('data-endpoint-root')) || '/backend/configuracion/api_tokens';
  const endpointFormat = (appRoot.getAttribute('data-endpoint-format') || 'php').toLowerCase();

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', initialize);
  }else{
    initialize();
  }

  function initialize(){
    attachEventListeners();
    refreshTokens();
  }

  function attachEventListeners(){
    if(elements.refreshBtn){
      elements.refreshBtn.addEventListener('click', function(){
        refreshTokens({ showMessage: true });
      });
    }

    if(elements.openCreateBtn){
      elements.openCreateBtn.addEventListener('click', openCreateModal);
    }

    if(elements.createForm){
      elements.createForm.addEventListener('submit', handleCreateSubmit);
    }

    if(elements.rotateForm){
      elements.rotateForm.addEventListener('submit', handleRotateSubmit);
    }

    if(elements.revokeForm){
      elements.revokeForm.addEventListener('submit', handleRevokeSubmit);
    }

    const closers = document.querySelectorAll('[data-close-modal]');
    closers.forEach(function(button){
      button.addEventListener('click', function(){
        closeModal(button.getAttribute('data-close-modal'));
      });
    });

    [elements.createModal, elements.rotateModal, elements.revokeModal].forEach(function(modal){
      if(!modal){ return; }
      modal.addEventListener('click', function(event){
        if(event.target === modal){
          closeModal(modal.id);
        }
      });
    });

    document.addEventListener('keydown', function(event){
      if(event.key === 'Escape'){
        [elements.createModal, elements.rotateModal, elements.revokeModal].forEach(function(modal){
          if(modal && modal.classList.contains('open')){
            closeModal(modal.id);
          }
        });
      }
    });

    if(elements.tableBody){
      elements.tableBody.addEventListener('click', function(event){
        const rotateBtn = event.target.closest('[data-action="rotate"]');
        const revokeBtn = event.target.closest('[data-action="revoke"]');

        if(rotateBtn){
          const tokenId = rotateBtn.getAttribute('data-token-id');
          openRotateModal(tokenId);
          return;
        }

        if(revokeBtn){
          const tokenId = revokeBtn.getAttribute('data-token-id');
          openRevokeModal(tokenId);
        }
      });
    }

    document.addEventListener('click', function(event){
      const copyBtn = event.target.closest('[data-copy-target]');
      if(!copyBtn){
        return;
      }
      const targetId = copyBtn.getAttribute('data-copy-target');
      const target = targetId ? document.getElementById(targetId) : null;
      const value = target ? target.textContent.trim() : '';
      if(!value){
        showPageMessage('error', 'No hay token disponible para copiar.');
        return;
      }
      copyToClipboard(value)
        .then(function(){
          showPageMessage('success', 'Token copiado al portapapeles.');
        })
        .catch(function(){
          showPageMessage('error', 'No fue posible copiar el token automáticamente.');
        });
    });
  }

  function normalizeRoot(root){
    if(!root){
      return '';
    }
    let value = String(root).trim();
    if(!value){
      return '';
    }
    if(!value.startsWith('/')){
      value = '/' + value.replace(/^\/+/, '');
    }
    return value.replace(/\/$/, '');
  }

  function buildEndpoint(action){
    const sanitizedAction = String(action || '').replace(/[^a-z0-9_-]/gi, '');
    if(!sanitizedAction){
      throw new Error('Acción inválida.');
    }
    let path = endpointRoot + '/' + sanitizedAction;
    if(endpointFormat === 'php'){
      path += '.php';
    }else if(endpointFormat === 'json'){
      path += '.json';
    }
    return path;
  }

  function resolveUrl(path){
    if(!path){
      return path;
    }
    if(/^https?:\/\//i.test(path)){
      return path;
    }
    const base = (typeof window.BASE_URL === 'string') ? window.BASE_URL : '';
    return (base || '') + path;
  }

  function setLoading(isLoading){
    if(!elements.loadingRow){
      return;
    }
    if(isLoading){
      elements.loadingRow.classList.add('show');
    }else{
      elements.loadingRow.classList.remove('show');
    }
  }

  function clearTable(){
    if(!elements.tableBody){
      return;
    }
    elements.tableBody.innerHTML = '';
  }

  function renderTokens(tokens){
    if(!elements.tableBody){
      return;
    }
    clearTable();

    if(!Array.isArray(tokens) || tokens.length === 0){
      const emptyRow = document.createElement('tr');
      emptyRow.className = 'empty-row';
      const cell = document.createElement('td');
      cell.colSpan = 6;
      cell.className = 'empty-state';
      cell.innerHTML = '<i class="fa fa-key"></i><div>No hay tokens registrados aún.</div>';
      emptyRow.appendChild(cell);
      elements.tableBody.appendChild(emptyRow);
      updateCounter(0);
      return;
    }

    tokens.forEach(function(token){
      const row = document.createElement('tr');
      row.setAttribute('data-token-id', safeId(token));

      const nameCell = document.createElement('td');
      nameCell.textContent = token && typeof token.name === 'string' ? token.name : 'Sin nombre';

      const tokenCell = document.createElement('td');
      tokenCell.innerHTML = '<span class="token-prefix">' + escapeHtml(maskToken(token)) + '</span>';

      const createdCell = document.createElement('td');
      createdCell.textContent = formatDate(token && token.created_at);

      const lastUsedCell = document.createElement('td');
      lastUsedCell.textContent = formatDate(token && (token.last_used_at || token.last_used));

      const statusCell = document.createElement('td');
      statusCell.innerHTML = buildStatusBadge(token && (token.status || token.state));

      const actionsCell = document.createElement('td');
      actionsCell.className = 'actions-column';
      actionsCell.appendChild(buildActionButton('rotate', safeId(token), 'Rotar', 'rotate-btn'));
      actionsCell.appendChild(buildActionButton('revoke', safeId(token), 'Revocar', 'revoke-btn'));

      row.appendChild(nameCell);
      row.appendChild(tokenCell);
      row.appendChild(createdCell);
      row.appendChild(lastUsedCell);
      row.appendChild(statusCell);
      row.appendChild(actionsCell);

      elements.tableBody.appendChild(row);
    });

    updateCounter(tokens.filter(function(token){
      const status = (token && (token.status || token.state || '')).toLowerCase();
      return status !== 'revocado' && status !== 'revoked';
    }).length);
  }

  function safeId(token){
    if(!token){
      return '';
    }
    if(token.id !== undefined && token.id !== null){
      return String(token.id);
    }
    if(token.token_id !== undefined && token.token_id !== null){
      return String(token.token_id);
    }
    return String(token.identifier || '');
  }

  function buildActionButton(action, tokenId, label, extraClass){
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = label;
    button.className = (extraClass ? extraClass + ' ' : '') + 'action-btn';
    button.setAttribute('data-action', action);
    button.setAttribute('data-token-id', tokenId);
    return button;
  }

  function updateCounter(count){
    if(elements.counter){
      elements.counter.textContent = String(count || 0);
    }
  }

  function maskToken(token){
    if(!token){
      return '••••';
    }
    if(typeof token.display === 'string' && token.display.trim()){
      return token.display.trim();
    }
    if(typeof token.masked === 'string' && token.masked.trim()){
      return token.masked.trim();
    }
    const prefix = (token.prefix || token.token_prefix || '').toString().trim();
    const last = (token.last_four || token.last4 || '').toString().trim();
    if(prefix && last){
      return prefix + '••••' + last;
    }
    if(prefix){
      return prefix + '••••';
    }
    if(last){
      return '••••' + last;
    }
    if(typeof token.partial === 'string' && token.partial.trim()){
      return token.partial.trim();
    }
    return '••••';
  }

  function buildStatusBadge(statusValue){
    const status = (statusValue || 'activo').toString().toLowerCase();
    const labelMap = {
      activo: 'Activo',
      active: 'Activo',
      revocado: 'Revocado',
      revoked: 'Revocado',
      expirado: 'Expirado',
      expired: 'Expirado'
    };
    const label = labelMap[status] || capitalize(status) || 'Activo';
    const badge = document.createElement('span');
    badge.className = 'badge-status';
    badge.setAttribute('data-status', status);
    badge.textContent = label;
    return badge.outerHTML;
  }

  function capitalize(value){
    if(!value){
      return '';
    }
    return value.charAt(0).toUpperCase() + value.slice(1);
  }

  function formatDate(value){
    if(!value){
      return '—';
    }
    try{
      const date = new Date(value);
      if(Number.isNaN(date.getTime())){
        return '—';
      }
      return date.toLocaleString('es-MX', {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      });
    }catch(error){
      return '—';
    }
  }

  function escapeHtml(value){
    if(value === null || value === undefined){
      return '';
    }
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function showPageMessage(type, message){
    if(!elements.messageContainer || !message){
      return;
    }
    elements.messageContainer.innerHTML = '';
    const alert = document.createElement('div');
    const kind = (type === 'success') ? 'success' : (type === 'info') ? 'info' : 'danger';
    alert.className = 'alert alert-' + kind + ' alert-dismissible fade show';
    alert.setAttribute('role', 'alert');
    alert.innerHTML = '<span>' + escapeHtml(message) + '</span>' +
      '<button type="button" class="btn-close" aria-label="Cerrar"></button>';
    elements.messageContainer.appendChild(alert);
    const closeBtn = alert.querySelector('.btn-close');
    if(closeBtn){
      closeBtn.addEventListener('click', function(){
        alert.remove();
      });
    }
  }

  function showModalFeedback(element, type, message){
    if(!element){
      return;
    }
    element.hidden = !message;
    element.textContent = message || '';
    element.classList.remove('error', 'success');
    if(message){
      element.classList.add(type === 'success' ? 'success' : 'error');
    }
  }

  function openCreateModal(){
    resetCreateModal();
    openModal('createTokenModal');
  }

  function openRotateModal(tokenId){
    const token = findToken(tokenId);
    if(!token){
      showPageMessage('error', 'No se encontró el token seleccionado.');
      return;
    }
    state.rotateTokenId = safeId(token);
    showModalFeedback(elements.rotateFeedback, null, '');
    toggleTokenResult(elements.rotateResult, elements.rotateTokenValue, '');
    if(elements.rotateTokenName){
      elements.rotateTokenName.textContent = token.name || maskToken(token);
    }
    openModal('rotateTokenModal');
  }

  function openRevokeModal(tokenId){
    const token = findToken(tokenId);
    if(!token){
      showPageMessage('error', 'No se encontró el token seleccionado.');
      return;
    }
    state.revokeTokenId = safeId(token);
    showModalFeedback(elements.revokeFeedback, null, '');
    if(elements.revokeTokenName){
      elements.revokeTokenName.textContent = token.name || maskToken(token);
    }
    openModal('revokeTokenModal');
  }

  function resetCreateModal(){
    if(elements.createForm){
      elements.createForm.reset();
    }
    showModalFeedback(elements.createFeedback, null, '');
    toggleTokenResult(elements.createResult, elements.createTokenValue, '');
    setButtonLoading(elements.createSubmit, false);
  }

  function openModal(id){
    const modal = id ? document.getElementById(id) : null;
    if(!modal){
      return;
    }
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
  }

  function closeModal(id){
    const modal = id ? document.getElementById(id) : null;
    if(!modal){
      return;
    }
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
    if(modal === elements.createModal){
      resetCreateModal();
    }
    if(modal === elements.rotateModal){
      state.rotateTokenId = null;
      showModalFeedback(elements.rotateFeedback, null, '');
      toggleTokenResult(elements.rotateResult, elements.rotateTokenValue, '');
      setButtonLoading(elements.rotateSubmit, false);
    }
    if(modal === elements.revokeModal){
      state.revokeTokenId = null;
      showModalFeedback(elements.revokeFeedback, null, '');
      setButtonLoading(elements.revokeSubmit, false);
    }
  }

  function toggleTokenResult(container, codeElement, tokenValue){
    if(!container || !codeElement){
      return;
    }
    if(tokenValue){
      container.classList.add('show');
      codeElement.textContent = tokenValue;
    }else{
      container.classList.remove('show');
      codeElement.textContent = '';
    }
  }

  function findToken(tokenId){
    if(!tokenId){
      return null;
    }
    const normalized = String(tokenId);
    return state.tokens.find(function(token){
      return safeId(token) === normalized;
    }) || null;
  }

  function handleCreateSubmit(event){
    event.preventDefault();
    const form = event.currentTarget;
    if(!form){
      return;
    }
    const formData = new FormData(form);
    const payload = {
      name: (formData.get('name') || '').toString().trim()
    };
    const expiresAt = formData.get('expires_at');
    if(expiresAt){
      payload.expires_at = expiresAt;
    }
    if(!payload.name){
      showModalFeedback(elements.createFeedback, 'error', 'Ingresa un nombre descriptivo para el token.');
      return;
    }
    showModalFeedback(elements.createFeedback, null, '');
    setButtonLoading(elements.createSubmit, true, 'Creando...');

    request('create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(function(response){
        const tokenValue = extractFullToken(response);
        const successMessage = response && response.message ? response.message : 'Token creado correctamente.';
        showModalFeedback(elements.createFeedback, 'success', tokenValue ? successMessage : successMessage + ' El valor completo del token no fue proporcionado.');
        toggleTokenResult(elements.createResult, elements.createTokenValue, tokenValue);
        refreshTokens();
        showPageMessage('success', 'Token creado correctamente.');
      })
      .catch(function(error){
        showModalFeedback(elements.createFeedback, 'error', extractErrorMessage(error, 'No fue posible crear el token.'));
      })
      .finally(function(){
        setButtonLoading(elements.createSubmit, false);
      });
  }

  function handleRotateSubmit(event){
    event.preventDefault();
    if(!state.rotateTokenId){
      showModalFeedback(elements.rotateFeedback, 'error', 'Selecciona un token para rotar.');
      return;
    }
    setButtonLoading(elements.rotateSubmit, true, 'Rotando...');
    showModalFeedback(elements.rotateFeedback, null, '');

    request('rotate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ token_id: state.rotateTokenId })
    })
      .then(function(response){
        const tokenValue = extractFullToken(response);
        const successMessage = response && response.message ? response.message : 'Token rotado correctamente.';
        showModalFeedback(elements.rotateFeedback, 'success', tokenValue ? successMessage : successMessage + ' El valor completo del token no fue proporcionado.');
        toggleTokenResult(elements.rotateResult, elements.rotateTokenValue, tokenValue);
        refreshTokens();
        showPageMessage('success', 'Token rotado correctamente.');
      })
      .catch(function(error){
        showModalFeedback(elements.rotateFeedback, 'error', extractErrorMessage(error, 'No fue posible rotar el token.'));
      })
      .finally(function(){
        setButtonLoading(elements.rotateSubmit, false);
      });
  }

  function handleRevokeSubmit(event){
    event.preventDefault();
    if(!state.revokeTokenId){
      showModalFeedback(elements.revokeFeedback, 'error', 'Selecciona un token para revocar.');
      return;
    }
    setButtonLoading(elements.revokeSubmit, true, 'Revocando...');
    showModalFeedback(elements.revokeFeedback, null, '');

    request('revoke', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ token_id: state.revokeTokenId })
    })
      .then(function(response){
        showModalFeedback(elements.revokeFeedback, 'success', response && response.message ? response.message : 'Token revocado correctamente.');
        refreshTokens();
        showPageMessage('success', 'Token revocado correctamente.');
        setTimeout(function(){
          closeModal('revokeTokenModal');
        }, 1200);
      })
      .catch(function(error){
        showModalFeedback(elements.revokeFeedback, 'error', extractErrorMessage(error, 'No fue posible revocar el token.'));
      })
      .finally(function(){
        setButtonLoading(elements.revokeSubmit, false);
      });
  }

  function extractErrorMessage(error, fallback){
    if(error){
      if(error.payload && typeof error.payload.message === 'string' && error.payload.message.trim()){
        return error.payload.message;
      }
      if(typeof error.message === 'string' && error.message.trim()){
        return error.message;
      }
    }
    return fallback;
  }

  function setButtonLoading(button, loading, loadingLabel){
    if(!button){
      return;
    }
    if(loading){
      if(!button.dataset.originalLabel){
        button.dataset.originalLabel = button.innerHTML;
      }
      button.disabled = true;
      button.textContent = loadingLabel || 'Procesando...';
    }else{
      button.disabled = false;
      if(button.dataset.originalLabel){
        button.innerHTML = button.dataset.originalLabel;
        delete button.dataset.originalLabel;
      }
    }
  }

  function request(action, options){
    const endpoint = resolveUrl(buildEndpoint(action));
    const requestOptions = Object.assign({
      credentials: 'include',
      headers: { 'Accept': 'application/json' }
    }, options || {});
    if(options && options.headers){
      requestOptions.headers = Object.assign({ 'Accept': 'application/json' }, options.headers);
    }
    return fetch(endpoint, requestOptions)
      .then(function(response){
        if(!response){
          throw new Error('Sin respuesta del servidor.');
        }
        return response.text()
          .then(function(text){
            let payload = {};
            if(text){
              try{
                payload = JSON.parse(text);
              }catch(parseError){
                const error = new Error('Respuesta inválida del servidor.');
                error.status = response.status;
                throw error;
              }
            }
            if(!response.ok || (payload && payload.success === false)){
              const message = payload && (payload.message || payload.error) ? (payload.message || payload.error) : ('Error ' + response.status);
              const error = new Error(message);
              error.status = response.status;
              error.payload = payload;
              throw error;
            }
            return payload;
          });
      });
  }

  function refreshTokens(options){
    const settings = options || {};
    setLoading(true);
    request('list', { method: 'GET' })
      .then(function(response){
        const tokens = extractTokens(response);
        state.tokens = tokens;
        renderTokens(tokens);
        if(settings.showMessage){
          showPageMessage('success', 'Lista de tokens actualizada.');
        }
      })
      .catch(function(error){
        showPageMessage('error', extractErrorMessage(error, 'No fue posible obtener los tokens.'));
      })
      .finally(function(){
        setLoading(false);
      });
  }

  function extractTokens(response){
    if(!response){
      return [];
    }
    if(Array.isArray(response.tokens)){
      return response.tokens;
    }
    if(Array.isArray(response.data)){
      return response.data;
    }
    if(response.data && Array.isArray(response.data.tokens)){
      return response.data.tokens;
    }
    if(response.result){
      if(Array.isArray(response.result.tokens)){
        return response.result.tokens;
      }
      if(Array.isArray(response.result)){
        return response.result;
      }
    }
    if(Array.isArray(response.items)){
      return response.items;
    }
    return [];
  }

  function extractFullToken(response){
    if(!response){
      return '';
    }
    if(typeof response.full_token === 'string'){
      return response.full_token;
    }
    if(typeof response.token_value === 'string'){
      return response.token_value;
    }
    if(response.data){
      if(typeof response.data.full_token === 'string'){
        return response.data.full_token;
      }
      if(typeof response.data.token_value === 'string'){
        return response.data.token_value;
      }
    }
    return '';
  }

  function copyToClipboard(text){
    if(navigator.clipboard && navigator.clipboard.writeText){
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function(resolve, reject){
      try{
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.setAttribute('readonly', '');
        textarea.style.position = 'fixed';
        textarea.style.opacity = '0';
        document.body.appendChild(textarea);
        textarea.select();
        const successful = document.execCommand('copy');
        document.body.removeChild(textarea);
        if(successful){
          resolve();
        }else{
          reject(new Error('copy-command-failed'));
        }
      }catch(error){
        reject(error);
      }
    });
  }

})(typeof window !== 'undefined' ? window : this, document);
