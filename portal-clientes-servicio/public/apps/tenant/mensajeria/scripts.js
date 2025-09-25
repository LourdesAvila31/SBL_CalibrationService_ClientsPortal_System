(function(){
  const state = {
    conversations: [],
    activeConversationId: null,
    empresaId: null,
    loadingList: false
  };

  const elements = {
    conversationList: document.getElementById('conversationList'),
    conversationEmpty: document.getElementById('conversationEmpty'),
    conversationSubject: document.getElementById('conversationSubject'),
    conversationMeta: document.getElementById('conversationMeta'),
    messagesPanel: document.getElementById('messagesPanel'),
    messagesEmpty: document.getElementById('messagesEmpty'),
    messageForm: document.getElementById('messageForm'),
    messageTextarea: document.getElementById('mensajeTexto'),
    messageAttachments: document.getElementById('mensajeAdjuntos'),
    messageAttachmentsSummary: document.getElementById('mensajeAdjuntosResumen'),
    messageStatus: null,
    newConversationForm: document.getElementById('newConversationForm'),
    newConversationSubject: document.getElementById('nuevoAsunto'),
    newConversationMessage: document.getElementById('nuevoMensaje'),
    newConversationAttachments: document.getElementById('nuevoAdjuntos'),
    newConversationAttachmentsSummary: document.getElementById('nuevoAdjuntosResumen'),
    newConversationStatus: null,
    btnNuevaConversacion: document.getElementById('btnNuevaConversacion'),
    btnNuevaConversacionHeader: document.getElementById('btnNuevaConversacionHeader'),
    btnCancelarNuevoHilo: document.getElementById('cancelarNuevoHilo')
  };

  elements.messageStatus = document.getElementById('mensajeEstado');
  elements.newConversationStatus = document.getElementById('nuevoEstado');

  function setText(target, text){
    if(!target) return;
    target.textContent = text || '';
  }

  function formatDateTime(value){
    if(!value) return '';
    const date = new Date(value.replace(' ', 'T'));
    if(Number.isNaN(date.getTime())){
      return value;
    }
    return date.toLocaleString('es-MX', {
      year: 'numeric', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit'
    });
  }

  function updateAttachmentSummary(input, summaryEl){
    if(!summaryEl) return;
    const files = input && input.files ? Array.from(input.files) : [];
    if(!files.length){
      summaryEl.textContent = '';
      return;
    }
    const names = files.map(function(file){ return file.name; });
    let text = names.slice(0, 3).join(', ');
    if(names.length > 3){
      text += ' y ' + (names.length - 3) + ' más';
    }
    summaryEl.textContent = text;
  }

  function clearMessagesPanel(){
    if(elements.messagesPanel){
      elements.messagesPanel.innerHTML = '';
    }
    if(elements.messagesEmpty){
      elements.messagesEmpty.classList.remove('is-hidden');
    }
    if(elements.messageForm){
      elements.messageForm.classList.add('is-hidden');
    }
    setText(elements.messageStatus, '');
  }

  function renderMessage(message){
    if(!elements.messagesPanel) return;
    const bubble = document.createElement('div');
    const authorType = (message.autor_tipo || '').toLowerCase();
    bubble.className = 'message-bubble';
    if(authorType === 'soporte'){
      bubble.classList.add('support');
    }

    const author = document.createElement('div');
    author.className = 'message-author';
    author.textContent = message.autor_nombre || (authorType === 'soporte' ? 'Equipo de soporte' : 'Tú');
    bubble.appendChild(author);

    const text = document.createElement('div');
    text.className = 'message-text';
    text.textContent = message.mensaje || '';
    bubble.appendChild(text);

    if(Array.isArray(message.adjuntos) && message.adjuntos.length){
      const attachments = document.createElement('div');
      attachments.className = 'attachments';
      message.adjuntos.forEach(function(adj){
        if(!adj || !adj.download_url) return;
        const link = document.createElement('a');
        link.href = resolveAppUrl(adj.download_url);
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.innerHTML = '<i class="fa fa-paperclip"></i><span>' + (adj.nombre || 'Adjunto') + '</span>';
        attachments.appendChild(link);
      });
      bubble.appendChild(attachments);
    }

    const footer = document.createElement('div');
    footer.className = 'message-footer';
    footer.textContent = formatDateTime(message.created_at);
    bubble.appendChild(footer);

    elements.messagesPanel.appendChild(bubble);
  }

  function renderMessages(messages){
    if(!elements.messagesPanel) return;
    elements.messagesPanel.innerHTML = '';
    if(!messages || !messages.length){
      if(elements.messagesEmpty){
        elements.messagesEmpty.classList.remove('is-hidden');
      }
      return;
    }
    if(elements.messagesEmpty){
      elements.messagesEmpty.classList.add('is-hidden');
    }
    messages.forEach(renderMessage);
    elements.messagesPanel.scrollTop = elements.messagesPanel.scrollHeight;
  }

  function buildConversationMeta(conversation){
    if(!conversation) return '';
    var updated = conversation.ultimo_created_at || conversation.updated_at || conversation.created_at;
    var meta = [];
    if(updated){
      meta.push('Actualizado ' + formatDateTime(updated));
    }
    var total = typeof conversation.total_mensajes === 'number' ? conversation.total_mensajes : null;
    if(total !== null){
      meta.push(total + (total === 1 ? ' mensaje' : ' mensajes'));
    }
    if(conversation.estado){
      meta.push('Estado: ' + conversation.estado);
    }
    return meta.join(' • ');
  }

  function renderConversations(){
    if(!elements.conversationList) return;
    elements.conversationList.innerHTML = '';
    if(!state.conversations.length){
      if(elements.conversationEmpty){
        elements.conversationEmpty.textContent = 'Sin conversaciones activas. ¡Inicia una nueva!';
        elements.conversationEmpty.classList.remove('is-hidden');
      }
      return;
    }
    if(elements.conversationEmpty){
      elements.conversationEmpty.classList.add('is-hidden');
    }
    state.conversations.forEach(function(conversation){
      const item = document.createElement('div');
      item.className = 'conversation-item';
      if(conversation.id === state.activeConversationId){
        item.classList.add('active');
      }
      item.dataset.id = String(conversation.id);

      const title = document.createElement('h3');
      title.textContent = conversation.asunto || 'Sin asunto';
      if(conversation.sin_leer && conversation.sin_leer > 0){
        const badge = document.createElement('span');
        badge.className = 'badge-unread';
        badge.textContent = conversation.sin_leer > 99 ? '99+' : String(conversation.sin_leer);
        title.appendChild(badge);
      }
      item.appendChild(title);

      const meta = document.createElement('div');
      meta.className = 'conversation-meta';
      const updated = conversation.ultimo_created_at || conversation.updated_at;
      meta.textContent = updated ? formatDateTime(updated) : 'Sin actividad';
      item.appendChild(meta);

      const preview = document.createElement('p');
      preview.className = 'conversation-preview';
      preview.textContent = conversation.ultimo_mensaje ? conversation.ultimo_mensaje.slice(0, 140) : 'Aún no hay mensajes.';
      item.appendChild(preview);

      item.addEventListener('click', function(){
        openConversation(conversation.id);
      });

      elements.conversationList.appendChild(item);
    });
  }

  function setActiveConversation(conversation){
    state.activeConversationId = conversation ? conversation.id : null;
    if(elements.conversationSubject){
      elements.conversationSubject.textContent = conversation ? (conversation.asunto || 'Conversación') : 'Selecciona una conversación';
    }
    if(elements.conversationMeta){
      elements.conversationMeta.textContent = conversation ? buildConversationMeta(conversation) : 'Aquí verás los detalles del hilo seleccionado.';
    }
    if(conversation){
      if(elements.messageForm){
        elements.messageForm.classList.remove('is-hidden');
      }
      if(elements.newConversationForm){
        elements.newConversationForm.classList.add('is-hidden');
      }
    }
  }

  function updateConversationFromResponse(payload){
    if(!payload || !payload.conversation) return;
    const info = payload.conversation;
    const existingIndex = state.conversations.findIndex(function(item){ return item.id === info.id; });
    const updatedConversation = {
      id: info.id,
      asunto: info.asunto,
      estado: info.estado,
      created_at: info.created_at,
      updated_at: info.updated_at,
      total_mensajes: info.total_mensajes || (payload.message ? 1 : 0),
      sin_leer: payload.unread && typeof payload.unread.mensajes === 'number' ? payload.unread.mensajes : 0,
      ultimo_mensaje: payload.message ? payload.message.mensaje : info.ultimo_mensaje,
      ultimo_autor: payload.message ? payload.message.autor_nombre : info.ultimo_autor,
      ultimo_autor_tipo: payload.message ? payload.message.autor_tipo : info.ultimo_autor_tipo,
      ultimo_created_at: payload.message ? payload.message.created_at : info.ultimo_created_at
    };

    if(existingIndex === -1){
      state.conversations.unshift(updatedConversation);
    } else {
      state.conversations.splice(existingIndex, 1);
      state.conversations.unshift(updatedConversation);
    }
    renderConversations();
  }

  function loadConversations(){
    if(state.loadingList) return;
    state.loadingList = true;
    if(elements.conversationEmpty){
      elements.conversationEmpty.textContent = 'Cargando conversaciones…';
      elements.conversationEmpty.classList.remove('is-hidden');
    }
    fetchJson(resolveAppUrl('/backend/mensajeria/list_conversations.php'))
      .then(function(data){
        state.loadingList = false;
        state.conversations = Array.isArray(data.conversaciones) ? data.conversaciones : [];
        state.empresaId = data.empresa_id || null;
        renderConversations();
        if(state.activeConversationId){
          var current = state.conversations.find(function(conv){ return conv.id === state.activeConversationId; });
          if(current){
            setActiveConversation(current);
          }
        }
        document.dispatchEvent(new Event('mensajeria:refresh'));
      })
      .catch(function(){
        state.loadingList = false;
        if(elements.conversationEmpty){
          elements.conversationEmpty.textContent = 'No fue posible cargar las conversaciones.';
          elements.conversationEmpty.classList.remove('is-hidden');
        }
      });
  }

  function markConversationRead(conversationId){
    var formData = new FormData();
    formData.append('conversation_id', conversationId);
    fetchJson(resolveAppUrl('/backend/mensajeria/mark_as_read.php'), {
      method: 'POST',
      body: formData
    }).then(function(response){
      if(response && response.unread && typeof response.unread.mensajes === 'number'){
        var conversation = state.conversations.find(function(item){ return item.id === conversationId; });
        if(conversation){
          conversation.sin_leer = 0;
        }
        renderConversations();
        document.dispatchEvent(new Event('mensajeria:refresh'));
      }
    }).catch(function(){
      // Silenciar errores de marcado
    });
  }

  function openConversation(conversationId){
    var current = state.conversations.find(function(item){ return item.id === conversationId; });
    setActiveConversation(current || null);
    if(!conversationId) return;
    fetchJson(resolveAppUrl('/backend/mensajeria/get_messages.php?conversation_id=' + encodeURIComponent(conversationId)))
      .then(function(data){
        var conversation = state.conversations.find(function(item){ return item.id === conversationId; });
        if(conversation && data && data.conversation){
          conversation.total_mensajes = data.messages ? data.messages.length : conversation.total_mensajes;
          conversation.updated_at = data.conversation.updated_at || conversation.updated_at;
          conversation.ultimo_created_at = data.messages && data.messages.length ? data.messages[data.messages.length - 1].created_at : conversation.ultimo_created_at;
          conversation.sin_leer = data.unread && typeof data.unread.mensajes === 'number' ? data.unread.mensajes : conversation.sin_leer;
          renderConversations();
          setActiveConversation(conversation);
        }
        renderMessages(data && data.messages ? data.messages : []);
        if(elements.messageForm){
          elements.messageForm.classList.remove('is-hidden');
        }
        markConversationRead(conversationId);
      })
      .catch(function(error){
        clearMessagesPanel();
        setText(elements.messagesEmpty, error && error.message ? error.message : 'No fue posible cargar los mensajes.');
      });
  }

  function toggleNewConversation(show){
    if(!elements.newConversationForm || !elements.messageForm) return;
    if(show){
      elements.newConversationForm.classList.remove('is-hidden');
      elements.messageForm.classList.add('is-hidden');
      if(elements.messagesEmpty){
        elements.messagesEmpty.classList.remove('is-hidden');
        elements.messagesEmpty.textContent = 'Completa el formulario para crear un nuevo hilo.';
      }
      state.activeConversationId = null;
      renderConversations();
      setActiveConversation(null);
    } else {
      elements.newConversationForm.classList.add('is-hidden');
      if(state.activeConversationId){
        elements.messageForm.classList.remove('is-hidden');
      }
      if(elements.messagesEmpty){
        elements.messagesEmpty.textContent = 'Selecciona un hilo para ver los mensajes o crea uno nuevo.';
      }
    }
  }

  function handleNewConversationSubmit(event){
    event.preventDefault();
    if(!elements.newConversationForm) return;
    setText(elements.newConversationStatus, 'Enviando…');
    var formData = new FormData(elements.newConversationForm);
    fetchJson(resolveAppUrl('/backend/mensajeria/create_conversation.php'), {
      method: 'POST',
      body: formData
    }).then(function(data){
      elements.newConversationForm.reset();
      updateAttachmentSummary(elements.newConversationAttachments, elements.newConversationAttachmentsSummary);
      setText(elements.newConversationStatus, 'Hilo creado correctamente.');
      toggleNewConversation(false);
      if(data && data.conversation){
        loadConversations();
        setTimeout(function(){
          openConversation(data.conversation.id);
        }, 100);
      }
      document.dispatchEvent(new Event('mensajeria:refresh'));
    }).catch(function(error){
      setText(elements.newConversationStatus, error && error.message ? error.message : 'No fue posible crear el hilo.');
    });
  }

  function handleMessageSubmit(event){
    event.preventDefault();
    if(!state.activeConversationId){
      setText(elements.messageStatus, 'Selecciona un hilo antes de enviar un mensaje.');
      return;
    }
    setText(elements.messageStatus, 'Enviando…');
    var formData = new FormData(elements.messageForm);
    formData.append('conversation_id', state.activeConversationId);
    fetchJson(resolveAppUrl('/backend/mensajeria/post_message.php'), {
      method: 'POST',
      body: formData
    }).then(function(data){
      elements.messageForm.reset();
      updateAttachmentSummary(elements.messageAttachments, elements.messageAttachmentsSummary);
      setText(elements.messageStatus, 'Mensaje enviado.');
      if(data && data.message){
        renderMessage(data.message);
        elements.messagesPanel.scrollTop = elements.messagesPanel.scrollHeight;
      }
      if(data && data.conversation){
        loadConversations();
      }
      document.dispatchEvent(new Event('mensajeria:refresh'));
    }).catch(function(error){
      setText(elements.messageStatus, error && error.message ? error.message : 'No fue posible enviar el mensaje.');
    });
  }

  function bindEvents(){
    if(elements.btnNuevaConversacion){
      elements.btnNuevaConversacion.addEventListener('click', function(){ toggleNewConversation(true); });
    }
    if(elements.btnNuevaConversacionHeader){
      elements.btnNuevaConversacionHeader.addEventListener('click', function(){ toggleNewConversation(true); });
    }
    if(elements.btnCancelarNuevoHilo){
      elements.btnCancelarNuevoHilo.addEventListener('click', function(){
        toggleNewConversation(false);
        setText(elements.newConversationStatus, '');
      });
    }
    if(elements.newConversationForm){
      elements.newConversationForm.addEventListener('submit', handleNewConversationSubmit);
    }
    if(elements.messageForm){
      elements.messageForm.addEventListener('submit', handleMessageSubmit);
    }
    if(elements.messageAttachments){
      elements.messageAttachments.addEventListener('change', function(){
        updateAttachmentSummary(elements.messageAttachments, elements.messageAttachmentsSummary);
      });
    }
    if(elements.newConversationAttachments){
      elements.newConversationAttachments.addEventListener('change', function(){
        updateAttachmentSummary(elements.newConversationAttachments, elements.newConversationAttachmentsSummary);
      });
    }
  }

  function init(){
    bindEvents();
    loadConversations();
  }

  document.addEventListener('DOMContentLoaded', init);
})();
