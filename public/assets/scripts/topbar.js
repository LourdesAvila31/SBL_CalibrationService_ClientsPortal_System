// Global Topbar initialization: user info + notifications + menu handlers
(function initTopbar(){
  const appUrl = typeof window !== 'undefined' && window.AppUrl ? window.AppUrl : {};
  const normalizeAppUrl = typeof appUrl.normalizeAppUrl === 'function'
    ? appUrl.normalizeAppUrl
    : (typeof window.normalizeAppUrl === 'function' ? window.normalizeAppUrl : (value => value));

  function resolveUrl(path){
    if(typeof normalizeAppUrl === 'function'){
      return normalizeAppUrl(path);
    }
    return path;
  }

  function ensureElement(parent, selector, factory){
    if(!parent) return null;
    var existing = parent.querySelector(selector);
    if(existing) return existing;
    var created = typeof factory === 'function' ? factory() : null;
    if(created){
      parent.appendChild(created);
    }
    return created || parent.querySelector(selector);
  }

  function findTopbar(node){
    var current = node;
    while(current){
      if(current.classList && current.classList.contains('topbar')){
        return current;
      }
      current = current.parentElement || current.parentNode;
    }
    return null;
  }

  function elementMatches(el, selector){
    if(!el || !selector) return false;
    var fn = el.matches || el.msMatchesSelector || el.webkitMatchesSelector;
    if(typeof fn === 'function'){
      try {
        return fn.call(el, selector);
      } catch (err) {
        return false;
      }
    }
    return false;
  }

  function nodeContainsTopbar(node){
    if(!node) return false;
    if(node.nodeType === 1){
      if(elementMatches(node, '.topbar')){
        return true;
      }
      if(typeof node.querySelector === 'function' && node.querySelector('.topbar')){
        return true;
      }
    }
    if(node.nodeType === 11 && node.childNodes){
      for(var i = 0; i < node.childNodes.length; i++){
        if(nodeContainsTopbar(node.childNodes[i])){
          return true;
        }
      }
    }
    return false;
  }

  function detectAppScope(contextNode){
    if(contextNode){
      if(contextNode.dataset && contextNode.dataset.appScope){
        return String(contextNode.dataset.appScope).toLowerCase();
      }
      var attrScope = contextNode.getAttribute && contextNode.getAttribute('data-app-scope');
      if(attrScope){
        return String(attrScope).toLowerCase();
      }
    }
    var pathname = '';
    if(typeof window !== 'undefined' && window.location && typeof window.location.pathname === 'string'){
      pathname = window.location.pathname;
    }
    if(pathname.indexOf('/apps/tenant/') !== -1){
      return 'tenant';
    }
    return 'internal';
  }

  function normalizeFeedbackUrl(value, contextNode){
    if(value === null || value === undefined){
      return value;
    }
    var raw = String(value).trim();
    if(!raw){
      return raw;
    }
    if(/^([a-z]+:)?\/\//i.test(raw)){
      return raw;
    }
    if(raw.startsWith('/')){
      return raw;
    }
    var sanitized = raw.replace(/^\.\/?/, '');
    if(sanitized.startsWith('apps/tenant/feedback/') || sanitized.startsWith('apps/internal/feedback/')){
      return '/' + sanitized;
    }
    if(sanitized.startsWith('feedback/')){
      sanitized = sanitized.slice('feedback/'.length);
    }
    var scope = detectAppScope(contextNode);
    var basePath = scope === 'tenant' ? '/apps/tenant/feedback/' : '/apps/internal/feedback/';
    return basePath + sanitized;
  }

  function detectDefaultFeedbackUrl(contextNode){
    var explicit = null;
    var topbar = findTopbar(contextNode);
    if(topbar){
      if(topbar.dataset && topbar.dataset.feedbackUrl){
        explicit = topbar.dataset.feedbackUrl;
      }
      if(!explicit){
        explicit = topbar.getAttribute('data-feedback-url');
      }
      if(!explicit){
        var icons = topbar.querySelector('.topbar-icons');
        if(icons){
          if(icons.dataset && icons.dataset.feedbackUrl){
            explicit = icons.dataset.feedbackUrl;
          }
          if(!explicit){
            explicit = icons.getAttribute('data-feedback-url');
          }
        }
      }
    }
    if(explicit){
      return normalizeFeedbackUrl(explicit, topbar || contextNode);
    }
    var scope = detectAppScope(topbar || contextNode);
    var fallback = scope === 'tenant' ? 'feedback_externos.html' : 'feedback_sbl.html';
    return normalizeFeedbackUrl(fallback, topbar || contextNode);
  }

  function readProfileUrlFromNode(node){
    if(!node){
      return null;
    }
    if(node.dataset && node.dataset.profileUrl){
      return node.dataset.profileUrl;
    }
    if(typeof node.getAttribute === 'function'){
      var attr = node.getAttribute('data-profile-url');
      if(attr){
        return attr;
      }
    }
    return null;
  }

  function sanitizeProfileUrl(value){
    if(value === null || value === undefined){
      return '';
    }
    var trimmed = String(value).trim();
    if(!trimmed){
      return '';
    }
    if(/^(?:[a-z][a-z0-9+.-]*:|\/\/)/i.test(trimmed)){
      return trimmed;
    }
    if(trimmed.charAt(0) !== '/'){
      trimmed = '/' + trimmed.replace(/^\/+/, '');
    }
    return trimmed;
  }

  function buildUserMenu(menu){
    if(!menu) return;
    menu.classList.add('dropdown-panel');
    if(!menu.classList.contains('is-hidden') && !menu.classList.contains('is-visible')){
      menu.classList.add('is-hidden');
    }

    var topbar = findTopbar(menu);
    var scope = detectAppScope(topbar || menu);
    var existingMenuButton = menu && typeof menu.querySelector === 'function'
      ? menu.querySelector('.btn-perfil')
      : null;
    var detectedProfileUrl = readProfileUrlFromNode(menu)
      || readProfileUrlFromNode(existingMenuButton)
      || readProfileUrlFromNode(topbar)
      || (topbar ? readProfileUrlFromNode(topbar.querySelector('.topbar-icons')) : null);

    menu.innerHTML = '';

    if(!detectedProfileUrl && scope === 'service'){
      detectedProfileUrl = 'apps/service/usuarios/my_information.html';
    }

    var profileUrl = sanitizeProfileUrl(detectedProfileUrl || '/apps/internal/usuarios/my_information.html');

    var sectionTitle = document.createElement('div');
    sectionTitle.className = 'menu-section-title';
    sectionTitle.textContent = 'Mi perfil';
    menu.appendChild(sectionTitle);

    var menuNombre = document.createElement('div');
    menuNombre.id = 'menuNombre';
    menu.appendChild(menuNombre);

    var menuRol = document.createElement('div');
    menuRol.id = 'menuRol';
    menu.appendChild(menuRol);

    var profileBtn = document.createElement('button');
    profileBtn.type = 'button';
    profileBtn.className = 'btn-perfil';
    profileBtn.textContent = 'Mi información';
    if(profileBtn.dataset){
      profileBtn.dataset.profileUrl = profileUrl;
    } else {
      profileBtn.setAttribute('data-profile-url', profileUrl);
    }
    profileBtn.addEventListener('click', function(){
      var targetUrl = sanitizeProfileUrl(readProfileUrlFromNode(profileBtn) || profileUrl);
      if(!targetUrl){
        return;
      }
      window.location.href = resolveUrl(targetUrl);
    });
    menu.appendChild(profileBtn);

    var logoutBtn = document.createElement('button');
    logoutBtn.type = 'button';
    logoutBtn.className = 'btn-cerrar';
    logoutBtn.textContent = 'Cerrar sesión';
    logoutBtn.addEventListener('click', function(){
      window.location.href = resolveUrl('/backend/usuarios/logout.php');
    });
    menu.appendChild(logoutBtn);
  }

  function normalizeTopbarStructure(){
    var topbars = document.querySelectorAll('.topbar');
    if(!topbars.length) return;

    topbars.forEach(function(topbar){
      var icons = ensureElement(topbar, '.topbar-icons', function(){
        var div = document.createElement('div');
        div.className = 'topbar-icons';
        return div;
      });
      if(!icons) return;

      var existingFeedbackBtn = icons.querySelector('#btnFeedback');
      var preferredFeedbackUrl = (existingFeedbackBtn && existingFeedbackBtn.dataset && existingFeedbackBtn.dataset.feedbackUrl)
        || (existingFeedbackBtn && existingFeedbackBtn.getAttribute('data-feedback-url'))
        || topbar.getAttribute('data-feedback-url')
        || icons.getAttribute('data-feedback-url')
        || detectDefaultFeedbackUrl(topbar);
      preferredFeedbackUrl = normalizeFeedbackUrl(preferredFeedbackUrl, topbar);

      var feedbackBtn = ensureElement(icons, '#btnFeedback', function(){
        var icon = document.createElement('i');
        icon.id = 'btnFeedback';
        icon.className = 'fa fa-comment-dots';
        if(preferredFeedbackUrl){
          icon.setAttribute('data-feedback-url', preferredFeedbackUrl);
        }
        return icon;
      });
      if(feedbackBtn){
        var anchor = icons.querySelector('#btnBell')
          || icons.querySelector('.topbar-user-box');
        if(anchor && anchor !== feedbackBtn){
          icons.insertBefore(feedbackBtn, anchor);
        }
      }
      if(feedbackBtn){
        feedbackBtn.id = 'btnFeedback';
        feedbackBtn.classList.add('fa', 'fa-comment-dots');
        var resolvedFeedbackUrl = (feedbackBtn.dataset && feedbackBtn.dataset.feedbackUrl)
          || feedbackBtn.getAttribute('data-feedback-url')
          || preferredFeedbackUrl
          || detectDefaultFeedbackUrl(feedbackBtn);
        resolvedFeedbackUrl = normalizeFeedbackUrl(resolvedFeedbackUrl, topbar || feedbackBtn);
        if(resolvedFeedbackUrl){
          feedbackBtn.setAttribute('data-feedback-url', resolvedFeedbackUrl);
          if(feedbackBtn.dataset){
            feedbackBtn.dataset.feedbackUrl = resolvedFeedbackUrl;
          }
        }
      }

      var userBox = ensureElement(icons, '.topbar-user-box', function(){
        var box = document.createElement('div');
        box.className = 'topbar-user-box';
        return box;
      });
      if(!userBox) return;

      var userIcon = ensureElement(userBox, '#btnUser', function(){
        var icon = document.createElement('i');
        icon.id = 'btnUser';
        icon.className = 'fa fa-user-circle';
        return icon;
      });
      if(userIcon){
        userIcon.id = 'btnUser';
        userIcon.classList.add('fa', 'fa-user-circle');
      }

      var userName = ensureElement(userBox, '#userName', function(){
        var span = document.createElement('span');
        span.id = 'userName';
        span.className = 'topbar-user';
        return span;
      });
      if(userName){
        userName.id = 'userName';
        userName.classList.add('topbar-user');
      }

      var userMenu = ensureElement(userBox, '#userMenu', function(){
        var menu = document.createElement('div');
        menu.id = 'userMenu';
        return menu;
      });
      buildUserMenu(userMenu);

      var notificationsMenu = topbar.querySelector('#notificationsMenu');
      if(notificationsMenu){
        notificationsMenu.classList.add('dropdown-panel');
        if(!notificationsMenu.innerHTML.trim()){
          notificationsMenu.innerHTML = '<div class="menu-section-title">Notificaciones</div>'
            + '<div id="notiList"><div class="text-muted-sm">No hay notificaciones nuevas.</div></div>';
        }
      }
    });
  }

  const originalUpdateUserDisplay = typeof window.updateUserDisplay === 'function'
    ? window.updateUserDisplay
    : null;

  if(originalUpdateUserDisplay){
    window.updateUserDisplay = function(user){
      originalUpdateUserDisplay.call(this, user);
      document.dispatchEvent(new CustomEvent('userprofileupdated', { detail: user }));
    };
  }

  function renderEmptyNotifications(){
    return '<div class="text-muted-sm">No hay notificaciones nuevas.</div>';
  }

  function escapeHtml(value){
    if(value === null || value === undefined) return '';
    return String(value).replace(/[&<>"']/g, function(ch){
      switch(ch){
        case '&': return '&amp;';
        case '<': return '&lt;';
        case '>': return '&gt;';
        case '"': return '&quot;';
        case "'": return '&#39;';
        default: return ch;
      }
    });
  }

  var notificationTypeLabels = {
    general: 'General',
    feedback_estado: 'Reporte',
    calibration_alert: 'Calibración'
  };

  function formatNotificationType(type){
    if(!type) return '';
    var key = String(type).toLowerCase();
    if(notificationTypeLabels.hasOwnProperty(key)){
      return notificationTypeLabels[key];
    }
    return key.replace(/_/g, ' ').replace(/\b\w/g, function(letter){ return letter.toUpperCase(); });
  }

  function renderNotificationItem(n){
    n = n || {};
    var metaParts = [];
    var typeLabel = formatNotificationType(n.tipo);
    if(typeLabel){
      metaParts.push('<span class="text-muted-sm">' + escapeHtml(typeLabel) + '</span>');
    }
    if(n.fecha){
      metaParts.push('<span class="text-muted-sm">' + escapeHtml(n.fecha) + '</span>');
    }
    var metaHtml = metaParts.length ? '<div class="notification-meta">' + metaParts.join(' · ') + '</div>' : '';
    var titulo = escapeHtml(n.titulo || '');
    var mensaje = typeof n.mensaje === 'string' ? n.mensaje : '';
    var classes = ['notification-item'];
    if(n && Object.prototype.hasOwnProperty.call(n, 'leido') && !n.leido){
      classes.push('is-unread');
    }
    return '<div class="' + classes.join(' ') + '">'
         + metaHtml
         + '<span class="notification-title">' + titulo + '</span><br>'
         + '<span class="notification-message">' + mensaje + '</span>'
         + '</div>';
  }

  function parseNotificationsPayload(data){
    var items = [];
    var unread = 0;
    if(Array.isArray(data)){
      items = data;
    } else if(data && typeof data === 'object'){
      if(Array.isArray(data.items)){
        items = data.items;
      }
      if(data.meta && typeof data.meta === 'object'){
        if(typeof data.meta.unread_user_notifications === 'number'){
          unread = data.meta.unread_user_notifications;
        } else if(data.meta.unread && typeof data.meta.unread.user === 'number'){
          unread = data.meta.unread.user;
        }
      }
      if(typeof data.unread_user_notifications === 'number'){
        unread = data.unread_user_notifications;
      } else if(data.unread && typeof data.unread.user === 'number'){
        unread = data.unread.user;
      }
    }
    return {
      items: Array.isArray(items) ? items : [],
      unread: typeof unread === 'number' && isFinite(unread) ? unread : 0
    };
  }

  function renderNotifications(items){
    var notiList = document.getElementById('notiList');
    if(!notiList) return;
    if(!Array.isArray(items) || !items.length){
      notiList.innerHTML = renderEmptyNotifications();
      return;
    }
    notiList.innerHTML = items.map(renderNotificationItem).join('');
  }

  function fetchUserNotifications(options){
    options = options || {};
    var markAsRead = options.markAsRead;
    var updateList = options.updateList !== false;
    var query = ['extended=1'];
    if(markAsRead === true){
      query.push('mark_as_read=1');
    } else if(markAsRead === false){
      query.push('mark_as_read=0');
    }
    if(typeof options.limit === 'number'){
      query.push('limit=' + encodeURIComponent(options.limit));
    }
    var url = '/backend/usuarios/notifications.php';
    if(query.length){
      url += '?' + query.join('&');
    }
    return fetch(resolveUrl(url))
      .then(function(r){
        if(!r.ok){ throw new Error('Request failed'); }
        return r.json();
      })
      .then(function(data){
        var parsed = parseNotificationsPayload(data);
        setUserNotificationUnread(parsed.unread);
        if(updateList){
          renderNotifications(parsed.items);
        }
        return parsed;
      })
      .catch(function(){
        if(updateList){
          var notiList = document.getElementById('notiList');
          if(notiList){
            notiList.innerHTML = renderEmptyNotifications();
          }
        }
        setUserNotificationUnread(notificationCounters.user || 0);
        return { items: [], unread: notificationCounters.user || 0 };
      });
  }

  function cargarNotificaciones(options){
    options = options || {};
    if(typeof options.markAsRead === 'undefined'){
      options.markAsRead = true;
    }
    return fetchUserNotifications({
      markAsRead: options.markAsRead,
      updateList: options.updateList !== false
    });
  }

  function refreshNotificationBadge(){
    return fetchUserNotifications({ markAsRead: false, updateList: false });
  }

  var notificationCounters = {
    user: 0,
    mensajeria: 0
  };

  var mensajeriaState = {
    unread: 0,
    conversations: 0
  };

  var notificationsTimer = null;

  function updateBellBadge(count){
    var btnBell = document.getElementById('btnBell');
    if(!btnBell) return;
    var badge = btnBell.querySelector('.topbar-badge');
    var parsed = typeof count === 'number' && isFinite(count) ? Math.max(0, count) : 0;
    if(parsed > 0){
      if(!badge){
        badge = document.createElement('span');
        badge.className = 'topbar-badge';
        btnBell.appendChild(badge);
      }
      badge.textContent = parsed > 99 ? '99+' : String(parsed);
      btnBell.classList.add('has-badge');
    }else if(badge){
      badge.remove();
      btnBell.classList.remove('has-badge');
    }else{
      btnBell.classList.remove('has-badge');
    }
  }

  function applyNotificationBadge(){
    var total = Math.max(0, (notificationCounters.user || 0) + (notificationCounters.mensajeria || 0));
    updateBellBadge(total);
  }

  function setUserNotificationUnread(count){
    var parsed = typeof count === 'number' && isFinite(count) ? Math.max(0, count) : 0;
    notificationCounters.user = parsed;
    applyNotificationBadge();
  }

  function setMensajeriaUnread(count){
    var parsed = typeof count === 'number' && isFinite(count) ? Math.max(0, count) : 0;
    mensajeriaState.unread = parsed;
    notificationCounters.mensajeria = parsed;
    applyNotificationBadge();
  }

  function fetchMensajeriaCounters(){
    var btnBell = document.getElementById('btnBell');
    if(!btnBell) return;
    fetch(resolveUrl('/backend/mensajeria/unread_count.php'))
      .then(function(r){ return r.ok ? r.json() : null; })
      .then(function(data){
        var unread = 0;
        var conversations = 0;
        if(data && data.unread){
          if(typeof data.unread.mensajes === 'number'){ unread = data.unread.mensajes; }
          if(typeof data.unread.conversaciones === 'number'){ conversations = data.unread.conversaciones; }
        }
        mensajeriaState.conversations = conversations;
        mensajeriaState.lastFetch = Date.now();
        setMensajeriaUnread(unread);
      })
      .catch(function(){
        setMensajeriaUnread(0);
      });
  }

  function scheduleNotificationPolling(){
    if(notificationsTimer){
      clearInterval(notificationsTimer);
    }
    notificationsTimer = setInterval(function(){
      fetchMensajeriaCounters();
      refreshNotificationBadge();
    }, 60000);
  }

  function bindMenus(){
    var btnUser = document.getElementById('btnUser');
    var btnBell = document.getElementById('btnBell');
    var btnSearch = document.getElementById('btnSearch');
    var btnFeedback = document.getElementById('btnFeedback');
    var userMenu = document.getElementById('userMenu');
    var notificationsMenu = document.getElementById('notificationsMenu');
    var searchMenu = document.getElementById('searchMenu');
    // Crear el buscador si no existe pero hay botón de lupa
    if(btnSearch && !searchMenu){
      searchMenu = document.createElement('div');
      searchMenu.id = 'searchMenu';
      searchMenu.className = 'dropdown-panel is-hidden';
      searchMenu.innerHTML = '<input type="text" class="topbar-search-input" placeholder="Buscar…" />';
      var host = btnSearch.parentElement || document.body;
      host.appendChild(searchMenu);
    }
    function showMenu(menu){
      if(!menu) return;
      menu.classList.remove('is-hidden');
      menu.classList.add('is-visible');
    }
    function hideMenu(menu){
      if(!menu) return;
      menu.classList.remove('is-visible');
      menu.classList.add('is-hidden');
    }
    function toggleMenu(menu){
      if(!menu) return;
      if(menu.classList.contains('is-visible')) hideMenu(menu);
      else showMenu(menu);
    }
    if(btnFeedback){
      var feedbackTopbar = findTopbar(btnFeedback);
      var defaultFeedbackUrl = detectDefaultFeedbackUrl(feedbackTopbar || btnFeedback);
      btnFeedback.onclick = function(e){
        hideMenu(userMenu);
        hideMenu(notificationsMenu);
        hideMenu(searchMenu);
        var targetUrl = (btnFeedback.dataset && btnFeedback.dataset.feedbackUrl)
          || btnFeedback.getAttribute('data-feedback-url')
          || defaultFeedbackUrl;
        targetUrl = normalizeFeedbackUrl(targetUrl, feedbackTopbar || btnFeedback);
        window.location.href = resolveUrl(targetUrl);
        if(e){ e.stopPropagation(); }
      };
    }
    if(btnUser && userMenu){
      btnUser.onclick = function(e){
        toggleMenu(userMenu);
        hideMenu(notificationsMenu);
        hideMenu(searchMenu);
        e.stopPropagation();
      };
      userMenu.onclick = function(e){ e.stopPropagation(); };
    }
    if(btnBell && notificationsMenu){
      btnBell.onclick = function(e){
        toggleMenu(notificationsMenu);
        hideMenu(userMenu);
        hideMenu(searchMenu);
        cargarNotificaciones({ markAsRead: true });
        fetchMensajeriaCounters();
        e.stopPropagation();
      };
      notificationsMenu.onclick = function(e){ e.stopPropagation(); };
    }
    var btnTutorial = document.getElementById('btnTutorial');
    if(btnTutorial){
      var tutorialUrlRaw = (btnTutorial.dataset && btnTutorial.dataset.tutorialUrl)
        || btnTutorial.getAttribute('data-tutorial-url')
        || '/apps/tenant/tutorial/index.html';
      var tutorialUrl = resolveUrl(tutorialUrlRaw);
      var navigateTutorial = function(event){
        if(event){ event.preventDefault(); }
        window.location.href = tutorialUrl;
      };
      btnTutorial.addEventListener('click', navigateTutorial);
      btnTutorial.addEventListener('keydown', function(event){
        if(event.key === 'Enter' || event.key === ' '){
          navigateTutorial(event);
        }
      });
    }
    if(btnSearch && searchMenu){
      btnSearch.onclick = function(e){
        toggleMenu(searchMenu);
        hideMenu(userMenu);
        hideMenu(notificationsMenu);
        var inp = searchMenu.querySelector('input.topbar-search-input');
        if(inp){ setTimeout(function(){ inp.focus(); }, 10); }
        e.stopPropagation();
      };
      searchMenu.onclick = function(e){ e.stopPropagation(); };
      var input = searchMenu.querySelector('input.topbar-search-input');
      if(input){
        input.addEventListener('input', function(){
          var target = document.querySelector('[data-topbar-search-target]');
          if(!target) return;
          var selector = target.getAttribute('data-topbar-search-target') || 'a';
          var q = (input.value||'').toLowerCase();
          var items = target.querySelectorAll(selector);
          items.forEach(function(el){
            var t = el.textContent.toLowerCase();
            var matches = !q || t.indexOf(q) !== -1;
            el.classList.toggle('is-hidden', !matches);
          });
        });
      }
    }
    document.body.addEventListener('click', function(){
      hideMenu(userMenu);
      hideMenu(notificationsMenu);
      hideMenu(searchMenu);
    });
  }

  function normalizeRole(source){
    if(!source) return '';
    if(typeof window.normalizeRoleName === 'function'){
      return window.normalizeRoleName(source);
    }
    return String(source).toLowerCase().trim();
  }

  function updateFeedbackBadge(){
    var btnFeedback = document.getElementById('btnFeedback');
    if(!btnFeedback) return;
    var badge = btnFeedback.querySelector('.topbar-badge');
    var user = window.currentUser || null;
    var role = '';
    if(user && typeof user === 'object'){
      role = normalizeRole(user.role_name || user.role || user.role_id || '');
    }
    var isSuperadmin = role === 'superadministrador';
    if(isSuperadmin){
      if(!badge){
        badge = document.createElement('span');
        badge.className = 'topbar-badge';
        btnFeedback.appendChild(badge);
      }
      if(badge && !badge.textContent){
        badge.textContent = '';
      }
      btnFeedback.classList.add('has-badge');
    }else if(badge){
      badge.remove();
      btnFeedback.classList.remove('has-badge');
    }else{
      btnFeedback.classList.remove('has-badge');
    }
  }

  function setup(){
    normalizeTopbarStructure();

    if(document.getElementById('userName')){
      if(window.currentUser && window.updateUserDisplay){
        window.updateUserDisplay(window.currentUser);
      }
      bindMenus();
    }
    updateFeedbackBadge();
    refreshNotificationBadge();
    fetchMensajeriaCounters();
    scheduleNotificationPolling();
  }

  var topbarObserver = null;

  function monitorTopbarInsertions(){
    if(typeof MutationObserver !== 'function'){
      return;
    }
    if(topbarObserver){
      return;
    }
    topbarObserver = new MutationObserver(function(mutations){
      if(!mutations || !mutations.length){
        return;
      }
      var shouldSetup = false;
      for(var i = 0; i < mutations.length; i++){
        var mutation = mutations[i];
        if(!mutation || mutation.type !== 'childList' || !mutation.addedNodes){
          continue;
        }
        for(var j = 0; j < mutation.addedNodes.length; j++){
          if(nodeContainsTopbar(mutation.addedNodes[j])){
            shouldSetup = true;
            break;
          }
        }
        if(shouldSetup){
          break;
        }
      }
      if(shouldSetup){
        setup();
      }
    });
    var observeTarget = document.documentElement || document.body;
    if(observeTarget){
      topbarObserver.observe(observeTarget, { childList: true, subtree: true });
    }
  }

  document.addEventListener('permissionsready', updateFeedbackBadge);
  document.addEventListener('userprofileupdated', updateFeedbackBadge);
  document.addEventListener('permissionsready', refreshNotificationBadge);
  document.addEventListener('userprofileupdated', refreshNotificationBadge);
  document.addEventListener('permissionsready', fetchMensajeriaCounters);
  document.addEventListener('userprofileupdated', fetchMensajeriaCounters);
  document.addEventListener('mensajeria:refresh', fetchMensajeriaCounters);

  monitorTopbarInsertions();

  const originalReadyState = document.readyState;
  if(originalReadyState === 'loading'){
    document.addEventListener('DOMContentLoaded', setup);
  } else {
    setup();
  }

  if(typeof window !== 'undefined'){
    window.AppTopbar = window.AppTopbar || {};
    window.AppTopbar.setup = setup;
    window.AppTopbar.refresh = setup;
  }
})();
