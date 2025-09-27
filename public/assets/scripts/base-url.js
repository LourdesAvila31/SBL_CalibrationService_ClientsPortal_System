(function(global){
  'use strict';

  const PROJECT_ROOT = '/SBL_SISTEMA_INTERNO/public';
  const ABSOLUTE_PROTOCOL_REGEX = /^(?:[a-z][a-z0-9+.-]*:|\/\/|#)/i;

  function trimTrailingSlash(value){
    if(typeof value !== 'string'){
      return '';
    }
    const trimmed = value.trim();
    if(!trimmed){
      return '';
    }
    return trimmed.replace(/\/+$/, '');
  }

  function readDeclaredBaseUrl(){
    const body = document.body;
    if(body && body.hasAttribute('data-base-url')){
      const value = body.getAttribute('data-base-url');
      if(value){
        return trimTrailingSlash(value);
      }
    }

    const meta = document.querySelector('meta[name="base-url"], meta[property="base-url"]');
    if(meta){
      const content = meta.getAttribute('content');
      if(content){
        return trimTrailingSlash(content);
      }
    }

    return '';
  }

  function detectBaseFromScripts(){
    try{
      const candidates = [];
      if(document.currentScript){
        candidates.push(document.currentScript);
      }
      const scripts = Array.from(document.getElementsByTagName('script'));
      scripts.forEach(function(script){
        const src = script.getAttribute('src') || '';
        if(/\/assets\/scripts\/(?:base-url|scripts)\.js$/.test(src)){
          candidates.push(script);
        }
      });

      for(let i = 0; i < candidates.length; i += 1){
        const script = candidates[i];
        if(!script){
          continue;
        }
        const rawSrc = script.getAttribute('src') || script.src || '';
        if(!rawSrc){
          continue;
        }
        const scriptUrl = new URL(rawSrc, window.location.origin + window.location.pathname);
        const match = scriptUrl.pathname.match(/^(.*)\/public\/assets\/scripts\/(?:base-url|scripts)\.js$/);
        if(match && typeof match[1] === 'string'){
          return trimTrailingSlash(match[1] + '/public');
        }
      }
    }catch(error){
      console.warn('No se pudo determinar la ruta base desde las etiquetas de script.', error);
    }

    if(window.location && typeof window.location.pathname === 'string' && window.location.pathname.includes(PROJECT_ROOT)){
      return trimTrailingSlash(PROJECT_ROOT);
    }

    return '';
  }

  function sanitizeBaseUrl(value){
    if(!value){
      return '';
    }
    if(value === '/'){
      return '';
    }
    if(!value.startsWith('/')){
      return '/' + value.replace(/^\/+/, '');
    }
    return trimTrailingSlash(value);
  }

  const declaredBase = sanitizeBaseUrl(readDeclaredBaseUrl());
  const detectedBase = declaredBase || sanitizeBaseUrl(detectBaseFromScripts());
  const BASE_URL = detectedBase;

  function normalizeAppUrl(value){
    if(!value){
      return value;
    }
    if(ABSOLUTE_PROTOCOL_REGEX.test(value)){
      return value;
    }

    let url;
    try{
      url = new URL(value, window.location.origin + window.location.pathname);
    }catch(error){
      console.warn('No se pudo normalizar la ruta', value, error);
      return value;
    }

    let pathname = url.pathname;

    if(!BASE_URL && pathname.startsWith(PROJECT_ROOT)){
      pathname = pathname.slice(PROJECT_ROOT.length) || '/';
    }

    if(!pathname.startsWith('/')){
      pathname = '/' + pathname;
    }

    const basePrefix = BASE_URL ? BASE_URL.replace(/\/$/, '') : '';
    if(basePrefix){
      const needsPrefix = pathname !== basePrefix
        && !pathname.startsWith(basePrefix + '/')
        && !(pathname === '/' && basePrefix === '');
      if(needsPrefix){
        pathname = basePrefix + pathname;
      }else if(pathname === '/' && basePrefix){
        pathname = basePrefix;
      }
    }

    return pathname + (url.search || '') + (url.hash || '');
  }

  function resolveAppUrl(path){
    if(!path){
      return path;
    }
    return normalizeAppUrl(path);
  }

  const api = {
    PROJECT_ROOT: PROJECT_ROOT,
    BASE_URL: BASE_URL,
    normalizeAppUrl: normalizeAppUrl,
    resolveAppUrl: resolveAppUrl
  };

  const existingApi = (typeof global.AppUrl === 'object' && global.AppUrl) ? global.AppUrl : {};
  global.AppUrl = Object.assign({}, existingApi, api);
  global.BASE_URL = BASE_URL;
  global.normalizeAppUrl = normalizeAppUrl;
  global.resolveAppUrl = resolveAppUrl;
})(typeof window !== 'undefined' ? window : this);
