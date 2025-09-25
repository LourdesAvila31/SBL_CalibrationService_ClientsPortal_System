(function(global){
  function normalizeColumns(columns){
    if(!Array.isArray(columns)) return [];
    return columns.map(function(col){
      if(typeof col === 'string'){
        return { key: col, label: col };
      }
      return {
        key: col && col.key ? col.key : '',
        label: col && col.label ? col.label : (col && col.key ? col.key : '')
      };
    }).filter(function(col){ return col.key; });
  }

  function getValueFromKeyPath(item, key){
    if(!key) return '';
    return key.split('.').reduce(function(acc, part){
      if(acc === null || acc === undefined) return '';
      var value = acc[part];
      return value === null || value === undefined ? '' : value;
    }, item);
  }

  function sanitizeValue(value){
    if(value === null || value === undefined) return '';
    if(value instanceof Date) return value.toISOString();
    return String(value);
  }

  function buildCsvContent(data, columns){
    var header = columns.map(function(col){
      return '"' + sanitizeValue(col.label).replace(/"/g, '""') + '"';
    }).join(',');

    var rows = data.map(function(item){
      return columns.map(function(col){
        var rawValue = getValueFromKeyPath(item, col.key);
        var cell = sanitizeValue(rawValue).replace(/"/g, '""');
        return '"' + cell + '"';
      }).join(',');
    });

    return [header].concat(rows).join('\r\n');
  }

  function downloadBlob(blob, filename){
    var link = document.createElement('a');
    var url = URL.createObjectURL(blob);
    link.href = url;
    link.download = filename;
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  function exportToExcel(data, columns, filename){
    if(!Array.isArray(data) || !data.length){
      console.warn('No hay datos para exportar a Excel.');
      return;
    }
    var normalizedColumns = normalizeColumns(columns);
    if(!normalizedColumns.length){
      console.warn('No se proporcionaron columnas para la exportación.');
      return;
    }
    var csvContent = '\uFEFF' + buildCsvContent(data, normalizedColumns);
    var blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    var finalName = filename && typeof filename === 'string' && filename.trim() ? filename.trim() : 'export';
    if(!/\.csv$/i.test(finalName)){
      finalName += '.csv';
    }
    downloadBlob(blob, finalName);
  }

  function exportToPDF(data, columns, filename){
    if(!Array.isArray(data) || !data.length){
      console.warn('No hay datos para exportar a PDF.');
      return;
    }
    if(!(window.jspdf && window.jspdf.jsPDF)){
      console.error('jsPDF no está disponible. Asegúrate de incluir la librería antes de exportar.');
      return;
    }
    var normalizedColumns = normalizeColumns(columns);
    if(!normalizedColumns.length){
      console.warn('No se proporcionaron columnas para la exportación.');
      return;
    }

    var jsPDF = window.jspdf.jsPDF;
    var orientation = normalizedColumns.length > 5 ? 'landscape' : 'portrait';
    var doc = new jsPDF({ orientation: orientation, unit: 'pt', format: 'a4' });
    var headers = normalizedColumns.map(function(col){ return sanitizeValue(col.label); });
    var rows = data.map(function(item){
      return normalizedColumns.map(function(col){
        var rawValue = getValueFromKeyPath(item, col.key);
        return sanitizeValue(rawValue);
      });
    });

    if (typeof doc.autoTable === 'function') {
      doc.autoTable({
        head: [headers],
        body: rows,
        startY: 50,
        styles: {
          fontSize: 9,
          cellPadding: 4,
          overflow: 'linebreak'
        },
        headStyles: {
          fillColor: [34, 123, 155],
          textColor: 255
        },
        alternateRowStyles: {
          fillColor: [245, 247, 250]
        },
        margin: { top: 50, right: 36, bottom: 40, left: 36 }
      });
    } else {
      var margin = 36;
      var pageWidth = doc.internal.pageSize.getWidth();
      var pageHeight = doc.internal.pageSize.getHeight();
      var usableWidth = pageWidth - margin * 2;
      var columnWidth = usableWidth / normalizedColumns.length;
      var startY = margin + 18;
      var lineHeight = 16;

      doc.setFontSize(12);
      headers.forEach(function(label, index){
        doc.text(String(label), margin + columnWidth * index, margin);
      });

      var currentY = startY;

      rows.forEach(function(row){
        if(currentY > pageHeight - margin){
          doc.addPage();
          currentY = startY;
          headers.forEach(function(label, index){
            doc.text(String(label), margin + columnWidth * index, margin);
          });
        }

        row.forEach(function(cell, index){
          var textLines = doc.splitTextToSize(cell, columnWidth - 4);
          doc.text(textLines, margin + columnWidth * index, currentY);
        });

        currentY += lineHeight;
      });
    }

    var finalName = filename && typeof filename === 'string' && filename.trim() ? filename.trim() : 'export';
    if(!/\.pdf$/i.test(finalName)){
      finalName += '.pdf';
    }
    doc.save(finalName);
  }

  global.exportHelpers = {
    exportToExcel: exportToExcel,
    exportToPDF: exportToPDF
  };
  global.exportToExcel = exportToExcel;
  global.exportToPDF = exportToPDF;
})(window);

(function(){
  document.addEventListener('DOMContentLoaded', function(){
    var modalEl = document.getElementById('documentExportModal');
    var modalForm = document.getElementById('documentExportModalForm');
    var confirmBtn = document.getElementById('confirmDocumentExport');
    if(!modalEl || !modalForm || !confirmBtn){
      return;
    }

    var formatSelect = document.getElementById('exportTemplateFormat');
    var formatHelp = modalEl.querySelector('[data-format-help]');
    var descriptionEl = modalEl.querySelector('.modal-description');
    var yearInput = document.getElementById('exportTemplateYear');
    var signaturesInput = document.getElementById('exportTemplateSignatures');
    var revisionInput = document.getElementById('exportTemplateRevision');
    var templateButtons = Array.prototype.slice.call(document.querySelectorAll('[data-template-id]'));

    var modalInstance = null;
    var templatesMap = {};
    var templatesPromise = null;
    var templatesLoaded = false;
    var currentTemplateId = null;
    var currentRole = '';
    var hiddenSubmitForm = null;

    function resolveAppUrl(path){
      if(typeof window.normalizeAppUrl === 'function'){
        return window.normalizeAppUrl(path);
      }
      if(!path){
        return path;
      }
      if(/^(?:[a-z][a-z0-9+.-]*:|\/\/)/i.test(path)){
        return path;
      }
      var base = '';
      if(typeof window.BASE_URL === 'string' && window.BASE_URL){
        base = window.BASE_URL;
      } else if(window.location && window.location.pathname && window.location.pathname.indexOf('/SISTEMA-COMPUTARIZADO-ISO-17025') !== -1){
        base = '/SISTEMA-COMPUTARIZADO-ISO-17025';
      }
      if(path.charAt(0) !== '/'){
        path = '/' + path;
      }
      return (base || '') + path;
    }

    function ensureModal(){
      if(modalInstance){
        return modalInstance;
      }
      if(window.bootstrap && typeof window.bootstrap.Modal === 'function'){
        modalInstance = new window.bootstrap.Modal(modalEl);
      }
      return modalInstance;
    }

    function getNormalizedRole(source){
      var raw = '';
      if(typeof source === 'string'){
        raw = source;
      } else if(source && typeof source === 'object'){
        raw = source.role_name || source.role || '';
      } else if(window.currentUser){
        raw = window.currentUser.role_name || window.currentUser.role || '';
      }
      if(typeof window.normalizeRoleName === 'function'){
        return window.normalizeRoleName(raw);
      }
      return typeof raw === 'string' ? raw.toLowerCase().trim() : '';
    }

    function isTemplateAllowed(template){
      if(!template){
        return false;
      }
      if(!Array.isArray(template.allowed_roles) || template.allowed_roles.length === 0){
        return true;
      }
      if(!currentRole){
        return false;
      }
      return template.allowed_roles.indexOf(currentRole) !== -1;
    }

    function updateButtonsAccess(){
      templateButtons.forEach(function(button){
        var templateId = button.getAttribute('data-template-id');
        var template = templatesMap[templateId];

        if(!templatesLoaded){
          button.disabled = true;
          button.hidden = false;
          button.setAttribute('aria-hidden', 'false');
          button.title = 'Cargando plantillas disponibles...';
          return;
        }

        if(!template){
          button.disabled = true;
          button.hidden = false;
          button.setAttribute('aria-hidden', 'false');
          button.title = 'Plantilla no disponible.';
          return;
        }

        var allowed = isTemplateAllowed(template);
        button.disabled = !allowed;
        button.hidden = !allowed;
        button.setAttribute('aria-hidden', allowed ? 'false' : 'true');
        button.title = allowed ? '' : 'No tienes permisos para generar este reporte.';
      });
    }

    function setRole(role){
      if(role === currentRole){
        return;
      }
      currentRole = role || '';
      updateButtonsAccess();
    }

    function clearForm(){
      modalForm.reset();
      var currentYear = new Date().getFullYear();
      if(yearInput){
        yearInput.value = currentYear;
      }
      if(formatHelp){
        formatHelp.textContent = '';
      }
    }

    function populateFormats(template){
      if(!formatSelect){
        return;
      }
      while(formatSelect.firstChild){
        formatSelect.removeChild(formatSelect.firstChild);
      }
      var formats = Array.isArray(template && template.formats) ? template.formats : [];
      if(formats.length === 0){
        var fallback = document.createElement('option');
        var defaultFormat = template && template.default_format ? template.default_format : '';
        fallback.value = defaultFormat;
        fallback.textContent = defaultFormat ? defaultFormat.toUpperCase() : 'Automático';
        formatSelect.appendChild(fallback);
        formatSelect.disabled = true;
        if(formatHelp){
          formatHelp.textContent = 'Este reporte se genera en un único formato disponible.';
        }
        return;
      }

      formats.forEach(function(fmt){
        if(!fmt){
          return;
        }
        var option = document.createElement('option');
        var value = fmt.value || fmt.id || '';
        option.value = value;
        option.textContent = fmt.label || (value ? value.toString().toUpperCase() : 'Formato');
        formatSelect.appendChild(option);
      });

      var defaultValue = template && template.default_format ? template.default_format : '';
      if(!defaultValue && formats[0]){
        defaultValue = formats[0].value || formats[0].id || '';
      }
      if(defaultValue){
        formatSelect.value = defaultValue;
      }
      formatSelect.disabled = formats.length === 1;
      if(formatHelp){
        formatHelp.textContent = formatSelect.disabled
          ? 'Este reporte se genera en un único formato disponible.'
          : 'Selecciona el formato con el que deseas generar el reporte.';
      }
    }

    function openModal(templateId){
      var template = templatesMap[templateId];
      if(!template){
        console.warn('Plantilla no encontrada para el identificador', templateId);
        return;
      }
      currentTemplateId = templateId;
      clearForm();
      populateFormats(template);
      if(descriptionEl){
        descriptionEl.textContent = template.description || '';
      }
      if(signaturesInput){
        signaturesInput.value = '';
      }
      if(revisionInput){
        revisionInput.value = '';
      }
      var instance = ensureModal();
      if(instance){
        instance.show();
      }
    }

    function ensureHiddenForm(){
      if(hiddenSubmitForm){
        return hiddenSubmitForm;
      }
      hiddenSubmitForm = document.createElement('form');
      hiddenSubmitForm.style.display = 'none';
      hiddenSubmitForm.method = 'POST';
      hiddenSubmitForm.target = '_blank';
      document.body.appendChild(hiddenSubmitForm);
      return hiddenSubmitForm;
    }

    function appendHiddenField(form, name, value){
      if(value === undefined || value === null){
        return;
      }
      var str = value;
      if(typeof str === 'string'){
        str = str.trim();
      }
      if(str === ''){
        return;
      }
      var input = document.createElement('input');
      input.type = 'hidden';
      input.name = name;
      input.value = value;
      form.appendChild(input);
    }

    function submitCurrentTemplate(){
      if(!currentTemplateId){
        return;
      }
      var template = templatesMap[currentTemplateId];
      if(!template){
        return;
      }
      if(!modalForm.reportValidity()){
        return;
      }
      var formData = new FormData(modalForm);
      var formatValue = '';
      if(formatSelect){
        formatValue = formatSelect.value || '';
      }
      if(!formatValue && template.default_format){
        formatValue = template.default_format;
      }

      var submitForm = ensureHiddenForm();
      submitForm.innerHTML = '';
      submitForm.setAttribute('action', resolveAppUrl(template.export_url));

      appendHiddenField(submitForm, 'format', formatValue);
      appendHiddenField(submitForm, 'year', formData.get('year'));
      appendHiddenField(submitForm, 'signatures', formData.get('signatures'));
      appendHiddenField(submitForm, 'revision', formData.get('revision'));
      appendHiddenField(submitForm, 'template_id', template.id || currentTemplateId);

      var instance = ensureModal();
      if(instance){
        instance.hide();
      }

      submitForm.submit();
    }

    function ensureTemplatesLoaded(){
      if(templatesLoaded){
        return Promise.resolve(templatesMap);
      }
      if(!templatesPromise){
        templatesPromise = loadTemplates();
      }
      return templatesPromise;
    }

    function bindButtons(){
      templateButtons.forEach(function(button){
        button.addEventListener('click', function(){
          var templateId = button.getAttribute('data-template-id');
          ensureTemplatesLoaded()
            .then(function(){
              openModal(templateId);
            })
            .catch(function(){
              // El manejo de errores ya se realiza dentro de loadTemplates.
            });
        });
      });
    }

    function loadTemplates(){
      if(templatesPromise){
        return templatesPromise;
      }

      templatesPromise = fetch(resolveAppUrl('/backend/reportes/templates_metadata.php'), { credentials: 'include' })
        .then(function(response){
          if(!response.ok){
            throw new Error('No se pudo obtener el catálogo de plantillas.');
          }
          return response.json();
        })
        .then(function(payload){
          var templates = Array.isArray(payload && payload.templates) ? payload.templates : [];
          templatesMap = templates.reduce(function(acc, tpl){
            if(tpl && tpl.id){
              acc[tpl.id] = tpl;
            }
            return acc;
          }, {});
          templatesLoaded = true;
          updateButtonsAccess();
        })
        .catch(function(error){
          console.error(error);
          templateButtons.forEach(function(button){
            button.disabled = true;
            button.title = 'No se pudieron cargar las plantillas disponibles.';
          });
          templatesLoaded = false;
          templatesPromise = null;
          throw error;
        });

      return templatesPromise;
    }

    confirmBtn.addEventListener('click', submitCurrentTemplate);

    bindButtons();
    clearForm();
    loadTemplates().catch(function(){ /* handled previamente */ });
    setRole(getNormalizedRole());

    document.addEventListener('permissionsready', function(){
      setRole(getNormalizedRole());
    });

    document.addEventListener('userprofileupdated', function(event){
      setRole(getNormalizedRole(event && event.detail));
    });
  });
})();
