// TechBid Marketplace — main.js

// Flash alert auto-dismiss handled in base.html

// Confirm dangerous actions
document.addEventListener('DOMContentLoaded', () => {
  // Prevent double-submit on any form with data-confirm
  document.querySelectorAll('form[data-confirm]').forEach(form => {
    form.addEventListener('submit', e => {
      if (!confirm(form.dataset.confirm)) e.preventDefault();
    });
  });

  // Prevent double-click on submit buttons
  document.querySelectorAll('form').forEach(form => {
    form.addEventListener('submit', () => {
      setTimeout(() => {
        form.querySelectorAll('button[type=submit]').forEach(btn => {
          btn.disabled = true;
          if (!btn.dataset.originalText) btn.dataset.originalText = btn.textContent;
          btn.textContent = 'Please wait…';
        });
      }, 10);
    });
  });
});
