// Enhanced Modal Management - Load this after main.js to override

// More robust show loading
function showLoading() {
    const modalElement = document.getElementById('loadingModal');
    if (!modalElement) {
        console.error('Loading modal element not found');
        return;
    }
    
    // Remove any existing modal instances
    const existingModal = bootstrap.Modal.getInstance(modalElement);
    if (existingModal) {
        existingModal.dispose();
    }
    
    // Create fresh modal instance and show
    const modal = new bootstrap.Modal(modalElement, {
        backdrop: 'static',
        keyboard: false
    });
    modal.show();
}

// Force hide loading with cleanup
function hideLoading() {
    const modalElement = document.getElementById('loadingModal');
    if (!modalElement) {
        console.error('Loading modal element not found');
        return;
    }
    
    try {
        // Get modal instance
        const modal = bootstrap.Modal.getInstance(modalElement);
        if (modal) {
            modal.hide();
            // Wait for animation then dispose
            setTimeout(() => {
                modal.dispose();
            }, 300);
        }
        
        // Force cleanup
        setTimeout(() => {
            // Remove all backdrops
            document.querySelectorAll('.modal-backdrop').forEach(backdrop => {
                backdrop.remove();
            });
            
            // Clean body classes and styles
            document.body.classList.remove('modal-open');
            document.body.style.overflow = '';
            document.body.style.paddingRight = '';
            
            // Hide modal element
            modalElement.classList.remove('show');
            modalElement.style.display = 'none';
            modalElement.setAttribute('aria-hidden', 'true');
            modalElement.removeAttribute('aria-modal');
            modalElement.removeAttribute('role');
        }, 350);
        
    } catch (error) {
        console.error('Error hiding modal:', error);
        // Force hide anyway
        document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
        document.body.classList.remove('modal-open');
        document.body.style.overflow = '';
        modalElement.style.display = 'none';
    }
}
