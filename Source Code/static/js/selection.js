// Selection Manager for S3 Object Operations

class SelectionManager {
    constructor() {
        this.selected = new Set();
        this.allResults = [];
    }
    
    // Toggle selection of a single item
    toggle(key) {
        if (this.selected.has(key)) {
            this.selected.delete(key);
        } else {
            this.selected.add(key);
        }
        this.updateUI();
    }
    
    // Select all items
    selectAll(keys) {
        keys.forEach(key => this.selected.add(key));
        this.updateUI();
    }
    
    // Clear all selections
    clear() {
        this.selected.clear();
        this.updateUI();
    }
    
    // Get selected items as array
    getSelected() {
        return Array.from(this.selected);
    }
    
    // Get count of selected items
    count() {
        return this.selected.size;
    }
    
    // Check if item is selected
    isSelected(key) {
        return this.selected.has(key);
    }
    
    // Update UI to reflect selection changes
    updateUI() {
        const count = this.count();
        const actionToolbar = document.getElementById('action-toolbar');
        const selectionCount = document.getElementById('selection-count');
        
        if (count > 0) {
            actionToolbar.classList.remove('hidden');
            selectionCount.textContent = `${count} file${count > 1 ? 's' : ''} selected`;
        } else {
            actionToolbar.classList.add('hidden');
        }
        
        // Update checkboxes
        document.querySelectorAll('.object-checkbox').forEach(checkbox => {
            checkbox.checked = this.isSelected(checkbox.dataset.key);
        });
        
        // Update select all checkbox
        const selectAllCheckbox = document.getElementById('select-all-checkbox');
        if (selectAllCheckbox) {
            selectAllCheckbox.checked = count > 0 && count === this.allResults.length;
        }
    }
    
    // Set current results (for select all functionality)
    setResults(results) {
        this.allResults = results;
    }
}

// Global selection manager instance
const selectionManager = new SelectionManager();

// Handle individual checkbox clicks
function toggleSelection(key) {
    selectionManager.toggle(key);
}

// Handle select all checkbox
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    
    if (selectAllCheckbox.checked) {
        // Select all current results
        selectionManager.selectAll(selectionManager.allResults.map(obj => obj.key));
    } else {
        // Clear all
        selectionManager.clear();
    }
}

// Copy selected files
function copySelected() {
    const selected = selectionManager.getSelected();
    
    if (selected.length === 0) {
        showToast('No files selected', 'warning');
        return;
    }
    
    // Show copy modal
    document.getElementById('modalTitle').textContent = 'Copy Files';
    document.getElementById('modalAction').value = 'copy';
    document.getElementById('fileCount').textContent = selected.length;
    
    const modal = new bootstrap.Modal(document.getElementById('actionModal'));
    modal.show();
}

// Move selected files
function moveSelected() {
    const selected = selectionManager.getSelected();
    
    if (selected.length === 0) {
        showToast('No files selected', 'warning');
        return;
    }
    
    // Show move modal
    document.getElementById('modalTitle').textContent = 'Move Files';
    document.getElementById('modalAction').value = 'move';
    document.getElementById('fileCount').textContent = selected.length;
    
    const modal = new bootstrap.Modal(document.getElementById('actionModal'));
    modal.show();
}

// Delete selected files
function deleteSelected() {
    const selected = selectionManager.getSelected();
    
    if (selected.length === 0) {
        showToast('No files selected', 'warning');
        return;
    }
    
    // Show delete confirmation modal
    document.getElementById('deleteCount').textContent = selected.length;
    const modal = new bootstrap.Modal(document.getElementById('deleteModal'));
    modal.show();
}

// Execute copy/move operation
async function executeAction() {
    const action = document.getElementById('modalAction').value;
    const destBucket = document.getElementById('destBucket').value.trim();
    const destPrefix = document.getElementById('destPrefix').value.trim();
    const preservePath = document.getElementById('preservePath').checked;
    const selected = selectionManager.getSelected();
    
    if (selected.length === 0) {
        showToast('No files selected', 'error');
        return;
    }
    
    // Validate destination bucket if provided
    if (destBucket) {
        showLoading('Validating destination bucket...');
        
        try {
            const validateResponse = await fetch(`/api/buckets/validate?bucket=${encodeURIComponent(destBucket)}`);
            const validateResult = await validateResponse.json();
            
            hideLoading();
            
            if (!validateResult.valid) {
                showToast(`Invalid destination bucket: ${validateResult.error}`, 'error');
                return;
            }
        } catch (error) {
            hideLoading();
            showToast(`Error validating bucket: ${error.message}`, 'error');
            return;
        }
    }
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('actionModal'));
    modal.hide();
    
    // Show progress
    showLoading(`${action === 'copy' ? 'Copying' : 'Moving'} ${selected.length} file(s)...`);
    
    try {
        const endpoint = action === 'copy' ? '/api/objects/copy' : '/api/objects/move';
        
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                keys: selected,
                destination_bucket: destBucket || null,
                destination_prefix: destPrefix,
                preserve_path: preservePath
            })
        });
        
        const result = await response.json();
        
        hideLoading();
        
        if (result.success) {
            const successCount = result.succeeded || result.copied || 0;
            const failedCount = result.failed || result.errors?.length || 0;
            
            if (failedCount > 0) {
                showToast(`${action === 'copy' ? 'Copied' : 'Moved'} ${successCount} file(s), ${failedCount} failed`, 'warning');
                console.error('Failed operations:', result.errors);
            } else {
                showToast(`Successfully ${action === 'copy' ? 'copied' : 'moved'} ${successCount} file(s)!`, 'success');
            }
            
            // Clear selection
            selectionManager.clear();
            
            // Refresh search results after a delay (to allow event processing)
            if (action === 'move') {
                setTimeout(() => {
                    showToast('Refreshing results...', 'info');
                    // Trigger re-search if possible
                }, 3000);
            }
        } else {
            showToast(`Error: ${result.error}`, 'error');
        }
    } catch (error) {
        hideLoading();
        showToast(`Error: ${error.message}`, 'error');
    }
}

// Execute delete operation
async function executeDelete() {
    const confirmCheckbox = document.getElementById('confirmDelete');
    
    if (!confirmCheckbox.checked) {
        showToast('Please confirm the deletion by checking the checkbox', 'warning');
        return;
    }
    
    const selected = selectionManager.getSelected();
    
    if (selected.length === 0) {
        showToast('No files selected', 'error');
        return;
    }
    
    // Close modal
    const modal = bootstrap.Modal.getInstance(document.getElementById('deleteModal'));
    modal.hide();
    
    // Show progress
    showLoading(`Deleting ${selected.length} file(s)...`);
    
    try {
        const response = await fetch('/api/objects/delete', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                keys: selected
            })
        });
        
        const result = await response.json();
        
        hideLoading();
        
        if (result.success) {
            const deletedCount = result.deleted || 0;
            const failedCount = result.failed || 0;
            
            if (failedCount > 0) {
                showToast(`Deleted ${deletedCount} file(s), ${failedCount} failed`, 'warning');
                console.error('Failed deletions:', result.errors);
            } else {
                showToast(`Successfully deleted ${deletedCount} file(s)!`, 'success');
            }
            
            // Clear selection
            selectionManager.clear();
            
            // Refresh search results after a delay (to allow event processing)
            setTimeout(() => {
                showToast('Results will update automatically via event processing', 'info');
            }, 2000);
        } else {
            showToast(`Error: ${result.error}`, 'error');
        }
        
        // Reset confirmation checkbox
        confirmCheckbox.checked = false;
    } catch (error) {
        hideLoading();
        showToast(`Error: ${error.message}`, 'error');
    }
}
