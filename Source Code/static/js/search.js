// Search functionality for S3 Search Engine

let currentResults = {
    dateRange: [],
    olderThan: [],
    latest: []
};

// Wait for DOM to be fully loaded before attaching event listeners
document.addEventListener('DOMContentLoaded', function() {
    console.log('Search.js: DOM loaded, attaching event listeners');
    
    // Date Range Search
    const dateRangeForm = document.getElementById('dateRangeForm');
    if (dateRangeForm) {
        dateRangeForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            console.log('Date range form submitted');
            
            const startDate = document.getElementById('startDate').value;
            const endDate = document.getElementById('endDate').value;
            const prefix = document.getElementById('prefixRange').value;
            const suffix = document.getElementById('suffixRange').value;
            
            await performSearch('/api/search/date-range', {
                start_date: startDate,
                end_date: endDate,
                prefix: prefix,
                suffix: suffix
            }, 'dateRangeResults', 'dateRangeStatus', 'exportDateRange', 'dateRange');
        });
    } else {
        console.error('dateRangeForm not found');
    }

    // Older Than N Days Search
    const olderThanForm = document.getElementById('olderThanForm');
    if (olderThanForm) {
        olderThanForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            console.log('Older than form submitted');
            
            const nDays = document.getElementById('nDaysOlder').value;
            const prefix = document.getElementById('prefixOlder').value;
            const suffix = document.getElementById('suffixOlder').value;
            
            await performSearch('/api/search/older-than', {
                n_days: nDays,
                prefix: prefix,
                suffix: suffix
            }, 'olderThanResults', 'olderThanStatus', 'exportOlder', 'olderThan');
        });
    } else {
        console.error('olderThanForm not found');
    }

    // Latest N Days Search
    const latestForm = document.getElementById('latestForm');
    if (latestForm) {
        latestForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            console.log('Latest form submitted');
            
            const nDays = document.getElementById('nDaysLatest').value;
            const prefix = document.getElementById('prefixLatest').value;
            const suffix = document.getElementById('suffixLatest').value;
            
            await performSearch('/api/search/latest', {
                n_days: nDays,
                prefix: prefix,
                suffix: suffix
            }, 'latestResults', 'latestStatus', 'exportLatest', 'latest');
        });
    } else {
        console.error('latestForm not found');
    }
    
    console.log('Search.js: All event listeners attached successfully');
});

// Perform search
async function performSearch(endpoint, data, resultsId, statusId, exportBtnId, resultType) {
    console.log('performSearch called:', endpoint, data);
    showLoading();
    
    try {
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        // Check if response is ok before parsing JSON
        if (!response.ok) {
            throw new Error(`Server error: ${response.status} ${response.statusText}`);
        }
        
        // Check if response has content
        const text = await response.text();
        if (!text) {
            throw new Error('Server returned empty response');
        }
        
        const result = JSON.parse(text);
        
        if (result.success) {
            currentResults[resultType] = result.results;
            displayResults(
                result.results, 
                resultsId, 
                statusId, 
                result.count, 
                result.search_time,
                result.truncated,
                result.total_count,
                result.max_results
            );
            document.getElementById(exportBtnId).disabled = result.results.length === 0;
        } else {
            displayError(result.error, statusId);
        }
    } catch (error) {
        console.error('Search error:', error);
        displayError(error.message, statusId);
    } finally {
        // Ensure modal is hidden even if errors occur
        setTimeout(() => hideLoading(), 100);
    }
}

// Display results
function displayResults(results, resultsId, statusId, count, searchTime, truncated, totalCount, maxResults) {
    const resultsDiv = document.getElementById(resultsId);
    const statusDiv = document.getElementById(statusId);
    
    // Update selection manager with current results
    if (typeof selectionManager !== 'undefined') {
        selectionManager.setResults(results);
    }
    
    // Provide defaults for optional parameters
    truncated = truncated || false;
    totalCount = totalCount || count;
    maxResults = maxResults || count;
    
    // Display status with truncation warning if applicable
    let statusHTML = `
        <div class="search-status success fade-in">
            <strong>Search completed!</strong> Found <strong>${count}</strong> object(s) in <strong>${searchTime}</strong> seconds.
        </div>
    `;
    
    if (truncated) {
        statusHTML += `
            <div class="alert alert-warning mt-2 fade-in">
                <strong>⚠️ Results Truncated:</strong> Found <strong>${totalCount.toLocaleString()}</strong> total objects, 
                but only showing the first <strong>${maxResults.toLocaleString()}</strong> to prevent browser overload.
                <br><small>💡 Tip: Use more specific filters (prefix/suffix) or export to CSV for full results.</small>
            </div>
        `;
    }
    
    statusDiv.innerHTML = statusHTML;
    
    // Display results
    if (results.length === 0) {
        resultsDiv.innerHTML = `
            <div class="alert alert-info mt-3 fade-in">
                No results found matching your criteria.
            </div>
        `;
        selectionManager.clear(); // Clear selection when no results
        return;
    }
    
    let tableHTML = `
        <div class="results-table mt-3 fade-in">
            <table class="table table-striped table-hover">
                <thead class="table-dark">
                    <tr>
                        <th width="40">
                            <input type="checkbox" id="select-all-checkbox" 
                                   class="form-check-input" 
                                   onclick="toggleSelectAll()"
                                   title="Select All">
                        </th>
                        <th width="50">#</th>
                        <th>Key</th>
                        <th width="200">Timestamp</th>
                        <th width="100">Size</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    results.forEach((obj, index) => {
        const isSelected = selectionManager.isSelected(obj.key);
        const sizeFormatted = obj.size ? formatBytes(obj.size) : 'N/A';
        
        tableHTML += `
            <tr class="${isSelected ? 'table-active' : ''}">
                <td>
                    <input type="checkbox" 
                           class="form-check-input object-checkbox" 
                           data-key="${escapeHtml(obj.key)}"
                           ${isSelected ? 'checked' : ''}
                           onclick="toggleSelection('${escapeHtml(obj.key)}')">
                </td>
                <td>${index + 1}</td>
                <td><code>${escapeHtml(obj.key)}</code></td>
                <td>${formatTimestamp(obj.timestamp)}</td>
                <td>${sizeFormatted}</td>
            </tr>
        `;
    });
    
    tableHTML += `
                </tbody>
            </table>
        </div>
    `;
    
    resultsDiv.innerHTML = tableHTML;
}

// Format bytes to human readable
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Display error
function displayError(error, statusId) {
    const statusDiv = document.getElementById(statusId);
    statusDiv.innerHTML = `
        <div class="search-status error fade-in">
            <strong>Error:</strong> ${error}
        </div>
    `;
}

// Export results to CSV
async function exportResults(resultsId) {
    let results = [];
    
    if (resultsId === 'dateRangeResults') {
        results = currentResults.dateRange;
    } else if (resultsId === 'olderThanResults') {
        results = currentResults.olderThan;
    } else if (resultsId === 'latestResults') {
        results = currentResults.latest;
    }
    
    if (results.length === 0) {
        showToast('No results to export', 'warning');
        return;
    }
    
    try {
        const response = await fetch('/api/export/csv', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ results: results })
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `s3_search_results_${new Date().getTime()}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
            showToast('Results exported successfully!', 'success');
        } else {
            showToast('Error exporting results', 'danger');
        }
    } catch (error) {
        showToast(`Error: ${error.message}`, 'danger');
    }
}
