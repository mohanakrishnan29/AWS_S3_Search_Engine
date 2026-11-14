// Visualization functionality using Chart.js

let monthlyChart = null;

// Load monthly statistics
async function loadMonthlyStats() {
    try {
        const response = await fetch('/api/stats/monthly');
        const data = await response.json();
        
        if (data.success) {
            createMonthlyChart(data.labels, data.data);
            displayStats(data);
        } else {
            document.getElementById('statsContent').innerHTML = `
                <div class="alert alert-danger">
                    Error loading statistics: ${data.error}
                </div>
            `;
        }
    } catch (error) {
        document.getElementById('statsContent').innerHTML = `
            <div class="alert alert-danger">
                Error loading statistics: ${error.message}
            </div>
        `;
    }
}

// Create monthly chart
function createMonthlyChart(labels, data) {
    const ctx = document.getElementById('monthlyChart').getContext('2d');
    
    if (monthlyChart) {
        monthlyChart.destroy();
    }
    
    monthlyChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Number of Objects',
                data: data,
                backgroundColor: 'rgba(54, 162, 235, 0.5)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                title: {
                    display: true,
                    text: 'S3 Objects Distribution by Month',
                    font: {
                        size: 16
                    }
                },
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Objects'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Month'
                    }
                }
            }
        }
    });
}

// Display statistics
function displayStats(data) {
    const total = data.data.reduce((sum, val) => sum + val, 0);
    const average = Math.round(total / data.data.length);
    const max = Math.max(...data.data);
    const min = Math.min(...data.data);
    
    const maxMonth = data.labels[data.data.indexOf(max)];
    const minMonth = data.labels[data.data.indexOf(min)];
    
    document.getElementById('statsContent').innerHTML = `
        <div class="row">
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title">Total Objects</h5>
                        <p class="display-6 text-primary">${total.toLocaleString()}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title">Average/Month</h5>
                        <p class="display-6 text-success">${average.toLocaleString()}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title">Peak Month</h5>
                        <p class="h5 text-warning">${maxMonth}</p>
                        <p class="text-muted">${max.toLocaleString()} objects</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card text-center">
                    <div class="card-body">
                        <h5 class="card-title">Lowest Month</h5>
                        <p class="h5 text-info">${minMonth}</p>
                        <p class="text-muted">${min.toLocaleString()} objects</p>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// Load stats on page load
document.addEventListener('DOMContentLoaded', () => {
    loadMonthlyStats();
});
