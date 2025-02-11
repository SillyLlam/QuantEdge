document.addEventListener('DOMContentLoaded', function() {
    // Tab switching functionality
    const tabs = document.querySelectorAll('.tab');
    const tabContents = document.querySelectorAll('.tab-content');

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.getAttribute('data-tab');
            
            // Update active tab
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            // Update active content
            tabContents.forEach(content => {
                if (content.getAttribute('id') === target) {
                    content.classList.add('active');
                } else {
                    content.classList.remove('active');
                }
            });
        });
    });

    // File upload handling
    const fileUpload = document.getElementById('file-upload');
    const fileUploadArea = document.querySelector('.file-upload-area');
    const uploadForm = document.getElementById('upload-form');
    const statusTable = document.getElementById('status-table');

    if (fileUploadArea) {
        fileUploadArea.addEventListener('click', () => fileUpload.click());
        fileUploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            fileUploadArea.classList.add('dragover');
        });
        fileUploadArea.addEventListener('dragleave', () => {
            fileUploadArea.classList.remove('dragover');
        });
        fileUploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            fileUploadArea.classList.remove('dragover');
            fileUpload.files = e.dataTransfer.files;
            updateFileLabel();
        });
    }

    if (fileUpload) {
        fileUpload.addEventListener('change', updateFileLabel);
    }

    function updateFileLabel() {
        const label = document.querySelector('.file-upload-label');
        if (fileUpload.files.length > 0) {
            label.textContent = fileUpload.files[0].name;
        }
    }

    // Form submission
    if (uploadForm) {
        uploadForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const formData = new FormData(uploadForm);
            
            try {
                const response = await fetch('/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                if (result.success) {
                    showAlert('File uploaded successfully!', 'success');
                    updateStatusTable();
                } else {
                    showAlert('Error uploading file: ' + result.error, 'error');
                }
            } catch (error) {
                showAlert('Error uploading file: ' + error.message, 'error');
            }
        });
    }

    // Status table updates
    function updateStatusTable() {
        if (!statusTable) return;

        fetch('/status')
            .then(response => response.json())
            .then(data => {
                const tbody = statusTable.querySelector('tbody');
                tbody.innerHTML = '';

                data.forEach(item => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>${item.filename}</td>
                        <td>${item.source_dept}</td>
                        <td>${item.target_dept}</td>
                        <td><span class="status-badge ${item.status.toLowerCase()}">${item.status}</span></td>
                        <td>${item.timestamp}</td>
                    `;
                    tbody.appendChild(row);
                });
            })
            .catch(error => {
                console.error('Error updating status table:', error);
            });
    }

    // Alert handling
    function showAlert(message, type) {
        const alertsContainer = document.getElementById('alerts');
        if (!alertsContainer) return;

        const alert = document.createElement('div');
        alert.className = `alert alert-${type}`;
        alert.textContent = message;

        alertsContainer.appendChild(alert);
        setTimeout(() => alert.remove(), 5000);
    }

    // Initial status table update
    if (statusTable) {
        updateStatusTable();
        setInterval(updateStatusTable, 5000);
    }
});
