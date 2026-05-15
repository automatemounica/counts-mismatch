# Counts Mismatch Verification Tool

## Overview

The Counts Mismatch Verification Tool is a Python-based automation solution designed to validate and compare dashboard counts, API counts, SQL Server data, and MongoDB data across multiple tenants and applications.

The application helps identify count mismatches between different data sources and generates CSV summary reports for quick analysis through a modern web-based interface.

---

# Features

* Multi-tenant validation support
* REST API count verification
* SQL Server count comparison
* MongoDB count comparison
* Dashboard vs API validation
* CSV report generation
* Web-based UI for execution and monitoring
* Automated mismatch detection
* Summary report generation
* Configurable tenant and application mapping
* Login page with authentication support
* Modern tab-based UI navigation
* Separate modules for:
  * Schedules
  * Reports
  * Email Configurations
  * Count Verification
* Dev environment configuration and execution support
* Improved execution monitoring and usability

---

# Technologies Used

* Python
* Flask
* SQL Server
* MongoDB
* HTML/CSS
* JavaScript
* REST APIs
* Git & GitHub

---

# Project Structure

```text
Counts Mismatch/
│
├── templates/
│   ├── index.html
│   ├── login.html
│   ├── schedules.html
│   ├── reports.html
│   └── email_configurations.html
│
├── static/
│   ├── logo.png
│   ├── styles.css
│   └── scripts.js
│
├── CSV files _New/
│   └── Generated CSV reports
│
├── csv_Generated_files_old/
│   └── Old generated reports
│
├── sql_test process.py
├── test_process.py
├── tenants.py
├── web_app.py
├── recipients.json
├── last_run.json
├── requirements.txt
└── README.md
```

---

# Setup Instructions

## 1. Clone Repository

```bash
git clone https://github.com/automatemounica/counts-mismatch.git
```

---

## 2. Navigate to Project Folder

```bash
cd counts-mismatch
```

---

## 3. Create Virtual Environment

```bash
python -m venv venv
```

Activate virtual environment:

### Windows

```bash
venv\Scripts\activate
```

### Linux / Mac

```bash
source venv/bin/activate
```

---

## 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Running the Application

## Run Flask Web Application

```bash
python web_app.py
```

Default URL:

```text
http://127.0.0.1:5000
```

---
# Functional Workflow

1. User logs in through the Login Page.
2. Validate tenants from the configured tenant and credential files.
3. Generate authentication tokens for the selected tenant/environment.
4. Fetch API counts dynamically from configured application endpoints.
5. Compare dashboard counts with API response counts.
6. Compare SQL Server and MongoDB counts for configured applications.
7. Identify mismatches across Dashboard, API, SQL Server, and MongoDB data sources.
8. Generate CSV summary reports with execution and mismatch details.
9. Display execution status and results in the web application.
10. Manage schedules, reports, and email configurations through the tab-based UI.
11. Execute and monitor validations in the Dev environment before production usage.

---

# Sample Use Cases

* Dashboard vs API count verification
* SQL Server and MongoDB data consistency validation
* Tenant-wise application monitoring
* Automated mismatch identification and reporting
* Production, UAT, and Dev environment validation
* REST API data verification
* Scheduled count verification execution
* Email-based report and notification management
* Multi-tenant monitoring and analysis
* Execution tracking through web-based UI

---

# Recommended Improvements

* Add automated scheduler execution support
* Add email notification and alert system
* Implement role-based authentication and access control
* Add live execution progress monitoring
* Add export functionality for Excel and PDF reports
* Implement automatic retry handling for failed executions
* Add centralized logging and monitoring support
* Add execution history and audit tracking
* Add graphical dashboard analytics and charts
* Add environment-wise execution management
* Add configurable threshold-based mismatch alerts
* Improve UI responsiveness and performance

---

# Git Commands

## Push New Changes

```bash
git add .
git commit -m "Updated project"
git push
```

---

# Author

Developed by Exceego Infolabs Pvt Ltd

GitHub:
[https://github.com/automatemounica](https://github.com/automatemounica)

---

# License

This project is intended for internal automation and verification purposes.
