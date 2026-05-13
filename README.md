# Counts Mismatch Verification Tool

## Overview

The Counts Mismatch Verification Tool is a Python-based automation solution designed to validate and compare dashboard counts, API counts, SQL Server data, and MongoDB data across multiple tenants and applications.

The application helps identify count mismatches between different data sources and generates CSV summary reports for quick analysis.

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
│   └── index.html
│
├── static/
│   └── logo.png
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

1. Validate tenants from configuration files
2. Generate authentication token
3. Fetch API counts from configured endpoints
4. Compare dashboard and API counts
5. Compare SQL Server and MongoDB counts
6. Identify mismatches
7. Generate CSV summary reports
8. Display execution results in web application

---

# Sample Use Cases

* Dashboard count verification
* Data consistency validation
* Tenant-wise count monitoring
* Automated mismatch reporting
* Production/UAT validation
* API data verification

---

# Recommended Improvements

* Add scheduler support
* Add email notification feature
* Add role-based authentication
* Add live execution monitoring
* Add export to Excel/PDF
* Add automated retry handling
* Add centralized logging

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
