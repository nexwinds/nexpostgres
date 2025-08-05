# Flask App Setup & Usage

This guide outlines the basic commands to set up and run your Flask application.

## 📦 Installation

Install all required dependencies:

```bash
pip install -r requirements.txt
```
Optional: Install Ruff for linting:
```bash
pip install ruff
```


## 🚀 Running the App

Set the required environment variable (replace the secret key in production):

```powershell
$env:SECRET_KEY = 'your-secret-key-here-change-in-production'
```

Start the Flask app:

```bash
python run.py
```

## 🧹 Code Linting with Ruff

Check code for linting issues:

```bash
ruff check .
```

Automatically fix common issues:

```bash
ruff check . --fix
```

---

> ⚠️ Remember to update the `SECRET_KEY` with a strong, secure value before deploying to production.