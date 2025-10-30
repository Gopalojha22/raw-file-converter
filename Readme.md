# 🧾 CSV ↔ RAW File Converter (Flask App)

This Flask-based project allows users to:

- ✅ Convert custom XML-style RAW files into CSV format
- ✅ Convert structured CSV files back into RAW format with fixed field order
- ✅ Prevent duplicate uploads using file hash (SHA256)
- ✅ Maintain persistent file IDs using a counter system

---

## 📁 Project Structure

```
.
├── app.py                  # Main Flask application
├── counter.json            # Tracks the latest used file ID
├── hashes.json             # Tracks file hashes to detect duplicates
├── uploads/                # RAW file uploads
├── csv_files/              # Generated CSVs
├── reconverted/            # Final RAW outputs from CSVs
├── templates/
│   └── index.html          # HTML upload UI
├── requirements.txt        # Python dependencies
└── README.md               # Project guide
```

---

## 🚀 How to Run the Project

### 🪟 1. For Windows (without Virtual Environment)

```bash
pip install -r requirements.txt
python app.py
```

Then open: `http://127.0.0.1:5000/` in your browser.

---

### 🔐 2. Using Virtual Environment (Recommended)

#### a. Install `venv` if not available:

- **Windows**

  ```bash
  python -m ensurepip --upgrade
  ```

- **Mac/Linux (Debian/Ubuntu/Fedora/Arch)**

  ```bash
  sudo apt install python3-venv
  ```

  or

  ```bash
  sudo dnf install python3-venv       # Fedora
  sudo pacman -S python-virtualenv    # Arch
  ```

---

#### b. Create and activate Virtual Environment

- **Windows**

  ```bash
  python -m venv venv
  venv\Scripts\activate
  ```

- **Mac/Linux**
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  ```

---

#### c. Install dependencies and run

```bash
pip install -r requirements.txt
python app.py
```

Visit: `http://127.0.0.1:5000/`

---

## 💡 Features Recap

- ✅ RAW → CSV using HTML-like tags extraction
- ✅ CSV → RAW with fixed structure and constants
- ✅ Unique output filenames using `Bnfcry.Date.ID` format
- ✅ Prevents counter increment if same CSV is re-uploaded
- ✅ SHA256-based content deduplication using `hashes.json`

---

## 🧪 Dependencies

Stored in `requirements.txt`:

```
Flask
python-dateutil
```

Install with:

```bash
pip install -r requirements.txt
```

---

## 📜 License

MIT License. Use freely and modify for your custom workflows.
