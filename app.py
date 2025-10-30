from flask import Flask, request, render_template, send_from_directory
import os
import csv
import re
import json
import hashlib
from datetime import datetime
from dateutil import parser
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

app = Flask(__name__)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# File directories
UPLOAD_FOLDER = 'uploads'
CSV_FOLDER = 'csv_files'
RECONVERTED_FOLDER = 'reconverted'
COUNTER_FILE = 'counter.json'

# NSDL/CDSL constants
Bnfcry = "1207650000865934"
RAW_HEADER_PREFIX = "076500DPADM"
CONSTANT_FIELDS = {
    "Bnfcry": Bnfcry,
    "Flg": "S",
    "Trf": "X",
    "Rsn": "2",
    "Paymod": "2"
}

# Tag orders for each depository
RAW_ORDER_NSDL = [
    "Tp", "Dt", "Bnfcry", "ISIN", "Qty", "Flg", "Trf", "Clnt", "Brkr",
    "Rsn", "Conamt", "Paymod", "Bnkno", "Bnkname", "Brnchname", "Xferdt", "Chqrefno"
]
RAW_ORDER_CDSL = [
    "Tp", "Dt", "Bnfcry", "CtrPty", "ISIN", "Qty", "Flg", "Trf",
    "Rsn", "Conamt", "Paymod", "Bnkno", "Bnkname", "Brnchname", "Xferdt", "Chqrefno"
]

# Database models
class FileCounter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    last_id = db.Column(db.Integer, default=1)
    last_date = db.Column(db.String(10))

class FileHash(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_hash = db.Column(db.String(64), unique=True)
    filename = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class UploadedFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_id = db.Column(db.String(10))
    original_filename = db.Column(db.String(255))
    csv_content = db.Column(db.Text)
    raw_content = db.Column(db.Text)
    raw_filename = db.Column(db.String(255))
    file_hash = db.Column(db.String(64))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(RECONVERTED_FOLDER, exist_ok=True)

# Initialize database (non-blocking)
def init_db():
    try:
        with app.app_context():
            db.create_all()
            print("Database initialized successfully")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        print("App will continue with filesystem fallback")

# Initialize database in background
import threading
threading.Thread(target=init_db, daemon=True).start()

def get_next_file_id():
    """
    Returns an incrementing integer starting from 1.
    Fallback to file-based counter if database fails.
    """
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        counter = FileCounter.query.first()
        if not counter:
            counter = FileCounter(last_id=1, last_date=today)
            db.session.add(counter)
            db.session.commit()
            return 1
        
        if counter.last_date != today:
            counter.last_id = 1
            counter.last_date = today
        else:
            counter.last_id += 1
        
        db.session.commit()
        return counter.last_id
    except Exception:
        # Fallback to file-based counter
        return get_file_counter()

def get_file_counter():
    """Fallback file-based counter"""
    today = datetime.now().strftime('%Y-%m-%d')
    if not os.path.exists(COUNTER_FILE):
        data = {"last_id": 1, "last_date": today}
        with open(COUNTER_FILE, 'w') as f:
            json.dump(data, f)
        return 1
    
    with open(COUNTER_FILE, 'r') as f:
        data = json.load(f)
    
    if data.get("last_date") != today:
        last_id = 1
        data["last_date"] = today
    else:
        last_id = data.get("last_id", 0) + 1
    
    data["last_id"] = last_id
    with open(COUNTER_FILE, 'w') as f:
        json.dump(data, f)
    
    return last_id

@app.route('/health')
def health():
    return "OK", 200

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    file = request.files.get('csv_file')
    if not file:
        return "No file uploaded."

    # Read CSV content
    csv_content = file.read().decode('utf-8')
    file_hash = hashlib.sha256(csv_content.encode()).hexdigest()
    
    # Check for duplicate (skip if database unavailable)
    try:
        existing_file = UploadedFile.query.filter_by(file_hash=file_hash).first()
        if existing_file:
            return f"File already exists: <a href='/download/reconverted/{existing_file.raw_filename}'>{existing_file.raw_filename}</a>"
    except Exception:
        pass  # Continue without duplicate check
    
    # Parse CSV
    reader = csv.reader(csv_content.splitlines())
    all_rows = list(reader)

    if len(all_rows) < 2:
        return "CSV format invalid."

    headers = [h.strip() for h in all_rows[0]]
    data_rows = all_rows[1:]
    row_count = str(len(data_rows)).zfill(6)

    # Extract Dt from first row (for validation)
    row_dict_first = dict(zip(headers, data_rows[0]))
    if "Dt" not in row_dict_first or not row_dict_first["Dt"]:
        return "Missing Dt in first row."

    try:
        file_date = parser.parse(row_dict_first["Dt"], dayfirst=True).strftime('%d%m%Y')
    except Exception:
        return f"Invalid Dt format in first row: {row_dict_first['Dt']}"

    # Ensure all rows share the same Dt
    for row in data_rows:
        row_dict = dict(zip(headers, row))
        if "Dt" in row_dict and row_dict["Dt"]:
            try:
                dt_val = parser.parse(row_dict["Dt"], dayfirst=True).strftime('%d%m%Y')
                if dt_val != file_date:
                    return f"Mismatch: Dt {dt_val} does not match file date {file_date}"
            except Exception:
                return f"Invalid Dt format: {row_dict['Dt']}"

    # --- Get file ID first ---
    file_id_int = get_next_file_id()
    file_id = str(file_id_int).zfill(5)
    
    # --- Naming and header per requirements ---
    todays_date = datetime.now().strftime('%d%m%Y')

    # Filename: 18<Bnfcry>.<todaysdate>.<counter>
    filename = f"18{Bnfcry}.{todays_date}.{file_id}"
    reconvert_path = os.path.join(RECONVERTED_FOLDER, filename)

    # RAW header: 076500DPADM <row_count><counter><todays_date>
    raw_header = f"{RAW_HEADER_PREFIX} {row_count}{file_id}{todays_date}"
    output_lines = [raw_header]

    # Build RAW body
    for row in data_rows:
        row_dict = {k: (v.strip() if isinstance(v, str) else v) for k, v in dict(zip(headers, row)).items()}

        # Normalize Dt from CSV to ddmmyyyy
        if "Dt" in row_dict and row_dict["Dt"]:
            try:
                row_dict["Dt"] = parser.parse(row_dict["Dt"], dayfirst=True).strftime('%d%m%Y')
            except Exception:
                return f"Invalid Dt format: {row_dict.get('Dt')}"

        # Force Xferdt = Dt (from CSV)
        row_dict["Xferdt"] = row_dict.get("Dt", "")

        # --- Decide NSDL vs CDSL on CtrPty and set Tp ---
        ctrpty = row_dict.get("CtrPty", "")
        if not ctrpty or len(ctrpty) != 16:
            return f"Invalid CtrPty: {ctrpty}. Must be exactly 16 characters."

        is_nsdl = ctrpty.startswith("IN")
        tp_value = "4" if is_nsdl else "5"

        # NSDL: split CtrPty into Brkr (first 8) and Clnt (last 8)
        if is_nsdl:
            row_dict["Brkr"] = ctrpty[:8]
            row_dict["Clnt"] = ctrpty[8:]
            # Do not include CtrPty tag in NSDL body
            # (keeping row_dict["CtrPty"] around is harmless since we won't render it)
            order = RAW_ORDER_NSDL
        else:
            # CDSL: keep full CtrPty and do NOT include Clnt/Brkr
            row_dict["CtrPty"] = ctrpty
            # Clean out any accidental Clnt/Brkr keys
            row_dict.pop("Clnt", None)
            row_dict.pop("Brkr", None)
            order = RAW_ORDER_CDSL

        # Format Qty and Conamt
        qty = row_dict.get("Qty", "").replace(",", "")
        if qty != "":
            try:
                row_dict["Qty"] = f"{float(qty):.3f}"
            except ValueError:
                return f"Invalid Qty: {row_dict.get('Qty')}"

        conamt = row_dict.get("Conamt", "").replace(",", "")
        if conamt != "":
            try:
                row_dict["Conamt"] = f"{float(conamt):.2f}"
            except ValueError:
                return f"Invalid Conamt: {row_dict.get('Conamt')}"

        # Ensure Chqrefno is 8 digits (zero-padded)
        chqref = (row_dict.get("Chqrefno") or "").strip()
        row_dict["Chqrefno"] = chqref.zfill(8) if chqref else "00000000"

        # Merge constants and override Tp
        merged = {**CONSTANT_FIELDS, **row_dict}
        merged["Tp"] = tp_value

        # Build the line in the correct tag order for this row
        line = ''.join([f"<{k}>{merged.get(k, '')}</{k}>" for k in order])
        output_lines.append(line)

    output_lines.append("")  # trailing newline
    raw_content = '\n'.join(output_lines)
    
    # Save to database (optional)
    try:
        uploaded_file = UploadedFile(
            file_id=file_id,
            original_filename=file.filename,
            csv_content=csv_content,
            raw_content=raw_content,
            raw_filename=filename,
            file_hash=file_hash
        )
        db.session.add(uploaded_file)
        db.session.commit()
    except Exception as e:
        print(f"Database save failed: {e}")
        # Continue with filesystem only
    
    # Also save to filesystem for backward compatibility
    csv_path = os.path.join(CSV_FOLDER, f"{file_id}.csv")
    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    
    with open(reconvert_path, 'w', encoding='utf-8') as f:
        f.write(raw_content)

    return f"Raw file created: <a href='/download/reconverted/{filename}'>{filename}</a>"

@app.route('/download/<folder>/<filename>')
def download_file(folder, filename):
    # Try database first
    if folder == 'reconverted':
        db_file = UploadedFile.query.filter_by(raw_filename=filename).first()
        if db_file:
            from flask import Response
            return Response(
                db_file.raw_content,
                mimetype='text/plain',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
    elif folder == 'csv':
        db_file = UploadedFile.query.filter_by(file_id=filename.replace('.csv', '')).first()
        if db_file:
            from flask import Response
            return Response(
                db_file.csv_content,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
    
    # Fallback to filesystem
    folder_path = {
        "csv": CSV_FOLDER,
        "reconverted": RECONVERTED_FOLDER
    }.get(folder)
    
    return send_from_directory(folder_path, filename, as_attachment=True)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)