from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill
import os
import re
from io import BytesIO

app = Flask(__name__)

# --- CORE LOGIC (From your original script) ---
def smart_read_excel(file_stream):
    df_raw = pd.read_excel(file_stream, header=None)
    header_row = None
    for i in range(min(20, len(df_raw))):
        if df_raw.iloc[i].notna().sum() > 2:
            header_row = i
            break
    if header_row is None:
        raise Exception("Header row not detected")
    
    file_stream.seek(0) # Reset stream pointer
    df = pd.read_excel(file_stream, header=header_row)
    df = df.loc[:, df.columns.notna()]
    df.columns = df.columns.astype(str).str.strip()
    return df

# --- WEB ENDPOINTS ---
@app.route('/')
def index():
    # Serves the awesome UI
    return render_template('index.html')

@app.route('/api/get_columns', methods=['POST'])
def get_columns():
    """Reads the first uploaded file just to extract column headers."""
    if 'files' not in request.files:
        return jsonify({"error": "No files uploaded"}), 400
    
    files = request.files.getlist('files')
    try:
        df = smart_read_excel(files[0])
        return jsonify({"columns": list(df.columns)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/process', methods=['POST'])
def process_files():
    """Processes all files and returns the compiled Excel report."""
    if 'files' not in request.files:
        return jsonify({"error": "No files uploaded"}), 400

    files = request.files.getlist('files')
    p_col = request.form.get('p_col')
    dt_col = request.form.get('dt_col')
    
    if not p_col or not dt_col:
        return jsonify({"error": "Missing column selections"}), 400

    today = datetime.today().date()
    hourly_list = []

    try:
        for file in files:
            filename = file.filename
            match = re.search(r'_(\d{4})', filename)
            if not match:
                return jsonify({"error": f"{filename} missing HHMM format."}), 400

            time_str = match.group(1)
            hour_int = int(time_str[:2])
            display_hour = datetime.strptime(time_str, "%H%M").strftime("%I:%M %p")

            df = smart_read_excel(file)
            df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
            df = df.dropna(subset=[p_col, dt_col])
            df["Date"] = df[dt_col].dt.date

            syncing = df[df["Date"] == today][[p_col, dt_col]].copy()
            not_syncing = df[df["Date"] != today][[p_col, dt_col]].copy()

            hourly_list.append({
                "hour_int": hour_int,
                "display_hour": display_hour,
                "syncing": syncing,
                "not_syncing": not_syncing
            })

        hourly_data = sorted(hourly_list, key=lambda x: x["hour_int"])

        # --- EXPORT LOGIC ---
        wb = Workbook()
        wb.remove(wb.active)

        for sheet_type in ["syncing", "not_syncing"]:
            ws = wb.create_sheet("synced table" if sheet_type == "syncing" else "not synced till date")
            col_index = 1
            previous_sync_set = set()

            for hour_data in hourly_data:
                hour_label = hour_data["display_hour"]
                df = hour_data[sheet_type]

                ws.cell(row=1, column=col_index, value=hour_label)
                ws.cell(row=2, column=col_index, value="Panchayat")
                ws.cell(row=2, column=col_index + 1, value="DateTime")

                current_set = set(df.iloc[:, 0])
                new_entries = current_set - previous_sync_set if sheet_type == "syncing" else set()

                for row_idx, (_, row) in enumerate(df.iterrows(), start=3):
                    ws.cell(row=row_idx, column=col_index, value=row.iloc[0])
                    ws.cell(row=row_idx, column=col_index + 1, value=str(row.iloc[1]))

                    if sheet_type == "syncing" and row.iloc[0] in new_entries:
                        fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                        ws.cell(row=row_idx, column=col_index).fill = fill
                        ws.cell(row=row_idx, column=col_index + 1).fill = fill

                previous_sync_set = current_set
                col_index += 2

        # Save to memory instead of disk (Best practice for web apps)
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return send_file(
            output, 
            download_name=f"Pro_Sync_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)