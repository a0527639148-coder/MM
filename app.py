import os
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor, execute_batch
from flask import Flask, render_template, request, redirect, url_for, flash, Response

app = Flask(__name__)
app.secret_key = 'super_secret_key_change_me'

# Configuration
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/mishloach_db")

def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT COUNT(*) as count FROM person")
    total_families = cur.fetchone()['count']
    cur.execute('SELECT COUNT(*) as count FROM "Order"')
    total_orders = cur.fetchone()['count']
    cur.execute("SELECT COUNT(*) as count FROM outerapporder WHERE status='waiting'")
    pending_external = cur.fetchone()['count']
    cur.close()
    conn.close()
    return render_template('index.html', families=total_families, orders=total_orders, pending=pending_external)

@app.route('/residents', methods=['GET', 'POST'])
def residents():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == 'POST':
        file = request.files['file']
        if file:
            try:
                df = None
                filename = file.filename.lower()
                print(f"DEBUG: Processing file: {filename}")

                # אסטרטגיה 1: אם זה אקסל, נסה לקרוא כאקסל
                if filename.endswith('.xlsx'):
                    try:
                        file.seek(0) # חובה לחזור להתחלה!
                        df = pd.read_excel(file, engine='openpyxl')
                        print("DEBUG: Success reading XLSX")
                    except Exception as e:
                        print(f"DEBUG: Failed reading as XLSX: {e}")
                
                # אסטרטגיה 2: אם זה CSV (או אם האקסל נכשל), נסה את כל הקידודים
                if df is None:
                    # סדר עדיפויות: קודם עברית ווינדוס (הכי נפוץ בייצוא), אח"כ UTF8
                    encodings = ['cp1255', 'utf-8', 'windows-1252', 'iso-8859-8']
                    
                    for enc in encodings:
                        try:
                            file.seek(0) # חובה לחזור להתחלה לפני כל ניסיון!
                            print(f"DEBUG: Trying encoding {enc}...")
                            df = pd.read_csv(file, encoding=enc)
                            print(f"DEBUG: Success with encoding {enc}")
                            break
                        except Exception as e:
                            print(f"DEBUG: Failed with {enc}")
                            continue
                
                if df is None:
                    raise ValueError("כשל בקריאת הקובץ בכל הפורמטים הידועים.")

                # נרמול שמות עמודות
                df.columns = [str(c).strip() for c in df.columns]
                print(f"DEBUG: Columns found: {list(df.columns)}")

                # מיפוי עמודות
                column_mapping = {
                    'שם משפחה': 'lastname', 'משפחה': 'lastname', 'שם': 'lastname',
                    'שם פרטי': 'father_name', 'פרטי': 'father_name', 'שם הבעל': 'father_name',
                    'שם האשה': 'mother_name', 'אשה': 'mother_name',
                    'רחוב': 'streetname', 'כתובת': 'streetname',
                    'מס בית': 'buildingnumber', 'מספר בית': 'buildingnumber', 'בית': 'buildingnumber',
                    'כניסה': 'entrance',
                    'דירה': 'apartmentnumber', 'מספר דירה': 'apartmentnumber',
                    'טלפון': 'phone', 'טלפון בבית': 'phone',
                    'נייד': 'mobile', 'סלולרי': 'mobile', 'נייד 1': 'mobile',
                    'נייד 2': 'mobile2', 'סלולרי 2': 'mobile2', 'נייד אשה': 'mobile2',
                    'מייל': 'email', 'דואר אלקטרוני': 'email', 'אימייל': 'email',
                    'קוד': 'code', 'קוד משפחה': 'code',
                    'הוראת קבע': 'standing_order'
                }
                df.rename(columns=column_mapping, inplace=True)

                # השלמת עמודות חסרות
                required_cols = ['code', 'lastname', 'father_name', 'mother_name', 'streetname', 'buildingnumber', 'entrance', 'apartmentnumber', 'phone', 'mobile', 'mobile2', 'email']
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = None
                
                df = df.where(pd.notnull(df), None)

                # שמירה למסד הנתונים
                cur.execute("TRUNCATE TABLE raw_residents_csv RESTART IDENTITY")
                
                data_values = []
                for _, row in df.iterrows():
                    val = (
                        str(row['code']) if row['code'] else None,
                        str(row['lastname']) if row['lastname'] else '',
                        str(row['father_name']) if row['father_name'] else '',
                        str(row['mother_name']) if row['mother_name'] else '',
                        str(row['streetname']) if row['streetname'] else '',
                        str(row['buildingnumber']) if row['buildingnumber'] else '',
                        str(row['entrance']) if row['entrance'] else '',
                        str(row['apartmentnumber']) if row['apartmentnumber'] else '',
                        str(row['phone']) if row['phone'] else '',
                        str(row['mobile']) if row['mobile'] else '',
                        str(row['mobile2']) if row['mobile2'] else '',
                        str(row['email']) if row['email'] else ''
                    )
                    data_values.append(val)

                insert_query = """
                INSERT INTO raw_residents_csv 
                (code, lastname, father_name, mother_name, streetname, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                execute_batch(cur, insert_query, data_values)
                cur.execute("SELECT raw_to_temp_stage()")
                cur.execute("SELECT process_residents_csv()")
                
                flash(f'הקובץ נקלט בהצלחה! {len(data_values)} רשומות עובדו.', 'success')

            except Exception as e:
                print(f"CRITICAL ERROR: {e}")
                flash(f'שגיאה: {str(e)}', 'danger')

    # שליפת לוגים
    cur.execute("SELECT * FROM missing_streets_log ORDER BY id DESC LIMIT 10")
    missing_streets = cur.fetchall()
    cur.execute("SELECT * FROM person_archive ORDER BY created_at DESC LIMIT 20")
    archive_log = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('residents.html', missing_streets=missing_streets, archive_log=archive_log)

@app.route('/orders', methods=['GET', 'POST'])
def orders():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'upload':
            file = request.files['file']
            if file:
                try:
                    filename = file.filename.lower()
                    if filename.endswith('.csv'):
                        # לוגיקה זהה גם כאן - CSV בעברית
                        try:
                            file.seek(0)
                            df = pd.read_csv(file, encoding='cp1255')
                        except:
                            file.seek(0)
                            df = pd.read_csv(file, encoding='utf-8')
                    else:
                        df = pd.read_excel(file)

                    data_values = []
                    for _, row in df.iterrows():
                        val = (str(row.get('sender_code', '')), str(row.get('invitees', '')), str(row.get('package_size', 'סמלי')), 'upload')
                        data_values.append(val)
                    insert_query = "INSERT INTO outerapporder (sender_code, invitees, package_size, origin) VALUES (%s, %s, %s, %s)"
                    execute_batch(cur, insert_query, data_values)
                    flash('הזמנות נטענו', 'info')
                except Exception as e:
                    flash(f'Error: {str(e)}', 'danger')
        elif action == 'distribute':
            try:
                cur.execute("SELECT distribute_all_outer_orders()")
                flash('הפצה בוצעה!', 'success')
            except Exception as e:
                flash(f'Error: {str(e)}', 'danger')
    cur.execute("SELECT * FROM v_outer_distribution_status LIMIT 50")
    status_rows = cur.fetchall()
    cur.execute("SELECT * FROM outerapporder_error_log ORDER BY id DESC LIMIT 20")
    errors = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('orders.html', status_rows=status_rows, errors=errors)

@app.route('/report/<view_name>')
def report(view_name):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f'SELECT * FROM "{view_name}" LIMIT 100')
        rows = cur.fetchall()
        columns = rows[0].keys() if rows else []
    except:
        rows = []
        columns = []
    cur.close()
    conn.close()
    return render_template('report.html', view_name=view_name, rows=rows, columns=columns)

@app.route('/export/<view_name>')
def export_csv(view_name):
    conn = get_db_connection()
    df = pd.read_sql_query(f'SELECT * FROM "{view_name}"', conn)
    conn.close()
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={view_name}.csv"})

@app.route('/apply_autoreturn', methods=['POST'])
def apply_autoreturn():
    family_id = request.form.get('family_id')
    conn = get_db_connection()
    cur = conn.cursor()
    cur.callproc('apply_autoreturn_for', [int(family_id)])
    conn.close()
    return redirect(url_for('report', view_name='v_families_balance'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)