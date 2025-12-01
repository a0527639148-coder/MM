import os
import psycopg2
import pandas as pd
import numpy as np
import re
from psycopg2.extras import RealDictCursor, execute_batch
from flask import Flask, render_template, request, redirect, url_for, flash, Response

app = Flask(__name__)
app.secret_key = 'super_secret_key_change_me'

DB_URL = os.getenv("DATABASE_URL")  # בלי ברירת מחדל

def get_db_connection():
    try:
        if not DB_URL:
            raise RuntimeError("DATABASE_URL is not set")
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None
++++++ OLD
#DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@db:5432/mishloach_db")
#def get_db_connection():
#    try:
 #       conn = psycopg2.connect(DB_URL)
  #      conn.autocommit = True
   #     return conn
   # except Exception as e:
   #     print(f"DB Connection Error: {e}")
    #    return None

# --- פונקציות עזר ---
def handle_series(val):
    if isinstance(val, pd.Series): return val.iloc[0]
    return val

def clean_int_str(val):
    val = handle_series(val)
    try:
        if pd.isna(val) or val is None or str(val).strip() == '': return None
        clean_val = re.sub(r'[^\d.]', '', str(val))
        return str(int(float(clean_val)))
    except: return str(val).strip()

def normalize_phone(p):
    if not p: return None
    clean = re.sub(r'\D', '', str(p))
    if not clean: return None
    if clean.startswith('972'): clean = '0' + clean[3:]
    clean = clean.lstrip('0')
    return '0' + clean

def safe_int(val):
    val = handle_series(val)
    try:
        if pd.isna(val): return 0
        return int(float(str(val).strip()))
    except: return 0

# --- קליטת נתונים חכמה ---
def extract_clean_data(df):
    clean_rows = []
    
    # ניקוי שמות עמודות (BOM, רווחים)
    df.columns = [str(c).strip().lower().replace('\ufeff', '').replace('"', '').replace("'", "") for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    
    field_options = {
        'code': ['code', 'order_code', 'קוד', 'קוד מזמין', 'id'],
        'lastname': ['lastname', 'last_name', 'משפחה', 'שם משפחה'],
        'father_name': ['father_name', 'father_first_name', 'שם אבא', 'שם פרטי', 'פרטי'],
        'mother_name': ['mother_name', 'mother_first_name', 'שם אמא', 'שם האשה', 'אשה'],
        'streetname': ['streetname', 'street', 'רחוב', 'כתובת'],
        'buildingnumber': ['buildingnumber', 'building_number', 'בנין', 'מס בית', 'מספר בית'],
        'entrance': ['entrance', 'כניסה'],
        'apartmentnumber': ['apartmentnumber', 'apartment_number', 'דירה', 'מספר דירה'],
        'phone': ['phone', 'home_phone', 'טלפון', 'טלפון בבית'],
        'mobile': ['mobile', 'נייד', 'נייד 1', 'סלולרי'],
        'mobile2': ['mobile2', 'נייד 2', 'נייד אשה', 'סלולרי 2'],
        'email': ['email', 'מייל', 'אימייל', 'דואר אלקטרוני'],
        'standing_order': ['standing_order', 'הוראת קבע']
    }

    # זיהוי עמודת קוד
    code_col_name = None
    for opt in field_options['code']:
        if opt in df.columns:
            code_col_name = opt
            break
    
    # Fallback: עמודה ראשונה אם היא מספרית
    if not code_col_name and len(df.columns) > 0:
        sample = df[df.columns[0]].dropna().head(5)
        if not sample.empty and all(str(x).replace('.','').isdigit() for x in sample):
            code_col_name = df.columns[0]

    detected_col_msg = f"עמודת קוד: {code_col_name}" if code_col_name else "לא זוהתה עמודת קוד (יקבלו מספר 100,000+)"

    for _, row in df.iterrows():
        new_row = {}
        val = handle_series(row[code_col_name]) if code_col_name else None
        new_row['code'] = safe_int(val) if val is not None else None

        for field, options in field_options.items():
            if field == 'code': continue
            value = None
            for opt in options:
                if opt in df.columns:
                    val = handle_series(row[opt])
                    if pd.notnull(val) and str(val).strip() != '':
                        value = val
                        break
            new_row[field] = value
        
        new_row['standing_order'] = safe_int(new_row['standing_order'])
        
        for k in ['lastname', 'father_name', 'mother_name', 'streetname', 'buildingnumber', 
                  'entrance', 'apartmentnumber', 'phone', 'mobile', 'mobile2', 'email']:
            new_row[k] = str(new_row[k]) if new_row[k] is not None else ''
        
        # דילוג על שורות ריקות
        if not new_row['lastname'] and not new_row['code']: continue
        
        clean_rows.append(new_row)
    return clean_rows, detected_col_msg

# --- מנוע הפצה (פייתון) ---
def distribute_in_python():
    conn = get_db_connection()
    if not conn: return 0, ["שגיאת חיבור"]
    cur = conn.cursor(cursor_factory=RealDictCursor)
    messages = []
    total_orders = 0
    
    try:
        # טעינת מפת תושבים (קוד וטלפון)
        cur.execute("SELECT personid, phone, mobile, mobile2 FROM person")
        people = cur.fetchall()
        id_map = {str(p['personid']): p['personid'] for p in people}
        phone_map = {}
        for p in people:
            pid = p['personid']
            for field in ['phone', 'mobile', 'mobile2']:
                norm = normalize_phone(p[field])
                if norm: phone_map[norm] = pid

        cur.execute("SELECT * FROM outerapporder WHERE status IN ('waiting', 'error')")
        orders = cur.fetchall()
        
        cur.execute("SELECT setting_value FROM delivery_settings WHERE setting_name='delivery_price'")
        res = cur.fetchone()
        price = float(res['setting_value']) if res else 10.0
        
        new_orders = []
        updates = []
        errors = []
        debug_sample = []

        for o in orders:
            try:
                sender_id = None
                raw_code = clean_int_str(o['sender_code'])
                
                # 1. לפי קוד
                if raw_code and raw_code in id_map:
                    sender_id = id_map[raw_code]
                
                # 2. לפי טלפון
                if not sender_id and o['sender_phone']:
                    norm_p = normalize_phone(o['sender_phone'])
                    if norm_p and norm_p in phone_map:
                        sender_id = phone_map[norm_p]
                
                if not sender_id:
                    if str(o['sender_code']).lower() in ['code', 'id', 'קוד', 'sender']: # כותרת
                        updates.append((o['id'], 'error'))
                        continue
                    msg = f'שולח לא נמצא (קוד: {o["sender_code"]})'
                    errors.append((o['id'], 'error', msg))
                    if len(debug_sample) < 2: debug_sample.append(msg)
                    continue

                # מוזמנים
                raw_invitees = str(o['invitees']) if o['invitees'] else ''
                tokens = re.split(r'[|,\s]+', raw_invitees)
                valid_invitees = []
                for t in tokens:
                    cln = clean_int_str(t)
                    if cln and cln in id_map:
                        valid_invitees.append(id_map[cln])
                
                if not valid_invitees:
                    errors.append((o['id'], 'error', 'אין מוזמנים תקינים'))
                    continue
                
                for getter_id in valid_invitees:
                    new_orders.append((sender_id, getter_id, price, o['id']))
                updates.append((o['id'], 'distributed'))
                
            except Exception as e:
                errors.append((o['id'], 'error', str(e)))

        if new_orders:
            insert_q = """
            INSERT INTO "Order" (delivery_sender_id, delivery_getter_id, price, origin_outer_id, order_date, origin_type, package_size)
            VALUES (%s, %s, %s, %s, CURRENT_DATE, 'invitees', 'סמלי')
            ON CONFLICT DO NOTHING
            """
            execute_batch(cur, insert_q, new_orders)
            total_orders = len(new_orders)
        
        for uid, status in updates:
            cur.execute("UPDATE outerapporder SET status=%s, processed_at=NOW(), error_message=NULL WHERE id=%s", (status, uid))
        for uid, status, msg in errors:
            cur.execute("UPDATE outerapporder SET status=%s, processed_at=NOW(), error_message=%s WHERE id=%s", (status, msg, uid))
            cur.execute("INSERT INTO outerapporder_error_log (outer_id, message) VALUES (%s, %s)", (uid, msg))

        if total_orders > 0:
            messages.append(f"הפצה הושלמה! {total_orders} הזמנות נוצרו.")
        else:
            messages.append("לא נוצרו הזמנות.")
            if debug_sample: messages.append(f"שגיאות לדוגמה: {'; '.join(debug_sample)}")
            
    except Exception as e:
        messages.append(f"Error: {e}")
    
    conn.close()
    return total_orders, messages

def auto_fix_database(cur):
    try:
        cur.execute("DROP INDEX IF EXISTS public.ux_person_unique_phone_address;")
        cur.execute("ALTER TABLE public.temp_residents_csv ADD COLUMN IF NOT EXISTS code integer;")
        cur.execute("ALTER TABLE public.outerapporder ADD COLUMN IF NOT EXISTS sender_phone text;")
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")
        
        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."raw_to_temp_stage"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        BEGIN
          TRUNCATE TABLE public.temp_residents_csv RESTART IDENTITY;
          INSERT INTO public.street (streetname)
          SELECT DISTINCT TRIM(r.streetname) FROM public.raw_residents_csv r
          WHERE TRIM(r.streetname) IS NOT NULL AND TRIM(r.streetname) <> ''
            AND NOT EXISTS (SELECT 1 FROM public.street s WHERE TRIM(s.streetname) = TRIM(r.streetname));
          INSERT INTO public.temp_residents_csv (code, lastname, father_name, mother_name, streetname, streetcode, buildingnumber, entrance, apartmentnumber, email, phone, mobile, mobile2, standing_order)
          SELECT NULLIF(regexp_replace(r.code, '[^0-9]', '', 'g'), '')::int, TRIM(r.lastname), TRIM(r.father_name), TRIM(r.mother_name), TRIM(r.streetname), s.streetcode, TRIM(r.buildingnumber), TRIM(r.entrance), TRIM(r.apartmentnumber), normalize_email(r.email), format_il_phone(r.phone), format_il_phone(r.mobile), format_il_phone(r.mobile2), r.standing_order
          FROM public.raw_residents_csv r LEFT JOIN public.street s ON TRIM(s.streetname) = TRIM(r.streetname);
        END;
        $func$;
        """)
        
        cur.execute(r"""
        CREATE OR REPLACE FUNCTION "public"."process_residents_csv"() RETURNS "void" LANGUAGE "plpgsql" AS $func$
        DECLARE
            rec RECORD; existing_person INT; target_id INT;
        BEGIN
            -- שינוי: הקפצת המונה ל-100,000 כדי לא להתנגש עם קודים ידניים (1-5000)
            PERFORM setval('public.person_personid_seq', GREATEST((SELECT COALESCE(MAX(personid), 0) + 1 FROM public.person), 100000), false);
            
            FOR rec IN SELECT * FROM temp_residents_csv LOOP
                target_id := rec.code; existing_person := NULL;
                IF target_id IS NOT NULL THEN SELECT personid INTO existing_person FROM person WHERE personid = target_id; END IF;
                IF existing_person IS NULL THEN SELECT personid INTO existing_person FROM person WHERE format_il_phone(phone) = format_il_phone(rec.phone) LIMIT 1; END IF;
                IF existing_person IS NOT NULL THEN
                    UPDATE person SET lastname = COALESCE(rec.lastname, lastname), email = COALESCE(rec.email, email), standing_order = COALESCE(rec.standing_order, standing_order), streetcode = COALESCE(rec.streetcode, streetcode) WHERE personid = existing_person;
                    UPDATE temp_residents_csv SET status = 'עודכן' WHERE temp_id = rec.temp_id;
                ELSE
                    IF target_id IS NOT NULL THEN
                        INSERT INTO person(personid, lastname, father_name, mother_name, streetcode, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order) VALUES (target_id, rec.lastname, rec.father_name, rec.mother_name, rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber, rec.phone, rec.mobile, rec.mobile2, rec.email, rec.standing_order) ON CONFLICT (personid) DO UPDATE SET lastname = EXCLUDED.lastname;
                        -- אין צורך לעדכן מונה כאן כי אנחנו משתמשים ב-ID ידני
                    ELSE
                        -- שימוש במונה האוטומטי (שכוון ל-100,000)
                        INSERT INTO person(lastname, father_name, mother_name, streetcode, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order) VALUES (rec.lastname, rec.father_name, rec.mother_name, rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber, rec.phone, rec.mobile, rec.mobile2, rec.email, rec.standing_order);
                    END IF;
                    UPDATE temp_residents_csv SET status = 'נוסף' WHERE temp_id = rec.temp_id;
                END IF;
            END LOOP;
        END;
        $func$;
        """)
    except: pass

@app.route('/')
def index():
    conn = get_db_connection()
    if not conn: return "DB Error", 500
    cur = conn.cursor(cursor_factory=RealDictCursor)
    auto_fix_database(cur)
    cur.execute("SELECT COUNT(*) as count FROM person")
    f = cur.fetchone()['count']
    cur.execute('SELECT COUNT(*) as count FROM "Order"')
    o = cur.fetchone()['count']
    cur.execute("SELECT COUNT(*) as count FROM outerapporder WHERE status='waiting'")
    p = cur.fetchone()['count']
    cur.close(); conn.close()
    return render_template('index.html', families=f, orders=o, pending=p)

@app.route('/reset_db', methods=['POST'])
def reset_db():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute('TRUNCATE TABLE "payment_ledger", "Order", "outerapporder", "person_archive", "raw_residents_csv", "temp_residents_csv" CASCADE;')
        cur.execute('DELETE FROM "person";')
        cur.execute("ALTER SEQUENCE public.person_personid_seq RESTART WITH 100000;") # איפוס חכם
        cur.execute("ALTER SEQUENCE public.order_id_seq RESTART WITH 1;")
        cur.execute("INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי') ON CONFLICT (streetcode) DO NOTHING;")
        flash('המערכת אופסה בהצלחה!', 'success')
    except Exception as e: flash(f'שגיאה: {e}', 'danger')
    conn.close()
    return redirect(url_for('index'))

@app.route('/residents', methods=['GET', 'POST'])
def residents():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    auto_fix_database(cur)
    if request.method == 'POST':
        file = request.files['file']
        if file:
            try:
                df = None
                fname = file.filename.lower()
                if fname.endswith('.xlsx'):
                    try: df = pd.read_excel(file, engine='openpyxl', header=None)
                    except: pass
                if df is None:
                    for enc in ['utf-8-sig', 'cp1255', 'utf-8', 'windows-1252', 'iso-8859-8']:
                        try:
                            file.seek(0)
                            df = pd.read_csv(file, encoding=enc, header=None, sep=None, engine='python')
                            if df.shape[1] > 1: break
                        except: continue
                
                if df is None: flash('לא ניתן לקרוא את הקובץ', 'danger')
                else:
                    # זיהוי כותרת
                    header_idx = -1
                    for i, row in df.head(30).iterrows():
                        txt = " ".join([str(v) for v in row.values]).lower()
                        if 'lastname' in txt or 'משפחה' in txt or 'code' in txt or 'קוד' in txt:
                            header_idx = i
                            break
                    if header_idx > -1:
                        df.columns = df.iloc[header_idx]
                        df = df[header_idx + 1:].reset_index(drop=True)
                    else:
                        df.columns = df.iloc[0]
                        df = df[1:].reset_index(drop=True)
                    
                    clean, diag_msg = extract_clean_data(df)
                    
                    cur.execute("TRUNCATE TABLE raw_residents_csv RESTART IDENTITY")
                    vals = []
                    sample_codes = []
                    for r in clean:
                        if r['code']: 
                            if len(sample_codes) < 5: sample_codes.append(str(r['code']))
                        vals.append((r['code'], r['lastname'], r['father_name'], r['mother_name'], r['streetname'], r['buildingnumber'], r['entrance'], r['apartmentnumber'], r['phone'], r['mobile'], r['mobile2'], r['email'], r['standing_order']))
                    
                    q = "INSERT INTO raw_residents_csv (code, lastname, father_name, mother_name, streetname, buildingnumber, entrance, apartmentnumber, phone, mobile, mobile2, email, standing_order) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    execute_batch(cur, q, vals)
                    
                    cur.execute("SELECT raw_to_temp_stage()")
                    cur.execute("SELECT process_residents_csv()")
                    
                    # בדיקת מקסימום
                    cur.execute("SELECT MAX(personid) as m FROM person")
                    mx = cur.fetchone()['m']
                    
                    flash(f'קובץ נקלט! {len(vals)} רשומות. {diag_msg}. (ID מקסימלי: {mx}, דוגמאות: {", ".join(sample_codes)})', 'success')
            except Exception as e: flash(f'שגיאה: {e}', 'danger')
    
    cur.execute("SELECT * FROM missing_streets_log ORDER BY id DESC LIMIT 10")
    missing = cur.fetchall()
    cur.execute("SELECT * FROM person_archive ORDER BY created_at DESC LIMIT 20")
    log = cur.fetchall()
    cur.close(); conn.close()
    return render_template('residents.html', missing_streets=missing, archive_log=log)

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
                    df = None
                    if file.filename.lower().endswith('.csv'):
                        try: file.seek(0); df = pd.read_csv(file, encoding='utf-8-sig')
                        except: file.seek(0); df = pd.read_csv(file, encoding='cp1255')
                    else: file.seek(0); df = pd.read_excel(file, engine='openpyxl')
                    
                    clean, _ = extract_clean_data(df)
                    # שחזור הזמנות
                    df.columns = [str(c).strip().lower() for c in df.columns]
                    col_sender = next((c for c in df.columns if 'sender' in c or 'order_code' in c or 'code' in c or 'קוד' in c), None)
                    col_invitees = next((c for c in df.columns if 'invite' in c or 'guest' in c or 'מוזמנים' in c), None)
                    col_phone = next((c for c in df.columns if 'phone' in c or 'mobile' in c or 'טלפון' in c), None)
                    
                    vals = []
                    for _, row in df.iterrows():
                        s = clean_int_str(row[col_sender]) if col_sender else ''
                        i = str(handle_series(row[col_invitees])) if col_invitees else ''
                        p = str(handle_series(row[col_phone])) if col_phone else None
                        if s or i: vals.append((s, i, 'סמלי', 'upload', p))
                    
                    q = "INSERT INTO outerapporder (sender_code, invitees, package_size, origin, sender_phone) VALUES (%s, %s, %s, %s, %s)"
                    execute_batch(cur, q, vals)
                    flash(f'נטענו {len(vals)} הזמנות.', 'info')
                except Exception as e: flash(f'שגיאה: {e}', 'danger')
        elif action == 'distribute':
            cnt, msgs = distribute_in_python()
            for m in msgs: flash(m, 'success' if cnt > 0 else 'danger')

    cur.execute("SELECT * FROM v_outer_distribution_status LIMIT 50")
    rows = cur.fetchall()
    cur.execute("SELECT * FROM outerapporder_error_log ORDER BY id DESC LIMIT 20")
    errs = cur.fetchall()
    cur.close(); conn.close()
    return render_template('orders.html', status_rows=rows, errors=errs)

@app.route('/report/<view_name>')
def report(view_name):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    search = request.args.get('search', '')
    q = f'SELECT * FROM "{view_name}"'
    p = {}
    if search:
        if view_name == 'v_accounts_summary': q += " WHERE sender_name LIKE %(search)s"
        elif view_name == 'v_families_balance': q += " WHERE lastname LIKE %(search)s"
        elif view_name == 'v_orders_details': q += " WHERE sender_name LIKE %(search)s OR getter_name LIKE %(search)s"
        elif view_name == 'v_packages_per_building': q += " WHERE streetname LIKE %(search)s"
        p['search'] = f'%{search}%'
    else: q += " LIMIT 100"
    try: cur.execute(q, p); rows = cur.fetchall(); cols = rows[0].keys() if rows else []
    except: rows, cols = [], []
    cur.close(); conn.close()
    return render_template('report.html', view_name=view_name, rows=rows, columns=cols)

@app.route('/export/<view_name>')
def export_csv(view_name):
    conn = get_db_connection()
    df = pd.read_sql_query(f'SELECT * FROM "{view_name}"', conn)
    conn.close()
    return Response(df.to_csv(index=False), mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={view_name}.csv"})

@app.route('/apply_autoreturn', methods=['POST'])
def apply_autoreturn():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.callproc('apply_autoreturn_for', [int(request.form.get('family_id'))])
    conn.close()
    return redirect(url_for('report', view_name='v_families_balance'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
