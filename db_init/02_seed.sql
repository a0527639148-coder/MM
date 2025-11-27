-- 1. הגדרות מערכת בסיסיות (מחיר משלוח)
INSERT INTO public.delivery_settings (setting_name, setting_value) VALUES 
('delivery_price', 15.00)
ON CONFLICT (setting_name) DO UPDATE SET setting_value = EXCLUDED.setting_value;

-- 2. תיקון קריטי: יצירת רחוב ברירת מחדל (999)
-- המערכת משתמשת בקוד הזה כאשר היא מוצאת רחוב חדש בקובץ האקסל
INSERT INTO public.street (streetcode, streetname) VALUES (999, 'רחוב כללי')
ON CONFLICT (streetcode) DO NOTHING;

-- 3. רחובות דוגמה (כדי שהמערכת תכיר רחובות נפוצים מראש)
INSERT INTO public.street (streetname) VALUES 
('הזית'), ('הגפן'), ('התאנה'), ('הרימון'), ('הדקל'), 
('השקד'), ('הברוש'), ('האלון'), ('האורן')
ON CONFLICT DO NOTHING;

-- 4. איפוס רצפים (Sequences) - למניעת התנגשויות
SELECT setval('public.street_streetcode_seq', (SELECT MAX(streetcode) FROM public.street));