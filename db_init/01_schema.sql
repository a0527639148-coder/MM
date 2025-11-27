SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "public";


ALTER SCHEMA "public" OWNER TO "pg_database_owner";


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE TYPE "public"."person_status_type" AS ENUM (
    'inserted',
    'merged',
    'skipped',
    'partial_match'
);


ALTER TYPE "public"."person_status_type" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."apply_autoreturn_for"("family_id" integer) RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN
    SELECT delivery_sender_id AS sender_id
    FROM "Order"
    WHERE delivery_getter_id = family_id
  LOOP
    INSERT INTO "Order"(delivery_sender_id, delivery_getter_id, package_size, order_date, origin_type)
    VALUES (family_id, rec.sender_id, 'סמלי', now(), 'autoreturn')
    ON CONFLICT DO NOTHING;
  END LOOP;
END;
$$;


ALTER FUNCTION "public"."apply_autoreturn_for"("family_id" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."apply_autoreturn_from_outer"("order_id" integer) RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- כאן חישוב ושליחת autoreturn לפי הזמנה קיימת
  RAISE NOTICE 'apply_autoreturn_from_outer executed for order %', order_id;
END;
$$;


ALTER FUNCTION "public"."apply_autoreturn_from_outer"("order_id" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."distribute_all_outer_orders"() RETURNS integer
    LANGUAGE "plpgsql"
    AS $_$
DECLARE
  rec RECORD;
  v_sender_id INT;
  v_excel_id INT;
  v_rows_this_rec INT;
  v_total_inserted INT := 0;
  v_delivery_price NUMERIC(10,2);
BEGIN
  -- 🧩 שלב 1: שלוף מחיר משלוח או קבע ברירת מחדל 10.00
  SELECT COALESCE(
    (SELECT setting_value::NUMERIC(10,2)
     FROM public.delivery_settings
     WHERE setting_name = 'delivery_price'
     LIMIT 1),
    10.00  -- ברירת מחדל
  ) INTO v_delivery_price;

  -- 🧩 שלב 2: עיבוד ההזמנות
  FOR rec IN
    SELECT * FROM public.outerapporder
    WHERE status = 'waiting'
    ORDER BY id
  LOOP
    v_rows_this_rec := 0;
    v_sender_id := public.normalize_sender_code(rec.sender_code::text);
    v_excel_id  := rec.id;

    IF rec.invitees IS NULL OR rec.invitees = '' THEN
      INSERT INTO public.outerapporder_error_log(outer_id, severity, reason_code, message, details)
      VALUES (rec.id, 'error', 'no_invitees', 'אין מוזמנים להזמנה זו (invitees ריק)',
              jsonb_build_object('sender_code', rec.sender_code));

      UPDATE public.outerapporder
      SET status = 'error',
          processed_at = NOW(),
          error_message = 'אין מוזמנים להזמנה זו'
      WHERE id = rec.id;
      CONTINUE;
    END IF;

    -- בדיקה אם השולח קיים
    IF NOT EXISTS (SELECT 1 FROM public.person WHERE personid = v_sender_id) THEN
      INSERT INTO public.outerapporder_error_log(outer_id, severity, reason_code, message, details)
      VALUES (
        rec.id,
        'error',
        'missing_sender',
        format('שולח %s לא קיים בטבלת person', v_sender_id),
        jsonb_build_object('sender_code', rec.sender_code)
      );

      UPDATE public.outerapporder
      SET status = 'error',
          processed_at = NOW(),
          error_message = 'שולח לא קיים בטבלת person'
      WHERE id = rec.id;

      CONTINUE;
    END IF;

    -- הכנסת ההזמנות עצמן
    WITH tokens AS (
      SELECT regexp_split_to_table(rec.invitees, '\|') AS token
    ),
    cleaned AS (
      SELECT NULLIF(trim(token), '') AS token_trim FROM tokens
    ),
    ints AS (
      SELECT DISTINCT token_trim::int AS invitee_id
      FROM cleaned
      WHERE token_trim ~ '^\d+$'
    )
    INSERT INTO public."Order"(
        delivery_sender_id,
        delivery_getter_id,
        order_date,
        excel_import_id,
        origin_type,
        origin_outer_id,
        price
    )
    SELECT 
        v_sender_id, 
        i.invitee_id, 
        CURRENT_DATE, 
        v_excel_id, 
        'invitees', 
        v_excel_id,
        v_delivery_price  -- 💰 מחיר ברירת מחדל או מטבלת הגדרות
    FROM ints i
    WHERE EXISTS (SELECT 1 FROM public.person p WHERE p.personid = i.invitee_id)
    ON CONFLICT DO NOTHING;

    GET DIAGNOSTICS v_rows_this_rec = ROW_COUNT;

    IF v_rows_this_rec = 0 THEN
      UPDATE public.outerapporder
      SET status = 'error',
          processed_at = NOW(),
          error_message = 'לא נוצרו הזמנות חדשות - כנראה כפילות או חוסר נתונים'
      WHERE id = rec.id;
    ELSE
      UPDATE public.outerapporder
      SET status = 'distributed',
          processed_at = NOW(),
          error_message = NULL
      WHERE id = rec.id;

      v_total_inserted := v_total_inserted + v_rows_this_rec;
    END IF;
  END LOOP;

  RETURN v_total_inserted;
END;
$_$;


ALTER FUNCTION "public"."distribute_all_outer_orders"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."format_il_phone"("p_text" "text", "p_default_area" "text" DEFAULT '02'::"text") RETURNS "text"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    e text := p_text;
BEGIN
    IF e IS NULL OR btrim(e) = '' THEN
        RETURN NULL;
    END IF;

    -- הסרת כל תו שאינו ספרה
    e := regexp_replace(e, '\D', '', 'g');

    -- אם המספר מתחיל ב־972 → החלף ב־0
    IF left(e,3) = '972' THEN
        e := '0' || substring(e FROM 4);
    END IF;

    -- אם אין אפס בתחילת המספר → הוסף קידומת ברירת מחדל
    IF left(e,1) <> '0' THEN
        e := p_default_area || e;
    END IF;

    -- ניקוי אפסים מיותרים בתחילת המספר (אם יש יותר מאחד)
    e := regexp_replace(e, '^0+', '0');

    -- אם המספר באורך 7 בלבד → הוסף קידומת ברירת מחדל
    IF length(e) = 7 THEN
        e := p_default_area || e;
    END IF;

    -- בדיקה סופית: רק אם באורך הגיוני נחזיר
    IF length(e) BETWEEN 7 AND 10 THEN
        RETURN e;
    ELSE
        RETURN NULL;
    END IF;
END;
$$;


ALTER FUNCTION "public"."format_il_phone"("p_text" "text", "p_default_area" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."import_outerapporder_from_temp"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- כאן הלוגיקה להעברת רשומות מ-temp ל-outerapporder
  RAISE NOTICE 'import_outerapporder_from_temp executed';
END;
$$;


ALTER FUNCTION "public"."import_outerapporder_from_temp"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_valid_email"("email" "text") RETURNS boolean
    LANGUAGE "plpgsql"
    AS $_$
BEGIN
  RETURN email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';
END;
$_$;


ALTER FUNCTION "public"."is_valid_email"("email" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."merge_people"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- כאן נכנסת הלוגיקה שלך לאיחוד לפי טלפון + כתובת
  -- בד"כ: להכניס ל-person אם לא קיים, אחרת לעדכן
  INSERT INTO person (lastname, streetcode, buildingnumber, apartmentnumber, email, phone, mobile)
  SELECT t.lastname, t.streetcode, t.buildingnumber, t.apartmentnumber, t.email, t.phone, t.mobile
  FROM temp_residents_csv t
  ON CONFLICT (phone, streetcode, buildingnumber, apartmentnumber)
  DO NOTHING;
END;
$$;


ALTER FUNCTION "public"."merge_people"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."normalize_email"("email" "text") RETURNS "text"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  RETURN regexp_replace(lower(trim(email)), '\s+', '', 'g');
END;
$$;


ALTER FUNCTION "public"."normalize_email"("email" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."normalize_email_basic"("email" "text") RETURNS "text"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  RETURN lower(trim(email));
END;
$$;


ALTER FUNCTION "public"."normalize_email_basic"("email" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."normalize_invitees"("invitees" "text") RETURNS integer[]
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  RETURN string_to_array(invitees, '|')::int[];
END;
$$;


ALTER FUNCTION "public"."normalize_invitees"("invitees" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."normalize_sender_code"("sender" "text") RETURNS integer
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  RETURN sender::int;
EXCEPTION WHEN invalid_text_representation THEN
  RETURN NULL;
END;
$$;


ALTER FUNCTION "public"."normalize_sender_code"("sender" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."process_residents_csv"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    rec RECORD;
    existing_person INT;
    clean_phone TEXT;
    clean_mobile TEXT;
    clean_mobile2 TEXT;
BEGIN
    FOR rec IN
        SELECT * FROM temp_residents_csv
    LOOP
        clean_phone   := format_il_phone(rec.phone);
        clean_mobile  := format_il_phone(rec.mobile);
        clean_mobile2 := format_il_phone(rec.mobile2);

        ------------------------------------------------------------------
        -- 🔍 בדיקה 1: התאמה מלאה (שם + כתובת + טלפונים)
        ------------------------------------------------------------------
        SELECT p.personid INTO existing_person
        FROM person p
        WHERE 
            COALESCE(p.lastname, '') = COALESCE(rec.lastname, '')
            AND COALESCE(p.father_name, '') = COALESCE(rec.father_name, '')
            AND COALESCE(p.mother_name, '') = COALESCE(rec.mother_name, '')
            AND COALESCE(p.streetcode, 0) = COALESCE(rec.streetcode, 0)
            AND COALESCE(p.buildingnumber, '') = COALESCE(rec.buildingnumber, '')
            AND COALESCE(p.apartmentnumber, '') = COALESCE(rec.apartmentnumber, '')
            AND (
                format_il_phone(p.phone)   = clean_phone OR
                format_il_phone(p.mobile)  = clean_mobile OR
                format_il_phone(p.mobile2) = clean_mobile2
            )
        LIMIT 1;

        IF existing_person IS NOT NULL THEN
            ------------------------------------------------------------------
            -- 🔁 מצב 1: התאמה מלאה → merged
            ------------------------------------------------------------------
            INSERT INTO person_archive(
                temp_id, personid_target, status, status_note,
                lastname, father_name, mother_name,
                streetcode, buildingnumber, entrance, apartmentnumber,
                phone, mobile, mobile2, email, standing_order
            )
            VALUES (
                rec.temp_id, existing_person, 'merged', 'נמצא אדם קיים עם פרטים זהים',
                rec.lastname, rec.father_name, rec.mother_name,
                rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber,
                clean_phone, clean_mobile, clean_mobile2, rec.email, rec.standing_order
            );

            UPDATE temp_residents_csv
            SET status = 'אוחד', processed_at = now()
            WHERE temp_id = rec.temp_id;

        ------------------------------------------------------------------
        -- 🔍 בדיקה 2: התאמה חלקית
        ------------------------------------------------------------------
        ELSIF EXISTS (
            SELECT 1 FROM person p
            WHERE 
                format_il_phone(p.phone)   = clean_phone OR
                format_il_phone(p.mobile)  = clean_mobile OR
                format_il_phone(p.mobile2) = clean_mobile2 OR
                (
                    COALESCE(p.streetcode, 0) = COALESCE(rec.streetcode, 0)
                    AND COALESCE(p.buildingnumber, '') = COALESCE(rec.buildingnumber, '')
                    AND COALESCE(p.apartmentnumber, '') = COALESCE(rec.apartmentnumber, '')
                )
        ) THEN
            ------------------------------------------------------------------
            -- 🟡 מצב 2: התאמה חלקית → partial_match
            ------------------------------------------------------------------
            INSERT INTO person_archive(
                temp_id, personid_target, status, status_note,
                lastname, father_name, mother_name,
                streetcode, buildingnumber, entrance, apartmentnumber,
                phone, mobile, mobile2, email, standing_order
            )
            VALUES (
                rec.temp_id, NULL, 'partial_match', 'התאמה חלקית – טלפון או כתובת קיימים',
                rec.lastname, rec.father_name, rec.mother_name,
                rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber,
                clean_phone, clean_mobile, clean_mobile2, rec.email, rec.standing_order
            );

            UPDATE temp_residents_csv
            SET status = 'התאמה חלקית', processed_at = now()
            WHERE temp_id = rec.temp_id;

        ------------------------------------------------------------------
        -- 🔍 בדיקה 3: נתונים חסרים → skipped
        ------------------------------------------------------------------
        ELSIF rec.lastname IS NULL OR rec.father_name IS NULL OR rec.streetcode IS NULL THEN

            INSERT INTO person_archive(
                temp_id, personid_target, status, status_note,
                lastname, father_name, mother_name,
                streetcode, buildingnumber, entrance, apartmentnumber,
                phone, mobile, mobile2, email, standing_order
            )
            VALUES (
                rec.temp_id, NULL, 'skipped', 'חסרים נתונים חיוניים',
                rec.lastname, rec.father_name, rec.mother_name,
                rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber,
                clean_phone, clean_mobile, clean_mobile2, rec.email, rec.standing_order
            );

            UPDATE temp_residents_csv
            SET status = 'נדחה', processed_at = now()
            WHERE temp_id = rec.temp_id;

        ------------------------------------------------------------------
        -- ✅ מצב 4: רשומה חדשה → inserted
        ------------------------------------------------------------------
        ELSE
            INSERT INTO person(
                lastname, father_name, mother_name,
                streetcode, buildingnumber, entrance, apartmentnumber,
                phone, mobile, mobile2, email, standing_order
            )
            VALUES (
                rec.lastname, rec.father_name, rec.mother_name,
                rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber,
                clean_phone, clean_mobile, clean_mobile2, rec.email, rec.standing_order
            )
            RETURNING personid INTO existing_person;

            INSERT INTO person_archive(
                temp_id, personid_target, status, status_note,
                lastname, father_name, mother_name,
                streetcode, buildingnumber, entrance, apartmentnumber,
                phone, mobile, mobile2, email, standing_order
            )
            VALUES (
                rec.temp_id, existing_person, 'inserted', 'נוספה רשומה חדשה',
                rec.lastname, rec.father_name, rec.mother_name,
                rec.streetcode, rec.buildingnumber, rec.entrance, rec.apartmentnumber,
                clean_phone, clean_mobile, clean_mobile2, rec.email, rec.standing_order
            );

            UPDATE temp_residents_csv
            SET status = 'הופץ', processed_at = now()
            WHERE temp_id = rec.temp_id;
        END IF;

    END LOOP;
END;
$$;


ALTER FUNCTION "public"."process_residents_csv"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."raw_to_temp_stage"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $_$
BEGIN
  -- ניקוי טבלת TEMP לפני טעינה חדשה
  TRUNCATE TABLE public.temp_residents_csv RESTART IDENTITY;

  -- שלב 1: רישום רחובות חדשים שלא קיימים בטבלת street ללוג
  INSERT INTO public.missing_streets_log (streetname)
  SELECT DISTINCT TRIM(r.streetname)
  FROM public.raw_residents_csv r
  LEFT JOIN public.street s 
         ON TRIM(s.streetname) = TRIM(r.streetname)
  WHERE s.streetcode IS NULL
    AND TRIM(r.streetname) IS NOT NULL
    AND TRIM(r.streetname) <> ''
    AND NOT EXISTS (
        SELECT 1 
        FROM public.missing_streets_log m 
        WHERE TRIM(m.streetname) = TRIM(r.streetname)
    );

  -- שלב 2: העתקת הנתונים מ-RAW אל TEMP
  INSERT INTO public.temp_residents_csv (
      lastname,
      father_name,
      mother_name,
      streetname,
      streetcode,
      buildingnumber,
      entrance,
      apartmentnumber,
      email,
      phone,
      mobile,
      mobile2,
      standing_order
  )
  SELECT 
      TRIM(r.lastname),
      TRIM(r.father_name),
      TRIM(r.mother_name),
      TRIM(r.streetname),
      COALESCE(s.streetcode, 999) AS streetcode,   -- אם לא נמצא – קוד 999
      TRIM(r.buildingnumber),
      TRIM(r.entrance),
      TRIM(r.apartmentnumber),
      normalize_email(r.email),
      format_il_phone(r.phone),
      format_il_phone(r.mobile),
      format_il_phone(r.mobile2),
      CASE
          WHEN TRIM(r.standing_order::text) ~ '^[0-9]+$' 
               THEN r.standing_order::int
          WHEN TRIM(r.standing_order::text) = '' OR r.standing_order IS NULL 
               THEN 0
          ELSE 0
      END AS standing_order
  FROM public.raw_residents_csv r
  LEFT JOIN public.street s 
         ON TRIM(s.streetname) = TRIM(r.streetname);

  RAISE NOTICE '✅ הנתונים הועתקו בהצלחה לטבלת temp_residents_csv. רחובות חדשים נרשמו בלוג עם קוד 999.';
END;
$_$;


ALTER FUNCTION "public"."raw_to_temp_stage"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."reset_temp_residents_for_rerun"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  TRUNCATE TABLE temp_residents_csv RESTART IDENTITY;
END;
$$;


ALTER FUNCTION "public"."reset_temp_residents_for_rerun"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."simulate_distribute_outer_orders"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- כאן חישוב יבש של מה יקרה בהפצה
  RAISE NOTICE 'simulate_distribute_outer_orders executed';
END;
$$;


ALTER FUNCTION "public"."simulate_distribute_outer_orders"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."simulate_distribute_outer_orders2"() RETURNS integer
    LANGUAGE "plpgsql"
    AS $$
DECLARE
    rec RECORD;
    matched_person_id INTEGER;
    street_code INTEGER;
    counter INTEGER := 0;
BEGIN
    FOR rec IN SELECT * FROM outerapporder WHERE is_distributed = false
    LOOP
        -- בדיקה האם sender_code קיים בטבלת person
        SELECT personid INTO matched_person_id
        FROM person
        WHERE personid = rec.sender_code::INTEGER
        LIMIT 1;

        IF matched_person_id IS NULL THEN
            RAISE NOTICE 'Order ID %: sender_code % לא נמצא בטבלת person, ייווצר person חדש', rec.id, rec.sender_code;
            -- סימולציה של יצירת אדם חדש (לא באמת נוצר)
            matched_person_id := -rec.sender_code::INTEGER;
        END IF;

        -- חיפוש קוד רחוב
        SELECT streetcode INTO street_code
        FROM street
        WHERE streetname = rec.street
        LIMIT 1;

        -- ניסיון למצוא התאמה מלאה לפי טלפון + כתובת
        SELECT personid INTO matched_person_id
        FROM person
        WHERE (phone = rec.home_phone OR mobile = rec.mobile)
          AND streetcode = street_code
          AND buildingnumber = rec.building
          AND apartmentnumber = rec.apartment
        LIMIT 1;

        IF matched_person_id IS NOT NULL THEN
            RAISE NOTICE 'Order ID %: נמצאה התאמה מלאה לאדם קיים (personid=%)', rec.id, matched_person_id;
        ELSE
            -- ניסיון למצוא התאמה חלקית (כתובת בלבד)
            SELECT personid INTO matched_person_id
            FROM person
            WHERE streetcode = street_code
              AND buildingnumber = rec.building
              AND apartmentnumber = rec.apartment
            LIMIT 1;

            IF matched_person_id IS NOT NULL THEN
                RAISE NOTICE 'Order ID %: נמצאה התאמה חלקית לפי כתובת בלבד (personid=%)', rec.id, matched_person_id;
            ELSE
                RAISE NOTICE 'Order ID %: לא נמצאה התאמה כלל, יש צורך ביצירת אדם חדש', rec.id;
            END IF;
        END IF;

        counter := counter + 1;
    END LOOP;

    RETURN counter;
END;
$$;


ALTER FUNCTION "public"."simulate_distribute_outer_orders2"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."validate_residents_import"() RETURNS "void"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
  -- דוגמה: לבדוק כפילויות טלפון
  IF EXISTS (
    SELECT phone FROM temp_residents_csv GROUP BY phone HAVING COUNT(*) > 1 AND phone IS NOT NULL
  ) THEN
    RAISE WARNING 'Duplicate phone numbers found in temp_residents_csv';
  END IF;
END;
$$;


ALTER FUNCTION "public"."validate_residents_import"() OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."order_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."order_id_seq" OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."Order" (
    "id" integer DEFAULT "nextval"('"public"."order_id_seq"'::"regclass") NOT NULL,
    "delivery_sender_id" integer,
    "delivery_getter_id" integer,
    "order_date" "date",
    "excel_import_id" integer,
    "package_size" "text",
    "price" numeric,
    "origin_type" "text",
    "origin_outer_id" integer
);


ALTER TABLE "public"."Order" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."person_personid_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."person_personid_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."person" (
    "personid" integer DEFAULT "nextval"('"public"."person_personid_seq"'::"regclass") NOT NULL,
    "lastname" "text",
    "phone" "text",
    "mobile" "text",
    "email" "text",
    "streetcode" integer,
    "buildingnumber" "text",
    "apartmentnumber" "text",
    "autoreturn" boolean,
    "father_name" "text",
    "mother_name" "text",
    "phone_key" "text" GENERATED ALWAYS AS (COALESCE(TRIM(BOTH FROM "mobile"), TRIM(BOTH FROM "phone"))) STORED,
    "standing_order" integer DEFAULT 0,
    "entrance" "text",
    "mobile2" "text"
);


ALTER TABLE "public"."person" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_families_balance" AS
 SELECT "p"."personid",
    "p"."lastname",
    COALESCE("sent"."sent_count", (0)::bigint) AS "sent",
    COALESCE("received"."received_count", (0)::bigint) AS "received",
    (COALESCE("sent"."sent_count", (0)::bigint) - COALESCE("received"."received_count", (0)::bigint)) AS "balance"
   FROM (("public"."person" "p"
     LEFT JOIN ( SELECT "o"."delivery_sender_id" AS "personid",
            "count"(*) AS "sent_count"
           FROM "public"."Order" "o"
          GROUP BY "o"."delivery_sender_id") "sent" ON (("sent"."personid" = "p"."personid")))
     LEFT JOIN ( SELECT "o"."delivery_getter_id" AS "personid",
            "count"(*) AS "received_count"
           FROM "public"."Order" "o"
          GROUP BY "o"."delivery_getter_id") "received" ON (("received"."personid" = "p"."personid")));


ALTER VIEW "public"."v_families_balance" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_family_receipts" AS
 SELECT "p"."personid",
    "p"."lastname",
    "p"."autoreturn",
    "count"("o"."id") AS "total_received",
    "count"(*) FILTER (WHERE ("o"."origin_type" <> 'autoreturn'::"text")) AS "received_regular",
    "count"(*) FILTER (WHERE ("o"."origin_type" = 'autoreturn'::"text")) AS "received_autoreturn"
   FROM ("public"."person" "p"
     LEFT JOIN "public"."Order" "o" ON (("o"."delivery_getter_id" = "p"."personid")))
  GROUP BY "p"."personid", "p"."lastname", "p"."autoreturn"
  ORDER BY "p"."lastname";


ALTER VIEW "public"."v_family_receipts" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_orders_details" AS
 SELECT "o"."id",
    "s"."lastname" AS "sender_name",
    "g"."lastname" AS "getter_name",
    "o"."package_size",
    "o"."price",
    "o"."order_date",
    "o"."origin_type"
   FROM (("public"."Order" "o"
     JOIN "public"."person" "s" ON (("s"."personid" = "o"."delivery_sender_id")))
     JOIN "public"."person" "g" ON (("g"."personid" = "o"."delivery_getter_id")))
  ORDER BY "o"."order_date" DESC;


ALTER VIEW "public"."v_orders_details" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."outerapporder_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."outerapporder_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."outerapporder" (
    "id" integer DEFAULT "nextval"('"public"."outerapporder_id_seq"'::"regclass") NOT NULL,
    "sender_code" "text",
    "getter_code" "text",
    "invitees" "text",
    "package_size" "text",
    "origin" "text",
    "created_at" timestamp without time zone,
    "status" "text" DEFAULT 'waiting'::"text",
    "processed_at" timestamp without time zone,
    "error_message" "text"
);


ALTER TABLE "public"."outerapporder" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_outer_errors_summary" AS
 SELECT "id",
    "sender_code",
    "getter_code",
    "package_size",
    'Mismatch or error'::"text" AS "error_reason"
   FROM "public"."outerapporder" "oao"
  WHERE (("getter_code" IS NULL) OR ("sender_code" IS NULL));


ALTER VIEW "public"."v_outer_errors_summary" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."street" (
    "streetcode" integer NOT NULL,
    "streetname" "text"
);


ALTER TABLE "public"."street" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_packages_per_building" AS
 SELECT "st"."streetname",
    "p"."buildingnumber",
    "count"("o"."id") AS "total_orders",
    "sum"(
        CASE
            WHEN ("o"."package_size" = 'סמלי'::"text") THEN 1
            ELSE 0
        END) AS "symbolic_count",
    "sum"(
        CASE
            WHEN ("o"."package_size" = 'מכובד'::"text") THEN 1
            ELSE 0
        END) AS "respectable_count",
    "sum"(
        CASE
            WHEN ("o"."package_size" = 'מפואר'::"text") THEN 1
            ELSE 0
        END) AS "fancy_count"
   FROM (("public"."Order" "o"
     JOIN "public"."person" "p" ON (("p"."personid" = "o"."delivery_getter_id")))
     JOIN "public"."street" "st" ON (("st"."streetcode" = "p"."streetcode")))
  GROUP BY "st"."streetname", "p"."buildingnumber"
  ORDER BY "st"."streetname", "p"."buildingnumber";


ALTER VIEW "public"."v_packages_per_building" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_residents_by_street" AS
 SELECT "st"."streetname",
    "p"."lastname",
    "p"."buildingnumber",
    "p"."apartmentnumber",
    "p"."phone",
    "p"."mobile"
   FROM ("public"."person" "p"
     JOIN "public"."street" "st" ON (("st"."streetcode" = "p"."streetcode")))
  ORDER BY "st"."streetname", "p"."buildingnumber", "p"."lastname";


ALTER VIEW "public"."v_residents_by_street" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."delivery_package_category" (
    "category" character varying(20) NOT NULL,
    "min_people" integer,
    "max_people" integer
);


ALTER TABLE "public"."delivery_package_category" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."delivery_price_yearly" (
    "year" integer NOT NULL,
    "base_price" numeric(10,2) NOT NULL
);


ALTER TABLE "public"."delivery_price_yearly" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."delivery_settings" (
    "id" integer NOT NULL,
    "setting_name" "text",
    "setting_value" numeric(10,2) DEFAULT 10.00 NOT NULL
);


ALTER TABLE "public"."delivery_settings" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."delivery_settings_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."delivery_settings_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."delivery_settings_id_seq" OWNED BY "public"."delivery_settings"."id";



CREATE TABLE IF NOT EXISTS "public"."missing_streets_log" (
    "id" integer NOT NULL,
    "streetname" "text" NOT NULL,
    "found_at" timestamp without time zone DEFAULT "now"(),
    "note" "text" DEFAULT 'רחוב חדש שלא נמצא בטבלת street'::"text"
);


ALTER TABLE "public"."missing_streets_log" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."missing_streets_log_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."missing_streets_log_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."missing_streets_log_id_seq" OWNED BY "public"."missing_streets_log"."id";



CREATE TABLE IF NOT EXISTS "public"."mytable" (
    "code" integer NOT NULL,
    "lastname" character varying(13) NOT NULL,
    "father_name" character varying(15) NOT NULL,
    "mother_name" character varying(11) NOT NULL,
    "streetname" character varying(20) NOT NULL,
    "buildingnumber" character varying(4) NOT NULL,
    "entrance" character varying(3) NOT NULL,
    "apartmentnumber" character varying(3) NOT NULL,
    "phone" character varying(12),
    "mobile" character varying(15) NOT NULL,
    "mobile2" character varying(12) NOT NULL,
    "email" character varying(21) NOT NULL,
    "standing_order" integer NOT NULL
);


ALTER TABLE "public"."mytable" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."outerapporder_error_log" (
    "id" integer NOT NULL,
    "outer_id" integer NOT NULL,
    "severity" "text" DEFAULT 'error'::"text",
    "reason_code" "text",
    "message" "text",
    "details" "jsonb",
    "created_at" timestamp without time zone DEFAULT "now"(),
    CONSTRAINT "outerapporder_error_log_severity_check" CHECK (("severity" = ANY (ARRAY['info'::"text", 'warning'::"text", 'error'::"text"])))
);


ALTER TABLE "public"."outerapporder_error_log" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."outerapporder_error_log_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."outerapporder_error_log_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."outerapporder_error_log_id_seq" OWNED BY "public"."outerapporder_error_log"."id";



CREATE TABLE IF NOT EXISTS "public"."outerapporder_match_log_backup" (
    "id" integer,
    "outerapporder_id" integer,
    "matched_personid" integer,
    "match_type" "text",
    "log_time" timestamp without time zone
);


ALTER TABLE "public"."outerapporder_match_log_backup" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."payment_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."payment_id_seq" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."payment_ledger" (
    "id" integer NOT NULL,
    "person_id" integer NOT NULL,
    "order_id" integer,
    "payment_type" character varying(20) NOT NULL,
    "source" character varying(50) NOT NULL,
    "amount" numeric(10,2) NOT NULL,
    "created_at" timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE "public"."payment_ledger" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."payment_ledger_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."payment_ledger_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."payment_ledger_id_seq" OWNED BY "public"."payment_ledger"."id";



CREATE TABLE IF NOT EXISTS "public"."person_archive" (
    "archive_id" integer NOT NULL,
    "temp_id" integer,
    "personid_target" integer,
    "status" "text",
    "status_note" "text",
    "lastname" "text",
    "streetcode" integer,
    "buildingnumber" "text",
    "entrance" "text",
    "apartmentnumber" "text",
    "phone" "text",
    "mobile" "text",
    "created_at" timestamp without time zone DEFAULT "now"(),
    "father_name" "text",
    "mother_name" "text",
    "mobile2" "text",
    "email" "text",
    "standing_order" integer,
    CONSTRAINT "person_archive_status_check" CHECK (("status" IS NOT NULL))
);


ALTER TABLE "public"."person_archive" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."person_archive_archive_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."person_archive_archive_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."person_archive_archive_id_seq" OWNED BY "public"."person_archive"."archive_id";



CREATE TABLE IF NOT EXISTS "public"."rating_levels" (
    "rating_id" integer NOT NULL,
    "rating_value" integer NOT NULL,
    "min_amount" integer,
    "max_amount" integer,
    "description" "text" NOT NULL,
    CONSTRAINT "rating_levels_rating_value_check" CHECK ((("rating_value" >= 0) AND ("rating_value" <= 5)))
);


ALTER TABLE "public"."rating_levels" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."rating_levels_rating_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."rating_levels_rating_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."rating_levels_rating_id_seq" OWNED BY "public"."rating_levels"."rating_id";



CREATE TABLE IF NOT EXISTS "public"."raw_residents_csv" (
    "raw_id" integer NOT NULL,
    "code" "text",
    "lastname" "text",
    "father_name" "text",
    "mother_name" "text",
    "streetname" "text",
    "buildingnumber" "text",
    "entrance" "text",
    "apartmentnumber" "text",
    "phone" "text",
    "mobile" "text",
    "mobile2" "text",
    "email" "text",
    "standing_order" integer DEFAULT 0,
    "batch_id" bigint DEFAULT 0,
    "uploaded_at" timestamp without time zone DEFAULT "now"(),
    CONSTRAINT "chk_standing_order_range" CHECK ((("standing_order" >= 0) AND ("standing_order" <= 5)))
);


ALTER TABLE "public"."raw_residents_csv" OWNER TO "postgres";


CREATE SEQUENCE IF NOT EXISTS "public"."raw_residents_csv_raw_id_seq"
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."raw_residents_csv_raw_id_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."raw_residents_csv_raw_id_seq" OWNED BY "public"."raw_residents_csv"."raw_id";



CREATE SEQUENCE IF NOT EXISTS "public"."street_streetcode_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE "public"."street_streetcode_seq" OWNER TO "postgres";


ALTER SEQUENCE "public"."street_streetcode_seq" OWNED BY "public"."street"."streetcode";



CREATE TABLE IF NOT EXISTS "public"."temp_residents_csv" (
    "temp_id" integer NOT NULL,
    "lastname" "text",
    "father_name" "text",
    "mother_name" "text",
    "streetname" "text",
    "buildingnumber" "text",
    "apartmentnumber" "text",
    "phone" "text",
    "mobile" "text",
    "mobile2" "text",
    "email" "text",
    "id_father" "text",
    "id_mother" "text",
    "children_count" "text",
    "processed" boolean DEFAULT false,
    "processed_at" timestamp without time zone,
    "entrance" "text",
    "standing_order" integer,
    "batch_id" "uuid",
    "src_row_id" "text",
    "streetcode" integer,
    "status" "text" DEFAULT 'ממתין'::"text"
);


ALTER TABLE "public"."temp_residents_csv" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."temp_residents_csv_archive" (
    "lastname" "text",
    "father_name" "text",
    "mother_name" "text",
    "streetname" "text",
    "buildingnumber" "text",
    "apartmentnumber" "text",
    "phone" "text",
    "mobile" "text",
    "mobile2" "text",
    "email" "text",
    "id_father" "text",
    "id_mother" "text",
    "children_count" "text",
    "temp_id" integer NOT NULL,
    "processed" boolean,
    "processed_at" timestamp without time zone,
    "src_row_id" "text",
    "entrance" "text",
    "standing_order" integer,
    "batch_id" "uuid"
);


ALTER TABLE "public"."temp_residents_csv_archive" OWNER TO "postgres";


ALTER TABLE "public"."temp_residents_csv" ALTER COLUMN "temp_id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "public"."temp_residents_csv_temp_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);



CREATE OR REPLACE VIEW "public"."v_accounts_summary" AS
 SELECT "s"."personid" AS "sender_id",
    "s"."lastname" AS "sender_name",
    "count"("o"."id") AS "total_orders",
    COALESCE("sum"("o"."price"), (0)::numeric) AS "total_amount",
        CASE
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (360)::numeric) THEN 20
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (180)::numeric) THEN 10
            ELSE 5
        END AS "discount_percent",
    "round"((COALESCE("sum"("o"."price"), (0)::numeric) *
        CASE
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (360)::numeric) THEN 0.20
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (180)::numeric) THEN 0.10
            ELSE 0.05
        END), 2) AS "discount_amount",
    "round"((COALESCE("sum"("o"."price"), (0)::numeric) *
        CASE
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (360)::numeric) THEN 0.80
            WHEN (COALESCE("sum"("o"."price"), (0)::numeric) > (180)::numeric) THEN 0.90
            ELSE 0.95
        END), 2) AS "final_amount"
   FROM ("public"."Order" "o"
     JOIN "public"."person" "s" ON (("s"."personid" = "o"."delivery_sender_id")))
  GROUP BY "s"."personid", "s"."lastname"
  ORDER BY "s"."lastname";


ALTER VIEW "public"."v_accounts_summary" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_autoreturn_activity" AS
 SELECT "o"."id" AS "order_id",
    "s"."lastname" AS "sender_name",
    "g"."lastname" AS "getter_name",
    "o"."order_date",
    "o"."origin_type"
   FROM (("public"."Order" "o"
     JOIN "public"."person" "s" ON (("s"."personid" = "o"."delivery_sender_id")))
     JOIN "public"."person" "g" ON (("g"."personid" = "o"."delivery_getter_id")))
  WHERE ("o"."origin_type" = 'autoreturn'::"text");


ALTER VIEW "public"."v_autoreturn_activity" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_orders_summary" AS
 SELECT "s"."personid" AS "sender_id",
    "s"."lastname" AS "sender_name",
    "g"."personid" AS "getter_id",
    "g"."lastname" AS "getter_name",
    "count"("o"."id") AS "total_orders"
   FROM (("public"."Order" "o"
     JOIN "public"."person" "s" ON (("s"."personid" = "o"."delivery_sender_id")))
     JOIN "public"."person" "g" ON (("g"."personid" = "o"."delivery_getter_id")))
  GROUP BY "s"."personid", "s"."lastname", "g"."personid", "g"."lastname";


ALTER VIEW "public"."v_orders_summary" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_outer_distribution_status" AS
 SELECT "id",
    "sender_code",
    "getter_code",
    "package_size",
    "created_at"
   FROM "public"."outerapporder" "oao"
  ORDER BY "created_at" DESC;


ALTER VIEW "public"."v_outer_distribution_status" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_outerapporder_full_summary" AS
 SELECT "id",
    "sender_code",
    "getter_code",
    "invitees",
    "package_size",
    "origin",
    "created_at"
   FROM "public"."outerapporder" "oao"
  ORDER BY "created_at" DESC;


ALTER VIEW "public"."v_outerapporder_full_summary" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."v_outerapporder_status" AS
 SELECT "id",
    "sender_code",
    "getter_code",
    "invitees",
    "package_size",
    "origin",
    "created_at"
   FROM "public"."outerapporder" "oao"
  ORDER BY "created_at" DESC;


ALTER VIEW "public"."v_outerapporder_status" OWNER TO "postgres";


ALTER TABLE ONLY "public"."delivery_settings" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."delivery_settings_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."missing_streets_log" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."missing_streets_log_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."outerapporder_error_log" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."outerapporder_error_log_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."payment_ledger" ALTER COLUMN "id" SET DEFAULT "nextval"('"public"."payment_ledger_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."person_archive" ALTER COLUMN "archive_id" SET DEFAULT "nextval"('"public"."person_archive_archive_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."rating_levels" ALTER COLUMN "rating_id" SET DEFAULT "nextval"('"public"."rating_levels_rating_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."raw_residents_csv" ALTER COLUMN "raw_id" SET DEFAULT "nextval"('"public"."raw_residents_csv_raw_id_seq"'::"regclass");



ALTER TABLE ONLY "public"."street" ALTER COLUMN "streetcode" SET DEFAULT "nextval"('"public"."street_streetcode_seq"'::"regclass");



ALTER TABLE ONLY "public"."delivery_package_category"
    ADD CONSTRAINT "delivery_package_category_pkey" PRIMARY KEY ("category");



ALTER TABLE ONLY "public"."delivery_price_yearly"
    ADD CONSTRAINT "delivery_price_yearly_pkey" PRIMARY KEY ("year");



ALTER TABLE ONLY "public"."delivery_settings"
    ADD CONSTRAINT "delivery_settings_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."delivery_settings"
    ADD CONSTRAINT "delivery_settings_setting_name_key" UNIQUE ("setting_name");



ALTER TABLE ONLY "public"."missing_streets_log"
    ADD CONSTRAINT "missing_streets_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."mytable"
    ADD CONSTRAINT "mytable_pkey" PRIMARY KEY ("code");



ALTER TABLE ONLY "public"."Order"
    ADD CONSTRAINT "order_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."outerapporder_error_log"
    ADD CONSTRAINT "outerapporder_error_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."outerapporder"
    ADD CONSTRAINT "outerapporder_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."payment_ledger"
    ADD CONSTRAINT "payment_ledger_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."person_archive"
    ADD CONSTRAINT "person_archive_pkey" PRIMARY KEY ("archive_id");



ALTER TABLE ONLY "public"."person"
    ADD CONSTRAINT "person_pkey" PRIMARY KEY ("personid");



ALTER TABLE ONLY "public"."rating_levels"
    ADD CONSTRAINT "rating_levels_pkey" PRIMARY KEY ("rating_id");



ALTER TABLE ONLY "public"."raw_residents_csv"
    ADD CONSTRAINT "raw_residents_csv_pkey" PRIMARY KEY ("raw_id");



ALTER TABLE ONLY "public"."street"
    ADD CONSTRAINT "street_pkey" PRIMARY KEY ("streetcode");



ALTER TABLE ONLY "public"."temp_residents_csv_archive"
    ADD CONSTRAINT "temp_residents_csv_archive_pk" PRIMARY KEY ("temp_id");



ALTER TABLE ONLY "public"."temp_residents_csv"
    ADD CONSTRAINT "temp_residents_csv_pkey" PRIMARY KEY ("temp_id");



ALTER TABLE ONLY "public"."temp_residents_csv_archive"
    ADD CONSTRAINT "ux_temp_archive_tempid" UNIQUE ("temp_id");



CREATE UNIQUE INDEX "ux_person_unique_phone_address" ON "public"."person" USING "btree" ("streetcode", "buildingnumber", COALESCE("apartmentnumber", ''::"text"), "phone_key");



ALTER TABLE ONLY "public"."Order"
    ADD CONSTRAINT "order_getter_fkey" FOREIGN KEY ("delivery_getter_id") REFERENCES "public"."person"("personid");



ALTER TABLE ONLY "public"."Order"
    ADD CONSTRAINT "order_sender_fkey" FOREIGN KEY ("delivery_sender_id") REFERENCES "public"."person"("personid");



ALTER TABLE ONLY "public"."payment_ledger"
    ADD CONSTRAINT "payment_ledger_order_id_fkey" FOREIGN KEY ("order_id") REFERENCES "public"."Order"("id");



ALTER TABLE ONLY "public"."payment_ledger"
    ADD CONSTRAINT "payment_ledger_person_id_fkey" FOREIGN KEY ("person_id") REFERENCES "public"."person"("personid");



ALTER TABLE ONLY "public"."person"
    ADD CONSTRAINT "person_streetcode_fkey" FOREIGN KEY ("streetcode") REFERENCES "public"."street"("streetcode");