# 📊 דשבורד הכספים (Streamlit)

ממשק ויזואלי שקורא מאותו גוגל שיט שהסקרייפר כותב אליו.
שלושה טאבים: **סיכום**, **הוצאות**, **השקעות**.

---

## הרצה מקומית (במחשב שלך)

```bat
cd "C:\Users\Nadav\OneDrive - Technion\Documents\Private\bank\app"
streamlit run dashboard.py
```

נפתח אוטומטית בדפדפן בכתובת http://localhost:8501

> דורש שה‑.env יהיה טעון (מכיל את GOOGLE_CREDENTIALS_JSON או שקובץ credentials.json קיים).

---

## פריסה חינמית בענן — Streamlit Community Cloud

כך תוכל להיכנס מהנייד מכל מקום, בלי שהמחשב יהיה דלוק.

### שלב 1 — היכנס ל‑Streamlit Cloud
1. גש ל‑https://share.streamlit.io
2. התחבר עם חשבון ה‑GitHub שלך (Rozenzwaige)

### שלב 2 — צור אפליקציה חדשה
1. לחץ **Create app** → **Deploy a public app from GitHub** (או Private אם זמין)
2. Repository: `Rozenzwaige/BankApp`
3. Branch: `master`
4. Main file path: `dashboard.py`

### שלב 3 — הגדר Secrets
ב‑**Advanced settings → Secrets** הדבק:

```toml
APP_PASSWORD = "בחר_סיסמה_חזקה_כאן"
FINANCE_SPREADSHEET_ID = "1FgKHWqo8_C7VHT9VIckKhUsW27e07WBAIN5iNuC7LDg"
GOOGLE_CREDENTIALS_JSON = '''
<<< כל תוכן ה‑credentials.json של חשבון השירות, בשורה אחת או רב‑שורתי >>>
'''
```

> את ה‑`GOOGLE_CREDENTIALS_JSON` קח מתוך קובץ ה‑credentials.json (אותו service account
> שכבר נתת לו הרשאות עריכה לגיליון). הדבק את כל ה‑JSON בין ה‑`'''`.

### שלב 4 — Deploy
לחץ **Deploy**. אחרי דקה‑שתיים האפליקציה באוויר בכתובת קבועה
(למשל `https://bankapp-xxxx.streamlit.app`).

---

## הוספה למסך הבית של האייפון (כמו אפליקציה)
1. פתח את הכתובת ב‑Safari
2. כפתור שיתוף → **הוספה למסך הבית**
3. ייפתח במסך מלא עם אייקון, בדיוק כמו אפליקציה

---

## אבטחה
- אם מוגדר `APP_PASSWORD` (ב‑Secrets או ב‑.env) — האפליקציה תדרוש סיסמה בכניסה.
- בלי APP_PASSWORD האפליקציה פתוחה (מתאים רק להרצה מקומית).
- ה‑credentials הם service account עם הרשאה לגיליון הזה בלבד.
