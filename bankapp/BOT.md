# 🤖 בוט ווטסאפ דו‑כיווני (שאלות המשך על התיק)

שרת webhook קטן שמקבל הודעות נכנסות מ‑GreenAPI ועונה עם Claude,
על בסיס נתוני התיק וההוצאות האמיתיים מהגיליון.

תגובה **מיידית** — רץ תמיד על Fly.io.

---

## חלק 1 — פריסה ל‑Fly.io

### דרישות מוקדמות
- חשבון Fly.io (כבר יש לך)
- כלי flyctl מותקן: https://fly.io/docs/flyctl/install/
  - Windows (PowerShell): `iwr https://fly.io/install.ps1 -useb | iex`

### צעדים
מתוך תיקיית הפרויקט:

```powershell
fly auth login

# צור את האפליקציה (בחר שם ייחודי; עדכן גם את app ב-fly.toml אם תשנה)
fly apps create bankapp-bot

# הגדר את ה-Secrets (העתק את הערכים מ-.env)
fly secrets set `
  GREEN_API_INSTANCE="7107642203" `
  GREEN_API_TOKEN="<מה-ENV>" `
  FINANCE_WHATSAPP_CHAT_ID="120363427326425508@g.us" `
  ANTHROPIC_API_KEY="<מה-ENV>" `
  FINANCE_SPREADSHEET_ID="1FgKHWqo8_C7VHT9VIckKhUsW27e07WBAIN5iNuC7LDg" `
  WEBHOOK_TOKEN="<בחר-סוד-אקראי>" `
  -a bankapp-bot

# את ה-credentials של גוגל (כל ה-JSON בשורה אחת)
fly secrets set GOOGLE_CREDENTIALS_JSON="$(Get-Content credentials.json -Raw)" -a bankapp-bot

# פרוס
fly deploy -a bankapp-bot
```

בסיום תקבל כתובת כמו `https://bankapp-bot.fly.dev`.
בדיקה: גלוש אליה — אמור להחזיר `ok`.

---

## חלק 2 — חיבור GreenAPI ל‑webhook

ב‑https://console.green-api.com → האינסטנס שלך → **Settings**:

1. **Webhook URL:** `https://bankapp-bot.fly.dev/webhook`
2. **Webhook URL token:** הדבק את אותו `WEBHOOK_TOKEN` שהגדרת ב‑Fly
   (GreenAPI ישלח אותו בכותרת `Authorization: Bearer ...`).
3. הפעל את ההתראות הנכנסות:
   - **Incoming message notifications** (`incomingWebhook`) → **On / Yes**
   - מספיק זה; אפשר להשאיר את השאר כבוי.
4. שמור.

---

## בדיקה
שלח הודעה בקבוצת הווטסאפ (אותה קבוצה שמקבלת את הדוחות), למשל:
> "כמה מהתיק שלי חשוף לחו"ל מול ישראל?"

הבוט אמור לענות תוך כמה שניות. שאלות לדוגמה:
- "למה כדאי לי לשקול להחליף נכס מסוים?"
- "מה דעתך על הריכוזיות בתיק?"
- "כמה הוצאתי החודש על מסעדות?"

---

## אבטחה והערות
- הבוט עונה **רק** לקבוצה שב‑`FINANCE_WHATSAPP_CHAT_ID`. הודעות מכל מקום אחר מתעלמות.
- `WEBHOOK_TOKEN` מונע מאחרים לקרוא ל‑endpoint שלך.
- היסטוריית השיחה נשמרת בזיכרון (לשאלות המשך) ומתאפסת בכל פריסה מחדש — מספיק לשיחה רציפה.
- הבוט מבהיר שזו אינה ייעוץ השקעות מורשה.
- עלות Claude זניחה (כמה אגורות לכל שאלה).
