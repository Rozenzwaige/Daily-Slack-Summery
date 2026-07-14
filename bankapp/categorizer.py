"""
Maps a transaction description to a Hebrew category.
Keywords are matched case-insensitively anywhere in the description+memo.

Special categories (excluded from spending summaries):
  "תשלום כרטיס"  — credit card payment from bank (already counted in credit sheet)
  "השקעות"       — securities purchases / investment transfers
"""

# ── Categories excluded from spending reports ────────────────────────────────
EXCLUDED_FROM_SPENDING = {"תשלום כרטיס", "השקעות"}

CATEGORIES: dict[str, list[str]] = {

    # ── EXCLUDED: credit card payment from bank account ──────────────────────
    "תשלום כרטיס": [
        "כרטיסי אשראי", "cal card", "visa cal",
        "ישראכרט", "max payment", "לאומי קארד",
    ],

    # ── EXCLUDED: securities / investment transfers ───────────────────────────
    "השקעות": [
        "נע-קניה", "נע קניה", "דנ\"ח ניע", "דנח ניע",
        "קניה בבורסה", "רכישת ניע", "פדיון ניע",
        "קרן השתלמות", "קרן פנסיה", "גמל",
        "רכישת מטח",  # foreign currency purchase for investment
    ],

    # ── Regular categories ────────────────────────────────────────────────────
    "מזון וסופרמרקט": [
        # supermarkets
        "שופרסל", "רמי לוי", "מגה", "יוחננוף", "פרש מרקט", "סיטי מרקט",
        "AM:PM", "AMPM", "חצי חינם", "ויקטורי", "מחיר למשתכן",
        "supermarket", "grocery", "מינימרקט החותרים",
        # cash withdrawals — user confirmed ~90% spent on food/market
        "כספומט", "סניפומט", "כספונט", "atm",
        # checks (often local market / small vendors)
        "משיכת שיק",
    ],
    "מסעדות וקפה": [
        "קפה", "cafe", "coffee", "מסעדה", "שווארמה", "פלאפל", "פיצה",
        "המבורגר", "burger", "sushi", "סושי", "חומוס", "restaurant",
        "גריל", "עסקית", "דלייט", "קיוסק", "ארוחה",
    ],
    "תחבורה ודלק": [
        "פנגו", "pango", "מוביט", "moovit",
        "דלק", "סונול", "פז", "ten", "טן", "bp",
        "רב קו", "רב-קו", "ravkav", "אגד", "דן", "מטרו", "רכבת",
        "גט", "yango", "bolt", "taxi", "חנייה", "parking",
    ],
    "פארם ובריאות": [
        "סופר-פארם", "super pharm", "superpharm", "בית מרקחת", "רוקח",
        "קופת חולים", "מאוחדת", "כללית", "לאומית", "מכבי",
        "בית חולים", "קליניקה", "מעבדה", "רופא",
    ],
    "ביגוד": [
        "זארה", "zara", "h&m", "castro", "קסטרו", "fox", "פוקס",
        "golf", "גולף", "renuar", "רנואר", "primark", "adidas", "nike",
    ],
    "חינוך וילדים": [
        "גן", "גנון", "ילדים", "צעצוע", "toys", "ספר", "book",
        "קורס", "שיעור", "מחנה", "בית ספר",
    ],
    "בילוי ופנאי": [
        "קולנוע", "cinema", "yes planet", "הצגה", "תיאטרון",
        "gym", "כושר", "בריכה", "בריכת החותרים", "netflix", "spotify",
        "apple", "google play", "steam", "playstation",
    ],
    "תשלומים קבועים": [
        "ארנונה", "חשמל", "מים", "גז", "HOT", "partner", "cellcom",
        "bezeq", "בזק", "אינטרנט", "סלולר", "פלאפון",
        "hot mobile", "ביטוח", "insurance", "שכירות",
    ],
    "עמלות בנק": [
        "עמלת", "דמי ניהול", "דמי כרטיס",
    ],
    "מיסים": [
        "מס הכנסה", "מע\"מ", "ביטוח לאומי", "מס רכוש",
    ],
}

UNCATEGORIZED = "שונות"



# Rules: (keyword, min_amount, max_amount, category)
AMOUNT_RULES: list[tuple[str, float, float, str]] = [
    ("חותרים", 2000, 2600, "חינוך וילדים"),
    ("חותרים",    0,  500, "מוסדות"),
    ("העברה מהחשבון", 240, 270, "מוסדות"),
    ("קיוסק בריכת החותרים", 0, 9999, "מזון ומשקאות"),
]


def categorize(description: str, memo: str = "", amount: float = 0.0) -> str:
    text = (description + " " + (memo or "")).lower()
    amt = abs(amount)
    for kw, lo, hi, cat in AMOUNT_RULES:
        if kw.lower() in text and lo <= amt <= hi:
            return cat
    for category, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw.lower() in text:
                return category
    return UNCATEGORIZED
