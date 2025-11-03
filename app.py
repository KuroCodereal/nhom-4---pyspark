# -*- coding: utf-8 -*-
import os, sys
from typing import List
from flask import Flask, request, jsonify, render_template
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, length, to_date, year, desc, asc,
    avg, count, lit
)
from pyspark.sql.types import IntegerType

# ===== ĐƯỜNG DẪN / CẤU HÌNH =====
TEMPLATE_DIR = r"C:\Users\USER\web\123\templates"  # chứa main.html, yelp.html, reviews.html
DATA_DIR = r"C:\Users\USER\web\123\data"
CSV_PATH_HOURS   = os.getenv("CSV_PATH",         os.path.join(DATA_DIR, "yelp_business_hours.csv"))
CSV_PATH_REVIEWS = os.getenv("CSV_PATH_REVIEWS", os.path.join(DATA_DIR, "yelp_review.csv"))

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "5000"))

DAYS = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]

# ===== Helpers thời gian (Hours Finder) =====
def parse_time_str(t: str):
    if t is None: return None
    s = str(t).strip()
    if s == "" or s.lower() == "none" or ":" not in s: return None
    h_s, m_s = s.split(":", 1)
    try:
        h = int(h_s); m = int(m_s)
        if not (0 <= h <= 23 and 0 <= m <= 59): return None
        return h*60 + m
    except ValueError:
        return None

def parse_range_str(r: str):
    if r is None: return None
    s = str(r).strip()
    if s == "" or s.lower() == "none" or "-" not in s: return None
    a, b = s.split("-", 1)
    sa = parse_time_str(a); sb = parse_time_str(b)
    if sa is None or sb is None: return None
    return (sa, sb)

def normalize_hhmm(m):
    if m is None: return None
    return f"{m//60:02d}:{m%60:02d}"

def range_to_display(r):
    if r is None: return None
    s, e = r
    return {"open": normalize_hhmm(s), "close": normalize_hhmm(e), "overnight": e < s}

def expand_interval(s, e):
    if e >= s: return [(s, e)]
    return [(s, 1440), (0, e)]

def intervals_overlap(a, b): return not (a[1] <= b[0] or b[1] <= a[0])

def range_overlaps(biz_range, q_range):
    for ai in expand_interval(*biz_range):
        for bi in expand_interval(*q_range):
            if intervals_overlap(ai, bi): return True
    return False

# ===== Flask =====
app = Flask(__name__, template_folder=TEMPLATE_DIR)

@app.after_request
def add_cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

# ===== Spark (config trước khi tạo) =====
# Ép PySpark dùng đúng Python của .venv
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

conf = (
    SparkConf()
    .setAppName("YelpUnifiedWeb")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.bindAddress", "127.0.0.1")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# =====================================================================================
#                                   BUSINESS HOURS
# =====================================================================================
# ---- Load CSV (Hours) ----
df_hours = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(CSV_PATH_HOURS)
)

expected_cols = {"business_id","name","address","city","state",*DAYS}
missing = expected_cols - set(df_hours.columns)
if missing:
    raise RuntimeError(f"yelp_business_hours.csv thiếu cột: {missing}")

# Chuẩn hoá text
norm_hours = df_hours.select(
    trim(df_hours["business_id"]).alias("business_id"),
    trim(df_hours["name"]).alias("name"),
    trim(df_hours["address"]).alias("address"),
    trim(df_hours["city"]).alias("city"),
    upper(trim(df_hours["state"])).alias("state"),
    *[trim(df_hours[d]).alias(d) for d in DAYS]
)

# Giữ các dòng có ít nhất một ngày có giờ
def is_nullish(cname: str):
    return (col(cname).isNull()) | (trim(lower(col(cname))) == "") | (trim(lower(col(cname))) == "none")

any_hours_expr = None
for d in DAYS:
    cond = ~is_nullish(d)
    any_hours_expr = cond if any_hours_expr is None else (any_hours_expr | cond)

clean_hours = norm_hours.where(any_hours_expr)

# Làm sạch state/city
states_df = clean_hours.select("state").where(
    (col("state").isNotNull()) & (col("state").rlike(r"^[A-Z]{2}$"))
).distinct()

cities_df = clean_hours.select("city").where(
    (col("city").isNotNull()) &
    (length(col("city")) > 0) &
    (~col("city").rlike(r'["0-9]')) &
    (~col("city").rlike(r'(?i)^\s*(ste|suite)\b'))
).distinct()

# Base DF
base_hours = clean_hours.select("business_id","name","address","city","state",*DAYS).cache()

# Metadata cho Hours
cities = [r[0] for r in cities_df.orderBy(col("city")).collect()]
states = [r[0] for r in states_df.orderBy(col("state")).collect()]
names  = [r[0] for r in base_hours.select("name").distinct().orderBy(col("name")).collect()]

# Parse giờ -> records
records: List[dict] = []
for r in base_hours.collect():
    rec = {
        "business_id": r["business_id"],
        "name": r["name"],
        "address": r["address"],
        "city": r["city"],
        "state": r["state"],
        "hours_raw": {d: (r[d] if d in r.asDict() else None) for d in DAYS}
    }
    parsed = {d: parse_range_str(rec["hours_raw"].get(d)) for d in DAYS}
    rec["hours_parsed"] = parsed
    rec["hours"] = {d: range_to_display(parsed[d]) for d in DAYS}
    records.append(rec)

# Dropdown time options
time_options = {}
for d in DAYS:
    starts_set, ends_set = set(), set()
    for rec in records:
        rng = rec["hours_parsed"].get(d)
        if rng:
            starts_set.add(normalize_hhmm(rng[0]))
            ends_set.add(normalize_hhmm(rng[1]))
    time_options[d] = {"starts": sorted(t for t in starts_set if t),
                       "ends":   sorted(t for t in ends_set   if t)}

# API Hours
@app.route("/api/meta")
def api_meta():
    return jsonify({
        "cities": cities, "states": states, "names": names,
        "days": DAYS, "time_options": time_options,
    })

def filter_records(name_q=None, city_q=None, state_q=None, day_q=None, start_q=None, end_q=None):
    res = records
    if name_q:
        nq = name_q.strip().lower()
        res = [r for r in res if nq in (r["name"] or "").lower()]
    if city_q:
        res = [r for r in res if (r["city"] or "") == city_q]
    if state_q:
        res = [r for r in res if (r["state"] or "") == state_q]
    if day_q:
        day_q = day_q.lower()
        if day_q not in DAYS: return []
        if not start_q or not end_q:
            res = [r for r in res if r["hours_parsed"][day_q] is not None]
        else:
            q = parse_range_str(f"{start_q}-{end_q}")
            if q is None: return []
            res = [r for r in res
                   if r["hours_parsed"][day_q] is not None
                   and range_overlaps(r["hours_parsed"][day_q], q)]
    return res

@app.route("/api/search")
def api_search():
    hits = filter_records(
        request.args.get("name"),
        request.args.get("city"),
        request.args.get("state"),
        request.args.get("day"),
        request.args.get("start"),
        request.args.get("end"),
    )
    return jsonify({
        "count": len(hits),
        "results": [{
            "business_id": r["business_id"],
            "name": r["name"],
            "address": r["address"],
            "city": r["city"],
            "state": r["state"],
            "hours": r["hours"],
        } for r in hits]
    })

# =====================================================================================
#                                         REVIEWS
# =====================================================================================
if not os.path.exists(CSV_PATH_REVIEWS):
    raise FileNotFoundError(f"Không tìm thấy file reviews: {CSV_PATH_REVIEWS}")

df_reviews_raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(CSV_PATH_REVIEWS)
)

expected_reviews = {"review_id","user_id","business_id","stars","date","text","useful","funny","cool"}
miss = expected_reviews - set(df_reviews_raw.columns)
if miss:
    raise RuntimeError(f"yelp_review.csv thiếu cột: {miss}")

df_reviews = df_reviews_raw.select(
    trim(col("review_id")).alias("review_id"),
    trim(col("user_id")).alias("user_id"),
    trim(col("business_id")).alias("business_id"),
    trim(col("text")).alias("text"),
    trim(col("date")).alias("date_str"),
    col("stars").cast(IntegerType()).alias("stars"),
    col("useful").cast(IntegerType()).alias("useful"),
    col("funny").cast(IntegerType()).alias("funny"),
    col("cool").cast(IntegerType()).alias("cool"),
).where(
    (col("business_id").isNotNull()) & (length(col("business_id")) > 0) &
    (col("stars").isNotNull()) &
    (col("date_str").isNotNull()) & (length(col("date_str")) > 0)
).withColumn("date", to_date(col("date_str"))) \
 .withColumn("year", year(col("date")))

base_reviews = df_reviews.select(
    "review_id","user_id","business_id","text","stars","date","year","useful","funny","cool"
).cache()

def clamp_int(v, lo, hi, default):
    try:
        iv = int(v)
        return max(lo, min(hi, iv))
    except Exception:
        return default

@app.route("/api/reviews/meta")
def reviews_meta():
    years = [r[0] for r in base_reviews.select("year").where(col("year").isNotNull())
             .distinct().orderBy("year").collect()]
    total = base_reviews.count()
    return jsonify({"years": years, "star_min": 1, "star_max": 5, "total_reviews": total})

@app.route("/api/reviews/search")
def reviews_search():
    q = (request.args.get("q") or "").strip()
    business_id = (request.args.get("business_id") or "").strip()
    user_id     = (request.args.get("user_id") or "").strip()

    stars_min = clamp_int(request.args.get("stars_min"), 1, 5, 1)
    stars_max = clamp_int(request.args.get("stars_max"), 1, 5, 5)
    if stars_min > stars_max:
        stars_min, stars_max = stars_max, stars_min

    date_from = (request.args.get("date_from") or "").strip()
    date_to   = (request.args.get("date_to") or "").strip()
    sort_key  = (request.args.get("sort") or "date_desc").strip().lower()

    limit  = clamp_int(request.args.get("limit"), 1, 200, 50)
    offset = max(0, clamp_int(request.args.get("offset"), 0, 10_000_000, 0))

    dfq: DataFrame = base_reviews
    if business_id:
        dfq = dfq.where(col("business_id") == business_id)
    if user_id:
        dfq = dfq.where(col("user_id") == user_id)
    if q:
        dfq = dfq.where(lower(col("text")).contains(q.lower()))

    dfq = dfq.where((col("stars") >= lit(stars_min)) & (col("stars") <= lit(stars_max)))
    if date_from:
        dfq = dfq.where(col("date") >= lit(date_from))
    if date_to:
        dfq = dfq.where(col("date") <= lit(date_to))

    total = dfq.count()

    if sort_key == "date_asc":
        dfq = dfq.orderBy(asc("date"))
    elif sort_key == "stars_desc":
        dfq = dfq.orderBy(desc("stars"), desc("date"))
    elif sort_key == "stars_asc":
        dfq = dfq.orderBy(asc("stars"), desc("date"))
    else:
        dfq = dfq.orderBy(desc("date"))

    rows = dfq.limit(offset + limit).collect()
    rows = rows[offset: offset + limit]

    def shorten(s, n=300):
        if s is None: return None
        s = s.strip()
        return s if len(s) <= n else s[:n-1] + "…"

    results = [{
        "review_id": r["review_id"],
        "business_id": r["business_id"],
        "user_id": r["user_id"],
        "stars": int(r["stars"]) if r["stars"] is not None else None,
        "date": str(r["date"]) if r["date"] else None,
        "text": shorten(r["text"], 300),
        "useful": int(r["useful"]) if r["useful"] is not None else 0,
        "funny": int(r["funny"]) if r["funny"] is not None else 0,
        "cool": int(r["cool"]) if r["cool"] is not None else 0,
    } for r in rows]

    return jsonify({"total": total, "offset": offset, "limit": limit, "results": results})

@app.route("/api/reviews/summary")
def reviews_summary():
    bid = (request.args.get("business_id") or "").strip()
    if not bid:
        return jsonify({"error": "missing business_id"}), 400

    dfx = base_reviews.where(col("business_id") == bid)
    total = dfx.count()
    if total == 0:
        return jsonify({
            "business_id": bid, "total": 0, "avg_stars": None,
            "by_star": {}, "by_year": {}
        })

    avg_star = dfx.agg(avg("stars").alias("avg")).collect()[0]["avg"]
    star_counts = dfx.groupBy("stars").agg(count(lit(1)).alias("cnt")).orderBy("stars").collect()
    by_star = {int(r["stars"]): int(r["cnt"]) for r in star_counts}

    year_counts = (dfx.where(col("year").isNotNull())
                      .groupBy("year").agg(count(lit(1)).alias("cnt"))
                      .orderBy("year").collect())
    by_year = {int(r["year"]): int(r["cnt"]) for r in year_counts}

    return jsonify({
        "business_id": bid,
        "total": int(total),
        "avg_stars": float(avg_star) if avg_star is not None else None,
        "by_star": by_star,
        "by_year": by_year
    })

# ============================== OVERVIEW ENDPOINTS ==============================
@app.route("/api/overview/business")
def overview_business():
    """Trả về hồ sơ business (từ hours) + sao trung bình (từ reviews)."""
    bid = (request.args.get("business_id") or "").strip()
    if not bid:
        return jsonify({"error": "missing business_id"}), 400

    row = base_hours.where(col("business_id") == bid).limit(1).collect()
    if not row:
        return jsonify({"error": "not_found", "business_id": bid}), 404
    row = row[0]

    def to_display(s):
        if not s: return None
        try:
            a, b = s.split("-", 1)
            a = a.strip(); b = b.strip()
            oa = parse_time_str(a); ob = parse_time_str(b)
            overnight = False if oa is None or ob is None else (ob < oa)
            return {"open": a, "close": b, "overnight": overnight}
        except:
            return None
    hours = {d: to_display(row[d]) for d in DAYS}

    dfx = base_reviews.where(col("business_id") == bid)
    total = dfx.count()
    if total == 0:
        summary = {"total": 0, "avg_stars": None}
    else:
        avg_star = dfx.agg(avg("stars").alias("avg")).collect()[0]["avg"]
        summary = {"total": int(total), "avg_stars": float(avg_star) if avg_star is not None else None}

    return jsonify({
        "business": {
            "business_id": bid,
            "name": row["name"],
            "address": row["address"],
            "city": row["city"],
            "state": row["state"],
            "hours": hours
        },
        "reviews_summary": summary
    })

@app.route("/api/overview/star-buckets")
def overview_star_buckets():
    """Đếm số review theo sao 1..5 (toàn bộ hoặc theo business_id)."""
    bid = (request.args.get("business_id") or "").strip()
    dfx = base_reviews
    if bid:
        dfx = dfx.where(col("business_id") == bid)
    rows = dfx.groupBy("stars").agg(count(lit(1)).alias("cnt")).orderBy("stars").collect()
    buckets = {int(r["stars"]): int(r["cnt"]) for r in rows if r["stars"] is not None}
    for s in range(1, 6): buckets.setdefault(s, 0)
    return jsonify({"buckets": buckets, "business_id": bid or None})

@app.route("/api/overview/reviews-by-year")
def overview_reviews_by_year():
    """Tổng số review theo năm (toàn bộ hoặc theo business_id)."""
    bid = (request.args.get("business_id") or "").strip()
    dfx = base_reviews.where(col("year").isNotNull())
    if bid:
        dfx = dfx.where(col("business_id") == bid)
    rows = dfx.groupBy("year").agg(count(lit(1)).alias("cnt")).orderBy("year").collect()
    by_year = {int(r["year"]): int(r["cnt"]) for r in rows}
    return jsonify({"by_year": by_year, "business_id": bid or None})

# ======= DANH SÁCH BUSINESS ĐỂ ĐỔ DROPDOWN =======
@app.route("/api/business/list")
def business_list():
    """Danh sách business để đổ dropdown (id, name, city/state)."""
    limit = int(request.args.get("limit") or 500)
    rows = (
        base_hours
        .select("business_id", "name", "city", "state")
        .distinct()
        .orderBy(col("name"))
        .limit(limit)
        .collect()
    )
    items = [
        {
            "business_id": r["business_id"],
            "name": r["name"],
            "city": r["city"],
            "state": r["state"],
        }
        for r in rows
    ]
    return jsonify({"count": len(items), "results": items})

# =====================================================================================
#                                         UI
# =====================================================================================
@app.route("/")
def home():
    return render_template("main.html")

@app.route("/yelp")
def yelp_page():
    return render_template("yelp.html")

@app.route("/reviews")
def reviews_page():
    return render_template("reviews.html")

@app.route("/favicon.ico")
def favicon():
    return ("", 204)

if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=True, use_reloader=False, threaded=True)
