# RFM-Analysis
## Executive Summary

Segmented users of a beauty e-commerce platform into **Gold / Silver / Bronze** membership tiers via weighted RFM scoring, then identified **Silver as the highest-leverage CRM target** 

- Silver members carry an AOV within 15% of Gold (51.1 vs 59.8) but purchase at nearly half the frequency (AOF 1.5 vs 3.2), meaning frequency is the untapped revenue lever. 
- Analysis then mapped *what* to promote (loss-leader promotions with 1.25× basket lift and only 0.25% cherry-picker rate) and *when* to send CRM messages (conversion peaks 9AM – 2PM weekdays; exploration peaks 5PM – 8PM).

---

## Key Results

| Metric | Value |
|--------|-------|
| Gold + Silver Revenue Contribution | ~75% of total platform revenue |
| Silver AOV vs. Gold AOV | 51.1 vs. 59.8 — only 14.6% lower |
| Silver AOF vs. Gold AOF | 1.5 vs. 3.2 — **53% lower** (the lever) |
| Silver Basket Lift (loss-leader sessions) | **1.25×** |
| Silver Cherry-picker Rate | 0.25% vs. 1.52% (Bronze) |
| ATC → Purchase within 1 Hour | **82%** of Silver conversions |
| Peak Purchase Conversion Window | 9AM – 2PM (weekdays) |
| Highest CVR Day of Week | Friday (16.21%) |
| Loss-leader Discount Threshold | ≥20% off weekly average price |

---

## Objectives

1. Clean raw e-commerce event log data — remove invalid sessions, price anomalies, and out-of-scope users
2. Segment users with a weighted RFM model into Gold / Silver / Bronze membership tiers
3. Identify which segment offers the highest incremental revenue opportunity per CRM dollar
4. Quantify the cross-sell impact of loss-leader promotions via basket lift and cherry-picker analysis
5. Map Silver's purchase funnel timing to prescribe CRM message types and send windows

---

## Methodology

### 1 · Data

> Source: [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop) — Michael Kechinov, Kaggle 2020

| Attribute | Detail |
|-----------|--------|
| Domain | Beauty e-commerce |
| Period | Oct 2019 – Feb 2020 |
| Event Types | `view`, `cart` (ATC), `purchase` |
| Member Definition | Users with **≥ 2** purchase events |
| Key Fields | `user_id`, `user_session`, `product_id`, `price`, `event_type`, `event_time` |

---

### 2 · Data Cleaning  `I_Data_Prep.sql`

| Step | Action |
|------|--------|
| **Price Filtering** | Identified sessions containing any `price ≤ 0` event and removed the entire session (not just the row) to preserve session integrity |
| **Session Scoping** | Excluded all records where `user_session IS NULL` |
| **Member Filtering** | Retained only users with `purchase_cnt ≥ 2` via a CTE filter before downstream analysis |
| **Price Outlier Audit** | Computed per-product price fluctuation (`max − min / min`) and audited daily price series for anomalous products |

---

### 3 · RFM Segmentation  `II_RFM___Funnel_Analysis.sql`

**Weighted RFM formula:** `(R × 2) + (F × 4) + (M × 4)`

F and M are double-weighted relative to R, reflecting that purchase volume and value are stronger retention signals than recency alone for a 5-month window.

| Component | Method |
|-----------|--------|
| R / F / M Scoring | `NTILE(5)` quintile ranking per metric |
| Weighted Score | `(r_score×2) + (f_score×4) + (m_score×4)` |
| Final Rank | `PERCENT_RANK()` over weighted score |
| **Gold** | Top 10% (`score_rank ≤ 0.10`) |
| **Silver** | 10th–40th percentile (`score_rank ≤ 0.40`) |
| **Bronze** | Bottom 60% |

**Segment Profiles:**

| Segment | Members | Revenue Share | AOV | AOF | Avg Recency (Days) |
|---------|---------|---------------|-----|-----|--------------------|
| Gold | 13.01% | 34.08% | 59.8 | 3.2 | 34.7 |
| Silver | 41.21% | ~41% | 51.1 | 1.5 | 67.2 |
| Bronze | 45.78% | 25.30% | 23.4 | 1.1 | 77.8 |

---

### 4 · Market Basket Analysis — Loss Leader & Cherry-Picker  `III_MBA_Analysis_.sql`

- **Loss leader definition:** A product in a given week where `weekly_avg_price` is ≥ 30% below its historical average price (`discount_depth ≥ 0.3`)
- **Basket Lift** = avg items in loss-leader sessions ÷ avg items in normal sessions; values > 1.0 indicate incremental cross-sell
- **Cherry-picker rate** = sessions where only the discounted item was purchased ÷ all loss-leader sessions

| Segment | Basket Lift | Cherry-picker Rate |
|---------|-------------|-------------------|
| Gold | 1.31× | 0.18% |
| Silver | 1.25× | 0.25% |
| Bronze | 1.03× | 1.52% |

> 82.96% of all purchases occur in the ≤15% discount range → 20% weekly discount meaningfully separates loss-leader events from routine price noise.

---

### 5 · Silver Segment Deep Dive  `IV_Silver_Segment_Analysis.sql`

| Analysis | Finding |
|----------|---------|
| **Day-of-week CVR** | Weekdays consistently outperform weekends; Friday peaks at 16.21% |
| **Hourly purchase pattern** | 38.13% of weekday purchases fall in 9AM–2PM window |
| **Hourly view & ATC pattern** | Dual peaks: 9AM–1PM (30.6% of ATCs) and 5PM–8PM (23.07%) |
| **ATC → Purchase latency** | 82% of conversions complete **within 1 hour** of add-to-cart |
| **View sessions before purchase** | Avg view sessions examined per product prior to first purchase |

---

## Results & Insights

### WHO — RFM Segmentation
Silver is the highest-ROI CRM segment: large enough to move revenue meaningfully (41% of members), with an AOV that already approaches Gold, but an AOF gap large enough that even modest frequency improvement compounds into significant revenue lift.

### WHAT — Loss Leader Promotions
Loss leaders are effective and **safe** for Silver. A 1.25× basket lift with only a 0.25% cherry-picker rate means promotional spend translates into genuine basket expansion — not discount exploitation. Silver responds to value signals the way Gold does, not the way Bronze does.

### WHEN — CRM Message Timing

| Time Window | Behavioral Signal | Recommended CRM Action |
|-------------|-------------------|------------------------|
| **9AM – 2PM (weekdays)** | Peak purchase intent; 82% of ATCs convert within 1 hour | **Time-limited flash deal** - convert high-intent sessions |
| **5PM – 8PM (weekdays)** | Peak browsing & ATC activity; low immediate conversion | **Related product recommendations** — plant seeds for next-day morning conversion |
| **Friday** | Highest weekly CVR (16.21%) | Priority send day for promotional pushes |

---

## File Structure

```
├── I_Data_Prep.sql                  # Data cleaning & EDA
├── II_RFM___Funnel_Analysis.sql     # RFM scoring, segmentation, funnel metrics
├── III_MBA_Analysis_.sql            # Loss leader, basket lift, cherry-picker
└── IV_Silver_Segment_Analysis.sql   # Silver segment behavioral deep dive
```

---

## Tools & Stack

`BigQuery (SQL)` · `Google Cloud Platform` · `Python` · `Kaggle Open Data`

`RFM Segmentation` · `Funnel Analysis` · `Market Basket Analysis` · `CRM Strategy`
