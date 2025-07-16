import json
from datetime import datetime

# Load the raw dataset
with open(r"C:\Users\jaysw\DEA\Fund-Profile-Data-Extraction-Agent\Data\visible.connect_final_investors_data_without_email.json", "r", encoding="utf-8") as f:
    raw_data = json.load(f)

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def parse_rounds(rounds):
    structured = {}
    for r in rounds:
        amount = safe_float(r.get("amount-raised"))
        
        # Round naming logic (you can customize based on your data range)
        if amount < 2000000:
            round_name = "Seed"
        elif amount < 10000000:
            round_name = "Series A"
        elif amount < 50000000:
            round_name = "Series B"
        else:
            round_name = "Growth"

        investment_date = r.get("announced-date", "")
        company = r.get("company", "").strip()

        if not investment_date or not company:
            continue

        key = (round_name, investment_date)
        structured.setdefault(key, []).append(company)

    return [
        {
            "round_name": round,
            "investment_date": date,
            "companies_invested_in": companies
        }
        for (round, date), companies in structured.items()
    ]

def parse_people(people):
    return [p["linkedin"] for p in people if p.get("linkedin")]

def clean_value(val):
    try:
        return float(val) if val else None
    except:
        return None

def format_date(date):
    if date:
        return date.split("T")[0]
    return ""

def format_profile(record):
    return {
        "Name": record.get("name", ""),
        "title": "",
        "fund": record.get("name", ""),
        "profile_image": record.get("img", ""),
        "Website": record.get("website", ""),
        "Global_HQ": ", ".join(filter(None, [record.get("city"), record.get("region"), record.get("country")])),
        "Countries": [record["country"]] if record.get("country") else [],
        "Stage": record.get("stages", []),
        "Overview": record.get("description", ""),
        "type": "Venture Capital Firm",  # Static assumption
        "Industry": record.get("tags", []),
        "business_models": [],
        "Cheque_range": "",  # Optional: dynamically map from cheque values
        "Linkedin_Company": record.get("linkedin_url", ""),
        "Email": "",  # Email field not provided
        "Linkedin_Personal": parse_people(record.get("people", [])),
        "Twitter": record.get("twitter_url", ""),
        "youtube": "",
        "crunchbase": "",
        "focus": record.get("focus", []),
        "investment_geography": record.get("investment_geography", []),
        "fund_types": record.get("fund_types", []),
        "leads_investments": record.get("leads_investments"),
        "co_invests": record.get("co_invests"),
        "takes_board_seats": record.get("takes_board_seats"),
        "thesis": record.get("thesis", ""),
        "traction_metrics": record.get("traction_metrics", ""),
        "exit_strategies_preference": [],
        "investor_statements": [],
        "verified": True,
        "min_check_size": clean_value(record.get("min_check_size")),
        "sweet_spot_check_size": clean_value(record.get("sweet_spot_check_size")),
        "max_check_size": clean_value(record.get("max_check_size")),
        "recent_fund_size": clean_value(record.get("recent_fund_size")),
        "recent_fund_close_date": format_date(record.get("recent_fund_close_date")),
        "rounds": parse_rounds(record.get("rounds", [])),
        "traction": f"Invested in {len(record.get('rounds', []))} companies." if record.get("rounds") else ""
    }

# Transform all profiles
structured_profiles = [format_profile(r) for r in raw_data]

# Save output
with open("investor_profiles_structured.json", "w", encoding="utf-8") as f:
    json.dump(structured_profiles, f, indent=2)

print(f"✅ Processed {len(structured_profiles)} investor profiles into structured JSON.")
