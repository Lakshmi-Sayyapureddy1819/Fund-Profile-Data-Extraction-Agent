import json
import csv

# Load the JSON file
with open(r"C:\Users\jaysw\DEA\Fund-Profile-Data-Extraction-Agent\Data\visible.connect_final_investors_data_without_email.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# List to hold all website URLs
all_websites = []

# Loop over top-level investor objects
for record in data:
    # Add primary firm website
    website = record.get("website")
    if website and isinstance(website, str):
        all_websites.append(website.strip())

    # Add company websites inside 'rounds'
    for round_entry in record.get("rounds", []):
        if isinstance(round_entry, dict):
            comp_website = round_entry.get("website")
            if comp_website and isinstance(comp_website, str):
                all_websites.append(comp_website.strip())

# Remove duplicates and sort
unique_websites = sorted(set(all_websites))

# Save to CSV
output_file = "extracted_website_urls.csv"
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Website"])
    for url in unique_websites:
        writer.writerow([url])

print(f"✅ Extracted {len(unique_websites)} unique website URLs and saved them to '{output_file}'.")
