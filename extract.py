import asyncio
import aiohttp
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
EMAIL_REGEX = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")

def extract_emails(text):
    return list(set(EMAIL_REGEX.findall(text or "")))

async def fetch(session, url, retry=3):
    """Fetch with retry and SSL verification disabled"""
    delay = 1
    for attempt in range(retry):
        try:
            async with session.get(url, headers=HEADERS, ssl=False, timeout=20) as resp:
                if resp.status == 200:
                    return await resp.text()
                elif resp.status in {403, 404}:
                    print(f"Permanent HTTP {resp.status} at {url}, skipping.")
                    return None
                else:
                    print(f"HTTP {resp.status} at {url}, retrying...")
        except Exception as e:
            print(f"Attempt {attempt+1} failed for {url} -- {e}")
        await asyncio.sleep(delay)
        delay *= 2
    print(f"Giving up on {url} after {retry} attempts.")
    return None

async def find_contact_pages(session, base_url):
    """Try locate /contact, /about, /team URLs and return list of full URLs"""
    html = await fetch(session, base_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text().lower()
        if any(x in href or x in text for x in ["contact", "about", "team", "staff"]):
            full_link = urljoin(base_url, a["href"])
            if full_link not in links:
                links.append(full_link)
    return links

async def get_emails_from_pages(session, urls):
    emails = set()
    for url in urls:
        content = await fetch(session, url)
        if content:
            emails.update(extract_emails(content))
    return list(emails)

def parse_profile(html, base_url, emails):
    soup = BeautifulSoup(html, "lxml")
    profile = {
        "Name": soup.title.string.strip() if soup.title and soup.title.string else "",
        "title": "",
        "fund": "",
        "profile_image": "",
        "Website": base_url,
        "Global_HQ": "",
        "Countries": [],
        "Stage": [],
        "Overview": "",
        "type": "",
        "Industry": [],
        "business_models": [],
        "Cheque_range": "",
        "Linkedin_Company": "",
        "Email": emails[0] if emails else "",
        "Linkedin_Personal": [],
        "Twitter": "",
        "youtube": "",
        "crunchbase": "",
        "focus": [],
        "investment_geography": [],
        "fund_types": [],
        "leads_investments": None,
        "co_invests": None,
        "takes_board_seats": None,
        "thesis": "",
        "traction_metrics": "",
        "exit_strategies_preference": [],
        "investor_statements": [],
        "verified": True,
        "min_check_size": None,
        "sweet_spot_check_size": None,
        "max_check_size": None,
        "recent_fund_size": None,
        "recent_fund_close_date": "",
        "rounds": [],
        "traction": "",
    }
    # Extract logo by finding an <img> with 'logo' in alt
    logo = soup.find("img", alt=re.compile("logo", re.I))
    if logo and logo.has_attr("src"):
        profile["profile_image"] = logo["src"]
    # Extract meta description or first paragraph as overview
    desc = soup.find("meta", attrs={"name": "description"})
    if desc and desc.has_attr("content"):
        profile["Overview"] = desc["content"].strip()
    else:
        p = soup.find("p")
        if p:
            profile["Overview"] = p.get_text(strip=True)
    return profile

async def process_url(session, url):
    html = await fetch(session, url)
    if not html:
        print(f"Failed to fetch homepage of {url}")
        return None
    emails = extract_emails(html)
    if not emails:
        contact_pages = await find_contact_pages(session, url)
        if contact_pages:
            emails = await get_emails_from_pages(session, contact_pages)
    profile = parse_profile(html, url, emails)
    return profile

async def main(url_file, output_file, concurrency=10):
    with open(url_file, "r", encoding="utf-8") as file:
        urls = [line.strip() for line in file if line.strip()]
    connector = aiohttp.TCPConnector(limit=concurrency)
    results = []
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(concurrency)
        async def sem_task(u):
            async with sem:
                return await process_url(session, u)
        tasks = [asyncio.create_task(sem_task(url)) for url in urls]
        for idx, task in enumerate(asyncio.as_completed(tasks), 1):
            profile = await task
            if profile:
                results.append(profile)
            if idx % 50 == 0:
                print(f"Processed {idx} URLs")
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(results, outfile, ensure_ascii=False, indent=2)
    print(f"Extraction complete! Saved {len(results)} profiles to {output_file}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main("C:\\Users\\jaysw\\DEA\\Fund-Profile-Data-Extraction-Agent\\extracted_website_urls.csv", "investor_profiles.json"))
