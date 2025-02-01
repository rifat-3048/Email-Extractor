import requests
from bs4 import BeautifulSoup
import re
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
import time

def clean_url(url):
    if not url:
        return ''
    url = url.strip().lower()
    parsed = urlparse(url)
    base_url = parsed.netloc
    if not base_url:
        base_url = url.split('/')[0]
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    print(f"Cleaned URL: {url}", file=sys.stderr)
    return url

def is_valid_email(email):
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    invalid_patterns = [r'\.png$', r'\.jpg$', r'\.gif$', r'\.jpeg$', r'\.webp$', 
                        r'example\.com', r'domain\.com', r'yourname', r'youremail', 
                        r'@2x', r'@\dx', r'@test', r'@sample', r'noreply', 
                        r'no-reply', r'donotreply']
    if not re.match(email_pattern, email):
        return False
    for pattern in invalid_patterns:
        if re.search(pattern, email.lower()):
            return False
    if len(email) > 254:
        return False
    domain = email.split('@')[1]
    if len(domain.split('.')) < 2:
        return False
    return True

def find_emails_in_text(text):
    email_patterns = [
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'email:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'contact:\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    ]
    emails = set()
    for pattern in email_patterns:
        found = re.findall(pattern, text, re.IGNORECASE)
        for email in found:
            if isinstance(email, tuple):
                email = email[0]
            email = email.strip()
            if is_valid_email(email):
                emails.add(email.lower())
    return emails

def get_all_links(soup, base_url):
    relevant_paths = ['contact', 'about', 'team', 'staff', 'company', 'info']
    links = set()
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if any(path in href for path in relevant_paths):
            full_url = urljoin(base_url, href)
            links.add(full_url)
    return links

def extract_emails_from_website(url):
    if not url:
        return set()
    url = clean_url(url)
    if not url:
        return set()
    print(f"Processing website: {url}", file=sys.stderr)
    emails = set()
    visited_urls = set()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        emails.update(find_emails_in_text(response.text))
        links_to_check = get_all_links(soup, url)
        for link in links_to_check:
            if link in visited_urls:
                continue
            try:
                print(f"Checking page: {link}", file=sys.stderr)
                visited_urls.add(link)
                response = requests.get(link, headers=headers, timeout=15)
                if response.status_code == 200:
                    found_emails = find_emails_in_text(response.text)
                    if found_emails:
                        print(f"Found emails on {link}: {found_emails}", file=sys.stderr)
                        emails.update(found_emails)
                time.sleep(1)
            except Exception as e:
                print(f"Error checking link {link}: {str(e)}", file=sys.stderr)
                continue
    except Exception as e:
        print(f"Error processing {url}: {str(e)}", file=sys.stderr)
    valid_emails = {email for email in emails if is_valid_email(email)}
    print(f"Final valid emails found for {url}: {valid_emails}", file=sys.stderr)
    return list(valid_emails)

def process_business(business):
    website = business.get('websiteLink', '').strip()
    print(f"\nProcessing business: {business.get('businessName', 'Unknown')}", file=sys.stderr)
    print(f"Website: {website}", file=sys.stderr)
    if website:
        emails = extract_emails_from_website(website)
        if emails:
            business['email'] = ', '.join(emails)
            print(f"Added valid emails to business: {emails}", file=sys.stderr)
    return business

def main():
    print("Starting email extraction process", file=sys.stderr)
    with open('businesses.json', 'r', encoding='utf-8') as f:
        input_data = json.load(f)
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_business = {executor.submit(process_business, business): business for business in input_data}
        for future in as_completed(future_to_business):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing business: {str(e)}", file=sys.stderr)
                results.append(future_to_business[future])
    results.sort(key=lambda x: input_data.index(x))  # Sort by input order
    with open('output.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
