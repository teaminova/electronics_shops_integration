import requests
from bs4 import BeautifulSoup
import asyncio
from playwright.sync_api import sync_playwright
import pandas as pd


def get_category_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    category_menu = soup.find("div", class_="all-categories")
    categories = category_menu.find_all("h4", {"class": "section-title"})

    category_a_tags = [
        category.find("a", href=True, recursive=False)
        for category in categories
        if category.find("a", href=True, recursive=False) is not None
    ]

    disallowed_keywords = ["vouchers"]
    category_urls = [
        link["href"].strip()
        for link in category_a_tags
        if not any(keyword in link["href"] for keyword in disallowed_keywords)
    ]

    print(f"Total categories found: {len(category_urls)}")
    return category_urls


def extract_products(page_html):
    soup = BeautifulSoup(page_html, "html.parser")
    products = []

    product_cards = soup.select("div.product-card")
    for product_card in product_cards:
        title = product_card.select_one('.product-name')
        price = product_card.select_one('.product-price')
        image = product_card.select_one('a.product-image img', src=True)
        link = product_card.select_one('a.product-name', href=True)

        products.append({
            'Title': title.text.strip() if title else 'N/A',
            'Price': price.text.strip() if price else 'N/A',
            'Image Src': image.attrs['src'] if image else 'N/A',
            'Link': link['href'] if link and link['href'].startswith("https://www.anhoch.com") else 'N/A'
        })

    return products


def get_total_pages(soup):
    pagination = soup.find("ul", class_="pagination")
    if pagination:
        page_items = pagination.find_all("li", class_="page-item")
        if len(page_items) >= 2:
            try:
                return int(page_items[-2].text.strip())
            except ValueError:
                pass
    return 1


def scrape_products_from_category(category_url, page):
    all_products = []

    page.goto(category_url, timeout=20000)

    try:
        page.wait_for_timeout(3000)
        page.wait_for_selector("div.product-card", timeout=10000)
    except:
        print(f"⚠️ No products found or timeout in {category_url}")
        return []

    soup = BeautifulSoup(page.content(), "html.parser")
    total_pages = get_total_pages(soup)
    print(f"Total pages in {category_url}: {total_pages}")

    for i in range(1, total_pages + 1):
        url = f"{category_url}?page={i}"
        print(f"Scraping page {i}: {url}")
        page.goto(url, timeout=20000)
        page.wait_for_timeout(2000)

        try:
            page.wait_for_selector("div.product-card", timeout=10000)
        except:
            print(f"⚠️ Skipping page {i} (no products)")
            continue

        products = extract_products(page.content())
        all_products.extend(products)

    return all_products


def get_products(category_urls):
    all_products = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for category_url in category_urls:
            print(f"Processing category: {category_url}")
            products = scrape_products_from_category(category_url, page)
            print(f"Found {len(products)} products in {category_url}")
            all_products.extend(products)

        browser.close()

    print(f"Total products found: {len(all_products)}")
    return all_products


def save_to_csv(all_products, file_name):
    df = pd.DataFrame(all_products)
    df.to_csv(file_name, index=False)


def main():
    # Get categories
    url = "https://www.anhoch.com/categories"
    category_urls = get_category_urls(url)

    # Get products
    all_products = get_products(category_urls)

    # Save to CSV
    file_name = "anhoch_products.csv"
    save_to_csv(all_products, file_name)


if __name__ == '__main__':
    main()
