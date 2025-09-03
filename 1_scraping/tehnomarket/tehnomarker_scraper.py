import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import re
import pandas as pd

CONCURRENCY_LIMIT = 5
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

def get_category_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    selector = 'ul.nav.cat-nav.clearfix > li > ul > li > a'
    category_menu = soup.select(selector)
    return [anchor.attrs['href'] for anchor in category_menu]

async def get_specs(context, url):
    try:
        page = await context.new_page()
        await page.goto(url)
        soup = BeautifulSoup(await page.content(), "html.parser")
        desc_div = soup.select_one('div.span12.clearfix.padl30.padt20')
        text = desc_div.get_text(separator='\n', strip=True)
        await page.close()
        return text
    except Exception as e:
        print(f'Unable to get specs for {url} with error {e}')
    return ""

async def scrape_products(context, url):
    async with semaphore:
        try:
            page = await context.new_page()
            await page.goto(url)
            select = await page.query_selector('div.limit.adjust-elems > select') #TODO: Maybe make this it's own function? Look into pass by reference/value for playwright pages
            if select:
                try:
                    await select.select_option(value='/offset/64/')
                    await page.wait_for_timeout(2000)
                except Exception as e:
                    print(f"Dropdown error: {e}")

            all_products = []
            soup = BeautifulSoup(await page.content(), "html.parser")
            while True:
                products = soup.select('ul.products.products-display-grid.thumbnails > li')
                for product in products:
                    link = product.select_one('div > div > a')['href']
                    title = product.select_one('div > div > a').text
                    price_spans = product.select('div.product-price.clearfix strong > span')
                    price = price_spans[0].text.strip() + price_spans[1].text.strip()
                    style = product.select_one('figure')['style']
                    match = re.search(r"background-image:\s*url\(['\"]?(.*?)['\"]?\)", style)
                    image_url = None
                    if match:
                        image_url = match.group(1)
                    else:
                        print('Failed to extract image')

                    specs = await get_specs(context, link)

                    all_products.append({
                        'Title': title,
                        'Price': price,
                        'Image': image_url,
                        'Link': link,
                        'Specs': specs
                    })
                try:
                    next_button = await page.query_selector("i.icon-angle-right")
                    if not next_button:
                        break
                    await next_button.click()
                    await page.wait_for_timeout(2000)
                except Exception as e:
                    print(f"[{url}] Pagination error: {e}")
                    break
            await page.close()
            return all_products
        except Exception as e:
            print(f'Scraping error: {e}')

def save_to_csv(all_products, file_name):
    df = pd.DataFrame(all_products)
    df.to_csv(file_name, index=False)

async def main():
    url = 'https://www.tehnomarket.com.mk/'
    category_urls = get_category_urls(url)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
            java_script_enabled=True
        )
        tasks = [scrape_products(context, url) for url in category_urls]
        results = await asyncio.gather(*tasks)
        all_products = [p for result in results if result for p in result]
        save_to_csv(all_products, "tehnomarker_products.csv")
        await context.close()
        await browser.close()


if __name__ == '__main__':
    asyncio.run(main())