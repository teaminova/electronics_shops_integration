import asyncio
import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

CONCURRENCY_LIMIT = 10
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

def get_category_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    category_menu = soup.select("#neptunMain > ul > li > ul > li > a")
    category_menu = category_menu[:61]
    return [
        url + anchor.attrs['href']
        for anchor in category_menu
        if anchor.has_attr('target') and anchor.attrs['target'] == '_self'
    ]


async def get_inner_categories(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, timeout=60000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        await page.wait_for_selector('.innerWrapperGrid a')
        anchors = soup.select('.innerWrapperGrid a')
        await page.close()
        return [a.attrs['href'] for a in anchors]
    except Exception as e:
        print(f"Failed to get inner categories from {url}: {e}")
        await page.close()
        return []


async def get_specs(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, timeout=60000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        panel_body = soup.find('div', class_='panel-body checks ng-binding ng-scope')
        list_items = panel_body.find_all('li')
        text_lines = [item.get_text(separator=' ', strip=True) for item in list_items]
        final_text = "\n".join(text_lines)
        await page.close()
        return final_text
    except Exception as e:
        print(f"Failed to get specs from {url}: {e}")
        await page.close()
        return ""


async def scrape_products(context, url):
    try:
        page = await context.new_page()
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(2000)

        selects = await page.query_selector_all(
            "#affix2 > div > div.product-list-filters-top > div.product-list-filters-top__item.product-list-filters-top__item--number > select"
        )
        if selects:
            # try:
            #     await selects[0].select_option(value="number:100")
            #     print(f"[{url}] Selected number:100")
            #     await page.wait_for_load_state("networkidle")
            #     # await page.wait_for_timeout(10000)
            # except Exception as e:
            #     print(f"[{url}] Dropdown error: {e}")
            try:
                async with page.expect_response(
                        lambda response: response.status == 200
                ) as response_info:
                    await selects[0].select_option(value="number:100")
                    print(f"[{url}] Selected number:100")

                response = await response_info.value
                print(f"[{url}] ✅ Received 200 OK from: {response.url}")

                await page.wait_for_timeout(2000)
            except Exception as e:
                print(f"[{url}] Dropdown error: {e}")

        else:
            print(f'{url} select not found')

        all_products = []

        while True:
            if page.is_closed():
                print(f"Page closed unexpectedly for {url}")
                break

            soup = BeautifulSoup(await page.content(), "html.parser")
            containers = soup.select("div.ng-scope.product-list-item") or soup.select("div.ng-scope.product-list-item-grid")

            for container in containers:
                try:
                    link = container.select_one("a").attrs.get("href")
                    name = container.select_one("a h2").text.strip()
                    image = container.select_one("div.product-list-item__image > div > img").attrs.get("src")

                    happy_card = container.select_one('div.HappyCard')
                    happy_price = None
                    if happy_card:
                        happy_price_elem = happy_card.select_one('.product-price__amount--value.ng-binding')
                        if happy_price_elem:
                            happy_price = happy_price_elem.text.strip()

                    price_elem = container.select_one("div.newPriceModel span.product-price__amount--value.ng-binding")
                    price = price_elem.text.strip() if price_elem else None

                    specs = await get_specs(context, "https://www.neptun.mk/"+link)

                    all_products.append({
                        "Title": name,
                        "Price": price,
                        "HappyPrice": happy_price,
                        "Image src": image,
                        "Link": link,
                        'Specs': specs
                    })
                except Exception as e:
                    print(f"[{url}] Error parsing product: {e}")

            next_btn_disabled = soup.select_one('li.pagination-next.ng-scope.disabled > a')
            if next_btn_disabled:
                print(f'{url} next button is disabled, products gathered: {len(all_products)}')
                break

            try:
                next_button = await page.query_selector("li.pagination-next.ng-scope > a")
                if not next_button:
                    print(f'[{url}] No next button')
                    break
                if await next_button.get_attribute('disabled') == 'disabled' or await next_button.get_attribute('tab-index') == '-1':
                    print(f"[{url}] Next button is disabled weirdly, products gathered: {len(all_products)}")
                    break
                await next_button.click()
                print(f'[{url}] Next button is pressed, products gathered: {len(all_products)}')
                await page.wait_for_timeout(2000)
            except Exception as e:
                print(f"[{url}] Pagination error: {e}")
                break

        await page.close()
        return all_products

    except Exception as e:
        print(f"[{url}] Failed to scrape products: {e}")
        return []


async def scrape_page(context, category_url, base_url):
    async with semaphore:
        try:
            all_products = []

            inner_categories = await get_inner_categories(context, category_url)
            if len(inner_categories) == 0:
                print(f'{category_url} has no inner categories')
            for inner in inner_categories:
                full_url = base_url + inner
                products = await scrape_products(context, full_url)
                all_products.extend(products)

            # Also scrape the main category itself
            main_products = await scrape_products(context, category_url)
            all_products.extend(main_products)

            print(f"✅ Scraped {len(all_products)} products from {category_url}")
            return all_products

        except Exception as e:
            print(f"❌ Failed to scrape {category_url}: {e}")
            return []


def save_to_csv(all_products, file_name):
    df = pd.DataFrame(all_products)
    df.to_csv(file_name, index=False)


async def main():
    url = "https://www.neptun.mk/"
    categories = get_category_urls(url)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            timezone_id="America/New_York",
            java_script_enabled=True
        )
        tasks = [scrape_page(context, cat_url, url) for cat_url in categories]
        results = await asyncio.gather(*tasks)
        all_products = [p for result in results if result for p in result]
        save_to_csv(all_products, "neptun_products.csv")
        await context.close()
        await browser.close()

if __name__ == '__main__':
    asyncio.run(main())