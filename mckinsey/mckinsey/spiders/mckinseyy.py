import scrapy
import random

class McKinseySpider(scrapy.Spider):
    name = 'mckinseyy'
    allowed_domains = ['mckinsey.com']
    start_urls = ['https://www.mckinsey.com/search?q=bank+ai']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,  # Disable robots.txt checking
        'DOWNLOAD_TIMEOUT': 30,   # Set timeout to 30 seconds
        'DOWNLOAD_DELAY': 2,      # Add delay between requests
        'CONCURRENT_REQUESTS': 1, # Be polite
        'RETRY_TIMES': 2,         # Retry failed requests
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    def parse(self, response):
        self.logger.info(f"Successfully visited: {response.url}")

        # Check if we got a valid response
        if response.status != 200:
            self.logger.error(f"Failed to fetch page. Status: {response.status}")
            return

        # Extract results
        results_found = 0
        for item in response.css('.item.result-template'):
            title = item.css('h3.headline::text').get()
            url = item.css('a.item-title-link::attr(href)').get()

            if url and not url.startswith('http'):
                url = response.urljoin(url)

            if title and url:
                yield {
                    'title': title.strip(),
                    'url': url,
                    'description': item.css('p.description::text').get('').strip(),
                }
                results_found += 1
                self.logger.info(f"Found: {title}")

        self.logger.info(f"Total results found: {results_found}")

        # If no results with primary selector, try alternatives
        if results_found == 0:
            yield from self.try_alternative_selectors(response)

    def try_alternative_selectors(self, response):
        """Try alternative CSS selectors if primary ones don't work"""
        self.logger.info("Trying alternative selectors...")

        # Alternative 1: Look for any article-like elements
        for article in response.css('[class*="article"], [class*="result"], [class*="item"]'):
            title = article.css('h1, h2, h3, h4::text').get()
            link = article.css('a::attr(href)').get()

            if title and link:
                if link and not link.startswith('http'):
                    link = response.urljoin(link)

                yield {
                    'title': title.strip(),
                    'url': link,
                    'description': article.css('p::text').get('').strip()[:100],  # First 100 chars
                }
                self.logger.info(f"Found with alternative selector: {title}")

        # Save the page for debugging
        with open('debug_page.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        self.logger.info("Saved page content to debug_page.html for inspection")