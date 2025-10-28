import scrapy
from urllib.parse import urljoin, urlparse
import re

class CnewsSpider(scrapy.Spider):
    name = 'bankcnews'
    allowed_domains = ['cnews.ru']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_TIMEOUT': 30,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    def start_requests(self):
        search_terms = ['банк', 'финансы', 'кредит', 'банки']

        for term in search_terms:
            search_url = f"https://www.cnews.ru/search?search={term}"
            yield scrapy.Request(
                url=search_url,
                callback=self.parse_search_results,
                meta={'search_term': term, 'page': 1}
            )

    def parse_search_results(self, response):
        search_term = response.meta['search_term']
        page = response.meta['page']

        self.logger.info(f"Parsing search results for '{search_term}' - page {page}")

        # Method 1: Extract proper article links using CSS selectors
        article_links = []

        # Look for links in search result items with proper structure
        search_items = response.css('.search-results a, .news-item a, .article-item a, a[href*="/news/"], a[href*="/articles/"]')

        for link in search_items:
            href = link.css('::attr(href)').get()
            if href:
                # Clean and validate the URL
                clean_url = self.clean_url(href)
                if clean_url and self.is_valid_article_url(clean_url):
                    article_links.append(clean_url)

        # Method 2: Extract from page text using regex (fallback)
        page_text = response.text
        # Look for URLs in the format /news/date_article_name or /articles/date_article_name
        url_patterns = [
            r'href=["\'](/news/\d{4}-\d{2}-\d{2}_[^"\'\s>]+)["\']',
            r'href=["\'](/articles/\d{4}-\d{2}-\d{2}_[^"\'\s>]+)["\']',
            r'href=["\'](/line/\d{4}-\d{2}-\d{2}_[^"\'\s>]+)["\']',
        ]

        for pattern in url_patterns:
            matches = re.findall(pattern, page_text)
            for match in matches:
                clean_url = self.clean_url(match)
                if clean_url and clean_url not in article_links:
                    article_links.append(clean_url)

        # Remove duplicates
        article_links = list(set(article_links))

        self.logger.info(f"Found {len(article_links)} valid article links")

        # Follow article links
        for article_url in article_links[:10]:  # Limit to first 10 articles per page
            yield scrapy.Request(
                url=article_url,
                callback=self.parse_article,
                meta={'search_term': search_term}
            )

        # Follow next page if exists
        if page < 2:
            next_page = response.css('a[rel="next"]::attr(href)').get()
            if not next_page:
                next_page = response.xpath('//a[contains(text(), "Далее") or contains(text(), "Next")]/@href').get()

            if next_page:
                next_url = urljoin('https://www.cnews.ru', next_page)
                yield scrapy.Request(
                    url=next_url,
                    callback=self.parse_search_results,
                    meta={'search_term': search_term, 'page': page + 1}
                )

    def clean_url(self, url):
        """Clean and normalize URL"""
        if not url:
            return None

        # Remove HTML entities and malformed content
        url = re.sub(r'%3Ca.*?href=%22', '', url)
        url = re.sub(r'%22.*%3E.*%3C/a%3E', '', url)
        url = re.sub(r'&amp;', '&', url)

        # Handle relative URLs
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin('https://www.cnews.ru', url)
        elif not url.startswith('http'):
            url = urljoin('https://www.cnews.ru', url)

        # Remove any remaining malformed parts
        if '%3C' in url or '%22' in url:
            return None

        return url

    def is_valid_article_url(self, url):
        """Check if URL is a valid article URL"""
        if not url.startswith('https://www.cnews.ru/'):
            return False

        valid_patterns = [
            '/news/',
            '/articles/',
            '/line/'
        ]

        return any(pattern in url for pattern in valid_patterns)

    def parse_article(self, response):
        search_term = response.meta['search_term']

        # Extract title
        title = (response.css('h1::text').get() or
                response.css('.article-title::text').get() or
                response.css('title::text').get() or
                "").strip()

        # Clean title
        if ' - CNews.ru' in title:
            title = title.split(' - CNews.ru')[0].strip()

        # Extract description
        description = self.extract_description(response)

        yield {
            'title': title,
            'url': response.url,
            'search_term': search_term,
            'description': description,
        }

    def extract_description(self, response):
        """Extract article description using multiple methods"""

        # Method 1: Get meta description
        meta_desc = response.css('meta[name="description"]::attr(content)').get()
        if meta_desc and len(meta_desc) > 20:
            return meta_desc.strip()

        # Method 2: Get og:description
        og_desc = response.css('meta[property="og:description"]::attr(content)').get()
        if og_desc and len(og_desc) > 20:
            return og_desc.strip()

        # Method 3: Extract from article content
        article_content = response.css('.news_container p::text, article p::text, .article-content p::text').getall()

        if article_content:
            description_parts = []
            for p in article_content:
                text = p.strip()
                if (len(text) > 50 and
                    not any(word in text.lower() for word in ['реклама', 'advertisement', 'читать также'])):
                    description_parts.append(text)
                    if len(description_parts) >= 2:
                        break

            if description_parts:
                return " ".join(description_parts)

        return "Description not available"