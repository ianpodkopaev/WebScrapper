import scrapy
from urllib.parse import urljoin
import re
from datetime import datetime, timedelta

class RbSpider(scrapy.Spider):
    name = 'rb'
    allowed_domains = ['rb.ru']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_TIMEOUT': 30,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'FEED_FORMAT': 'json',
        'FEED_URI': 'file:///app/data/rb_articles_%(time)s.json',
    }

    def __init__(self, *args, **kwargs):
        super(RbSpider, self).__init__(*args, **kwargs)
        # Calculate date threshold (today - 1 month)
        self.date_threshold = datetime.now() - timedelta(days=30)
        self.logger.info(f"Date threshold: {self.date_threshold.strftime('%d %B %Y')}")

    def start_requests(self):
        search_terms = ['банк', 'финансы', 'кредит', 'банки', 'финтех']

        for term in search_terms:
            search_url = f"https://rb.ru/search/?query={term}"
            yield scrapy.Request(
                url=search_url,
                callback=self.parse_search_results,
                meta={'search_term': term, 'page': 1}
            )

    def parse_search_results(self, response):
        search_term = response.meta['search_term']
        page = response.meta['page']

        self.logger.info(f"Parsing RB.ru search results for '{search_term}' - page {page}")

        # Extract article items with dates
        article_items = response.css('.news-item, .search-result-item, [class*="item"]')
        article_links = []

        for item in article_items:
            # Extract date from time.news-item__date
            date_element = item.css('time.news-item__date::text, .news-item__date::text, time::text').get()
            if date_element:
                article_date = self.parse_russian_date(date_element.strip())
                if article_date and article_date >= self.date_threshold:
                    # Date is within range, extract link
                    link = item.css('a.news-item__title::attr(href)').get()
                    if not link:
                        link = item.css('a[href*="/news/"], a[href*="/article/"]::attr(href)').get()

                    if link:
                        full_url = self.clean_url(link)
                        if full_url and full_url not in article_links:
                            article_links.append(full_url)
                            self.logger.info(f"Recent article found: {article_date.strftime('%d.%m.%Y')} - {full_url}")
                elif article_date:
                    self.logger.debug(f"Article too old: {article_date.strftime('%d.%m.%Y')}")

        # If no date filtering found articles, fallback to all articles
        if not article_links:
            self.logger.info("No recent articles found with dates, falling back to all articles")
            title_links = response.css('a.news-item__title')
            for link in title_links:
                href = link.css('::attr(href)').get()
                if href:
                    full_url = self.clean_url(href)
                    if full_url and full_url not in article_links:
                        article_links.append(full_url)

        self.logger.info(f"Found {len(article_links)} article links on RB.ru (after date filtering)")

        # Follow article links to get full content
        for article_url in article_links[:15]:
            yield scrapy.Request(
                url=article_url,
                callback=self.parse_article,
                meta={'search_term': search_term}
            )

        # Pagination for RB.ru
        if page < 3:
            next_page = response.css('a.pagination__next::attr(href)').get()
            if not next_page:
                next_page = response.css('a[rel="next"]::attr(href)').get()

            if next_page:
                next_url = self.clean_url(next_page)
                if next_url:
                    yield scrapy.Request(
                        url=next_url,
                        callback=self.parse_search_results,
                        meta={'search_term': search_term, 'page': page + 1}
                    )

    def parse_russian_date(self, date_str):
        """
        Parse Russian date format using regex
        """
        try:
            # Use regex to extract date parts
            pattern = r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})'
            match = re.search(pattern, date_str)

            if match:
                day = int(match.group(1))
                month_ru = match.group(2).lower()
                year = int(match.group(3))

                month_mapping = {
                    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
                    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
                    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
                }

                if month_ru in month_mapping:
                    return datetime(year, month_mapping[month_ru], day)

            return None
        except Exception as e:
            self.logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None

    def clean_url(self, url):
        """Clean and normalize RB.ru URLs"""
        if not url:
            return None

        # Handle relative URLs
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin('https://rb.ru', url)
        elif not url.startswith('http'):
            url = urljoin('https://rb.ru', url)

        # Ensure it's an RB.ru URL
        if not url.startswith('https://rb.ru/'):
            return None

        return url

    def parse_article(self, response):
        search_term = response.meta['search_term']

        # Extract title - multiple selectors for RB.ru
        title = (response.css('h1.news-item__title::text').get() or
                response.css('h1.article__title::text').get() or
                response.css('h1::text').get() or
                response.css('title::text').get() or "").strip()

        # Clean title
        if ' | RB.RU' in title:
            title = title.split(' | RB.RU')[0].strip()

        # Extract article date from the article page
        article_date = None
        date_element = response.css('time.news-item__date::text, .news-item__date::text, time::text').get()
        if date_element:
            article_date = self.parse_russian_date(date_element.strip())

        # Extract description - get first meaningful paragraph from article content
        description = self.extract_first_paragraph(response)

        yield {
            'title': title,
            'url': response.url,
            'search_term': search_term,
            'description': description,
            'article_date': article_date.isoformat() if article_date else None,
        }

    def extract_first_paragraph(self, response):
        """Extract first meaningful paragraph from article content"""

        # Try different content containers used by RB.ru
        content_selectors = [
            '.news-item__content p',
            '.article__content p',
            '.post-content p',
            '.content p',
            '.text p'
        ]

        for selector in content_selectors:
            paragraphs = response.css(selector)
            for p in paragraphs:
                text = p.css('::text').get()
                if text:
                    clean_text = self.clean_paragraph(text)
                    if clean_text and len(clean_text) > 30:
                        return clean_text

        # Fallback: get any first paragraph
        first_p = response.css('p::text').get()
        if first_p:
            clean_text = self.clean_paragraph(first_p)
            if clean_text:
                return clean_text

        return "Description not available"

    def clean_paragraph(self, text):
        """Clean paragraph text"""
        if not text:
            return ""

        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        # Filter out unwanted content
        unwanted_patterns = [
            'Подпишитесь',
            'читайте также',
            'реклама',
            'advertisement',
            'Фото:',
            'Фотография:',
            'Источник:'
        ]

        for pattern in unwanted_patterns:
            if pattern.lower() in text.lower():
                return ""

        return text