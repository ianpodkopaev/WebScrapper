import scrapy
from urllib.parse import urljoin
import re
from datetime import datetime, timedelta

class BankinformSpider(scrapy.Spider):
    name = 'bankinform'
    allowed_domains = ['bankinform.ru']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_TIMEOUT': 30,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'FEED_FORMAT': 'json',
        'FEED_URI': 'file:///app/data/bankinform_articles_%(time)s.json',
    }

    def __init__(self, *args, **kwargs):
        super(BankinformSpider, self).__init__(*args, **kwargs)
        # Calculate date threshold (today - 1 month)
        self.date_threshold = datetime.now() - timedelta(days=30)
        self.logger.info(f"Date threshold: {self.date_threshold.strftime('%d %B %Y')}")

    def start_requests(self):
        start_url = "https://bankinform.ru/news/tag/2149"
        yield scrapy.Request(
            url=start_url,
            callback=self.parse_article_list,
            meta={'page': 1}
        )

    def parse_article_list(self, response):
        page = response.meta['page']

        self.logger.info(f"Parsing Bankinform.ru page {page}")

        # Extract article links with titles and dates
        articles_data = self.extract_articles_with_data(response)

        self.logger.info(f"Found {len(articles_data)} articles on page {page}")

        # Follow article links to get full content
        for article_data in articles_data:
            if article_data['date'] is None or article_data['date'] >= self.date_threshold:
                yield scrapy.Request(
                    url=article_data['url'],
                    callback=self.parse_article,
                    meta={
                        'title': article_data['title'],
                        'article_date': article_data['date']
                    }
                )
            else:
                self.logger.info(f"Skipping old article: {article_data['date']}")

        # Pagination - look for next page
        if page < 3 and len(articles_data) > 0:
            next_page = self.find_next_page(response)
            if next_page:
                self.logger.info(f"Found next page: {next_page}")
                yield scrapy.Request(
                    url=next_page,
                    callback=self.parse_article_list,
                    meta={'page': page + 1}
                )

    def extract_articles_with_data(self, response):
        """
        Extract articles with titles and dates from bankinform.ru
        """
        articles_data = []

        # Find all article links with the specified class
        article_links = response.css('a.text-decoration-none')

        for link in article_links:
            href = link.css('::attr(href)').get()
            title = link.css('::text').get()

            if href and title and title.strip():
                # Find date - look for time element with date class
                date_element = link.xpath('./following-sibling::time[contains(@class, "date")] | '
                                        '../time[contains(@class, "date")] | '
                                        '../../time[contains(@class, "date")]').get()

                date_text = None
                if date_element:
                    date_selector = scrapy.Selector(text=date_element)
                    date_text = date_selector.css('::text').get()

                article_date = self.parse_date_text(date_text) if date_text else None

                full_url = self.clean_url(href)
                if full_url:
                    articles_data.append({
                        'url': full_url,
                        'title': title.strip(),
                        'date': article_date
                    })

        return articles_data

    def parse_date_text(self, date_text):
        """Parse date text to datetime object"""
        if not date_text:
            return None

        clean_text = self.clean_date_text(date_text)
        if not clean_text:
            return None

        # Try Russian date format
        date_obj = self.parse_russian_date(clean_text)
        if date_obj:
            return date_obj

        # Try relative dates
        date_obj = self.parse_relative_date(clean_text)
        if date_obj:
            return date_obj

        # Try standard date formats
        date_obj = self.parse_standard_date(clean_text)
        if date_obj:
            return date_obj

        return None

    def parse_russian_date(self, date_str):
        """
        Parse Russian date format to datetime object
        Handles formats like: '27 октября 2025'
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
            self.logger.warning(f"Failed to parse Russian date '{date_str}': {e}")
            return None

    def parse_standard_date(self, date_str):
        """Parse standard date formats like DD.MM.YYYY"""
        try:
            # Try DD.MM.YYYY
            pattern1 = r'(\d{1,2})\.(\d{1,2})\.(\d{4})'
            match1 = re.search(pattern1, date_str)
            if match1:
                day = int(match1.group(1))
                month = int(match1.group(2))
                year = int(match1.group(3))
                return datetime(year, month, day)

            # Try YYYY-MM-DD
            pattern2 = r'(\d{4})-(\d{1,2})-(\d{1,2})'
            match2 = re.search(pattern2, date_str)
            if match2:
                year = int(match2.group(1))
                month = int(match2.group(2))
                day = int(match2.group(3))
                return datetime(year, month, day)

            return None
        except Exception as e:
            self.logger.warning(f"Failed to parse standard date '{date_str}': {e}")
            return None

    def parse_relative_date(self, date_str):
        """Parse relative dates like '1 день назад', '2 часа назад'"""
        try:
            numbers = re.findall(r'\d+', date_str)
            if numbers:
                amount = int(numbers[0])

                if 'день' in date_str or 'дня' in date_str or 'дней' in date_str:
                    return datetime.now() - timedelta(days=amount)
                elif 'час' in date_str or 'часа' in date_str or 'часов' in date_str:
                    return datetime.now() - timedelta(hours=amount)
                elif 'минут' in date_str:
                    return datetime.now() - timedelta(minutes=amount)
                elif 'недел' in date_str:
                    return datetime.now() - timedelta(weeks=amount)

            return None
        except Exception as e:
            self.logger.warning(f"Failed to parse relative date '{date_str}': {e}")
            return None

    def clean_date_text(self, date_text):
        """Clean and validate date text"""
        if not date_text:
            return None

        date_text = date_text.strip()

        # Remove icons and extra text
        date_text = re.sub(r'[⏰🕒📅]', '', date_text)
        date_text = re.sub(r'\s+', ' ', date_text)

        # Check if it looks like a date
        date_patterns = [
            r'\d{1,2}\s+[а-яё]+\s+\d{4}',
            r'\d{1,2}\.\d{1,2}\.\d{4}',
            r'\d{1,2}/\d{1,2}/\d{4}',
            r'\d{4}-\d{1,2}-\d{1,2}',
            r'\d{1,2}\s+[а-яё]+',
            r'\d+\s+(час|день|дня|дней|минут|недел)'
        ]

        for pattern in date_patterns:
            if re.search(pattern, date_text, re.IGNORECASE):
                return date_text

        return None

    def find_next_page(self, response):
        """Find next page link"""
        next_selectors = [
            'a.next::attr(href)',
            'a[rel="next"]::attr(href)',
            '.pagination a:contains("Далее")::attr(href)',
            '.pagination a:contains("Next")::attr(href)',
            'a:contains("›")::attr(href)',
            'a:contains("»")::attr(href)',
            '.pager-next a::attr(href)'
        ]

        for selector in next_selectors:
            next_page = response.css(selector).get()
            if next_page:
                return self.clean_url(next_page)

        return None

    def clean_url(self, url):
        """Clean and normalize URLs"""
        if not url:
            return None

        # Handle relative URLs
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin('https://bankinform.ru', url)
        elif not url.startswith('http'):
            url = urljoin('https://bankinform.ru', url)

        # Ensure it's a bankinform.ru URL
        if not url.startswith('https://bankinform.ru/'):
            return None

        return url

    def parse_article(self, response):
        title = response.meta.get('title')
        article_date = response.meta.get('article_date')

        # If title wasn't passed from list page, extract it from article
        if not title:
            title = (response.css('h1::text').get() or
                    response.css('.article-title::text').get() or
                    response.css('title::text').get() or "").strip()

        # Extract description - look for <p> tags in the article content
        description = self.extract_description(response)

        yield {
            'title': title,
            'url': response.url,
            'search_term': 'bankinform-fintech',
            'description': description,
            'article_date': article_date.isoformat() if article_date else None,
            'scraped_at': datetime.now().isoformat(),
        }

    def extract_description(self, response):
        """Extract description from article content - first meaningful <p> tag"""

        # Try to find the main article content
        content_selectors = [
            'article p',
            '.article-content p',
            '.post-content p',
            '.content p',
            '.entry-content p',
            '.news-detail p',
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
            'Источник:',
            'По материалам'
        ]

        for pattern in unwanted_patterns:
            if pattern.lower() in text.lower():
                return ""

        return text