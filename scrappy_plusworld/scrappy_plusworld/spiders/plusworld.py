import scrapy
from urllib.parse import urljoin
import re
from datetime import datetime, timedelta

class PlusworldSpider(scrapy.Spider):
    name = 'plusworld'
    allowed_domains = ['plusworld.ru']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_TIMEOUT': 30,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'FEED_FORMAT': 'json',
        'FEED_URI': 'file:///app/data/plusworld_articles_%(time)s.json',
    }

    def __init__(self, *args, **kwargs):
        super(PlusworldSpider, self).__init__(*args, **kwargs)
        # Calculate date threshold (today - 1 month)
        self.date_threshold = datetime.now() - timedelta(days=30)
        self.logger.info(f"Date threshold: {self.date_threshold.strftime('%d %B %Y')}")

    def start_requests(self):
        # Define all the sections to scrape
        sections = [
            {
                'url': 'https://plusworld.ru/digital-banking/',
                'search_term': 'digital-banking'
            },
            {
                'url': 'https://plusworld.ru/finteh/',
                'search_term': 'fintech'
            },
            {
                'url': 'https://plusworld.ru/iskusstvennyy-intellekt-i-big-data/',
                'search_term': 'ai-big-data'
            }
        ]

        for section in sections:
            self.logger.info(f"Starting scrape for section: {section['search_term']} at {section['url']}")
            yield scrapy.Request(
                url=section['url'],
                callback=self.parse_article_list,
                meta={
                    'page': 1,
                    'search_term': section['search_term'],
                    'base_url': section['url']
                },
                dont_filter=False  # Ensure requests are processed
            )

    def parse_article_list(self, response):
        page = response.meta['page']
        search_term = response.meta['search_term']
        base_url = response.meta['base_url']

        self.logger.info(f"Parsing Plusworld.ru {search_term} page {page} - URL: {response.url}")

        # Extract article links with dates from main page
        articles_data = self.extract_articles_with_dates(response)

        self.logger.info(f"Found {len(articles_data)} raw articles for {search_term} on page {page}")

        # Skip first 7 articles only on page 1
        if page == 1 and len(articles_data) > 7:
            articles_data = articles_data[7:]
            self.logger.info(f"Skipped first 7 articles, {len(articles_data)} remaining for {search_term}")

        self.logger.info(f"Processing {len(articles_data)} articles for {search_term} after filtering")

        # Follow article links to get full content
        article_count = 0
        for article_data in articles_data[:15]:  # Limit to 15 articles per page
            if article_data['date'] is None or article_data['date'] >= self.date_threshold:
                article_count += 1
                self.logger.info(f"Yielding article request for {search_term}: {article_data['title'][:50]}...")
                yield scrapy.Request(
                    url=article_data['url'],
                    callback=self.parse_article,
                    meta={
                        'search_term': search_term,
                        'article_date': article_data['date'],
                        'title': article_data['title']
                    }
                )
            else:
                self.logger.info(f"Skipping old article from {search_term}: {article_data['date']}")

        self.logger.info(f"Yielded {article_count} article requests for {search_term} page {page}")

        # Pagination - look for next page
        if page < 3 and len(articles_data) > 0:
            next_page = self.find_next_page(response)
            if next_page:
                self.logger.info(f"Found next page for {search_term}: {next_page}")
                yield scrapy.Request(
                    url=next_page,
                    callback=self.parse_article_list,
                    meta={
                        'page': page + 1,
                        'search_term': search_term,
                        'base_url': base_url
                    }
                )
            else:
                self.logger.info(f"No next page found for {search_term}")
        else:
            self.logger.info(f"Reached page limit or no articles for {search_term}")

    def extract_articles_with_dates(self, response):
        """
        Extract articles with their dates from the main page listing
        Based on the actual HTML structure
        """
        articles_data = []

        # Method 1: Look for cards in the "Также по теме" or "Другие статьи" sections
        cards = response.css('.card, .pw-wide .card, .section .card')
        self.logger.info(f"Found {len(cards)} card elements")

        for card in cards:
            link = card.css('a::attr(href)').get()
            title = card.css('.card__title::text, .card-title::text, h3::text, h4::text').get()

            if link and title and title.strip():
                # Extract date from the card
                date_text = card.css('.meta span::text, .date::text, time::text').get()
                article_date = self.parse_date_text(date_text) if date_text else None

                full_url = self.clean_url(link)
                if full_url and '/articles/' in full_url:
                    articles_data.append({
                        'url': full_url,
                        'title': title.strip(),
                        'date': article_date
                    })
                    self.logger.debug(f"Found article from card: {title[:50]}...")

        # Method 2: Look for direct article links in content
        if not articles_data:
            article_links = response.css('a[href*="/articles/"]')
            self.logger.info(f"Found {len(article_links)} article links")
            for link in article_links:
                href = link.css('::attr(href)').get()
                title = link.css('::text').get()

                if href and title and title.strip() and len(title.strip()) > 10:
                    # Look for date in parent or nearby elements
                    date_text = self.find_date_near_element(link)
                    article_date = self.parse_date_text(date_text) if date_text else None

                    full_url = self.clean_url(href)
                    if full_url:
                        articles_data.append({
                            'url': full_url,
                            'title': title.strip(),
                            'date': article_date
                        })
                        self.logger.debug(f"Found article from link: {title[:50]}...")

        # Method 3: Look for articles in specific sections
        if not articles_data:
            # Try to find articles in popular sections
            popular_sections = response.css('.popular-embed, .popular-line, .box-news')
            self.logger.info(f"Found {len(popular_sections)} popular sections")
            for section in popular_sections:
                link = section.css('a[href*="/articles/"]::attr(href)').get()
                title = section.css('a::text').get()

                if link and title and title.strip():
                    date_text = section.css('.date::text, .meta::text').get()
                    article_date = self.parse_date_text(date_text) if date_text else None

                    full_url = self.clean_url(link)
                    if full_url:
                        articles_data.append({
                            'url': full_url,
                            'title': title.strip(),
                            'date': article_date
                        })
                        self.logger.debug(f"Found article from popular section: {title[:50]}...")

        # Remove duplicates by URL
        unique_articles = {}
        for article in articles_data:
            if article['url'] not in unique_articles:
                unique_articles[article['url']] = article

        result = list(unique_articles.values())
        self.logger.info(f"After deduplication: {len(result)} unique articles")
        return result

    def find_date_near_element(self, element):
        """Find date text near a link element"""
        # Look in sibling elements
        date_selectors = [
            'following-sibling::span//text()',
            'preceding-sibling::span//text()',
            '../span//text()',
            '../../span//text()',
            '../div[contains(@class, "meta")]//text()',
            '../../div[contains(@class, "meta")]//text()'
        ]

        for selector in date_selectors:
            date_text = element.xpath(selector).get()
            if date_text:
                clean_date = self.clean_date_text(date_text)
                if clean_date:
                    return clean_date

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
            r'\d{1,2}\s+[а-яё]+',
            r'\d+\s+(час|день|дня|дней|минут|недел)',
            r'\d+\s+(hour|day|days|minute|week)'
        ]

        for pattern in date_patterns:
            if re.search(pattern, date_text, re.IGNORECASE):
                return date_text

        return None

    def parse_date_text(self, date_text):
        """Parse date text to datetime object"""
        if not date_text:
            return None

        clean_text = self.clean_date_text(date_text)
        if not clean_text:
            return None

        # Try Russian date format first
        date_obj = self.parse_russian_date(clean_text)
        if date_obj:
            return date_obj

        # Try relative dates
        date_obj = self.parse_relative_date(clean_text)
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

    def find_next_page(self, response):
        """Find next page link"""
        next_selectors = [
            'a.next::attr(href)',
            'a[rel="next"]::attr(href)',
            '.pagination a:contains("Далее")::attr(href)',
            '.pagination a:contains("Next")::attr(href)',
            'a:contains("›")::attr(href)',
            'a:contains("»")::attr(href)'
        ]

        for selector in next_selectors:
            next_page = response.css(selector).get()
            if next_page:
                return self.clean_url(next_page)

        return None

    def clean_url(self, url):
        """Clean and normalize Plusworld.ru URLs"""
        if not url:
            return None

        # Handle relative URLs
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = urljoin('https://plusworld.ru', url)
        elif not url.startswith('http'):
            url = urljoin('https://plusworld.ru', url)

        # Ensure it's a Plusworld.ru URL
        if not url.startswith('https://plusworld.ru/'):
            return None

        return url

    def parse_article(self, response):
        search_term = response.meta.get('search_term', 'digital-banking')
        article_date = response.meta.get('article_date')
        title_from_list = response.meta.get('title')

        # Extract title - prefer the one from list page, fallback to article page
        title = title_from_list
        if not title:
            title = (response.css('h1::text').get() or
                    response.css('.article-title::text').get() or
                    response.css('title::text').get() or "").strip()

        # Clean title
        if ' | Plusworld.ru' in title:
            title = title.split(' | Plusworld.ru')[0].strip()

        # Extract description - get first meaningful paragraph from article content
        description = self.extract_first_paragraph(response)

        self.logger.info(f"Successfully scraped article from {search_term}: {title[:50]}...")

        yield {
            'title': title,
            'url': response.url,
            'search_term': search_term,
            'description': description,
            'article_date': article_date.isoformat() if article_date else None,
            'scraped_at': datetime.now().isoformat(),
        }

    def extract_first_paragraph(self, response):
        """Extract first meaningful paragraph from article content"""

        # Try different content containers
        content_selectors = [
            '.article-content p',
            '.post-content p',
            '.content p',
            '.entry-content p',
            '.text p',
            'article p',
            '.pw-detail .content p'
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