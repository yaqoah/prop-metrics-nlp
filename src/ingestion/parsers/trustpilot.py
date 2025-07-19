import re
from src.utils.logger import get_logger
from api.data_models import Firm, Review
from datetime import datetime


class Trustpilot:
    def __init__(self):
        self.logger = get_logger("parsers.trustpilot")

    def parse_firm(self, soup, url):
        try:

             # Extract firm name
            name_elem = soup.find('span', {'class': re.compile('title_displayName')})
            name = name_elem.text.strip() if name_elem else "Unknown"
            
            # Extract overall rating
            rating_elem = soup.find('p', {'data-rating-typography': 'true'})
            rating = float(rating_elem.text.strip()) if rating_elem else 0.0
            
            # Extract total reviews
            reviews_elem = soup.find('p', {'data-reviews-count-typography': 'true'})
            total_reviews_text = reviews_elem.text if reviews_elem else "0"
            total_reviews = self._parse_review_count(total_reviews_text)

            # Extract firm claim status & status
            claimed = False
            for card in soup.select('div#business-unit-title'):
                # Claimed profile
                claimed_tag = card.select_one('.styles_labelWrapper__ONqtM')
                if claimed_tag and "Claimed profile" in claimed_tag.text:
                    claimed = True

                # Category
                category_tag = card.select_one('.styles_breadcrumb__klHaT a')
                category = category_tag.text.strip() if category_tag else "Unknown"
            
            # Extract company description
            desc_elem = (soup.select_one('.styles_companyDetailsCard__k6mCF p[data-relevant-review-text-typography="true"]')
                        or soup.select_one('.styles_container__NG5iv.customer-generated-content')
                        or soup.select_one('.styles_companyDetailsCard__k6mCF p'))

            if desc_elem:
                description = desc_elem.get_text(separator=' ', strip=True)
            else:
                description = "No description found"

            # Extract rating distribution 
            rating_container = soup.find("div", {"data-reviews-overview-paper": True})
            if rating_container:
                rating_distribution=self._parse_rating_distribution(rating_container)
            else:
                rating_distribution= {}

            
            # Extract website
            website_elem = soup.find('a', {'data-visit-website-button-link': 'true'})
            website = website_elem.get('href') if website_elem else None

            return Firm(
                name=name,
                trustpilot_url=url,
                rating=rating,
                total_reviews=total_reviews,
                rating_distribution=rating_distribution,
                claimed=claimed,
                website=website,
                category=category,
                description=description
            )
        except Exception as e:
            self.logger.error(f"Error parsing firm's data: {e}")
            return None
    
    def parse_reviews(self, soup):
        reviews = []
        # Find all review cards
        review_cards = soup.find_all('article', {'data-service-review-card-paper': 'true'})

        for card in review_cards:
            try:
                review = self._parse_single_review(card)
                if review:
                    errors = review.validate()
                    if errors:
                        self.logger.warning(f"Review validation errors: {errors}")
                    else:
                        reviews.append(review)
            except Exception as e:
                self.logger.error(f"Error parsing review: {e}")
                continue
        
        return reviews
    
    def _parse_single_review(self, card):
        review_id = ""
        # Try to extract from the review link
        review_link = card.find('a', {'href': re.compile('/reviews/[a-f0-9]+')})
        if review_link:
            href = review_link.get('href', '')
            match = re.search(r'/reviews/([a-f0-9]+)', href)
            if match:
                review_id = match.group(1)

        if not review_id:
            # Try to extract from URL
            link = card.find('a', {'data-review-title-typography': 'true'})
            if link and 'href' in link.attrs:
                review_id = link['href'].split('/')[-1]
        
        # Extract author name
        author_elem = card.find('span', {'data-consumer-name-typography': 'true'})
        author_name = author_elem.text.strip() if author_elem else "Anonymous"
    
        # Extract rating
        rating_container = card.find('div', class_='styles_reviewHeader__DzoAZ')
        rating = self._extract_rating(rating_container) or 0

        # Extract content
        review_content_section = card.find('div', class_='styles_reviewContent__tuXiN')
        content = ""
        if review_content_section:
            paragraphs = review_content_section.find_all('p')
            for p in paragraphs:
                if not p.text.strip().startswith("Date of experience"):
                    content = p.text.strip()
                    break
        
        # Extract tile
        title_elem = card.find('h2', {'data-service-review-title-typography': 'true'})
        if not title_elem and review_content_section:
            title_elem = review_content_section.find('h2')
        title = title_elem.text.strip() if title_elem else ""

        # Extract dates
        date_elem = card.find('time')
        date_posted = self._parse_date(date_elem.get('datetime')) if date_elem else datetime.now()

        # Extract date of experience
        date_of_experience = None
        if review_content_section:
            paragraphs = review_content_section.find_all('p')
            for p in paragraphs:
                if "Date of experience" in p.text:
                    date_of_experience = self._parse_experience_date(p.text)
                    break

        # Check verfied
        verified = card.find(string=lambda s: s and "Verified" in s) is not None

        # Extract reply if exists
        reply_content, reply_date = self._parse_reply(card)

        # Extract author info
        author_info = self._parse_author_info(card)

        return Review(
            review_id = review_id or f"unknown_{author_name}_{date_posted}",
            author_name = author_name,
            rating = rating,
            title = title,
            content = content,
            date_posted = date_posted,
            date_of_experience = date_of_experience,
            verified = verified,
            reply_content = reply_content,
            reply_date = reply_date,
            author_reviews_count=author_info.get('reviews_count'),
            author_location = author_info.get('location')
        )

    def _parse_reply(self, card):
        reply_content = None
        reply_date = None

        # Use exact data- attribute selector for reply text
        reply_text_elem = card.select_one('p[data-service-review-business-reply-text-typography]')
        if reply_text_elem:
            reply_content = reply_text_elem.get_text(" ", strip=True)

        # Now get the time inside the reply info div
        reply_date_elem = card.select_one('div.styles_replyInfo__41_in time')
        if reply_date_elem and reply_date_elem.has_attr('datetime'):
            reply_date = self._parse_date(reply_date_elem['datetime'])

        return reply_content, reply_date

    def _extract_rating(self, rating_elem):
        if not rating_elem:
            return 0
    
        # Try to get directly from attribute
        if rating_elem.has_attr('data-service-review-rating'):
            return int(rating_elem['data-service-review-rating'])
        
        # Fallback to original method
        img = rating_elem.find('img')
        if img and 'alt' in img.attrs:
            match = re.search(r'Rated (\d+) out of', img['alt'])
            if match:
                return int(match.group(1))
        
        # Last resort: count stars
        filled_stars = rating_elem.find_all('img', {'alt': re.compile('star')})
        return len(filled_stars)
    
    def _parse_date(self, date_str):
        if not date_str:
            return datetime.now()
        
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            return datetime.now()
    
    def _parse_experience_date(self, text):
        if not text:
            return None
        
        # Look for patterns like "Date of experience: October 25, 2023"
        match = re.search(r'Date of experience:\s*(.+)', text)
        if match:
            date_text = match.group(1).strip()
            try:
                return datetime.strptime(date_text, "%B %d, %Y")
            except:
                try:
                    return datetime.strptime(date_text, "%b %d, %Y")
                except:
                    pass
        
        return None
    
    def _parse_author_info(self, card):
        info = {}

        # Find location as before
        location_elem = card.find('span', {'data-consumer-country-typography': 'true'})
        if location_elem:
            info['location'] = location_elem.get_text(strip=True)
        
        # Find review count
        reviews_count_elem = card.find('span', {'data-consumer-reviews-count-typography': 'true'})
        if reviews_count_elem:
            match = re.search(r'(\d+)', reviews_count_elem.get_text())
            if match:
                info['reviews_count'] = int(match.group(1))
            else:
                info['reviews_count'] = 1
        else:
            info['reviews_count'] = 1

        return info
    
    def _parse_rating_distribution(self, soup):
        rating_distribution = {}
    
        # Finds all <label> rows under the container that hold rating info
        rows = soup.find_all("label", {"class": "styles_row__4BwV6"})
        
        for row in rows:
            # Extract the star level from the <p> with data-rating-label-typography
            label_tag = row.find("p", {"data-rating-label-typography": True})
            if not label_tag:
                continue
            
            label_text = label_tag.get_text(strip=True)
            
            # Typically like "5-star"
            star_value = label_text.split("-")[0] if "-" in label_text else None
            
            # Get the percentage
            percent_tag = row.find("p", {"data-rating-distribution-row-percentage-typography": True})
            percent_text = percent_tag.get_text(strip=True) if percent_tag else "0%"
            
            # Some have "<1%", you may convert to 0 or keep as "<1%"
            if "<" in percent_text:
                percent_value = "0"
            else:
                percent_value = percent_text.replace("%", "").strip()
            
            if star_value:
                rating_distribution[star_value] = percent_value
        
        return rating_distribution
    
    def _parse_review_count(self, text):
        if not text:
            return 0
        
        # Extract number
        match = re.search(r'([\d,]+)', text)
        if match:
            # Remove commas and convert to int
            return int(match.group(1).replace(',', ''))
        
        return 0
    
    def has_next_page(self, soup):
        # Check for next button
        next_button = soup.find('a', {'name': 'pagination-button-next'})
        if next_button:
            # Check if it's disabled
            return 'disabled' not in next_button.get('class', [])
        
        return False
    
