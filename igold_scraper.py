#!/usr/bin/env python3
"""
iGold.bg Web Scraper
Scrapes categories, subcategories, and products from https://igold.bg/
and saves the data to an Excel file.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from urllib.parse import urljoin, urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IGoldScraper:
    def __init__(self):
        self.base_url = "https://igold.bg"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.categories = []
        self.subcategories = []
        self.products = []
        
    def get_page(self, url, max_retries=3):
        """Get a web page with error handling and retries."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching: {url}")
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def get_categories(self):
        """Scrape all main categories from the menu-product-types-box div."""
        logger.info("Scraping main categories from menu-product-types-box...")
        response = self.get_page(self.base_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        categories = []
        
        # Look specifically for the menu-product-types-box div
        menu_box = soup.find('div', class_='menu-product-types-box')
        if menu_box:
            # Find all links within this div
            category_links = menu_box.find_all('a')
            for link in category_links:
                try:
                    name = link.get_text(strip=True)
                    url = link.get('href', '')
                    
                    # Make URL absolute if it's relative
                    if url and not url.startswith('http'):
                        url = urljoin(self.base_url, url)
                    
                    if name and url:
                        # Try to extract category ID from URL or parent elements
                        category_id = None
                        
                        # Look for rootcategoryid in parent elements
                        parent = link.find_parent(['li', 'div'], {'rootcategoryid': True})
                        if parent:
                            category_id = parent.get('rootcategoryid')
                        
                        # If no rootcategoryid found, try to extract from URL
                        if not category_id:
                            if '/srebro' in url:
                                category_id = '2'
                            elif '/platina' in url:
                                category_id = '3'
                            elif '/paladiy' in url:
                                category_id = '4'
                            elif '/promotzii' in url or 'promo' in url.lower():
                                category_id = '5'
                            elif 'zlat' in url.lower() or url == self.base_url or url == self.base_url + '/':
                                category_id = '1'
                        
                        # Skip ПРОМО category
                        if category_id and category_id != '5' and name.lower() != 'промо':
                            categories.append({
                                'id': category_id,
                                'name': name,
                                'url': url
                            })
                            logger.info(f"Found category: {name} (ID: {category_id})")
                except Exception as e:
                    logger.warning(f"Error processing category item: {e}")
                    continue
        
        self.categories = categories
        logger.info(f"Found {len(categories)} categories from menu-product-types-box")
        return categories
    
    def get_subcategories(self, category_id):
        """Get subcategories for a specific category."""
        logger.info(f"Scraping subcategories for category {category_id}...")
        
        # Find the category URL
        category_url = None
        for cat in self.categories:
            if cat['id'] == category_id:
                category_url = cat['url']
                break
        
        if not category_url:
            logger.warning(f"No URL found for category {category_id}")
            return []
        
        response = self.get_page(category_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        subcategories = []
        
        # Look for subcategory div with the specific ID pattern
        subcategory_div = soup.find('div', id=f'sub-category-{category_id}')
        if subcategory_div:
            subcategory_links = subcategory_div.find_all('a')
            for link in subcategory_links:
                try:
                    name = link.get_text(strip=True)
                    url = link.get('href', '')
                    
                    if url and not url.startswith('http'):
                        url = urljoin(self.base_url, url)
                    
                    if name and url:
                        subcategories.append({
                            'id': len(subcategories) + 1,
                            'name': name,
                            'url': url,
                            'parent_category_id': category_id
                        })
                        logger.info(f"Found subcategory: {name}")
                except Exception as e:
                    logger.warning(f"Error processing subcategory: {e}")
                    continue
        
        # Also look for other subcategory patterns
        subcategory_links = soup.find_all('a', href=re.compile(r'/subcategory/'))
        for link in subcategory_links:
            try:
                name = link.get_text(strip=True)
                url = link.get('href', '')
                
                if url and not url.startswith('http'):
                    url = urljoin(self.base_url, url)
                
                if name and url:
                    # Check if we already have this subcategory
                    if not any(sub['url'] == url for sub in subcategories):
                        subcategories.append({
                            'id': len(subcategories) + 1,
                            'name': name,
                            'url': url,
                            'parent_category_id': category_id
                        })
                        logger.info(f"Found subcategory from links: {name}")
            except Exception as e:
                logger.warning(f"Error processing subcategory link: {e}")
                continue
        
        logger.info(f"Found {len(subcategories)} subcategories for category {category_id}")
        return subcategories
    
    def get_product_links(self, url, category_id=None):
        """Extract all product links from a category page."""
        logger.info(f"Extracting product links from category page: {url}")

        response = self.get_page(url)
        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        product_links = set()  # Use set to avoid duplicates

        # Look specifically for li.kv__member-item containers (the main product containers)
        product_containers = soup.find_all('li', class_='kv__member-item')
        logger.info(f"Found {len(product_containers)} li.kv__member-item containers")
        
        for container in product_containers:
            # Look for "Вижте повече" links within each container
            view_more_links = container.find_all('a', string=re.compile(r'Вижте повече'))
            for link in view_more_links:
                href = link.get('href', '')
                if href:
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    product_links.add(href)
            
            # Also look for any other links within the container
            all_links = container.find_all('a')
            for link in all_links:
                href = link.get('href', '')
                if href and href not in product_links:
                    if not href.startswith('http'):
                        href = urljoin(self.base_url, href)
                    # Only add if it looks like a product link
                    if any(keyword in href.lower() for keyword in ['kyulche', 'moneta', 'platina', 'paladiy', 'srebro', 'zlat']):
                        product_links.add(href)

        # Convert set back to list and sort for consistent ordering
        product_links = sorted(list(product_links))
        
        logger.info(f"Found {len(product_links)} unique product links from {url}")
        return product_links

    def scrape_individual_product(self, product_url, category_id=None, subcategory_id=None):
        """Scrape detailed product information from an individual product page."""
        logger.info(f"Scraping individual product: {product_url}")

        response = self.get_page(product_url)
        if not response:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        product_data = {
                'category_id': category_id or '',
                'subcategory_id': subcategory_id or '',
                'product_name': '',
                'image_url_1': '',
                'image_url_2': '',
                'country': '',
                'refinery': '',
                'weight': '',
                'purity': '',
                'fine_gold': '',
                'diameter_size': '',
                'buy_price': '',
                'sell_price': '',
                'other_properties': '',
                'product_url': product_url
            }

        try:
            # Extract product name from page title or main heading
            title = soup.find('title')
            if title:
                product_data['product_name'] = title.get_text(strip=True)
            
            # Try to find main product heading
            main_heading = soup.find('h1') or soup.find('h2')
            if main_heading:
                product_data['product_name'] = main_heading.get_text(strip=True)

            # Extract all text content for analysis
            page_text = soup.get_text(strip=True)

            # Extract weight
            weight_match = re.search(r'(\d+\.?\d*)\s*гр\.', page_text)
            if weight_match:
                product_data['weight'] = weight_match.group(1) + ' гр.'

            # Extract prices (buy and sell) - including 0 prices
            price_matches = re.findall(r'(\d+\.?\d*)\s*лв', page_text)
            if len(price_matches) >= 2:
                product_data['buy_price'] = price_matches[0] + ' лв.'
                product_data['sell_price'] = price_matches[1] + ' лв.'
            elif len(price_matches) == 1:
                product_data['buy_price'] = price_matches[0] + ' лв.'

            # Look for 0 prices specifically
            if '0 лв' in page_text or '0.00 лв' in page_text:
                if not product_data['buy_price']:
                    product_data['buy_price'] = '0 лв.'
                if not product_data['sell_price']:
                    product_data['sell_price'] = '0 лв.'

            # Extract country/refinery information
            if 'Valcambi' in page_text:
                product_data['refinery'] = 'Valcambi'
                product_data['country'] = 'Швейцария'
            elif 'Argor-Heraeus' in page_text or 'Argor Heraeus' in page_text:
                product_data['refinery'] = 'Argor-Heraeus'
                product_data['country'] = 'Швейцария'
            elif 'Pamp' in page_text:
                product_data['refinery'] = 'Pamp'
                product_data['country'] = 'Швейцария'
            elif 'Royal Mint' in page_text:
                product_data['refinery'] = 'Royal Mint'
                product_data['country'] = 'Великобритания'

            # Extract purity
            purity_match = re.search(r'(\d{3,4}\.?\d*)\s*(?:проба|purity)', page_text, re.IGNORECASE)
            if purity_match:
                product_data['purity'] = purity_match.group(1)
            elif 'злато' in page_text.lower() or 'gold' in page_text.lower():
                product_data['purity'] = '999.9'
            elif 'сребро' in page_text.lower() or 'silver' in page_text.lower():
                product_data['purity'] = '999.0'

            # Extract fine gold content
            if product_data['weight'] and product_data['purity']:
                try:
                    weight_num = float(re.search(r'(\d+\.?\d*)', product_data['weight']).group(1))
                    purity_num = float(product_data['purity'])
                    fine_gold = weight_num * (purity_num / 1000)
                    product_data['fine_gold'] = f"{fine_gold:.2f} гр."
                except:
                    pass

            # Extract images
            images = soup.find_all('img')
            image_urls = []
            for img in images:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                if img_src and 'product' in img_src.lower():
                    image_urls.append(img_src)

            # Separate images into two columns
            if len(image_urls) >= 1:
                product_data['image_url_1'] = image_urls[0]
            if len(image_urls) >= 2:
                product_data['image_url_2'] = image_urls[1]

            # Extract other properties
            if 'Холограмна Защита' in page_text:
                product_data['other_properties'] = 'Холограмна Защита'
            elif 'с обков и кутия' in page_text:
                product_data['other_properties'] = 'с обков и кутия'

            # Filter out non-product pages
            product_name = product_data['product_name'].lower()
            
            # Skip non-product pages
            skip_keywords = [
                'youtube', 'whatsapp', 'chat on', 'facebook live', 'igold в медиите',
                'за контакти', 'контакти', 'помощ', 'faq', 'общи условия', 'terms',
                'за вас', 'zavas', 'blog', 'златен бръмбар', 'промо', 'отстъпка',
                'google maps', 'goo.gl', 'm.me', 'wa.me', 'tel:', 'viber://',
                'cdn-cgi', 'email-protection', 'contactus', 'istoricheski', 'moderni',
                'kyulcheta-s-numizmatichen', 'zlatni-kyulcheta', 'zlatni-moneti',
                'zlatni-numizmatichni', 'moderni-zlatni-moneti', 'paladiy', 'platina',
                'srebro', 'promotzii', 'promo'
            ]
            
            # Check if this is a real product
            is_real_product = True
            for keyword in skip_keywords:
                if keyword in product_name or keyword in product_url.lower():
                    is_real_product = False
                    break
            
                # Additional checks for real products
                if is_real_product:
                    # Must have weight or be a recognizable product type
                    has_weight = bool(product_data['weight'])
                    is_recognized_product = any(keyword in product_name for keyword in [
                        'кюлче', 'монета', 'kyulche', 'moneta', 'гр.', 'toz', 'oz',
                        'valcambi', 'pamp', 'argor-heraeus', 'royal mint', 'perth mint',
                        'кругерранд', 'британия', 'американски орел', 'кленов лист',
                        'филхармония', 'кенгуру', 'коала', 'панда', 'лунар', 'lunar',
                        'платина', 'platina', 'сребро', 'srebro', 'злато', 'zlat'
                    ])
                    
                    # For platinum products, be more lenient - if it has a name and is from a product URL, accept it
                    if category_id == 3 and product_data['product_name']:  # Platinum category
                        is_real_product = True
                    elif not (has_weight or is_recognized_product):
                        is_real_product = False
            
                # Only return if it's a real product
                if is_real_product and product_data['product_name']:
                    return product_data
                else:
                    logger.warning(f"Product filtered out - is_real_product: {is_real_product}, has_name: {bool(product_data['product_name'])}, name: '{product_data['product_name']}'")
                    return None

        except Exception as e:
            logger.warning(f"Error extracting product from individual page: {e}")
            logger.warning(f"Product URL: {product_url}")
            return None

    def get_products(self, url, category_id=None, subcategory_id=None):
        """Scrape all products from a specific category page by visiting individual product pages."""
        logger.info(f"Scraping products from category page: {url}")

        # First, get all product links from the category page
        product_links = self.get_product_links(url, category_id)
        
        if not product_links:
            logger.warning(f"No product links found on {url}")
            return []

        products = []
        for i, product_url in enumerate(product_links):
            try:
                product_data = self.scrape_individual_product(product_url, category_id, subcategory_id)
                if product_data:
                    products.append(product_data)
                    logger.info(f"Scraped product {i+1}/{len(product_links)}: {product_data.get('product_name', 'Unknown')}")
                else:
                    logger.warning(f"Failed to scrape product from: {product_url}")

                # Throttling between product pages
                time.sleep(1)

            except Exception as e:
                logger.warning(f"Error processing product {product_url}: {e}")
                continue

        logger.info(f"Scraped {len(products)} products from {len(product_links)} product links")
        return products
    
    def is_valid_product_block(self, block):
        """Check if a block is a valid product block and not some other element."""
        try:
            block_text = block.get_text(strip=True).lower()
            
            # If it's a kv__member-item, it's likely a product
            if block.get('class') and 'kv__member-item' in block.get('class'):
                # Additional validation for kv__member-item
                has_price = bool(re.search(r'\d+\.?\d*\s*лв', block_text))
                has_view_more = 'вижте повече' in block_text
                return has_price and has_view_more
            
            # Exclude non-product elements
            exclude_patterns = [
                'безплатна доставка',
                'застраховка на пратка',
                'консултация',
                'за контакти',
                'ако бързате',
                'общи условия',
                'политика за поверителност',
                'разбрах',
                'igold в медиите',
                'facebook live',
                'защо да инвестираме',
                'защо да купите злато от igold'
            ]
            
            for pattern in exclude_patterns:
                if pattern in block_text:
                    return False
            
            # Must contain product-related keywords
            product_keywords = [
                'златно кюлче', 'златна монета', 'сребърно кюлче', 'сребърна монета',
                'кюлче платина', 'кюлче паладий', 'valcambi', 'pamp', 'argor-heraeus',
                'кругерранд', 'британия', 'американски орел', 'канадски кленов лист',
                'виенска филхармония', 'австралийско кенгуру', 'монета', 'кюлче',
                'франка', 'лири', 'долар', 'евро', 'рупия', 'реал', 'йена'
            ]
            
            has_product_keyword = any(keyword in block_text for keyword in product_keywords)
            
            # Must have price information
            has_price = bool(re.search(r'\d+\.?\d*\s*лв', block_text))
            
            # Must have weight or be a recognizable coin
            has_weight = bool(re.search(r'\d+\.?\d*\s*гр\.', block_text))
            is_coin = any(coin in block_text for coin in ['монета', 'франка', 'лири', 'долар', 'евро'])
            
            # Must have "Вижте повече" link (indicates it's a product)
            has_view_more = 'вижте повече' in block_text
            
            return has_product_keyword and has_price and (has_weight or is_coin) and has_view_more
            
        except Exception as e:
            logger.warning(f"Error validating product block: {e}")
            return False
    
    def extract_product_from_block(self, block, category_id=None):
        """Extract product information from a product block on the main page."""
        try:
            product_data = {
                'category_id': category_id or '',
                'product_name': '',
                'image_url_1': '',
                'image_url_2': '',
                'country': '',
                'refinery': '',
                'weight': '',
                'purity': '',
                'fine_gold': '',
                'diameter_size': '',
                'buy_price': '',
                'sell_price': '',
                'other_properties': '',
                'product_url': ''
            }
            
            # Get all text from the block, but exclude nested divs with status information
            # First, remove nested divs that contain status text
            block_copy = block.__copy__() if hasattr(block, '__copy__') else block
            
            # Find and remove nested divs with status information
            status_divs = block_copy.find_all('div', style=re.compile(r'margin-top.*margin-bottom'))
            for div in status_divs:
                div.decompose()  # Remove the div and its contents
            
            # Also remove spans with specific styling that contain status text
            status_spans = block_copy.find_all('span', style=re.compile(r'color.*font-size'))
            for span in status_spans:
                span.decompose()  # Remove the span and its contents
            
            block_text = block_copy.get_text(strip=True)
            
            # Extract product name - clean version without status text
            # Look for patterns like "X гр. Златно Кюлче" or "Златна Монета"
            name_patterns = [
                r'(\d+\.?\d*\s*гр\.\s*[^0-9]+?)(?=\s*\d+\.?\d*\s*лв|\s*Вижте|\s*$)',
                r'(Златна Монета[^0-9]+?)(?=\s*\d+\.?\d*\s*гр|\s*\d+\.?\d*\s*лв|\s*Вижте|\s*$)',
                r'(Златно Кюлче[^0-9]+?)(?=\s*\d+\.?\d*\s*гр|\s*\d+\.?\d*\s*лв|\s*Вижте|\s*$)'
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, block_text)
                if match:
                    product_data['product_name'] = match.group(1).strip()
                    break
            
            # If no pattern matched, try to get the first meaningful line as product name
            if not product_data['product_name']:
                lines = block_text.split('\n')
                for line in lines:
                    line = line.strip()
                    # Skip lines with prices, status text, or non-product content
                    if (line and 
                        not re.match(r'^\d+\.?\d*\s*лв', line) and 
                        'Вижте' not in line and
                        'Изчерпани' not in line and
                        'Поръчайте авансово' not in line and
                        'Налични' not in line and
                        'ниско качество' not in line and
                        'с повреди' not in line and
                        'сив' not in line and
                        'лунар' not in line and
                        'прасе' not in line and
                        'купуваме' not in line.lower() and
                        'продаваме' not in line.lower() and
                        'за вас' not in line.lower() and
                        'контакти' not in line.lower() and
                        'общи условия' not in line.lower() and
                        'продаваме злато' not in line.lower() and
                        'отстъпка' not in line.lower() and
                        'защо да купите' not in line.lower() and
                        'конкурентни цени' not in line.lower() and
                        'добри наличности' not in line.lower() and
                        'актуализация' not in line.lower() and
                        'само световно' not in line.lower() and
                        'достъпна и дискретна' not in line.lower() and
                        'безплатна доставка' not in line.lower() and
                        'facebook live' not in line.lower() and
                        'igold в медиите' not in line.lower()):
                        product_data['product_name'] = line
                        break
            
            # Clean up the product name by removing any remaining status text
            if product_data['product_name']:
                # Remove common status text patterns
                status_patterns = [
                    r'Изчерпани\..*',
                    r'Поръчайте авансово.*',
                    r'Налични.*',
                    r'ниско качество.*',
                    r'с повреди.*',
                    r'сив.*',
                    r'лунар.*',
                    r'прасе.*',
                    r'\(\s*\+?\d+\s*лв\.?\s*\)',
                    r'\(\s*-?\d+\s*лв\.?\s*\)'
                ]
                
                for pattern in status_patterns:
                    product_data['product_name'] = re.sub(pattern, '', product_data['product_name']).strip()
                
                # Remove extra whitespace
                product_data['product_name'] = ' '.join(product_data['product_name'].split())
            
            # Extract weight
            weight_match = re.search(r'(\d+\.?\d*)\s*гр\.', block_text)
            if weight_match:
                product_data['weight'] = weight_match.group(1) + ' гр.'
            
            # Extract prices (buy and sell)
            price_matches = re.findall(r'(\d+\.?\d*)\s*лв', block_text)
            if len(price_matches) >= 2:
                # Usually the first price is buy price, second is sell price
                product_data['buy_price'] = price_matches[0] + ' лв.'
                product_data['sell_price'] = price_matches[1] + ' лв.'
            elif len(price_matches) == 1:
                product_data['buy_price'] = price_matches[0] + ' лв.'
            
            # Extract country/refinery information
            if 'Valcambi' in block_text:
                product_data['refinery'] = 'Valcambi'
                product_data['country'] = 'Швейцария'
            elif 'Argor-Heraeus' in block_text or 'Argor Heraeus' in block_text:
                product_data['refinery'] = 'Argor-Heraeus'
                product_data['country'] = 'Швейцария'
            elif 'Pamp' in block_text:
                product_data['refinery'] = 'Pamp'
                product_data['country'] = 'Швейцария'
            
            # Extract purity (usually 999.9 or similar for gold)
            purity_match = re.search(r'(\d{3,4}\.?\d*)\s*(?:проба|purity)', block_text, re.IGNORECASE)
            if purity_match:
                product_data['purity'] = purity_match.group(1)
            elif 'злато' in block_text.lower() or 'gold' in block_text.lower():
                product_data['purity'] = '999.9'  # Standard for investment gold
            
            # Extract fine gold content
            if product_data['weight'] and product_data['purity']:
                try:
                    weight_num = float(re.search(r'(\d+\.?\d*)', product_data['weight']).group(1))
                    purity_num = float(product_data['purity'])
                    fine_gold = weight_num * (purity_num / 1000)
                    product_data['fine_gold'] = f"{fine_gold:.2f} гр."
                except:
                    pass
            
            # Look for image URLs (separate into two columns)
            images = block.find_all('img')
            image_urls = []
            for img in images:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                if img_src:
                    image_urls.append(img_src)
            
            # Separate images into two columns
            if len(image_urls) >= 1:
                product_data['image_url_1'] = image_urls[0]
            if len(image_urls) >= 2:
                product_data['image_url_2'] = image_urls[1]
            
            # Look for "Вижте повече" link
            view_more_link = block.find('a', string=re.compile(r'Вижте повече'))
            if view_more_link:
                href = view_more_link.get('href', '')
                if href and not href.startswith('http'):
                    href = urljoin(self.base_url, href)
                product_data['product_url'] = href
            
            # Only return if we have meaningful product data
            # Must have at least a product name AND weight AND price (to ensure it's a real product)
            if (product_data['product_name'] and 
                product_data['weight'] and
                (product_data['buy_price'] or product_data['sell_price'])):
                return product_data
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error extracting product from block: {e}")
            return None
    
    def scrape_product_details(self, product_url):
        """Scrape detailed information from a product page."""
        try:
            response = self.get_page(product_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            product_data = {
                'product_name': '',
                'image_url': '',
                'country': '',
                'refinery': '',
                'weight': '',
                'purity': '',
                'fine_gold': '',
                'diameter_size': '',
                'buy_price': '',
                'sell_price': '',
                'other_properties': '',
                'product_url': product_url
            }
            
            # Product name (H1)
            h1 = soup.find('h1')
            if h1:
                product_data['product_name'] = h1.get_text(strip=True)
            
            # Image URL
            img = soup.find('img')
            if img:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                product_data['image_url'] = img_src
            
            # Look for product details in various formats
            # Try to find tables or lists with product information
            details_selectors = [
                'table tr',
                '.product-details tr',
                '.details tr',
                '.specifications tr',
                'dl dt, dl dd',
                '.product-info p',
                '.description p'
            ]
            
            for selector in details_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    if ':' in text:
                        key, value = text.split(':', 1)
                        key = key.strip().lower()
                        value = value.strip()
                        
                        # Map Bulgarian labels to English fields
                        if 'държава' in key or 'country' in key:
                            product_data['country'] = value
                        elif 'монетен двор' in key or 'рафинерия' in key or 'refinery' in key:
                            product_data['refinery'] = value
                        elif 'тегло' in key or 'weight' in key:
                            product_data['weight'] = value
                        elif 'проба' in key or 'purity' in key:
                            product_data['purity'] = value
                        elif 'чисто злато' in key or 'fine gold' in key:
                            product_data['fine_gold'] = value
                        elif 'диаметър' in key or 'размери' in key or 'diameter' in key or 'size' in key:
                            product_data['diameter_size'] = value
                        elif 'продаваме' in key or 'buy' in key:
                            product_data['buy_price'] = value
                        elif 'купуваме' in key or 'sell' in key:
                            product_data['sell_price'] = value
                        else:
                            # Add to other properties
                            if product_data['other_properties']:
                                product_data['other_properties'] += f"; {key}: {value}"
                            else:
                                product_data['other_properties'] = f"{key}: {value}"
            
            # Try to find prices in different formats
            price_elements = soup.find_all(text=re.compile(r'[\d,]+\.?\d*\s*лв'))
            for price_text in price_elements:
                price_text = price_text.strip()
                # Try to determine if it's buy or sell price based on context
                parent = price_text.parent if hasattr(price_text, 'parent') else None
                if parent:
                    parent_text = parent.get_text(strip=True).lower()
                    if 'продаваме' in parent_text or 'buy' in parent_text:
                        product_data['buy_price'] = price_text
                    elif 'купуваме' in parent_text or 'sell' in parent_text:
                        product_data['sell_price'] = price_text
            
            return product_data
            
        except Exception as e:
            logger.warning(f"Error scraping product details from {product_url}: {e}")
            return None
    
    def save_to_excel(self, filename='igold_data.xlsx'):
        """Save all scraped data to Excel file with multiple sheets."""
        logger.info(f"Saving data to {filename}...")
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Categories sheet
                if self.categories:
                    df_categories = pd.DataFrame(self.categories)
                    df_categories.to_excel(writer, sheet_name='Categories', index=False)
                    logger.info(f"Saved {len(self.categories)} categories")
                
                # Subcategories sheet
                if self.subcategories:
                    df_subcategories = pd.DataFrame(self.subcategories)
                    df_subcategories.to_excel(writer, sheet_name='Subcategories', index=False)
                    logger.info(f"Saved {len(self.subcategories)} subcategories")
                
                # Products sheet
                if self.products:
                    df_products = pd.DataFrame(self.products)
                    df_products.to_excel(writer, sheet_name='Products', index=False)
                    logger.info(f"Saved {len(self.products)} products")
            
            logger.info(f"Data successfully saved to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            return False
    
    def run(self):
        """Main function to run the complete scraping process."""
        logger.info("Starting iGold.bg scraper...")
        
        try:
            # Step 1: Get categories
            categories = self.get_categories()
            if not categories:
                logger.error("No categories found. Exiting.")
                return False
            
            # Step 2: Get subcategories for each category
            for category in categories:
                category_id = category['id']
                subcategories = self.get_subcategories(category_id)
                self.subcategories.extend(subcategories)
                
                # Throttling between categories
                time.sleep(2)
            
            # Step 3: Get products from each category page
            for category in categories:
                category_id = category['id']
                category_url = category['url']
                category_name = category['name']
                
                logger.info(f"DEBUG: category_id = {category_id}, type = {type(category_id)}")
                if category_id == 1 or category_id == '1':  # Gold category - scrape by subcategories
                    logger.info(f"Scraping products for category {category_id}: {category_name} (by subcategories)")
                    
                    # Get subcategories for gold
                    gold_subcategories = [sub for sub in self.subcategories if sub['parent_category_id'] == 1 or sub['parent_category_id'] == '1']
                    
                    for subcategory in gold_subcategories:
                        subcategory_id = subcategory['id']
                        subcategory_url = subcategory['url']
                        subcategory_name = subcategory['name']
                        
                        logger.info(f"Scraping subcategory {subcategory_id}: {subcategory_name}")
                        products = self.get_products(subcategory_url, category_id, subcategory_id)
                        self.products.extend(products)
                        
                        logger.info(f"Found {len(products)} products in subcategory {subcategory_name}")
                        
                        # Throttling between subcategories
                        time.sleep(2)
                else:
                    # For other categories, scrape from main category page
                    logger.info(f"Scraping products for category {category_id}: {category_name}")
                    products = self.get_products(category_url, category_id)
                    self.products.extend(products)
                    
                    logger.info(f"Found {len(products)} products in {category_name} category")
                
                # Throttling between categories
                time.sleep(2)
            
            # Step 4: Save to Excel
            success = self.save_to_excel()
            
            if success:
                logger.info("Scraping completed successfully!")
                logger.info(f"Total categories: {len(self.categories)}")
                logger.info(f"Total subcategories: {len(self.subcategories)}")
                logger.info(f"Total products: {len(self.products)}")
                return True
            else:
                logger.error("Failed to save data to Excel")
                return False
                
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return False

def main():
    """Main function to run the scraper."""
    scraper = IGoldScraper()
    success = scraper.run()
    
    if success:
        print("\n✅ Scraping completed successfully!")
        print("📊 Data saved to igold_data.xlsx")
    else:
        print("\n❌ Scraping failed. Check the logs for details.")

if __name__ == "__main__":
    main()
