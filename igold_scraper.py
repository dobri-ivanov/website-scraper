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
import uuid

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
        self.images = []
        self.product_counter = 0
        self.vendors = []
        self.vendor_counter = 0
        self.processed_urls = set()  # Track processed product URLs to avoid duplicates
        
        
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
        # Check if we've already processed this URL
        if product_url in self.processed_urls:
            logger.info(f"Skipping duplicate product URL: {product_url}")
            return None
            
        logger.info(f"Scraping individual product: {product_url}")

        response = self.get_page(product_url)
        if not response:
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        self.product_counter += 1
        product_data = {
                'product_id': self.product_counter,
                'category_id': category_id or '',
                'vendor_id': '',
                'product_name': '',
                'description': '',
                'country': '',
                'weight': '',
                'purity': '',
                'buy_price': '',
                'sell_price': '',
                'slug': '',
                'vat': ''
            }

        try:
            # Extract product URL - only slug without domain
            from urllib.parse import urlparse
            parsed_url = urlparse(product_url)
            product_data['slug'] = parsed_url.path.lstrip('/')
            
            # Set VAT based on category
            if category_id == '1':  # Злато
                product_data['vat'] = 'без ддс'
            elif category_id == '2':  # Сребро
                product_data['vat'] = 'с ддс на маржа'
            else:  # Останалите категории (Платина, Паладий)
                product_data['vat'] = 'с ддс'
            
            # Extract product name from page title or main heading
            title = soup.find('title')
            if title:
                product_data['product_name'] = title.get_text(strip=True)
            
            # Try to find main product heading
            main_heading = soup.find('h1') or soup.find('h2')
            if main_heading:
                product_data['product_name'] = main_heading.get_text(strip=True)

            # Extract description from class descriptionOnly - keep HTML tags
            description_element = soup.find(class_='descriptionOnly')
            if description_element:
                product_data['description'] = str(description_element)

            # Extract all text content for analysis
            page_text = soup.get_text(strip=True)

            # Try to extract refinery/mint information from HTML structure
            # Look for patterns like "Монетен двор: <strong>Vendor Name</strong>"
            extracted_refinery_name = ''
            
            # First, try to find structured HTML patterns
            refinery_labels = ['Монетен двор:', 'Рафинерия:', 'Refinery:', 'Mint:', 'Производител:', 'Manufacturer:']
            
            for label in refinery_labels:
                # Look for the label followed by a strong tag
                pattern = rf'{re.escape(label)}\s*<strong[^>]*>(.*?)</strong>'
                match = re.search(pattern, str(soup), re.IGNORECASE | re.DOTALL)
                if match:
                    extracted_refinery_name = match.group(1).strip()
                    # Clean up HTML entities and extra whitespace
                    extracted_refinery_name = extracted_refinery_name.replace('&nbsp;', ' ').replace('&amp;', '&')
                    extracted_refinery_name = ' '.join(extracted_refinery_name.split())
                    if extracted_refinery_name:
                        break
            
            # If no structured HTML found, try text patterns as fallback
            if not extracted_refinery_name:
                refinery_patterns = [
                    r'Монетен двор:\s*([^<\n]+?)(?=\s*Тегло|\s*Проба|\s*Чисто|\s*Диаметър|\s*Гурт|\s*Номинал|\s*Валута|\s*Опаковка|\s*Монетата|\s*Снимките|\s*Продаваме|\s*Купуваме|\s*Година|\s*$)',  # Stop at next field
                    r'Рафинерия:\s*([^<\n]+?)(?=\s*Тегло|\s*Проба|\s*Чисто|\s*Диаметър|\s*Гурт|\s*Номинал|\s*Валута|\s*Опаковка|\s*Монетата|\s*Снимките|\s*Продаваме|\s*Купуваме|\s*Година|\s*$)',  # Stop at next field
                    r'Refinery:\s*([^<\n]+?)(?=\s*Weight|\s*Purity|\s*Fine|\s*Diameter|\s*Edge|\s*Nominal|\s*Currency|\s*Packaging|\s*The coin|\s*Images|\s*Sell|\s*Buy|\s*Year|\s*$)',  # Stop at next field
                    r'Mint:\s*([^<\n]+?)(?=\s*Weight|\s*Purity|\s*Fine|\s*Diameter|\s*Edge|\s*Nominal|\s*Currency|\s*Packaging|\s*The coin|\s*Images|\s*Sell|\s*Buy|\s*Year|\s*$)',  # Stop at next field
                    r'Производител:\s*([^<\n]+?)(?=\s*Тегло|\s*Проба|\s*Чисто|\s*Диаметър|\s*Гурт|\s*Номинал|\s*Валута|\s*Опаковка|\s*Монетата|\s*Снимките|\s*Продаваме|\s*Купуваме|\s*Година|\s*$)',  # Stop at next field
                    r'Manufacturer:\s*([^<\n]+?)(?=\s*Weight|\s*Purity|\s*Fine|\s*Diameter|\s*Edge|\s*Nominal|\s*Currency|\s*Packaging|\s*The coin|\s*Images|\s*Sell|\s*Buy|\s*Year|\s*$)'  # Stop at next field
                ]
                
                for pattern in refinery_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        extracted_refinery_name = match.group(1).strip()
                        # Clean up the extracted name - remove any trailing text that might have been captured
                        if extracted_refinery_name:
                            # Remove common trailing words/phrases
                            extracted_refinery_name = re.sub(r'\s+(Тегло|Проба|Чисто|Диаметър|Гурт|Номинал|Валута|Опаковка|Монетата|Снимките|Продаваме|Купуваме|Година).*$', '', extracted_refinery_name)
                            extracted_refinery_name = re.sub(r'\s+(Weight|Purity|Fine|Diameter|Edge|Nominal|Currency|Packaging|The coin|Images|Sell|Buy|Year).*$', '', extracted_refinery_name)
                            extracted_refinery_name = extracted_refinery_name.strip()
                            if extracted_refinery_name:
                                break

            # Extract weight - only numeric value
            weight_match = re.search(r'(\d+\.?\d*)\s*гр\.', page_text)
            if weight_match:
                product_data['weight'] = weight_match.group(1)

            # Extract prices using new CSS classes - only numeric values
            buy_price_element = soup.find('span', class_='productUpdatePriceBuy')
            sell_price_element = soup.find('span', class_='productUpdatePriceSell')
            
            if buy_price_element:
                buy_price_text = buy_price_element.get_text(strip=True)
                # Extract only the numeric part, remove "лв." and any other text
                buy_price_match = re.search(r'(\d+\.?\d*)', buy_price_text)
                if buy_price_match:
                    product_data['buy_price'] = buy_price_match.group(1)
            
            if sell_price_element:
                sell_price_text = sell_price_element.get_text(strip=True)
                # Extract only the numeric part, remove "лв." and any other text
                sell_price_match = re.search(r'(\d+\.?\d*)', sell_price_text)
                if sell_price_match:
                    product_data['sell_price'] = sell_price_match.group(1)
            
            # Fallback to old method if new CSS classes not found
            if not product_data['buy_price'] and not product_data['sell_price']:
                price_matches = re.findall(r'(\d+\.?\d*)\s*лв', page_text)
                if len(price_matches) >= 2:
                    product_data['buy_price'] = price_matches[0]
                    product_data['sell_price'] = price_matches[1]
                elif len(price_matches) == 1:
                    product_data['buy_price'] = price_matches[0]

            # Look for 0 prices specifically
            if '0 лв' in page_text or '0.00 лв' in page_text:
                if not product_data['buy_price']:
                    product_data['buy_price'] = '0'
                if not product_data['sell_price']:
                    product_data['sell_price'] = '0'

            # Extract country/refinery information
            refinery_name = ''
            country = ''
            
            # First, try to use extracted refinery name from patterns
            if extracted_refinery_name:
                refinery_name = extracted_refinery_name
                # Try to determine country based on refinery name
                if 'Banco de México' in refinery_name or 'Mexico' in refinery_name:
                    country = 'Мексико'
                elif 'Valcambi' in refinery_name:
                    country = 'Швейцария'
                elif 'Argor' in refinery_name or 'Heraeus' in refinery_name:
                    country = 'Швейцария'
                elif 'Pamp' in refinery_name:
                    country = 'Швейцария'
                elif 'Royal Mint' in refinery_name:
                    country = 'Великобритания'
                elif 'Perth Mint' in refinery_name:
                    country = 'Австралия'
                elif 'Canadian Mint' in refinery_name:
                    country = 'Канада'
                elif 'United States Mint' in refinery_name or 'US Mint' in refinery_name:
                    country = 'САЩ'
                elif 'Austrian Mint' in refinery_name or 'Münze Österreich' in refinery_name:
                    country = 'Австрия'
                else:
                    # Default country if not recognized
                    country = 'Неизвестна'
            else:
                # Fallback to hardcoded patterns if no structured data found
                if 'Valcambi' in page_text:
                    refinery_name = 'Valcambi'
                    country = 'Швейцария'
                elif 'Argor-Heraeus' in page_text or 'Argor Heraeus' in page_text:
                    refinery_name = 'Argor-Heraeus'
                    country = 'Швейцария'
                elif 'Pamp' in page_text:
                    refinery_name = 'Pamp'
                    country = 'Швейцария'
                elif 'Royal Mint' in page_text:
                    refinery_name = 'Royal Mint'
                    country = 'Великобритания'
                elif 'Perth Mint' in page_text:
                    refinery_name = 'Perth Mint'
                    country = 'Австралия'
                elif 'Canadian Mint' in page_text or 'Royal Canadian Mint' in page_text:
                    refinery_name = 'Royal Canadian Mint'
                    country = 'Канада'
                elif 'US Mint' in page_text or 'United States Mint' in page_text:
                    refinery_name = 'United States Mint'
                    country = 'САЩ'
                elif 'Austrian Mint' in page_text or 'Münze Österreich' in page_text:
                    refinery_name = 'Austrian Mint'
                    country = 'Австрия'
            
            # Set country and vendor
            if refinery_name:
                product_data['country'] = country
                
                # Get or create vendor and set vendor_id
                vendor_id = self.get_or_create_vendor(refinery_name, country)
                product_data['vendor_id'] = vendor_id

            # Extract purity
            purity_match = re.search(r'(\d{3,4}\.?\d*)\s*(?:проба|purity)', page_text, re.IGNORECASE)
            if purity_match:
                product_data['purity'] = purity_match.group(1)
            elif 'злато' in page_text.lower() or 'gold' in page_text.lower():
                product_data['purity'] = '999.9'
            elif 'сребро' in page_text.lower() or 'silver' in page_text.lower():
                product_data['purity'] = '999.0'

            # Extract images - only product images
            images = soup.find_all('img')
            product_image_urls = []
            for img in images:
                img_src = img.get('src', '')
                if img_src and not img_src.startswith('http'):
                    img_src = urljoin(self.base_url, img_src)
                
                # Only include actual product images
                if img_src and any(product_indicator in img_src.lower() for product_indicator in [
                    'kyulche', 'moneta', 'zlat', 'srebro', 'platina', 'paladiy', 
                    'valcambi', 'pamp', 'argor', 'royal', 'perth', 'krugerrand',
                    'britania', 'eagle', 'philharmonia', 'kangaroo', 'koala', 'panda'
                ]) and not any(exclude in img_src.lower() for exclude in [
                    'logo', 'icon', 'banner', 'header', 'footer', 'social', 'facebook', 
                    'twitter', 'instagram', 'youtube', 'whatsapp', 'viber', 'email',
                    'phone', 'contact', 'menu', 'nav', 'button', 'arrow', 'close',
                    'loading', 'spinner', 'placeholder', 'default', 'no-image', 'bloomberg'
                ]):
                    product_image_urls.append(img_src)
                    logger.info(f"Found product image: {img_src}")

            # Store images in images list (no longer in product_data)
            if len(product_image_urls) >= 1:
                self.images.append({
                    'product_id': product_data['product_id'],
                    'image_url': product_image_urls[0],
                    'image_order': 1
                })
                logger.info(f"Added product image 1 for product {product_data['product_id']}: {product_image_urls[0]}")
            if len(product_image_urls) >= 2:
                self.images.append({
                    'product_id': product_data['product_id'],
                    'image_url': product_image_urls[1],
                    'image_order': 2
                })
                logger.info(f"Added product image 2 for product {product_data['product_id']}: {product_image_urls[1]}")


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
                    # Mark this URL as processed
                    self.processed_urls.add(product_url)
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
        skipped_duplicates = 0
        
        for i, product_url in enumerate(product_links):
            try:
                # Check if we've already processed this URL
                if product_url in self.processed_urls:
                    skipped_duplicates += 1
                    logger.info(f"Skipping already processed URL: {product_url}")
                    continue
                
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

        logger.info(f"Scraped {len(products)} products from {len(product_links)} product links (skipped {skipped_duplicates} duplicates)")
        return products
    
    def get_or_create_vendor(self, refinery_name, country=''):
        """Get existing vendor or create new one, return vendor_id."""
        if not refinery_name:
            return None
            
        # Check if vendor already exists
        for vendor in self.vendors:
            if vendor['name'].lower() == refinery_name.lower():
                logger.info(f"Found existing vendor: {vendor['name']} (ID: {vendor['vendor_id']})")
                return vendor['vendor_id']
        
        # Create new vendor
        self.vendor_counter += 1
        new_vendor = {
            'vendor_id': self.vendor_counter,
            'name': refinery_name,
            'country': country
        }
        self.vendors.append(new_vendor)
        return self.vendor_counter
    
    def remove_duplicate_products(self):
        """Remove duplicate products based on slug (URL path) which is the most reliable identifier."""
        logger.info("Removing duplicate products...")
        
        original_count = len(self.products)
        seen_slugs = set()
        unique_products = []
        duplicate_count = 0
        
        for product in self.products:
            # Use slug as the primary unique identifier since it represents the URL path
            product_slug = product.get('slug', '').strip()
            
            if product_slug and product_slug not in seen_slugs:
                seen_slugs.add(product_slug)
                unique_products.append(product)
            elif product_slug:
                duplicate_count += 1
                logger.info(f"Removing duplicate product by slug: {product.get('product_name', 'Unknown')} - {product_slug}")
            else:
                # If no slug, fallback to name + weight combination
                product_key = (
                    product.get('product_name', '').lower().strip(),
                    product.get('weight', '').strip()
                )
                
                if product_key not in seen_slugs:
                    seen_slugs.add(product_key)
                    unique_products.append(product)
                else:
                    duplicate_count += 1
                    logger.info(f"Removing duplicate product by name+weight: {product.get('product_name', 'Unknown')} - {product.get('weight', 'Unknown')}")
        
        self.products = unique_products
        logger.info(f"Removed {duplicate_count} duplicate products. Original: {original_count}, Unique: {len(self.products)}")
        
        # Also clean up images for removed products
        if duplicate_count > 0:
            self.cleanup_orphaned_images()
    
    def cleanup_orphaned_images(self):
        """Remove images that belong to products that were removed as duplicates."""
        logger.info("Cleaning up orphaned images...")
        
        # Get all valid product IDs
        valid_product_ids = {product['product_id'] for product in self.products}
        
        # Filter images to keep only those with valid product IDs
        original_image_count = len(self.images)
        self.images = [img for img in self.images if img['product_id'] in valid_product_ids]
        
        removed_images = original_image_count - len(self.images)
        if removed_images > 0:
            logger.info(f"Removed {removed_images} orphaned images. Original: {original_image_count}, Remaining: {len(self.images)}")
    
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
            
            # Extract prices (buy and sell) - only numeric values, no "лв."
            price_matches = re.findall(r'(\d+\.?\d*)\s*лв', block_text)
            if len(price_matches) >= 2:
                # Usually the first price is buy price, second is sell price
                product_data['buy_price'] = price_matches[0]
                product_data['sell_price'] = price_matches[1]
            elif len(price_matches) == 1:
                product_data['buy_price'] = price_matches[0]
            
            # Extract country information
            if 'Valcambi' in block_text:
                product_data['country'] = 'Швейцария'
            elif 'Argor-Heraeus' in block_text or 'Argor Heraeus' in block_text:
                product_data['country'] = 'Швейцария'
            elif 'Pamp' in block_text:
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
                
                # Images sheet
                if self.images:
                    df_images = pd.DataFrame(self.images)
                    df_images.to_excel(writer, sheet_name='Images', index=False)
                    logger.info(f"Saved {len(self.images)} images")
                
                # Vendors sheet
                if self.vendors:
                    df_vendors = pd.DataFrame(self.vendors)
                    df_vendors.to_excel(writer, sheet_name='Vendors', index=False)
                    logger.info(f"Saved {len(self.vendors)} vendors")
                
                
            
            logger.info(f"Data successfully saved to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            return False
    
    def run(self, test_mode=False, test_category_id=None):
        """Main function to run the complete scraping process."""
        if test_mode:
            logger.info(f"Starting iGold.bg scraper in TEST MODE for category {test_category_id}...")
        else:
            logger.info("Starting iGold.bg scraper...")
        
        try:
            # Step 1: Get categories
            categories = self.get_categories()
            if not categories:
                logger.error("No categories found. Exiting.")
                return False
            
            # In test mode, filter categories
            if test_mode and test_category_id:
                categories = [cat for cat in categories if str(cat['id']) == str(test_category_id)]
                logger.info(f"Test mode: Filtered to {len(categories)} categories")
            
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
            
            # Step 4: Remove duplicate products
            self.remove_duplicate_products()
            
            # Step 5: Save to Excel
            filename = 'igold_data_test.xlsx' if test_mode else 'igold_data.xlsx'
            success = self.save_to_excel(filename)
            
            if success:
                if test_mode:
                    logger.info("TEST MODE scraping completed successfully!")
                else:
                    logger.info("Scraping completed successfully!")
                logger.info(f"Total categories: {len(self.categories)}")
                logger.info(f"Total subcategories: {len(self.subcategories)}")
                logger.info(f"Total products: {len(self.products)}")
                logger.info(f"Total images: {len(self.images)}")
                logger.info(f"Total vendors: {len(self.vendors)}")
                return True
            else:
                logger.error("Failed to save data to Excel")
                return False
                
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return False

def main():
    """Main function to run the scraper."""
    import sys
    
    # Check for test mode argument
    test_mode = len(sys.argv) > 1 and sys.argv[1] == '--test'
    test_category_id = '2'  # Silver category ID
    
    scraper = IGoldScraper()
    
    if test_mode:
        print("🧪 Running in TEST MODE - Silver only")
        success = scraper.run(test_mode=True, test_category_id=test_category_id)
        if success:
            print("\n✅ TEST MODE scraping completed successfully!")
            print("📊 Data saved to igold_data_test.xlsx")
        else:
            print("\n❌ TEST MODE scraping failed. Check the logs for details.")
    else:
        success = scraper.run()
        if success:
            print("\n✅ Scraping completed successfully!")
            print("📊 Data saved to igold_data.xlsx")
        else:
            print("\n❌ Scraping failed. Check the logs for details.")

if __name__ == "__main__":
    main()
