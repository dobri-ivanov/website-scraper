# iGold.bg Web Scraper

A Python web scraper that extracts data from https://igold.bg/ and saves it to an Excel file.

## Features

- Scrapes main categories from the website menu
- Extracts subcategories for each main category
- Collects detailed product information including:
  - Product name, image URL, country, refinery
  - Weight, purity, fine gold content
  - Diameter/size, buy/sell prices
  - Other available properties
- Saves data to Excel file with multiple sheets
- Includes error handling and throttling to be respectful to the website

## Installation

1. Make sure Python 3.7+ is installed
2. Install required dependencies:
   ```bash
   py -m pip install -r requirements.txt
   ```

## Usage

Run the scraper:
```bash
py igold_scraper.py
```

The script will:
1. Scrape all main categories
2. Extract subcategories for each category
3. Collect product details from each subcategory
4. Save all data to `igold_data.xlsx`

## Output

The Excel file contains three sheets:
- **Categories**: Main categories (id, name, url)
- **Subcategories**: Subcategories (id, name, url, parent_category_id)
- **Products**: Product details (all available fields)

## Technical Details

- Uses `requests` and `BeautifulSoup` for web scraping
- Implements throttling (1.5-2 second delays) to avoid overloading the server
- Includes comprehensive error handling and logging
- Structured with separate functions for each scraping task
- Respects website structure and extracts data from various HTML patterns

## Notes

- The scraper includes delays between requests to be respectful to the website
- All errors are logged for debugging purposes
- Missing data fields are left empty in the Excel output
- The script handles various HTML structures and patterns found on the website
