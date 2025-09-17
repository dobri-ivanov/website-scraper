#!/usr/bin/env python3
"""
Image Downloader Script
Downloads all images from the Images table in igold_data.xlsx
"""

import pandas as pd
import requests
import os
import logging
from urllib.parse import urlparse
from pathlib import Path
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ImageDownloader:
    def __init__(self, excel_file='igold_data.xlsx', download_folder='downloaded_images'):
        """
        Initialize the image downloader
        
        Args:
            excel_file (str): Path to the Excel file containing image URLs
            download_folder (str): Folder to save downloaded images
        """
        self.excel_file = excel_file
        self.download_folder = download_folder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Create download folder if it doesn't exist
        Path(self.download_folder).mkdir(parents=True, exist_ok=True)
        
        # Statistics
        self.total_images = 0
        self.downloaded_count = 0
        self.failed_count = 0
        self.skipped_count = 0

    def get_image_filename(self, url):
        """
        Extract filename from URL (only the part after the last '/')
        
        Args:
            url (str): Image URL
            
        Returns:
            str: Filename extracted from URL
        """
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        
        # If no filename in path, generate one from URL
        if not filename or '.' not in filename:
            # Use a hash of the URL as filename
            import hashlib
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = f"image_{url_hash}.jpg"
        
        return filename

    def download_image(self, url, filename):
        """
        Download a single image
        
        Args:
            url (str): Image URL
            filename (str): Local filename to save the image
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            filepath = os.path.join(self.download_folder, filename)
            
            # Always download, even if file exists (overwrite)
            # if os.path.exists(filepath):
            #     logger.info(f"Skipping {filename} - already exists")
            #     self.skipped_count += 1
            #     return True
            
            # Download the image
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Save the image
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded: {filename}")
            self.downloaded_count += 1
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            self.failed_count += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            self.failed_count += 1
            return False

    def load_image_urls(self):
        """
        Load image URLs from Excel file
        
        Returns:
            list: List of tuples (product_id, image_url, image_order)
        """
        try:
            logger.info(f"Loading image URLs from {self.excel_file}")
            df = pd.read_excel(self.excel_file, sheet_name='Images')
            
            # Convert to list of tuples
            image_data = []
            for _, row in df.iterrows():
                image_data.append((
                    row['product_id'],
                    row['image_url'],
                    row['image_order']
                ))
            
            self.total_images = len(image_data)
            logger.info(f"Loaded {self.total_images} image URLs")
            return image_data
            
        except Exception as e:
            logger.error(f"Error loading image URLs: {e}")
            return []

    def download_all_images(self):
        """
        Download all images from the Excel file
        """
        logger.info("Starting image download process...")
        
        # Load image URLs
        image_data = self.load_image_urls()
        if not image_data:
            logger.error("No image URLs found. Exiting.")
            return False
        
        logger.info(f"Starting download of {self.total_images} images...")
        
        # Download each image
        for i, (product_id, image_url, image_order) in enumerate(image_data, 1):
            logger.info(f"Processing image {i}/{self.total_images} (Product ID: {product_id}, Order: {image_order})")
            
            # Extract filename from URL
            filename = self.get_image_filename(image_url)
            
            # Download the image
            self.download_image(image_url, filename)
            
            # Add small delay to be respectful to the server
            time.sleep(0.1)
            
            # Progress update every 50 images
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{self.total_images} images processed")
        
        # Print final statistics
        self.print_statistics()
        return True

    def print_statistics(self):
        """
        Print download statistics
        """
        logger.info("=" * 50)
        logger.info("DOWNLOAD STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Total images: {self.total_images}")
        logger.info(f"Successfully downloaded: {self.downloaded_count}")
        logger.info(f"Already existed (skipped): {self.skipped_count}")
        logger.info(f"Failed downloads: {self.failed_count}")
        logger.info(f"Success rate: {((self.downloaded_count + self.skipped_count) / self.total_images * 100):.1f}%")
        logger.info(f"Images saved to: {os.path.abspath(self.download_folder)}")
        logger.info("=" * 50)

def main():
    """
    Main function to run the image downloader
    """
    print("🖼️  Image Downloader for IGold Scraper")
    print("=" * 50)
    
    # Initialize downloader
    downloader = ImageDownloader()
    
    # Check if Excel file exists
    if not os.path.exists(downloader.excel_file):
        print(f"❌ Error: Excel file '{downloader.excel_file}' not found!")
        print("Please make sure the file exists and run the scraper first.")
        return False
    
    # Start downloading
    try:
        success = downloader.download_all_images()
        if success:
            print("\n✅ Image download completed successfully!")
            print(f"📁 Images saved to: {os.path.abspath(downloader.download_folder)}")
        else:
            print("\n❌ Image download failed. Check the logs for details.")
        return success
        
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user.")
        downloader.print_statistics()
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    main()
