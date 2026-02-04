import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class ReportDownloadError(Exception):
    """Custom exception for report download failures"""
    pass

class ReportDownloader:
    def __init__(self, username: str, password: str, download_dir: str = None):
        if not username or not password:
            raise ValueError("Username and password cannot be empty")
        
        self.username = username
        self.password = password
        self.download_dir = download_dir or os.getcwd()
        self.driver = None
        
    def setup_driver(self):
        """Configure Chrome driver with download settings"""
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            # For headless mode in GitHub Actions
            if os.getenv("GITHUB_ACTIONS") == "true":
                options.add_argument("--headless")
            
            # Configure download behavior
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "directory_upgrade": True,
                "safebrowsing.enabled": False,
                "safebrowsing.disable_download_protection": True
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.implicitly_wait(10)
            logging.info("WebDriver initialized successfully")
            
        except Exception as e:
            raise ReportDownloadError(f"Failed to setup WebDriver: {str(e)}")
    
    def login(self):
        """Login to the application"""
        try:
            url = "http://drogcidade.ddns.net:4647/sgfpod1/Login.pod"
            logging.info(f"Navigating to {url}")
            self.driver.get(url)
            
            # Wait for and fill username
            username_field = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "id_cod_usuario"))
            )
            username_field.send_keys(self.username)
            
            # Fill password
            password_field = self.driver.find_element(By.ID, "nom_senha")
            password_field.send_keys(self.password)
            
            # Click login
            login_button = self.driver.find_element(By.NAME, "login")
            login_button.click()
            
            # Wait for login to complete
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState === 'complete'")
            )
            
            # Check for login errors
            time.sleep(3)
            if "login" in self.driver.current_url.lower():
                raise ReportDownloadError("Login failed - still on login page")
                
            logging.info("Login successful")
            
        except TimeoutException as e:
            raise ReportDownloadError(f"Login timeout: {str(e)}")
        except Exception as e:
            raise ReportDownloadError(f"Login failed: {str(e)}")
    
    def navigate_to_report(self):
        """Navigate to the target report page"""
        try:
            # Fullscreen (F11)
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.F11)
            time.sleep(2)
            
            # Search and select menu item
            search_box = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "sideMenuSearch"))
            )
            search_box.send_keys("Funcionários / Vendedores")
            search_box.click()
            time.sleep(2)
            
            menu_item = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[title="Funcionários / Vendedores"]'))
            )
            menu_item.click()
            
            # Wait for page load
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState === 'complete'")
            )
            logging.info("Navigated to report page successfully")
            
        except TimeoutException as e:
            raise ReportDownloadError(f"Navigation timeout: {str(e)}")
        except Exception as e:
            raise ReportDownloadError(f"Navigation failed: {str(e)}")
    
    def configure_report(self):
        """Configure report options"""
        try:
            WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.ID, "status_1"))
            ).click()
            
            self.driver.find_element(By.ID, "det_endereco").click()
            self.driver.find_element(By.ID, "saida4").click()  # XLS format
            logging.info("Report configuration complete")
            
        except Exception as e:
            raise ReportDownloadError(f"Report configuration failed: {str(e)}")
    
    def download_report(self, new_filename: str = "raw_users_trier.xls") -> str:
        """Trigger download and rename file"""
        try:
            # Start download
            download_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "runReport"))
            )
            download_button.click()
            
            logging.info("Download triggered, waiting for file...")
            time.sleep(15)  # Increased wait for headless mode
            
            # Find downloaded files
            downloaded_files = [
                f for f in os.listdir(self.download_dir) 
                if f.endswith('.xls')
            ]
            
            if not downloaded_files:
                # Check for .xls.crdownload (Chrome download in progress)
                temp_files = [f for f in os.listdir(self.download_dir) if '.crdownload' in f]
                if temp_files:
                    raise ReportDownloadError("Download still in progress")
                else:
                    raise ReportDownloadError("No XLS files found after download")
            
            # Get most recent file
            latest_file = max(
                downloaded_files,
                key=lambda f: os.path.getmtime(os.path.join(self.download_dir, f))
            )
            
            old_path = os.path.join(self.download_dir, latest_file)
            new_path = os.path.join(self.download_dir, new_filename)
            
            # Remove existing file if it exists
            if os.path.exists(new_path):
                os.remove(new_path)
            
            # Rename the file
            os.rename(old_path, new_path)
            
            file_size = os.path.getsize(new_path)
            if file_size == 0:
                raise ReportDownloadError("Downloaded file is empty (0 bytes)")
            
            logging.info(f"✓ File saved as {new_filename} ({file_size:,} bytes)")
            return new_path
            
        except Exception as e:
            raise ReportDownloadError(f"Download failed: {str(e)}")
    
    def run(self):
        """Main execution flow - any failure here will raise an exception"""
        try:
            logging.info("Starting report download process...")
            self.setup_driver()
            self.login()
            self.navigate_to_report()
            self.configure_report()
            file_path = self.download_report()
            
            logging.info("✓ Report download completed successfully")
            return file_path
            
        except Exception as e:
            logging.error(f"✗ Report download failed: {str(e)}")
            raise  # Re-raise to ensure GitHub Actions sees the failure
            
        finally:
            if self.driver:
                logging.info("Cleaning up WebDriver...")
                self.driver.quit()


def main():
    """Entry point - any uncaught exception here will stop GitHub Actions"""
    try:
        # Get credentials from environment
        username = os.getenv("trier_user")  # Using specific name
        password = os.getenv("trier_password")
        
        if not username:
            raise ReportDownloadError("trier_user environment variable not set")
        if not password:
            raise ReportDownloadError("trier_password environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        
        # Create and run downloader
        downloader = ReportDownloader(username, password)
        downloaded_file = downloader.run()
        
        logging.info(f"✓ Success: Report downloaded to {downloaded_file}")
        return 0  # Success exit code
        
    except Exception as e:
        logging.error(f"✗ Fatal error: {str(e)}")
        return 1  # Failure exit code


if __name__ == "__main__":
    # Exit with non-zero code on failure
    exit_code = main()
    exit(exit_code)
