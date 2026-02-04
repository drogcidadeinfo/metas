import os
import time
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class SCIDownloadError(Exception):
    """Custom exception for SCI download failures"""
    pass

class SCIDownloader:
    def __init__(self, username: str, password: str):
        if not username or not password:
            raise SCIDownloadError("Username and password cannot be empty")
        
        self.username = username
        self.password = password
        self.download_dir = os.path.abspath(os.path.join(os.getcwd(), "downloads"))
        self.driver = None
        self.wait = None
        
        # Create download directory
        os.makedirs(self.download_dir, exist_ok=True)
        logging.info(f"Download directory: {self.download_dir}")
        
        # Calculate competência
        hoje = datetime.now()
        mes = hoje.month + 1
        ano = hoje.year
        if mes == 13:
            mes = 1
            ano += 1
        self.competencia = f"{mes:02d}/{ano}"
        logging.info(f"Competência: {self.competencia}")
        
        # Define XPaths (kept exactly as original)
        self.xpaths_filiais = [
            f'//*[@id="nav"]/ul/li[14]/ul/li[{i}]/a'
            for i in list(range(1, 12)) + list(range(13, 19))
        ]
        
        self.xpaths_desmarcar = [
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Cadastrais\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Residêncianoexterior\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Fisicos\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Histórico\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Documentos\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Familiar\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'FGTS\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Vínculos\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Profissional\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Dadosdiários\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Observação\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'eSocial\');")]',
            '//a[contains(@onclick, "checkboxAba(\'desmarca\',\'Opções\');")]',
        ]
        
        self.checkboxes = {
            '137': '//input[@name="aCampo[]" and @value="137"]',
            '1': '//input[@name="aCampo[]" and @value="1"]',
            '4': '//input[@name="aCampo[]" and @value="4"]',
            '5': '//input[@name="aCampo[]" and @value="5"]',
            '6': '//input[@name="aCampo[]" and @value="6"]',
            '7': '//input[@name="aCampo[]" and @value="7"]',
            '8': '//input[@name="aCampo[]" and @value="8"]',
            '9': '//input[@name="aCampo[]" and @value="9"]',
            '10': '//input[@name="aCampo[]" and @value="10"]',
            '11': '//input[@name="aCampo[]" and @value="11"]',
            '198': '//input[@name="aCampo[]" and @value="198"]',
            '146': '//input[@name="aCampo[]" and @value="146"]',
            '12': '//input[@name="aCampo[]" and @value="12"]',
            '13': '//input[@name="aCampo[]" and @value="13"]',
            '16': '//input[@name="aCampo[]" and @value="16"]',
            '17': '//input[@name="aCampo[]" and @value="17"]',
            '21': '//input[@name="aCampo[]" and @value="21"]',
            '188': '//input[@name="aCampo[]" and @value="188"]',
            '26': '//input[@name="aCampo[]" and @value="26"]',
            '24': '//input[@name="aCampo[]" and @value="24"]',
            '27': '//input[@name="aCampo[]" and @value="27"]',
            '25': '//input[@name="aCampo[]" and @value="25"]',
            '189': '//input[@name="aCampo[]" and @value="189"]',
            '199': '//input[@name="aCampo[]" and @value="199"]',
            '41': '//input[@name="aCampo[]" and @value="41"]',
            '42': '//input[@name="aCampo[]" and @value="42"]',
            '154': '//input[@name="aCampo[]" and @value="154"]',
            '44': '//input[@name="aCampo[]" and @value="44"]',
            '200': '//input[@name="aCampo[]" and @value="200"]',
            '52': '//input[@name="aCampo[]" and @value="52"]',
            '53': '//input[@name="aCampo[]" and @value="53"]',
            '56': '//input[@name="aCampo[]" and @value="56"]',
            '57': '//input[@name="aCampo[]" and @value="57"]',
            '58': '//input[@name="aCampo[]" and @value="58"]',
            '59': '//input[@name="aCampo[]" and @value="59"]',
            '60': '//input[@name="aCampo[]" and @value="60"]',
            '91': '//input[@name="aCampo[]" and @value="91"]',
            '92': '//input[@name="aCampo[]" and @value="92"]',
            '93': '//input[@name="aCampo[]" and @value="93"]',
            '127': '//input[@name="aCampo[]" and @value="127"]',
            '112': '//input[@name="aCampo[]" and @value="112"]',
            '113': '//input[@name="aCampo[]" and @value="113"]',
            '114': '//input[@name="aCampo[]" and @value="114"]',
            '180': '//input[@name="aCampo[]" and @value="180"]',
            '178': '//input[@name="aCampo[]" and @value="178"]',
            '192': '//input[@name="aCampo[]" and @value="192"]',
            '195': '//input[@name="aCampo[]" and @value="195"]',
            '197': '//input[@name="aCampo[]" and @value="197"]'
        }
    
    def setup_driver(self):
        """Configure Chrome driver with original settings"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--ignore-certificate-errors")
            
            # For headless mode in GitHub Actions
            if os.getenv("GITHUB_ACTIONS") == "true":
                chrome_options.add_argument("--headless")
            
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "directory_upgrade": True,
                "safebrowsing.enabled": True,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 100)
            
            logging.info("WebDriver initialized successfully")
            
        except Exception as e:
            raise SCIDownloadError(f"Failed to setup WebDriver: {str(e)}")
    
    def clicar_elemento(self, xpath: str):
        """Click element using JavaScript (original implementation)"""
        try:
            elemento = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            self.driver.execute_script("arguments[0].scrollIntoView(true);", elemento)
            self.driver.execute_script("arguments[0].click();", elemento)
            return True
        except Exception as e:
            raise SCIDownloadError(f"Error clicking {xpath}: {str(e)}")
    
    def esperar_download_concluir(self, nome_arquivo: str):
        """Wait for download to complete (original implementation)"""
        arquivos_iniciais = set(os.listdir(self.download_dir))
        inicio = time.time()
        timeout = 120  # Increased timeout for headless mode
        
        while True:
            arquivos_atuais = set(os.listdir(self.download_dir))
            novos = arquivos_atuais - arquivos_iniciais
            
            if novos:
                arquivo = novos.pop()
                origem = os.path.join(self.download_dir, arquivo)
                destino = os.path.join(self.download_dir, f"{nome_arquivo}.csv")
                
                try:
                    os.rename(origem, destino)
                    # Verify file is not empty
                    if os.path.getsize(destino) == 0:
                        raise SCIDownloadError(f"Downloaded file is empty: {destino}")
                    
                    logging.info(f"✓ File saved as: {destino}")
                    return destino
                except Exception as e:
                    raise SCIDownloadError(f"Failed to rename file: {str(e)}")
            
            if time.time() - inicio > timeout:
                raise SCIDownloadError(f"Download timeout after {timeout} seconds for {nome_arquivo}")
            
            time.sleep(1)
    
    def login(self):
        """Login to SCI website"""
        try:
            logging.info("Navigating to SCI website")
            self.driver.get("https://sciweb.com.br/")
            
            # Enter credentials
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="usuario"]')
            )).send_keys(self.username)
            
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="senha"]')
            )).send_keys(self.password)
            
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="btLoginPrincipal"]')
            )).click()
            
            # Click RH net social
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="rhnetsocial"]')
            )).click()
            
            logging.info("Login successful")
            
        except TimeoutException as e:
            raise SCIDownloadError(f"Login timeout: {str(e)}")
        except Exception as e:
            raise SCIDownloadError(f"Login failed: {str(e)}")
    
    def process_filial(self, filial_xpath: str):
        """Process a single filial"""
        try:
            # Extract index from XPath
            index = filial_xpath.split("[")[-1].split("]")[0]
            logging.info(f"Processing filial {index}...")
            
            # Navigate through menus
            self.clicar_elemento(filial_xpath)
            self.clicar_elemento('//*[@id="menu999"]')
            self.clicar_elemento('//*[@id="menu9"]')
            self.clicar_elemento('//*[@id="menu82"]/span[3]')
            self.clicar_elemento('//*[@id="menu83"]/span[2]')
            
            # Click all "Desmarcar todos" links
            for xpath in self.xpaths_desmarcar:
                self.clicar_elemento(xpath)
            
            # Click all checkboxes
            for name, xpath in self.checkboxes.items():
                self.clicar_elemento(xpath)
            
            # Select CSV output
            self.clicar_elemento('//input[@id="1-saida" and @name="saida" and @value="CSV"]')
            
            # Fill title field
            try:
                text_field_xpath = '//input[@id="titulo" and @name="titulo"]'
                text_field_element = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, text_field_xpath))
                )
                text_field_element.clear()
                text_field_element.send_keys("COLABORADORES")
            except Exception as e:
                raise SCIDownloadError(f"Failed to fill title field: {str(e)}")
            
            # Select "Somente ativos" from dropdown
            try:
                select2_box = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#s2id_situacaoFuncionario .select2-choice"))
                )
                select2_box.click()
                
                option = self.wait.until(EC.element_to_be_clickable((
                    By.XPATH, "//div[@class='select2-result-label' and contains(normalize-space(), 'Somente ativos')]"
                )))
                option.click()
                time.sleep(3)
            except Exception as e:
                raise SCIDownloadError(f"Failed to select 'Somente ativos': {str(e)}")
            
            # Click emit button
            self.clicar_elemento('//button[@type="button" and contains(text(), "Emitir")]')
            
            # Wait for download
            nome_arquivo = f"COLABORADORES - {index.zfill(2)}"
            file_path = self.esperar_download_concluir(nome_arquivo)
            
            logging.info(f"✓ Filial {index} completed successfully")
            return file_path
            
        except Exception as e:
            raise SCIDownloadError(f"Failed to process filial {filial_xpath}: {str(e)}")
    
    def process_filial_12(self):
        """Special handling for filial 12 (restart driver)"""
        try:
            logging.info("Processing filial 12 (with driver restart)...")
            
            # Restart driver
            self.driver.quit()
            self.setup_driver()
            
            # Login again
            self.driver.get("https://sciweb.com.br/")
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="usuario"]')
            )).send_keys(self.username)
            
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="senha"]')
            )).send_keys(self.password)
            
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="btLoginPrincipal"]')
            )).click()
            
            self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="rhnetsocial"]')
            )).click()
            
            # Process filial 12
            filial12 = '//*[@id="nav"]/ul/li[14]/ul/li[12]/a'
            self.clicar_elemento(filial12)
            self.clicar_elemento('//*[@id="menu999"]')
            self.clicar_elemento('//*[@id="menu9"]')
            self.clicar_elemento('//*[@id="menu82"]/span[3]')
            self.clicar_elemento('//*[@id="menu83"]/span[2]')
            
            for xpath in self.xpaths_desmarcar:
                self.clicar_elemento(xpath)
            
            for name, xpath in self.checkboxes.items():
                self.clicar_elemento(xpath)
            
            self.clicar_elemento('//input[@id="1-saida" and @name="saida" and @value="CSV"]')
            
            # Fill title field
            try:
                text_field_xpath = '//input[@id="titulo" and @name="titulo"]'
                text_field_element = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, text_field_xpath))
                )
                text_field_element.clear()
                text_field_element.send_keys("COLABORADORES")
            except Exception as e:
                raise SCIDownloadError(f"Failed to fill title field for filial 12: {str(e)}")
            
            # Select dropdown
            try:
                select2_box = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#s2id_situacaoFuncionario .select2-choice"))
                )
                select2_box.click()
                
                option = self.wait.until(EC.element_to_be_clickable((
                    By.XPATH, "//div[@class='select2-result-label' and contains(normalize-space(), 'Somente ativos')]"
                )))
                option.click()
                time.sleep(3)
            except Exception as e:
                raise SCIDownloadError(f"Failed to select 'Somente ativos' for filial 12: {str(e)}")
            
            self.clicar_elemento('//button[@type="button" and contains(text(), "Emitir")]')
            
            nome_arquivo = "COLABORADORES - 12"
            file_path = self.esperar_download_concluir(nome_arquivo)
            
            logging.info("✓ Filial 12 completed successfully")
            return file_path
            
        except Exception as e:
            raise SCIDownloadError(f"Failed to process filial 12: {str(e)}")
    
    def run(self):
        """Main execution flow"""
        downloaded_files = []
        
        try:
            logging.info("Starting SCI download process...")
            self.setup_driver()
            self.login()
            
            # Process all filiais except 12
            for filial_xpath in self.xpaths_filiais:
                try:
                    file_path = self.process_filial(filial_xpath)
                    downloaded_files.append(file_path)
                except SCIDownloadError as e:
                    logging.error(f"✗ {str(e)}")
                    # Continue with other filiais but mark as partial failure
            
            # Process filial 12 (special case)
            try:
                file_path = self.process_filial_12()
                downloaded_files.append(file_path)
            except SCIDownloadError as e:
                logging.error(f"✗ {str(e)}")
                raise  # Re-raise to fail workflow on filial 12
            
            # Check if we got any files
            if not downloaded_files:
                raise SCIDownloadError("No files were downloaded")
            
            logging.info(f"✓ Process completed. Downloaded {len(downloaded_files)} files.")
            return downloaded_files
            
        except Exception as e:
            logging.error(f"✗ Fatal error in main process: {str(e)}")
            raise  # Re-raise to fail GitHub Actions workflow
            
        finally:
            if self.driver:
                logging.info("Cleaning up WebDriver...")
                self.driver.quit()


def main():
    """Entry point with proper exit codes for GitHub Actions"""
    try:
        # Get credentials from environment
        usuario = os.getenv("sci_user")
        senha = os.getenv("sci_password")
        
        if not usuario:
            raise SCIDownloadError("sci_user environment variable not set")
        if not senha:
            raise SCIDownloadError("sci_password environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        
        # Create and run downloader
        downloader = SCIDownloader(usuario, senha)
        downloaded_files = downloader.run()
        
        logging.info(f"✓ Success: Downloaded {len(downloaded_files)} files")
        for file in downloaded_files:
            logging.info(f"  - {os.path.basename(file)}")
        
        return 0  # Success exit code
        
    except SCIDownloadError as e:
        logging.error(f"✗ SCI Download Error: {str(e)}")
        return 1
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
