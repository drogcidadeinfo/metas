import os
import time
import logging
import shutil 
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# set up logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
username = os.getenv("trier_user")
password = os.getenv("trier_password")

if not username or not password:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

inicio = (datetime.now() - timedelta(days=365)).strftime("%d%m%Y")
fim = datetime.now().strftime("%d%m%Y")  

download_dir = os.getcwd()  

# set up chrome options for headless mode/configure download behavior
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

prefs = {
    "download.default_directory": download_dir,  # set download path
    "download.prompt_for_download": False,  # disable prompt
    "directory_upgrade": True,  # auto-overwrite existing files
    "safebrowsing.enabled": False,  # disable safe browsing (meh)
    "safebrowsing.disable_download_protection": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

# initialize webdriver
driver = webdriver.Chrome(options=chrome_options)

# start download process 
try:
    logging.info("Navigate to the target URL and login")
    driver.get("http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")

    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "id_cod_usuario"))).send_keys(username)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "nom_senha"))).send_keys(password)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "login"))).click()

    # wait til page loads completely
    WebDriverWait(driver, 10).until(lambda x: x.execute_script("return document.readyState === 'complete'"))
    time.sleep(10)

    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.F11)
    time.sleep(2)

    # access "Compras Fornecedores"
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sideMenuSearch")))
    driver.find_element(By.ID, "sideMenuSearch").send_keys("Funcionários / Vendedores")
    driver.find_element(By.ID, "sideMenuSearch").click()
    driver.implicitly_wait(2)

    driver.find_element(By.CSS_SELECTOR, '[title="Funcionários / Vendedores"]').click()

    WebDriverWait(driver, 10).until(lambda x: x.execute_script("return document.readyState === 'complete'"))

    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "status_1"))).click()

    # report format; downloads xls file
    driver.find_element(By.ID, "saida4").click()  

    # trigger report download
    logging.info("Triggering report download...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "runReport"))).click()

    # log download start
    logging.info("Download has started.")
    # wait for download to complete
    time.sleep(10)

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith(('.xls', '.xlsx'))]
    if downloaded_files:
        # sort files by modifi time
        downloaded_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)))
        most_recent_file = downloaded_files[-1]  # get the most recent file
        downloaded_file_path = os.path.join(download_dir, most_recent_file)

        # rename the file to "filial<ID>"
        new_filename = f"raw_users_trier.xls"
        new_filepath = os.path.join(download_dir, new_filename)

        # make sure not to overwrite existing file
        if os.path.exists(new_filepath):
            os.remove(new_filepath)

        shutil.move(downloaded_file_path, new_filepath)

        file_size = os.path.getsize(new_filepath)
        logging.info(f"File renamed to {new_filename}. Size: {file_size} bytes. File path: {new_filepath}")
    else:
        logging.error("Download failed. No files found.")

finally:
    driver.quit()
