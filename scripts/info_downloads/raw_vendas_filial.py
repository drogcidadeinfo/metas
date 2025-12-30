import os
import time
import shutil
import logging
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
# chrome_options.add_argument("--window-size=1920,1080")  # Set dimensions
chrome_options.add_argument("--start-maximized")  # Maximize window

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
    time.sleep(5)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "sideMenuSearch"))
        )
    
    except TimeoutException as e:
        logging.error("Element 'sideMenuSearch' not found — taking screenshot")
    
        # ✅ Saved in repo root (same place as README.md)
        driver.save_screenshot("timeout_sideMenuSearch.png")
    
        raise  # IMPORTANT: keeps GitHub Actions red ❌
    
    finally:
        pass

    '''# access "Compras Fornecedores"
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sideMenuSearch")))
    driver.find_element(By.ID, "sideMenuSearch").send_keys("Vendas Produtos")
    driver.find_element(By.ID, "sideMenuSearch").click()
    driver.implicitly_wait(2)

    driver.find_element(By.CSS_SELECTOR, '[title="Vendas Produtos"]').click()

    WebDriverWait(driver, 10).until(lambda x: x.execute_script("return document.readyState === 'complete'"))

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "agrup_fil_2"))).click()
    time.sleep(2)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tabTabdhtmlgoodies_tabView1_4"))).click()
    time.sleep(2)

    today = datetime.now()

    # Se for o primeiro dia do mês, pegar o primeiro e o último dia do mês anterior
    if today.day == 1:
        mes_anterior = today.replace(day=1) - timedelta(days=1)  # Último dia do mês anterior
        data_inicio = mes_anterior.replace(day=1).strftime('%d/%m/%Y')  # Primeiro dia do mês anterior
        data_fim = mes_anterior.strftime('%d/%m/%Y')  # Último dia do mês anterior
    else:
        data_inicio = today.replace(day=1).strftime('%d/%m/%Y')  # Primeiro dia do mês atual
        data_fim = (today - timedelta(days=1)).strftime('%d/%m/%Y')  # Ontem

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "dat_inicio"))).send_keys(data_inicio) # (data_inicio)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "dat_fim"))).send_keys(data_fim) # (data_fim)
    print(f"Período configurado: {data_inicio} a {data_fim}", flush=True)

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="selecaoI"]'))).click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="selecaoI"]/option[12]'))).click()

    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tabTabdhtmlgoodies_tabView1_5"))).click()
    time.sleep(2)
  
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "saida4"))).click()

    # trigger report download
    logging.info("Triggering report download...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "runReport"))).click()

    # log download start
    logging.info("Download has started.")
    # wait for download to complete
    time.sleep(50)

    # get the most recent downloaded file
    files = os.listdir(download_dir)
    downloaded_files = [f for f in files if f.endswith(('.xls', '.xlsx'))]
    if downloaded_files:
        # sort files by modifi time
        downloaded_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)))
        most_recent_file = downloaded_files[-1]  # get the most recent file
        downloaded_file_path = os.path.join(download_dir, most_recent_file)

        # rename the file to "filial<ID>"
        new_filename = f"raw_vendas_filial.xls"
        new_filepath = os.path.join(download_dir, new_filename)

        # make sure not to overwrite existing file
        if os.path.exists(new_filepath):
            os.remove(new_filepath)

        shutil.move(downloaded_file_path, new_filepath)

        file_size = os.path.getsize(new_filepath)
        logging.info(f"File renamed to {new_filename}. Size: {file_size} bytes. File path: {new_filepath}")
    else:
        logging.error("Download failed. No files found.")'''

finally:
    driver.quit()
