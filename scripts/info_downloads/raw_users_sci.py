import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

download_dir = os.path.abspath(os.path.join(os.getcwd(), "downloads"))
os.makedirs(download_dir, exist_ok=True)

print(f"Download directory: {download_dir}", flush=True)

usuario = os.getenv("sci_user")
senha = os.getenv("sci_password")

if not usuario or not senha:
    raise ValueError("Environment variables 'user' and/or 'password' not set.")

# chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--allow-running-insecure-content")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--unsafely-treat-insecure-origin-as-secure=https://sciweb.com.br/")

prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True,
}
chrome_options.add_experimental_option("prefs", prefs)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
wait = WebDriverWait(driver, 100)

def clicar_elemento(xpath):
    try:
        elemento = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        driver.execute_script("arguments[0].scrollIntoView(true);", elemento)
        driver.execute_script("arguments[0].click();", elemento)
    except Exception as e:
        print(f"Erro ao clicar em {xpath}: {e}", flush=True)


def esperar_download_concluir(nome_arquivo):
    arquivos_iniciais = set(os.listdir(download_dir))
    inicio = time.time()

    while True:
        arquivos_atuais = set(os.listdir(download_dir))
        novos = arquivos_atuais - arquivos_iniciais

        if novos:
            arquivo = novos.pop()
            origem = os.path.join(download_dir, arquivo)
            destino = os.path.join(download_dir, f"{nome_arquivo}.csv")
            os.rename(origem, destino)
            print(f"File saved as: {destino}", flush=True)
            break

        if time.time() - inicio > 60:
            print("Download timeout!", flush=True)
            break

        time.sleep(1)

hoje = datetime.now()
mes = hoje.month + 1
ano = hoje.year

if mes == 13:
    mes = 1
    ano += 1

competencia = f"{mes:02d}/{ano}"
print(f"Competência: {competencia}", flush=True)

xpaths_filiais = [
    f'//*[@id="nav"]/ul/li[14]/ul/li[{i}]/a'
    for i in list(range(1, 12)) + list(range(13, 19))
]

try:
    print("\nInitiating SCI process\n", flush=True)

    # Abre site
    driver.get("https://sciweb.com.br/")

    # Login
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="usuario"]'))).send_keys(usuario)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="senha"]'))).send_keys(senha)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="btLoginPrincipal"]'))).click()

    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="rhnetsocial"]'))).click()

    # Loop Filiais exceto 12
    for filial_xpath in xpaths_filiais:
        try:
            index = filial_xpath.split("[")[-1].split("]")[0]

            clicar_elemento(filial_xpath)
            clicar_elemento('//*[@id="menu999"]')
            clicar_elemento('//*[@id="menu9"]')
            clicar_elemento('//*[@id="menu82"]/span[3]')
            clicar_elemento('//*[@id="menu83"]/span[2]')

            # Lista de XPath dos links com "Desmarcar todos"
            xpaths = [
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

            for xpath in xpaths:
                clicar_elemento(xpath)

            # Localiza e clica nos checkboxes
            checkboxes = {
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

            for name, xpath in checkboxes.items():
                clicar_elemento(xpath)
            
            # Botão de saída CSV
            clicar_elemento('//input[@id="1-saida" and @name="saida" and @value="CSV"]')

            # Preenche o campo de texto com "COLABORADORES"
            try:
                text_field_xpath = '//input[@id="titulo" and @name="titulo"]'
                text_field_element = wait.until(EC.presence_of_element_located((By.XPATH, text_field_xpath)))
                text_field_element.clear()
                text_field_element.send_keys("COLABORADORES")
            except Exception as e:
                print(f"Erro ao preencher o campo de texto: {e}", flush=True)
                
            # click Select2 container 
            select2_box = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#s2id_situacaoFuncionario .select2-choice")))
            select2_box.click()

            # click option by visible text
            option = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//div[@class='select2-result-label' and contains(normalize-space(), 'Somente ativos')]"
            )))
            option.click()
            time.sleep(3)
            
            # Clica no botão "Emitir"
            clicar_elemento('//button[@type="button" and contains(text(), "Emitir")]')

            nome_arquivo = f"COLABORADORES - {index.zfill(2)}"
            esperar_download_concluir(nome_arquivo)

            print(f"OK Filial {index}", flush=True)

        except Exception as e:
            print(f"Erro filial {filial_xpath}: {e}", flush=True)
            
    print("\nRestarting driver for filial 12...\n", flush=True)

    driver.quit()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 100)

    driver.get("https://sciweb.com.br/")
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="usuario"]'))).send_keys(usuario)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="senha"]'))).send_keys(senha)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="btLoginPrincipal"]'))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="rhnetsocial"]'))).click()

    try:
        filial12 = '//*[@id="nav"]/ul/li[14]/ul/li[12]/a'
        clicar_elemento(filial12)
        clicar_elemento('//*[@id="menu999"]')
        clicar_elemento('//*[@id="menu9"]')
        clicar_elemento('//*[@id="menu82"]/span[3]')
        clicar_elemento('//*[@id="menu83"]/span[2]')

        for xpath in xpaths:
            clicar_elemento(xpath)

        for name, xpath in checkboxes.items():
            clicar_elemento(xpath)
        
        # Botão de saída CSV
        clicar_elemento('//input[@id="1-saida" and @name="saida" and @value="CSV"]')

        text_field_element = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@id="titulo" and @name="titulo"]')))
        text_field_element.clear()
        text_field_element.send_keys("COLABORADORES")

        clicar_elemento('//button[@type="button" and contains(text(), "Emitir")]')

        nome_arquivo = "COLABORADORES - 12"
        esperar_download_concluir(nome_arquivo)

        print("OK Filial 12", flush=True)

    except Exception as e:
        print(f"Error filial 12: {e}", flush=True)

except Exception as e:
    print(f"General error: {e}", flush=True)

finally:
    time.sleep(5)
    driver.quit()
