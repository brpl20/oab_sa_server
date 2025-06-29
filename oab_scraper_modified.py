import time
import requests
import os
import sys
import signal
import gc
import asyncio
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import json
import re
import logging
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import tempfile

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("oab_scraper")

# Get credentials from environment variables
PROXY_USERNAME = os.getenv('PROXY_USERNAME')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')
PROXY_HOST = os.getenv('PROXY_HOST')

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET = os.getenv('AWS_BUCKET')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')

# Validate environment variables
required_env_vars = {
    'PROXY_USERNAME': PROXY_USERNAME,
    'PROXY_PASSWORD': PROXY_PASSWORD,
    'PROXY_HOST': PROXY_HOST,
    'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
    'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
    'AWS_BUCKET': AWS_BUCKET,
    'AWS_DEFAULT_REGION': AWS_DEFAULT_REGION
}

missing_vars = [var for var, value in required_env_vars.items() if not value]
if missing_vars:
    print(f"❌ Variáveis de ambiente faltando: {', '.join(missing_vars)}")
    print("   Certifique-se de ter um arquivo .env com todas as credenciais necessárias")
    sys.exit(1)

PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}"

PROXY_CONFIG = {
    'http': PROXY_URL,
    'https': PROXY_URL
}

# Initialize AWS S3 client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )
    # Test S3 connection
    s3_client.head_bucket(Bucket=AWS_BUCKET)
    print(f"✅ Conexão S3 estabelecida com bucket: {AWS_BUCKET}")
except NoCredentialsError:
    print("❌ Credenciais AWS inválidas")
    sys.exit(1)
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        print(f"❌ Bucket S3 não encontrado: {AWS_BUCKET}")
    else:
        print(f"❌ Erro ao conectar com S3: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro na configuração AWS: {e}")
    sys.exit(1)

# Global variables for signal handler
enhanced_lawyers = []
current_batch_file = ""
error_log = []
batch_counter = 0

# Initialize error log
error_log = []

# ===== MODIFICAÇÕES PARA SESSÃO PERSISTENTE E LOG DE IPs =====

# Global requests session and counter for proxy IP rotation
global_requests_session = None
requests_session_use_count = 0
MAX_REQUESTS_PER_SESSION = 100  # Novo limite de requisições por sessão
current_proxy_ip = None

def upload_to_s3(data, key, content_type='application/json'):
    """Upload data to S3 bucket"""
    try:
        if isinstance(data, (dict, list)):
            # Convert dict/list to JSON string
            content = json.dumps(data, ensure_ascii=False, indent=2)
        else:
            content = str(data)
        
        s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=content.encode('utf-8'),
            ContentType=content_type,
            ServerSideEncryption='AES256'
        )
        return f"s3://{AWS_BUCKET}/{key}"
    except Exception as e:
        print(f"❌ Erro ao fazer upload para S3: {e}")
        return None

def upload_file_to_s3(local_file_path, s3_key):
    """Upload local file to S3"""
    try:
        s3_client.upload_file(
            local_file_path, 
            AWS_BUCKET, 
            s3_key,
            ExtraArgs={'ServerSideEncryption': 'AES256'}
        )
        return f"s3://{AWS_BUCKET}/{s3_key}"
    except Exception as e:
        print(f"❌ Erro ao fazer upload do arquivo para S3: {e}")
        return None

def save_to_s3_and_local_backup(data, filename, content_type='application/json'):
    """Save data to S3 and keep local backup for emergency"""
    try:
        # Save to S3
        s3_key = f"oab_data/{filename}"
        s3_url = upload_to_s3(data, s3_key, content_type)
        
        if s3_url:
            print(f"  ✅ Salvo no S3: {s3_url}")
            
            # Create local backup (small file for emergency recovery)
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    if isinstance(data, (dict, list)):
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    else:
                        f.write(str(data))
                print(f"  📁 Backup local: {filename}")
            except Exception as e:
                print(f"  ⚠️ Backup local falhou: {e}")
            
            return s3_url
        else:
            # Fallback to local only
            print(f"  ⚠️ S3 falhou, salvando apenas localmente")
            with open(filename, 'w', encoding='utf-8') as f:
                if isinstance(data, (dict, list)):
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
            return filename
            
    except Exception as e:
        print(f"  ❌ Erro no salvamento: {e}")
        # Emergency local save
        try:
            with open(f"emergency_{filename}", 'w', encoding='utf-8') as f:
                if isinstance(data, (dict, list)):
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
            return f"emergency_{filename}"
        except:
            return None

def signal_handler(signum, frame):
    """Handle Ctrl+C interruption and save current progress"""
    print("\n\n🛑 INTERRUPÇÃO DETECTADA!")
    print("💾 Salvando progresso atual...")
    
    if enhanced_lawyers:
        emergency_filename = save_enhanced_lawyers_to_file(
            enhanced_lawyers, 
            current_batch_file, 
            emergency=True
        )
        print(f"✅ Dados salvos em: {emergency_filename}")
        print(f"📊 Total processado: {len(enhanced_lawyers)} advogados")
    else:
        print("⚠️ Nenhum dado para salvar")
    
    if error_log:
        batch_base = os.path.splitext(os.path.basename(current_batch_file))[0] if current_batch_file else "unknown"
        error_file_name = f"error_log_{batch_base}_emergency_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        save_to_s3_and_local_backup(
            "\n".join([f"Log de Erros de Emergência - {current_batch_file}", "="*50, ""] + error_log),
            error_file_name,
            'text/plain'
        )
        print(f"📝 Log de erros salvo: {error_file_name}")
    
    print("🚪 Saindo...")
    sys.exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

def clean_state(state):
    """Clean state field to keep only valid Brazilian state codes (2 letters)"""
    if not state:
        return state
    
    # Remove any non-alphabetic characters and convert to uppercase
    cleaned = re.sub(r'[^A-Za-z]', '', str(state)).upper()
    
    # Keep only first 2 characters if longer
    if len(cleaned) > 2:
        cleaned = cleaned[:2]
    
    # Validate it's a valid Brazilian state code
    valid_states = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    }
    
    if cleaned in valid_states:
        return cleaned
    else:
        print(f"⚠️ Estado inválido encontrado: '{state}' -> '{cleaned}' (não é um estado brasileiro válido)")
        return cleaned  # Return anyway, let the API handle validation

def should_process_record(record):
    """Determine if a record should be processed based on the criteria"""
    # Scenario 1: Record doesn't have "processed": true
    if not record.get('processed', False):
        return True, "não processado"
    
    # Scenario 2: Record is processed but has empty society arrays
    society_basic = record.get('society_basic_details', [])
    society_complete = record.get('society_complete_details', [])
    
    # If has_society is True but either array is empty, reprocess
    if record.get('has_society', False):
        if not society_basic or not society_complete:
            return True, "sociedades incompletas"
    
    # If has_society is None or False, check if it should have been True
    # (This handles cases where the initial processing failed to detect societies)
    if record.get('has_society') is None:
        return True, "status de sociedade não determinado"
    
    # Record is complete and doesn't need reprocessing
    return False, "completo"

# ===== FUNÇÕES MODIFICADAS PARA SESSÃO PERSISTENTE =====

def get_current_ip(session=None):
    """Get the current IP address being used by the proxy"""
    try:
        current_session = session if session else get_requests_session_with_proxy_managed()
        if current_session is None:
            return None
        response = current_session.get('https://ip.decodo.com/json', timeout=10, verify=False)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.warning(f"Erro ao obter IP atual: {e}")
    return None

def save_ip_log(ip_data, filename="proxy_ip_log.json"):
    """Save IP data to S3 and local backup"""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "ip_data": ip_data,
            "session_request_count": requests_session_use_count
        }
        
        # Para S3, vamos criar um arquivo de log diário com append
        s3_key = f"logs/proxy_ip_log_{time.strftime('%Y%m%d')}.jsonl"
        log_line = json.dumps(log_entry, ensure_ascii=False) + "\n"
        
        # Tenta ler o conteúdo existente e adicionar, ou cria um novo
        try:
            existing_object = s3_client.get_object(Bucket=AWS_BUCKET, Key=s3_key)
            existing_content = existing_object['Body'].read().decode('utf-8')
            new_content = existing_content + log_line
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                new_content = log_line
            else:
                raise
        
        s3_client.put_object(
            Bucket=AWS_BUCKET,
            Key=s3_key,
            Body=new_content.encode('utf-8'),
            ContentType='application/jsonl',
            ServerSideEncryption='AES256'
        )
        
        # Local backup
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"Erro ao salvar log de IP: {e}")

def get_requests_session_with_proxy_managed():
    """
    Returns a requests session configured with the rotating proxy.
    Manages session creation based on MAX_REQUESTS_PER_SESSION.
    """
    global global_requests_session, requests_session_use_count, current_proxy_ip

    if global_requests_session is None or requests_session_use_count >= MAX_REQUESTS_PER_SESSION:
        if global_requests_session:
            print(f"🔄 Fechando sessão anterior após {requests_session_use_count} requisições.")
            try:
                global_requests_session.close()
            except Exception as e:
                logger.warning(f"Erro ao fechar sessão requests anterior: {e}")

        print("🔄 Criando nova sessão requests com proxy...")
        try:
            session = requests.Session()
            session.proxies.update(PROXY_CONFIG)
            session.timeout = 30
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            })
            # Adicionar verify=False para ignorar erros de certificado (para teste)
            session.verify = False 
            
            global_requests_session = session
            requests_session_use_count = 0
            
            # Log e print do IP do proxy
            ip_data = get_current_ip(session=global_requests_session)
            if ip_data:
                current_proxy_ip = ip_data.get('ip', 'unknown')
                country = ip_data.get('country', 'unknown')
                city = ip_data.get('city', 'unknown')
                ip_info = f"🌍 IP do Proxy: {current_proxy_ip} ({city}, {country})"
                print(f"✅ Nova sessão requests criada. {ip_info}")
                logger.info(f"Nova sessão requests criada. {ip_info}")
                save_ip_log(ip_data, "proxy_ip_log.json")
            else:
                current_proxy_ip = "unknown"
                print("⚠️ Nova sessão requests criada, mas não foi possível verificar o IP do proxy.")
                logger.warning("Nova sessão requests criada, mas não foi possível verificar o IP do proxy.")

        except Exception as e:
            logger.error(f"Erro ao criar sessão requests: {str(e)}")
            global_requests_session = None
            requests_session_use_count = 0
            current_proxy_ip = None
            return None
            
    requests_session_use_count += 1
    return global_requests_session

def verify_proxy_connection():
    """Verify that the proxy connection is working properly"""
    ip_data = get_current_ip(session=get_requests_session_with_proxy_managed())
    return ip_data is not None

def make_request_with_retry(method, url, max_retries=4, retry_delay=5, **kwargs):
    """Make HTTP request with retry logic for None responses and other errors"""
    global requests_session_use_count, global_requests_session

    for attempt in range(max_retries):
        try:
            session = get_requests_session_with_proxy_managed()
            if session is None:
                print(f"        ⚠️ Tentativa {attempt + 1}: Falha ao obter sessão requests.")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception("Failed to get requests session after all retries")
            
            # Print do IP atual a cada 10 requisições
            if requests_session_use_count % 10 == 1:
                print(f"        📊 Requisição #{requests_session_use_count}/{MAX_REQUESTS_PER_SESSION} - IP: {current_proxy_ip}")
            
            # Make the request
            if method.upper() == 'POST':
                response = session.post(url, **kwargs)
            elif method.upper() == 'GET':
                response = session.get(url, **kwargs)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check if response is None (shouldn't happen with requests, but for safety)
            if response is None:
                print(f"        ⚠️ Tentativa {attempt + 1}: Resposta None recebida")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception("Response is None after all retries")
            
            # Check status code
            response.raise_for_status()
            return response
            
        except requests.exceptions.ProxyError as e:
            error_msg = f"ProxyError na URL {url}: {str(e)}"
            print(f"        ⚠️ Tentativa {attempt + 1} falhou: {error_msg}")
            error_log.append(error_msg)
            
            # Se for um ProxyError, forçar a criação de uma nova sessão na próxima tentativa
            if global_requests_session:
                try:
                    global_requests_session.close()
                except:
                    pass
            global_requests_session = None
            requests_session_use_count = 0
            
            if attempt >= max_retries - 1:
                raise Exception(f"Request failed after {max_retries} attempts: {error_msg}")
            
            print(f"        ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
            time.sleep(retry_delay)
        except Exception as e:
            error_msg = str(e)
            print(f"        ⚠️ Tentativa {attempt + 1} falhou: {error_msg}")
            error_log.append(error_msg)
            
            # Se for um erro geral, também forçar a criação de uma nova sessão
            if global_requests_session:
                try:
                    global_requests_session.close()
                except:
                    pass
            global_requests_session = None
            requests_session_use_count = 0
            
            # If it's the last attempt, raise the error
            if attempt >= max_retries - 1:
                raise Exception(f"Request failed after {max_retries} attempts: {error_msg}")
            
            # Wait before retry
            print(f"        ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
            time.sleep(retry_delay)
    
    # This should never be reached, but just in case
    raise Exception(f"Request failed after {max_retries} attempts")

# ===== FIM DAS MODIFICAÇÕES DE SESSÃO =====

# Webdriver management with proxy support (HEADLESS)
def get_chrome_driver_with_proxy():
    """Create Chrome driver with proxy configuration (HEADLESS with virtual environment)"""
    options = ChromeOptions()
    options.add_argument(f'--proxy-server={PROXY_URL}')
    options.add_argument('--headless')  # HEADLESS MODE
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-logging')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-plugins')
    
    # Enable JavaScript and allow all content for modal functionality
    options.add_argument('--enable-javascript')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-cross-origin-auth-prompt')
    
    # Adicionar para ignorar erros de certificado
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-insecure-localhost')
    
    # Set user agent to appear more like a real browser
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36')
    
    # Enable virtual display for better modal detection
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(
            service=webdriver.chrome.service.Service(ChromeDriverManager().install()),
            options=options
        )
        
        # Configure driver for better modal detection
        driver.set_page_load_timeout(45)  # Increased timeout
        driver.implicitly_wait(15)  # Increased implicit wait
        
        # Execute script to hide automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    except Exception as e:
        logger.error(f"Failed to create Chrome driver: {str(e)}")
        return None

def get_firefox_driver_with_proxy():
    """Create Firefox driver with proxy configuration (HEADLESS with virtual environment)"""
    options = Options()
    options.add_argument("--headless")  # HEADLESS MODE
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    
    # Configure proxy for Firefox
    proxy_host, proxy_port = PROXY_HOST.split(':')
    
    # Em Selenium 4.x, as preferências são definidas diretamente nas Options
    options.set_preference("network.proxy.type", 1)
    options.set_preference("network.proxy.http", proxy_host)
    options.set_preference("network.proxy.http_port", int(proxy_port))
    options.set_preference("network.proxy.ssl", proxy_host)
    options.set_preference("network.proxy.ssl_port", int(proxy_port))
    options.set_preference("network.proxy.share_proxy_settings", True)
    options.set_preference("network.proxy.autoconfig_url", "")
    
    # Enable JavaScript and content for modal functionality
    options.set_preference("javascript.enabled", True)
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference('useAutomationExtension', False)
    
    # Adicionar para ignorar erros de certificado
    options.set_preference("security.enterprise_roots.enabled", True)
    options.set_preference("security.cert_pinning.untrusted_root_removal", False)
    
    # Set user agent
    options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0")
    
    try:
        driver = webdriver.Firefox(
            service=webdriver.firefox.service.Service(GeckoDriverManager().install()),
            options=options
        )
        
        # Configure driver for better modal detection
        driver.set_page_load_timeout(45)  # Increased timeout
        driver.implicitly_wait(15)  # Increased implicit wait
        
        return driver
    except Exception as e:
        logger.error(f"Failed to create Firefox driver: {str(e)}")
        return None

def get_driver_with_proxy():
    """Get a webdriver with proxy support (tries Chrome first, then Firefox)"""
    driver = get_chrome_driver_with_proxy()
    if driver is None:
        driver = get_firefox_driver_with_proxy()
    
    if driver is None:
        logger.error("Failed to create any webdriver with proxy")
        raise Exception("Could not create webdriver with proxy")
    
    return driver

def get_initial_cookies(max_retries=4, retry_delay=2):
    """Get initial cookies and token from OAB website with retry logic"""
    for attempt in range(max_retries):
        driver = None
        try:
            print(f"    🍪 Tentativa {attempt + 1} de obter cookies...")
            driver = get_driver_with_proxy()
            driver.get("https://cna.oab.org.br/")
            
            # Wait for page to load completely (important for headless)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)

            cookies = driver.get_cookies()
            cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}

            # More robust token finding for headless mode
            try:
                token_element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.NAME, "__RequestVerificationToken"))
                )
                token = token_element.get_attribute("value")
            except TimeoutException:
                # Fallback: try to find token in page source
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'html.parser')
                token_input = soup.find('input', {'name': '__RequestVerificationToken'})
                if token_input and token_input.get('value'):
                    token = token_input.get('value')
                else:
                    raise Exception("Could not find verification token")

            print(f"    ✅ Cookies e token obtidos com sucesso!")
            return cookie_dict, token
            
        except Exception as e:
            print(f"    ⚠️ Tentativa {attempt + 1} falhou: {str(e)}")
            if attempt < max_retries - 1:
                print(f"    ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to get initial cookies after {max_retries} attempts: {str(e)}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

def extract_modal_data(modal_html):
    """Extract all data from the modal content"""
    soup = BeautifulSoup(modal_html, 'html.parser')

    result = {
        'firm_name': soup.select_one('.modal-title b').text.strip() if soup.select_one('.modal-title b') else None,
        'inscricao': None,
        'estado': None,
        'situacao': soup.select_one('.label').text.strip() if soup.select_one('.label') else None,
        'endereco': None,
        'telefones': None,
        'socios': []
    }

    # Extract inscricao
    inscricao_elem = soup.find('b', string=lambda text: text and 'Inscrição:' in text)
    if inscricao_elem:
        result['inscricao'] = inscricao_elem.parent.get_text(strip=True).replace('Inscrição:', '').strip()

    # Extract estado
    estado_elem = soup.find('b', string=lambda text: text and 'Estado:' in text)
    if estado_elem:
        result['estado'] = estado_elem.parent.get_text(strip=True).replace('Estado:', '').strip()

    # Extract endereco
    endereco_elem = soup.find('b', string=lambda text: text and 'Endereço:' in text)
    if endereco_elem:
        result['endereco'] = endereco_elem.parent.get_text(strip=True).replace('Endereço:', '').strip()

    # Extract telefones
    telefones_elem = soup.find('b', string=lambda text: text and 'Telefones:' in text)
    if telefones_elem:
        result['telefones'] = telefones_elem.parent.get_text(strip=True).replace('Telefones:', '').strip()

    # Extract partners data
    for row in soup.select('.socContainer tr'):
        cols = row.find_all('td')
        if len(cols) >= 4:
            result['socios'].append({
                'numero': cols[0].get_text(strip=True),
                'nome': cols[1].get_text(strip=True),
                'nome_social': cols[2].get_text(strip=True),
                'tipo': cols[3].get_text(strip=True),
                'cna_link': row.get('data-cnalink', '')
            })

    return result

def get_modal_data_with_selenium(url, max_wait=30, max_retries=4, retry_delay=2):
    """Get modal data from sociedade URL using Selenium with retry logic"""
    for attempt in range(max_retries):
        driver = None
        try:
            print(f"        🌐 Tentativa {attempt + 1}: Navegando para: {url}")
            driver = get_driver_with_proxy()
            driver.get(url)

            # Wait specifically for the modal content to appear
            print(f"        ⏳ Aguardando modal aparecer...")
            wait = WebDriverWait(driver, max_wait)
            modal = wait.until(
                EC.visibility_of_element_located((By.CLASS_NAME, "modal-content"))
            )

            # Additional wait for content to load completely
            time.sleep(3)

            # Get the complete modal HTML
            print(f"        📋 Extraindo dados do modal...")
            modal_html = modal.get_attribute('outerHTML')
            
            # Extract structured data using the specific parser
            modal_data = extract_modal_data(modal_html)
            
            if modal_data:
                print(f"        ✅ Modal extraído com sucesso:")
                print(f"             - Firma: {modal_data.get('firm_name', 'N/A')}")
                print(f"             - Inscrição: {modal_data.get('inscricao', 'N/A')}")
                print(f"             - Estado: {modal_data.get('estado', 'N/A')}")
                print(f"             - Sócios: {len(modal_data.get('socios', []))}")
                
                # Return structured data with metadata
                return {
                    'extraction_method': 'specific_modal_parser',
                    'content_loaded': True,
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'url': url,
                    'modal_data': modal_data,
                    'extraction_success': 5 if modal_data.get('firm_name') else 3
                }
            else:
                print(f"        ❌ Falha na extração dos dados do modal")
                if attempt < max_retries - 1:
                    print(f"        ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
                    time.sleep(retry_delay)
                    continue
                
                return {
                    'extraction_method': 'specific_modal_parser',
                    'content_loaded': False,
                    'error': 'Failed to extract modal data after all retries',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'url': url,
                    'extraction_success': 0
                }

        except TimeoutException:
            error_msg = f"Timeout waiting for modal to appear at {url}"
            print(f"        ⏰ Tentativa {attempt + 1}: Modal não apareceu em {max_wait}s")
            
            if attempt < max_retries - 1:
                print(f"        ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
                time.sleep(retry_delay)
                continue
            
            return {
                'extraction_method': 'specific_modal_parser',
                'content_loaded': False,
                'error': error_msg,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'url': url,
                'extraction_success': 0
            }
        except Exception as e:
            error_msg = f"Error getting modal data from {url}: {str(e)}"
            logger.error(error_msg)
            print(f"        ❌ Tentativa {attempt + 1}: Erro geral: {str(e)}")
            
            if attempt < max_retries - 1:
                print(f"        ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
                time.sleep(retry_delay)
                continue
            
            return {
                'extraction_method': 'specific_modal_parser',
                'content_loaded': False,
                'error': str(e),
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'url': url,
                'extraction_success': 0
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

async def process_sociedade_async(sociedade, state, insc, lawyer_name, executor):
    """Process a single sociedade asynchronously with retry logic"""
    try:
        print(f"      📋 Processando sociedade: {sociedade['NomeSoci']} ({sociedade['Insc']})")

        final_url = "https://cna.oab.org.br" + sociedade['Url']

        # Run the selenium function in thread pool with retry
        loop = asyncio.get_event_loop()
        modal_data = await loop.run_in_executor(
            executor,
            get_modal_data_with_selenium,
            final_url,
            25,  # timeout
            4,   # max_retries
            2    # retry_delay
        )

        if not modal_data or not modal_data.get('content_loaded', False):
            error_message = f"Failed to get modal data for sociedade {sociedade['Insc']} after all retries"
            print(f"      ❌ ERRO: {error_message}")
            error_log.append(error_message)
            return None

        # Combine all data into final result
        final_result = {
            'lawyer_info': {
                'lawyer_name': lawyer_name,
                'lawyer_state': state,
                'lawyer_insc': insc
            },
            'basic_info': {
                'Insc': sociedade['Insc'],
                'NomeSoci': sociedade['NomeSoci'],
                'IdtSoci': sociedade['IdtSoci'],
                'SiglUf': sociedade['SiglUf'],
                'source_url': final_url
            },
            'modal_data': modal_data,
            'processed_at': time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }

        # Save to S3 instead of local file
        filename = f"sociedade_{state}_{insc}_{sanitize_filename(sociedade['Insc'])}_{int(time.time())}.json"
        s3_url = save_to_s3_and_local_backup(final_result, filename)
        
        if s3_url:
            print(f"      ✅ Sociedade salva: {filename}")
        else:
            print(f"      ⚠️ Problema ao salvar sociedade: {filename}")

        return final_result

    except Exception as e:
        error_message = f"Error processing sociedade {sociedade['Insc']}: {str(e)}"
        print(f"      ❌ ERRO: {error_message}")
        error_log.append(error_message)
        return None

def sanitize_filename(filename):
    """Remove invalid characters from filename"""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

async def search_lawyer_with_updates(insc, state, cookies, token, original_record, max_retries=4, retry_delay=2):
    """Search for lawyer and update record with external data, plus extract sociedades ASYNC with robust retry"""
    search_url = "https://cna.oab.org.br/Home/Search"
    search_data = {
        "__RequestVerificationToken": token,
        "IsMobile": "false",
        "NomeAdvo": "",
        "Insc": str(insc),
        "Uf": state,
        "TipoInsc": ""
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Content-Type": "application/json",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    # Create enhanced record structure
    enhanced_record = original_record.copy()
    enhanced_record['processed'] = True
    enhanced_record['has_society'] = False
    enhanced_record['corrected_full_name'] = None
    enhanced_record['society_link'] = None
    enhanced_record['society_basic_details'] = []
    enhanced_record['society_complete_details'] = []

    for attempt in range(max_retries):
        try:
            print(f"    🔍 Tentativa {attempt + 1}: Buscando advogado...")
            
            # Step 1: Initial search with retry
            response = make_request_with_retry(
                'POST', 
                search_url, 
                max_retries=4,
                retry_delay=2,
                json=search_data, 
                headers=headers, 
                cookies=cookies
            )
            
            search_result = response.json()

            if not (search_result['Success'] and search_result['Data']):
                error_message = f"Search failed or no results found for {state} {insc}"
                print(f"    ❌ {error_message}")
                if attempt < max_retries - 1:
                    print(f"    ⏳ Aguardando {retry_delay}s antes da próxima tentativa...")
                    time.sleep(retry_delay)
                    continue
                error_log.append(error_message)
                return enhanced_record, True

            # Compare and update full_name only if different
            external_name = search_result['Data'][0].get('Nome')
            original_name = enhanced_record.get('full_name', '').strip()

            if external_name and external_name.strip():
                external_name_clean = external_name.strip()
                if original_name.upper() != external_name_clean.upper():
                    print(f"    🔄 NOME DIFERENTE - Atualizando:")
                    print(f"        Original: '{original_name}'")
                    print(f"        Correto:  '{external_name_clean}'")
                    enhanced_record['corrected_full_name'] = external_name_clean
                else:
                    print(f"    ✅ Nome confere: '{original_name}'")

            # Step 2: Get detail URL with retry
            detail_url = "https://cna.oab.org.br" + search_result['Data'][0]['DetailUrl']
            enhanced_record['society_link'] = detail_url

            print(f"    🔍 Buscando detalhes da sociedade...")
            detail_response = make_request_with_retry(
                'GET',
                detail_url,
                max_retries=4,
                retry_delay=2,
                headers=headers,
                cookies=cookies
            )
            
            detail_result = detail_response.json()

            if not (detail_result['Success'] and 'Sociedades' in detail_result['Data']):
                print(f"    ℹ️  Sem dados de sociedades para {enhanced_record['full_name']}")
                return enhanced_record, True

            # Process sociedades
            sociedades_data = detail_result['Data']['Sociedades']

            if sociedades_data is None or len(sociedades_data) == 0:
                print(f"    ℹ️  {enhanced_record['full_name']} não possui sociedades")
                return enhanced_record, True

            # Update has_society flag
            enhanced_record['has_society'] = True
            
            # Store basic sociedades info
            basic_sociedades = []
            for soc in sociedades_data:
                basic_info = {
                    'Insc': soc['Insc'],
                    'NomeSoci': soc['NomeSoci'],
                    'IdtSoci': soc['IdtSoci'],
                    'SiglUf': soc['SiglUf'],
                    'Url': soc['Url']
                }
                basic_sociedades.append(basic_info)
            
            enhanced_record['society_basic_details'] = basic_sociedades

            print(f"    🏢 Encontradas {len(sociedades_data)} sociedades - Processando detalhes...")

            # Process detailed sociedades data ASYNC
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:  # Reduced workers for headless
                tasks = []
                for sociedade in sociedades_data:
                    task = process_sociedade_async(
                        sociedade,
                        state,
                        insc,
                        enhanced_record.get('corrected_full_name') or enhanced_record['full_name'],
                        executor
                    )
                    tasks.append(task)

                sociedades_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                complete_details = []
                for i, result in enumerate(sociedades_results):
                    if isinstance(result, Exception):
                        error_message = f"Async error processing sociedade {i}: {str(result)}"
                        print(f"    ❌ ERRO: {error_message}")
                        error_log.append(error_message)
                    elif result is not None:
                        complete_details.append(result)
                        print(f"      ✅ {result['basic_info']['NomeSoci']} ({result['basic_info']['SiglUf']})")

                enhanced_record['society_complete_details'] = complete_details

            print(f"    🎉 Processamento completo - {len(complete_details)} sociedades processadas")
            return enhanced_record, True

        except RequestException as e:
            if hasattr(e, 'response') and e.response and e.response.status_code in [401, 403, 419] or "token" in str(e).lower():
                print(f"    🔄 Sessão expirada: {str(e)}")
                return enhanced_record, False

            print(f"    ⚠️  Tentativa {attempt + 1} falhou (RequestException): {str(e)}")
            if attempt < max_retries - 1:
                print(f"    ⏳ Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                error_message = f"Max retries exceeded for {state} {insc}: {str(e)}"
                print(f"    ❌ {error_message}")
                error_log.append(error_message)
                return enhanced_record, True
        except Exception as e:
            print(f"    ⚠️  Tentativa {attempt + 1} falhou (Exception): {str(e)}")
            if attempt < max_retries - 1:
                print(f"    ⏳ Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                error_message = f"Unexpected error for {state} {insc}: {str(e)}"
                print(f"    ❌ {error_message}")
                error_log.append(error_message)
                return enhanced_record, True

    return enhanced_record, True

def save_enhanced_lawyers_to_file(enhanced_lawyers_list, batch_name, batch_num=None, emergency=False):
    """Save enhanced lawyer records to S3 and local backup"""
    if not enhanced_lawyers_list:
        print("  Nenhum advogado para salvar")
        return None

    # Extract batch name without extension for filename
    batch_base = os.path.splitext(os.path.basename(batch_name))[0]
    
    if emergency:
        filename = f"lawyers_enhanced_{batch_base}_EMERGENCY_{time.strftime('%Y%m%d_%H%M%S')}.json"
    elif batch_num is not None:
        filename = f"lawyers_enhanced_{batch_base}_part_{batch_num:03d}_{time.strftime('%Y%m%d_%H%M%S')}.json"
    else:
        filename = f"lawyers_enhanced_{batch_base}_FINAL_{time.strftime('%Y%m%d_%H%M%S')}.json"

    # Save to S3 and local backup
    s3_url = save_to_s3_and_local_backup(enhanced_lawyers_list, filename)
    
    if s3_url:
        print(f"  ✅ Salvos {len(enhanced_lawyers_list)} registros de advogados em {filename}")
        return s3_url
    else:
        print(f"  ⚠️ Problema ao salvar {len(enhanced_lawyers_list)} registros")
        return filename

def cleanup_memory():
    """Clean up memory"""
    gc.collect()

async def main():
    """Main async function to process batch of lawyers"""
    global enhanced_lawyers, current_batch_file, error_log, batch_counter
    
    # Check for command line argument
    if len(sys.argv) != 2:
        print("❌ Uso: python script.py <arquivo_batch.json>")
        print("   Exemplo: python script.py lawyers_batch_01.json")
        sys.exit(1)

    batch_file = sys.argv[1]
    current_batch_file = batch_file  # Set global for signal handler
    
    # Check if file exists
    if not os.path.exists(batch_file):
        print(f"❌ Arquivo não encontrado: {batch_file}")
        sys.exit(1)

    # Load batch data
    try:
        with open(batch_file, 'r', encoding='utf-8') as f:
            batch_data = json.load(f)
    except Exception as e:
        print(f"❌ Erro ao carregar arquivo: {e}")
        sys.exit(1)

    # Filter records that need processing
    records_to_process = []
    skipped_count = 0
    
    for record in batch_data:
        should_process, reason = should_process_record(record)
        if should_process:
            records_to_process.append((record, reason))
        else:
            skipped_count += 1

    total_records = len(batch_data)
    to_process_count = len(records_to_process)

    print("="*80)
    print("📊 ANÁLISE DE REGISTROS:")
    print(f"  - Total de registros: {total_records}")
    print(f"  - Para processar: {to_process_count}")
    print(f"  - Já completos (pulados): {skipped_count}")
    print("💾 Salvamento automático a cada 400 advogados")
    print("🖥️  Modo HEADLESS ativado")
    print("🔄 Sistema de retry: 4 tentativas com delay de 5s")
    print("☁️  Dados salvos no S3: s3://oab-jsons-sa2/oab_data/")
    print("🌍 Monitoramento de IP do proxy ativado")
    print(f"📊 Sessão persistente: {MAX_REQUESTS_PER_SESSION} requisições por sessão")
    print("="*80)

    if to_process_count == 0:
        print("✅ Todos os registros já foram processados!")
        return

    # Verify proxy connection
    print("🔍 Verificando conexão com proxy...")
    if not verify_proxy_connection():
        print("❌ Falha na conexão com proxy. Verifique suas credenciais.")
        sys.exit(1)
    print("✅ Proxy conectado e funcionando")

    # Get initial cookies and token
    print("🍪 Obtendo cookies e token iniciais...")
    try:
        cookies, token = get_initial_cookies()
        print("✅ Cookies e token obtidos")
    except Exception as e:
        print(f"❌ Falha ao obter cookies: {e}")
        sys.exit(1)

    # Process records
    enhanced_lawyers = []
    batch_counter = 0
    
    for i, (record, reason) in enumerate(records_to_process, 1):
        try:
            # Clean state field
            state = clean_state(record.get('state'))
            insc = record.get('insc')
            full_name = record.get('full_name', 'NOME_DESCONHECIDO')

            print(f"\n[{i}/{to_process_count}] 👨‍💼 {full_name} ({state} {insc})")
            print(f"    📋 Motivo: {reason}")

            # Search and enhance record
            enhanced_record, session_valid = await search_lawyer_with_updates(
                insc, state, cookies, token, record
            )

            if not session_valid:
                print("    🔄 Renovando sessão...")
                try:
                    cookies, token = get_initial_cookies()
                    print("    ✅ Nova sessão obtida")
                    
                    # Retry with new session
                    enhanced_record, session_valid = await search_lawyer_with_updates(
                        insc, state, cookies, token, record
                    )
                    
                    if not session_valid:
                        print("    ❌ Falha mesmo com nova sessão")
                        continue
                        
                except Exception as e:
                    print(f"    ❌ Falha ao renovar sessão: {e}")
                    continue

            enhanced_lawyers.append(enhanced_record)
            
            # Determine completion status
            if enhanced_record.get('has_society'):
                societies_count = len(enhanced_record.get('society_complete_details', []))
                print(f"    ✅ Concluído: {societies_count} sociedades processadas")
            else:
                print(f"    ✅ Concluído: sem sociedades")

            # Auto-save every 400 records
            if len(enhanced_lawyers) % 400 == 0:
                batch_counter += 1
                print(f"\n💾 SALVAMENTO AUTOMÁTICO #{batch_counter}")
                save_enhanced_lawyers_to_file(enhanced_lawyers, batch_file, batch_counter)
                print(f"📊 Progresso: {i}/{to_process_count} ({(i/to_process_count)*100:.1f}%)")
                
                # Memory cleanup
                cleanup_memory()

        except KeyboardInterrupt:
            print("\n🛑 Interrupção detectada pelo usuário")
            break
        except Exception as e:
            error_message = f"Erro inesperado processando {record.get('full_name', 'UNKNOWN')}: {str(e)}"
            print(f"    ❌ {error_message}")
            error_log.append(error_message)
            continue

    # Final save
    if enhanced_lawyers:
        print(f"\n💾 SALVAMENTO FINAL")
        final_filename = save_enhanced_lawyers_to_file(enhanced_lawyers, batch_file)
        print(f"✅ Processamento concluído!")
        print(f"📊 Total processado: {len(enhanced_lawyers)} advogados")
        print(f"📁 Arquivo final: {final_filename}")
    else:
        print("⚠️ Nenhum registro foi processado")

    # Save error log if any
    if error_log:
        batch_base = os.path.splitext(os.path.basename(batch_file))[0]
        error_file_name = f"error_log_{batch_base}_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        save_to_s3_and_local_backup(
            "\n".join([f"Log de Erros - {batch_file}", "="*50, ""] + error_log),
            error_file_name,
            'text/plain'
        )
        print(f"📝 Log de erros salvo: {error_file_name}")

    print("🎉 Processamento finalizado!")

if __name__ == "__main__":
    asyncio.run(main())

