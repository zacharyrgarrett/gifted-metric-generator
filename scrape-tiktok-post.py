import time
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

tiktok_links = [
    "https://www.tiktok.com/@fa81724pxn/video/7195843281929030917",
    "https://www.tiktok.com/@fa81724pxn/video/7203226358380629254",
    "https://www.tiktok.com/@shaquillescott352/video/7174623552703008046",
    "https://www.tiktok.com/@jeremylynch/video/7201163678215916805",
    "https://www.tiktok.com/@diegocastro231/video/7203189625924603182",
    "https://www.tiktok.com/@mamabenjyfishy/video/7197438086375083269",
]

chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('user-data-dir=C:\\Users\\Zachary\\AppData\\Local\\Google\\Chrome\\User Data')

caps = DesiredCapabilities.CHROME
caps["pageLoadStrategy"] = "none"
caps["applicationCacheEnabled"] = False
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
}

def get_likes_element(driver):
    elem = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[2]/div[2]/div[1]/div[2]/div[1]/div[1]/div[1]/div[4]/button[1]/strong')))
    return elem.get_attribute('innerText')

driver = webdriver.Chrome("resources/chromedriver_win.exe", desired_capabilities=caps, options=chrome_options)
for link in tiktok_links:
    driver.get(link)
    attempts = 0
    likes = 0
    while attempts <= 5:
        try:
            likes = get_likes_element(driver)
            break
        except StaleElementReferenceException:
            attempts += 1
    print(f"{link} - {likes}")
    