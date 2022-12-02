import requests
from celery import Celery
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from lxml.html import fromstring
from crawl import save_dice_data_to_db, save_indeed_data_to_db

celery = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    print(proxies)
    return proxies

import random
test_list = list(get_proxies())
proxy = random.choice(test_list)
print(str(proxy))

@celery.task
def extract_dice_jobs(tech="python", page=1):
    FILE_NAME = 'dice.csv'
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.headless = True
    options.add_argument(f'user-agent={user_agent}')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument("--disable-extensions")
    options.add_argument("--proxy-server='direct://'")
    options.add_argument("--proxy-bypass-list=*")
    options.add_argument("--start-maximized")
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument("--proxy-server=%s" % proxy)
    driver = webdriver.Chrome(executable_path="C:\Program Files\Google\chromedriver\chromedriver.exe", options=options)

    driver.maximize_window()
    time.sleep(3)

    job_titles_list, company_name_list, location_list, job_types_list = [], [], [], []

    job_posted_dates_list, job_descriptions_list = [], []
    for k in range(1, int(page)):
        URL = f"https://www.dice.com/jobs?q={tech}&countryCode=US&radius=30&radiusUnit=mi&page={k}&pageSize=20&language=en&eid=S2Q_,bw_1"

        driver.get(URL)

        driver.maximize_window()
        try:

            input = driver.find_element(By.ID, "typeaheadInput")
            input.click()
        except:
            time.sleep(5)

        job_titles = driver.find_elements(By.CLASS_NAME, "card-title-link")
        company_name = driver.find_elements(
            By.XPATH, '//div[@class="card-company"]/a')
        job_locations = driver.find_elements(
            By.CLASS_NAME, "search-result-location")
        job_types = driver.find_elements(
            By.XPATH, '//span[@data-cy="search-result-employment-type"]')
        job_posted_dates = driver.find_elements(By.CLASS_NAME, "posted-date")
        job_descriptions = driver.find_elements(By.CLASS_NAME, "card-description")

        # company_name
        for i in company_name:
            company_name_list.append(i.text)

        # job titles list
        for i in job_titles:
            job_titles_list.append(i.text)

        # #locations
        for i in job_locations:
            location_list.append(i.text)

        # job types
        for i in job_types:
            job_types_list.append(i.text)

        # job posted dates
        for i in job_posted_dates:
            job_posted_dates_list.append(i.text)

        # job_descriptions
        for i in job_descriptions:
            job_descriptions_list.append(i.text)

        print(len(job_titles_list), len(job_descriptions_list),
              len(job_posted_dates_list), len(job_types_list),
              len(company_name_list), len(location_list))
        df = pd.DataFrame()
        df['Job Title'] = job_titles_list
        df['Company Name'] = company_name_list
        df['description'] = job_descriptions_list
        df['Posted Date'] = job_posted_dates_list
        df['Job Type'] = job_types_list
        df['Location'] = location_list
        df.to_csv(f'./static/{FILE_NAME}', index=False)
        save_dice_data_to_db()

BASE_URL = 'https://in.indeed.com'
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36"
options = webdriver.ChromeOptions()
options.headless = True
options.add_argument(f'user-agent={user_agent}')
options.add_argument("--window-size=1920,1080")
options.add_argument('--ignore-certificate-errors')
options.add_argument('--allow-running-insecure-content')
options.add_argument("--disable-extensions")
options.add_argument("--proxy-server='direct://'")
options.add_argument("--proxy-bypass-list=*")
options.add_argument("--start-maximized")
options.add_argument('--disable-gpu')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument("--proxy-server=%s" % proxy)
description_list, company_name_list, designation_list, salary_list, company_url = [], [], [], [], []
location_list, qualification_list = [], []
driver = webdriver.Chrome(executable_path="C:\Program Files\Google\chromedriver\chromedriver.exe", options=options)
language = "python"
query_param = f'{language}-jobs'
job_detail_links = []


@celery.task
def get_job_detail_links(page=1):
    for page in range(0, page):
        time.sleep(5)
        URL = f"https://in.indeed.com/jobs?q={language}&start={page * 10}"
        try:
            driver.get(URL)
        except WebDriverException:
            print("page down")

        soup = BeautifulSoup(driver.page_source, 'lxml')

        for outer_artical in soup.findAll(attrs={'class': "css-1m4cuuf e37uo190"}):
            for inner_links in outer_artical.findAll(
                    attrs={'class': "jobTitle jobTitle-newJob css-bdjp2m eu4oa1w0"}):
                job_detail_links.append(
                    f"{BASE_URL}{inner_links.a.get('href')}")


@celery.task
def scrap_details(page=1):
    print("___________", "Indeed")
    get_job_detail_links(page)
    time.sleep(2)

    for link in range(len(job_detail_links)):

        time.sleep(5)
        driver.get(job_detail_links[link])
        soup = BeautifulSoup(driver.page_source, 'lxml')
        a = soup.findAll(
            attrs={'class': "jobsearch-InlineCompanyRating-companyHeader"})
        company_name_list.append(a[1].text)
        try:
            company_url.append(a[1].a.get('href'))
        except:
            company_url.append('NA')

        salary = soup.findAll(
            attrs={'class': "jobsearch-JobMetadataHeader-item"})
        if salary:
            for i in salary:
                x = i.find('span')
                if x:
                    salary_list.append(x.text)
                else:
                    salary_list.append('NA')
        else:
            salary_list.append('NA')

        description = soup.findAll(
            attrs={'class': "jobsearch-jobDescriptionText"})

        if description:
            for i in description:
                description_list.append(i.text)
        else:
            description_list.append('NA')

        designation = soup.findAll(
            attrs={'class': 'jobsearch-JobInfoHeader-title-container'})
        if designation:
            designation_list.append(designation[0].text)
        else:
            designation_list.append('NA')


        for Tag in soup.find_all('div', class_="icl-Ratings-count"):
            Tag.decompose()
        for Tag in soup.find_all('div', class_="jobsearch-CompanyReview--heading"):
            Tag.decompose()
        location = soup.findAll(
            attrs={'class': "jobsearch-CompanyInfoWithoutHeaderImage"})
        if location:
            for i in location:
                location_list.append(i.text)
        else:
            location_list.append('NA')

            # Qualification
        qualification = soup.findAll(
            attrs={"class": 'jobsearch-ReqAndQualSection-item--wrapper'})
        if qualification:
            for i in qualification:
                qualification_list.append(i.text)
        else:
            qualification_list.append('NA')

    FILE_NAME = 'indeed.csv'
    df = pd.DataFrame()
    df['Company Name'] = company_name_list
    df['Company_url'] = company_url
    df['salary'] = salary_list
    # df['description_list'] = description_list
    df['designation_list'] = designation_list
    df['location_list'] = location_list
    df['qualification_list'] = qualification_list
    df.to_csv(f'./static/{FILE_NAME}', index=False)
    save_indeed_data_to_db()