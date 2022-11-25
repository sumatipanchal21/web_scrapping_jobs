
from selenium import webdriver
from selenium.webdriver.common.by import B
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import pdb

def extract_dice_jobs():

    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)


    driver.maximize_window()
    time.sleep(3)
    pdb.set_trace()

    job_titles_list, company_name_list, location_list, job_types_list = [], [], [], []

    job_posted_dates_list, job_descriptions_list = [], []
    for k in range(1, 4):
        URL = f"https://www.dice.com/jobs?q=python&page={k}"

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
        resp = df.to_csv('dice_jobs.csv', index=False)
        driver.close()
    return resp

extract_dice_jobs()

    
