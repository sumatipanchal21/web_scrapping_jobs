
import random
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class ExtractIndeed:

    BASE_URL = 'https://in.indeed.com'
    FILE_NAME = 'indeed_jobs_python.csv'
    description_list, company_name_list, designation_list, salary_list, company_url = [], [], [], [], []
    location_list, qualification_list = [], []

    def __init__(self, language):
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(ChromeDriverManager().install())
        self.language = language.lower()
        self.job_detail_links = []
        

    def get_job_detail_links(self, page=2):
        for page in range(0, page):
            query_param = f'{self.language}-jobs'
            time.sleep(5)
            URL = f"https://in.indeed.com/jobs?q={self.language}&start={page*10}"
            self.driver.get(URL)
            soup = BeautifulSoup(self.driver.page_source, 'lxml')

            for outer_artical in soup.findAll(attrs={'class': "css-1m4cuuf e37uo190"}):
                for inner_links in outer_artical.findAll(attrs={'class': "jobTitle jobTitle-newJob css-bdjp2m eu4oa1w0"}):
                    self.job_detail_links.append(
                        f"{self.BASE_URL}{inner_links.a.get('href')}")


    def scrap_details(self):
        print("___________", "Indeed")
        self.get_job_detail_links()
        time.sleep(2)

        for link in range(len(self.job_detail_links)):

            time.sleep(5)
            self.driver.get(self.job_detail_links[link])
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            a = soup.findAll(
                attrs={'class': "jobsearch-InlineCompanyRating-companyHeader"})
            self.company_name_list.append(a[1].text)
            try:
                self.company_url.append(a[1].a.get('href'))
            except:
                self.company_url.append('NA')

            salary = soup.findAll(
                attrs={'class': "jobsearch-JobMetadataHeader-item"})
            if salary:
                for i in salary:
                    x = i.find('span')
                    if x:
                        self.salary_list.append(x.text)
                    else:
                        self.salary_list.append('NA')
            else:
                self.salary_list.append('NA')

            description = soup.findAll(
                attrs={'class': "jobsearch-jobDescriptionText"})
         
            if description:
                for i in description:
                    self.description_list.append(i.text)
            else:
                self.description_list.append('NA')
          

            designation = soup.findAll(
                attrs={'class': 'jobsearch-JobInfoHeader-title-container'})
            if designation:
                self.designation_list.append(designation[0].text)
            else:
                self.designation_list.append('NA')

            # location

            for Tag in soup.find_all('div', class_="icl-Ratings-count"):
                Tag.decompose()
            for Tag in soup.find_all('div', class_="jobsearch-CompanyReview--heading"):
                Tag.decompose()
            location = soup.findAll(
                attrs={'class': "jobsearch-CompanyInfoWithoutHeaderImage"})
            if location:
                for i in location:
                    self.location_list.append(i.text)
            else:
                self.location_list.append('NA')

            # Qualification
            qualification = soup.findAll(
                attrs={"class": 'jobsearch-ReqAndQualSection-item--wrapper'})
            if qualification:
                for i in qualification:
                    self.qualification_list.append(i.text)
            else:
                self.qualification_list.append('NA')

        job_data = {
            'company_name': self.company_name_list,
            'location': self.location_list,
            'company_url': self.company_url,
            'description_list': self.description_list,
            'designation_list': self.designation_list,
            'qualification_list': self.qualification_list,
            'salary_list': self.salary_list
        }
        return job_data

    def generate_csv(self):
        
        df = pd.DataFrame()
        df['Company Name'] = self.company_name_list
        df['Company_url'] = self.company_url
        df['salary'] = self.salary_list
        # df['description_list'] = description_list
        df['designation_list'] = self.designation_list
        df['location_list'] = self.location_list
        df['qualification_list'] = self.qualification_list
        df.to_csv(f'./static/{self.FILE_NAME}', index=False)
        # return pd.read_csv(self.FILE_NAME)

        


# scrap_naukri = ExtractIndeed('python')
# scrap_naukri.scrap_details()
# print(scrap_naukri.generate_csv())
