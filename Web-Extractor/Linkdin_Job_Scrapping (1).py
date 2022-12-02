from selenium import webdriver
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import pandas as pd
import os,csv,time,logging
from dotenv import load_dotenv
load_dotenv()
 
# USER_EMAIL 
# USER_PASSWORD 

class LinkdinHeaders:
    LD_DESIGNATION = 'Designation'
    LD_COMPANY_NAME = 'Company Name'
    LD_LOCATION = 'Location'
    LD_POST_DATE = 'Post Date'
    LD_POST_TYPE = 'Job Type'
    LD_EMPLOYEES = 'Employees'
    LD_JOB_DESCRIPTION = 'Job Description'
    LD_URL = 'URL'
    LD_POST_BY = 'Post By'
    LD_POST_DESIGNATION = 'Post Designation'
    LD_INDUSTRY_TYPE = 'Industry'

class ScrapLinkdinJobs:
    LINKDIN_LOGIN_URL = "https://linkedin.com/uas/login"
    FILE_NAME = "linkdin_jobs.csv"
    HEADERS_LIST = [
        LinkdinHeaders.LD_DESIGNATION,
        LinkdinHeaders.LD_COMPANY_NAME,
        LinkdinHeaders.LD_LOCATION,
        LinkdinHeaders.LD_POST_DATE,
        LinkdinHeaders.LD_POST_TYPE,
        LinkdinHeaders.LD_EMPLOYEES,
        LinkdinHeaders.LD_JOB_DESCRIPTION,
        LinkdinHeaders.LD_URL,
        LinkdinHeaders.LD_POST_BY,
        LinkdinHeaders.LD_POST_DESIGNATION,
        LinkdinHeaders.LD_INDUSTRY_TYPE
    ]
    LINKDIN_JOB_SEARCH_URL = 'https://www.linkedin.com/jobs/search/'
    LINKDIN_INDUSTRY_TYPE = 'f_I=6%2C43%2C112%2C25%2C47%2C80%2C14%2C27%2C24%2C116%2C68%2C91%2C48%2C19%2C1%2C82%2C75%2C143%2C98&f_PP=105214831%2C105556991%2C103671728%2C106888327%2C106164952%2C104793846%2C104869687%2C115702354%2C105282602%2C104990346&'
    LINKDIN_GEO_ID = '102713980'

    def __init__(self,area_of_search=None):
        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--incognito')
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(ChromeDriverManager().install())    

    def linkdin_login(self):  
        self.driver.get(self.LINKDIN_LOGIN_URL)
        time.sleep(5)

        soup= BeautifulSoup(self.driver.page_source, 'lxml')
        username= self.driver.find_element(By.ID, "username")
        username.send_keys(os.environ.get('USER_EMAIL'))
        pword = self.driver.find_element(By.ID, "password")
        pword.send_keys(os.environ.get('USER_PASSWORD'))		
        self.driver.find_element(By.XPATH, "//button[@type='submit']").click()
        time.sleep(3)

        self.scrap_linkdin_jobs()

        time.sleep(5)
        self.driver.close()

    def scrap_linkdin_jobs(self):           

        with open(self.FILE_NAME, 'a',encoding="utf-8") as csv_file:
            dict_object = csv.DictWriter(csv_file, fieldnames=self.HEADERS_LIST)
            dict_object.writeheader()

            for start in range(1,501):
                context = {}               

                time.sleep(3)
                URL = f"{self.LINKDIN_JOB_SEARCH_URL}?{self.LINKDIN_INDUSTRY_TYPE}keywords={area_of_search}+&geoId={self.LINKDIN_GEO_ID}&refresh=true&sortBy=R&start={str(start)}"    

                self.driver.get(URL)

                time.sleep(10)
                
                try:
                    industry_type = self.driver.find_element(By.XPATH,'/html/body/div[5]/div[3]/div[4]/div/div/main/div/section[2]/div/div[2]/div[1]/div/div[4]/section/section/div[1]/div[2]').text.split()[0]
                except Exception as e:
                    industry_type = "NA"               
                

                try:
                    designation = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/a/h2').text         
                except Exception as e:
                    designation = "NA"

                try:
                    company=self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/div/span/span').text
                except Exception as e:
                    company = "NA"

                try:
                    location=self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/div/span/span[2]').text
                except Exception as e:
                    location = "NA"              
                
                try:
                    post_date = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/div/span[2]/span').text
                except Exception as e:
                    post_date = "NA"

                time.sleep(3)

                try:
                    job_type = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/div[2]/ul/li[1]/span').text
                except Exception as e:
                    job_type = "NA"

                try:
                    employees = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div/div[2]/ul/li[2]/span').text
                except Exception as e:
                    employees = "NA"    
                

                time.sleep(3)

                try:
                    description = self.driver.find_element(By.XPATH,'//*[@id="job-details"]/span').text  
                except Exception as e:
                    description = "NA"                       
                             

                time.sleep(2)
                
                try:
                    url =self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[1]/div/div[1]/div[1]/div/span/span/a').get_attribute('href')
                except Exception as e:
                    url  = "NA"

                                
                try:
                    post_by = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[2]/div/div[2]/div[2]/a').text                    
                except Exception as e:
                    post_by = "NA"                    

                try:
                    post_designation = self.driver.find_element(By.XPATH,'//*[@id="main"]/div/section[2]/div/div[2]/div[1]/div/div[2]/div/div[2]/div[2]/div[2]/div').text                   
                except Exception as e:
                    post_designation = "NA"


                context.update({
                    LinkdinHeaders.LD_DESIGNATION:designation,
                    LinkdinHeaders.LD_COMPANY_NAME:company,
                    LinkdinHeaders.LD_LOCATION:location,
                    LinkdinHeaders.LD_POST_DATE:post_date,
                    LinkdinHeaders.LD_POST_TYPE:job_type,
                    LinkdinHeaders.LD_EMPLOYEES:employees,
                    LinkdinHeaders.LD_JOB_DESCRIPTION:description,
                    LinkdinHeaders.LD_URL:url,
                    LinkdinHeaders.LD_POST_BY:post_by,
                    LinkdinHeaders.LD_POST_DESIGNATION:post_designation,
                    LinkdinHeaders.LD_INDUSTRY_TYPE:industry_type
                })
                dict_object.writerow(context)
                time.sleep(10)          
        

logging.warning("{0} Program start time...".format(time.time()))
area_of_search = "php"
ScrapLinkdinJobs(area_of_search).linkdin_login()
logging.warning("{0} Execution completed...".format(time.time()))
