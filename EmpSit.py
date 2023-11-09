from openai import OpenAI
from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime, timedelta
from pytz import timezone
from pause import until

class EmpSitScrape():

    def __init__(self):
        self.s = requests.Session()
        self.s.headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Host": "www.bls.gov",
            "sec-ch-ua": '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "Windows",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}

    def pull_empsit_schedule(self):
        release_dates_url = "https://www.bls.gov/schedule/news_release/empsit.htm"
        release_date_response = self.s.get(release_dates_url)
        soup = BeautifulSoup(release_date_response.text)
        table_soup = soup.find("table", {"class": "release-list"})
        table_rows_soup = table_soup.findAll("tr")[1:]
        release_dates = [datetime.strftime(datetime.strptime((date.findAll("td")[1].text).replace(".", "").replace(",", "").replace(" ",""), "%b%d%Y"), "%m%d%Y") for date in table_rows_soup]
        return release_dates

    def release_date_check(self):
        today = datetime.strftime(datetime.today(), "%m%d%Y")
        release_dates = self.pull_empsit_schedule()
        if today in release_dates:
            return {"date_check": True,
                    "date": today}
        else:
            return {"date_check": False,
                    "date": today}
      
    def current_eastern_time(self):
        loc_dt = timezone('US/Eastern').localize(datetime.now() - timedelta(hours=5))
        current_time = str(loc_dt.strftime('%H:%M'))
        return current_time  
      
    def release_time_check(self, pull_times:list):
        current_time = self.current_eastern_time()
        if current_time in pull_times:
            return {"time_check": True,
                    "time": current_time}
        else:
            return {"time_check": False,
                    "time": current_time}
        
    def pull_empsit_text(self):
        url = "https://www.bls.gov/news.release/empsit.nr0.htm"
        response = self.s.get(url)
        soup = BeautifulSoup(response.text)
        empsit_last_update_date = datetime.strftime(datetime.strptime(soup.find("span", {"class": "update"}).text.replace("Last Modified Date: ", "").replace("\n", ""), "%B %d, %Y"), "%m%d%Y")
        for item in soup.find_all("div", {"class": "normalnews"}):
            for sub_item in item.find_all("pre"):
                if not "Household Survey Data" in sub_item.text:continue
                text = sub_item.text
        return {"text":text,
                "text_soup": soup,
                "empsit_last_update_date":empsit_last_update_date}
    
    def response_api(self, prompt, key):
        client = OpenAI(api_key=openai_key)
        response = client.chat.completions.create( 
        #model="gpt-3.5-turbo",
        # model="gpt-4",
        model="gpt-3.5-turbo-16k",
        messages=[
            {"role": "user",
            "content": f"{prompt}"},
            {
            "role": "assistant",
            "content": "{\n  \"unemployment_rate\": 9.6,\n  \"labor_force_participation_rate\": 64.5,\n  \"number_of_people_not_in_labor_force_who_want_a_job\": 2.6,\n  \"average_hourly_earnings_of_all_employees\": 22.73,\n  \"average_hourly_earnings_of_private_sector_employees\": 19.17,\n}"
            }
            ],
        temperature=0.0,
        max_tokens=250,
        top_p=1
        )
        return response
    
    def pull_new_data(self, openai_key:str, pull_times=["08:29", "8:30", "8:31"]):
        correct_date = self.release_date_check()
        correct_time = self.release_time_check(pull_times)
        # Checks to see if the date and times are correct before attempting any extraction.

        # if correct_date['date_check'] == True and correct_time['time_check'] == True:
        if correct_date['date_check'] == False and correct_time['time_check'] == False:

            print(datetime.today(), "The time is right for some BLS data!")
            today = correct_date['date']
            print("Extracting EmpSit text extracted from BLS...")
            text_dict = self.pull_empsit_text()
            print("EmpSit text extracted from BLS.")
            empsit_last_update_date = text_dict['empsit_last_update_date']
            # Checks to see if the posted last update date is from today. If it is then it means that there is new data, if not then the report is old.

            # if empsit_last_update_date == today:
            if empsit_last_update_date != today:

                text = text_dict['text']
                prompt = f"Extract the following data in JSON format: unemployment_rate, labor_force_participation_rate, number_of_people_not_in_labor_force_who_want_a_job, average_hourly_earnings_of_all_employees, average_hourly_earnings_of_private_sector_employees from {text[:15000]}"
                print("Submitted to ChatGPT...")
                data = self.response_api(prompt, openai_key)
                json_string = data.choices[0].message.content
                json_data = json.loads(json_string.replace("\n", ""))
                print("Data returned from ChatGPT!")
                json_data['date'] =  datetime.strptime(today, "%m%d%Y").date()
                return {"extracted_data": json_data,
                    "text_dict": text_dict}
            else:
                print("Did not submit to ChatGPT. EmpSit last update date is", empsit_last_update_date)
        else:
            print(datetime.today(), "The time is not right for some BLS data.")

    def add_to_delta_table(self, data:list, table:str):
        new_line_sdf = spark.createDataFrame([data['extracted_data']])
        sdf = spark.sql(f"select * from {table}")
        new_sdf = sdf.union(new_line_sdf)
        new_sdf = new_sdf.dropDuplicates()
        new_sdf.write.format("delta").mode("overwrite").saveAsTable(table)
        print(f"New record saved to {table}")

    def perma_run(self, openai_key, pull_times):
        while True:
            data = ""
            data = self.pull_new_data(openai_key, pull_times)
            time.sleep(3)
            #If a dictionary is returned then it confirms that new BLS data was pulled and we can put the script into hybernation.
            if type(data) == dict:
                today = datetime.today()
                next_month = (today.replace(day=1) + timedelta(days=32)).replace(day=1)
                next_month_midnight = datetime(next_month.year, next_month.month, next_month.day, 0, 0, 0)
                print(f"Sleeping until {next_month_midnight}")
                until(next_month_midnight)