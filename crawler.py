import codecs
import os
import random
import sqlite3
import time
import requests
import re
from func_timeout import func_timeout, FunctionTimedOut

class liaoBotsEvaluate:
    def __init__(self, content):
        self.content = content
        self.cookies = {
            'gkp2': 'rCzWzqia9syNz0xbdCVH',
        }
        self.headers = {
            'authority': 'liaobots.work',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            # 'cookie': 'gkp2=rCzWzqia9syNz0xbdCVH',
            'origin': 'https://liaobots.work',
            'pragma': 'no-cache',
            'referer': 'https://liaobots.work/',
            'sec-ch-ua': '"Not A(Brand";v="99", "Brave";v="121", "Chromium";v="121"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'x-auth-code': '2kqN3aqxIQTkP',
        }

        self.json_data = {
            'conversationId': '15cbf4f5-d2bc-4766-b2ba-8a336d45adf4',
            'model': {
                'id': 'gpt-3.5-turbo',
                'name': 'GPT-3.5-Turbo',
                'maxLength': 48000,
                'tokenLimit': 14000,
                'context': '16K',
            },
            'messages': [
                {
                    'role': 'user',
                    'content': f'只返回0-100的评分数字，不需要任何解释、分析，只需要返回结果数字。\n\n请评估以下内容：\n\n{content}'
                },
            ],
            'key': '',
            'prompt': '你是一个严格的中文质量评估专家，请以中国标准出版文章(如报纸、书籍、论文)的水平为标准80分水平，负责评估以下中文内容的连贯性、完整性和语法。'
                      '要求：'
                      '1. 基础分为50分'
                      '2. 语句通畅加10分，语句完整加10分，语法规范加10分，断句规范加5分，主谓宾完整加5分，语意表达清晰加10分；'
                      '3. 语素不完整扣10分，语句不连贯扣10分，缺少主谓宾扣10分，错字扣10分，乱序扣10分，不够正式的扣5分；'
                      '4. 存在严重语病、缺少主要成分，语意不明的则严重扣20分'
        }

    def getResult(self):
        retries = 10
        Wait = True
        while retries > 0:
            try:
                response = func_timeout(15, requests.post, ('https://liaobots.work/api/chat',),
                                        {'cookies': self.cookies, 'headers': self.headers, 'json': self.json_data})
                # response = response.json()['choices'][0]['message']['content']
                print(response.text)
                if response.text == 'Error':
                    if Wait:
                        time.sleep(300)
                        Wait = False
                    else:
                        retries -= 5
                return float(response.text)
            except FunctionTimedOut:
                print("Timeout")
                retries -= 2
                time.sleep(random.random())
            except Exception as e:
                print(e)
                retries -= 1
                time.sleep(random.random())
        return 0


class txtBookParser:
    def __init__(self, filelocation):
        self.__location__ = filelocation
        self.book = None
        self.bookParas = []
        self.paraNum = 0
        self.score = 0
        self.exist = False

    def __readBook__(self):
        try:
            with codecs.open(self.__location__, encoding='gb18030', errors='replace') as file:
                self.book = file.read()
        except FileNotFoundError:
            print(self.__location__)
            with open(self.__location__, 'r', encoding='utf-8') as file:
                self.book = file.read()
        except Exception as e:
            print(self.__location__)
            print(f"An error occurred: {str(e)}")

    def __cleanBook__(self):
        self.book = re.sub(r"\r\n", "\n", self.book)
        self.book = re.sub("===+", "\n\n", self.book)

    def read(self):
        self.__readBook__()
        self.__cleanBook__()
        for i in re.split(r"\n\n+", self.book):
            if len(i) > 800 and len(i) < 6000:
                self.bookParas.append(re.sub("\u3000", "  ", i))
        self.paraNum = len(self.bookParas)
        if self.paraNum > 100:
            self.save()

    def evaluate(self):
        scores = []
        for i in range(10):
            scores.append(liaoBotsEvaluate(self.bookParas[random.randint(0, self.paraNum - 1)]).getResult())
        self.score = sum(scores) / len(scores)

    def save(self):
        conn = sqlite3.connect('database.db')
        cursor = conn.cursor()
        cursor.execute('''SELECT title FROM scores WHERE title = ?''', (self.__location__,))
        result = cursor.fetchone()
        if result is None:
            self.evaluate()
            print(self.score)
            cursor.execute('''INSERT INTO scores (title, score) VALUES (?, ?)''', (self.__location__, self.score))
            conn.commit()


def readBook(location):
    book = txtBookParser(location)
    time.sleep(random.random())
    book.read()


if __name__ == "__main__":
    locationDir = "/run/media/hp/main/txt/"
    locations = [locationDir + i for i in os.listdir(locationDir)]
    random.shuffle(locations)
    for i in locations:
        readBook(i)

