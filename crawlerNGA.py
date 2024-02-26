import random
import time

import func_timeout
import rocksdict
import requests
from bs4 import BeautifulSoup
import re


def remove_bbs_code_and_content(text):
    prev_text = None
    if re.search(r"\[s:a.{3,20}\]", text):
        return ""
    if re.search(r"杂种|爹|/|笑死|典|急|xxn|拳|屁|畜|孝|帽子|政治|zzzq", text):
        return ""
    while prev_text != text:
        prev_text = text
        text = re.sub(r"\[.*?\].*?\[\/.*?\]", "", text)
    text = re.sub(r"\[.+\]", "\n", text)
    text = re.sub(r"\([0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+\):", "\n", text)
    text = re.sub("\n\n+", "\n", text)
    text = re.sub(r"^ +\n", "", text)
    text = re.sub(r" +\n$", "", text)
    text = re.sub(r"ROLL : d100=d100\([0-9]+\)=[0-9]+", "\n", text)
    # 返回清理后的文本
    return text


def calChinesePuncPercent(text):
    return len(re.findall(r"[，。、；：（）《》“”‘’？！……——\n]", text)) / len(text)


def db_options():
    opt = rocksdict.Options()
    opt.set_max_background_jobs(12)
    opt.set_write_buffer_size(32 * 1024 * 1024)
    opt.increase_parallelism(12)
    opt.set_target_file_size_base(512 * 1024 * 1024)
    opt.set_max_write_buffer_size_to_maintain(128 * 1024 * 1024)
    opt.set_enable_pipelined_write(True)
    opt.set_compaction_style(rocksdict.DBCompactionStyle.universal())
    opt.set_compression_type(rocksdict.DBCompressionType.none())
    return opt


def requestNGARecommend(fid, page_num):
    cookies = {
        "ngacn0comUserInfo": "%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7%09%25E5%2585%25AD%25E6%25B0%25AF%25E8%258B%25AF%25E7%258E%25AF%0939%0939%09%0910%090%094%090%090%0922_60",
        "ngacn0comUserInfoCheck": "cc441e50eef8c3dfa3d575345532f1cb",
        "ngacn0comInfoCheckTime": "1708700726",
        "ngaPassportUid": "64006228",
        "ngaPassportUrlencodedUname": "%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7",
        "ngaPassportCid": "X9em6m8l9r3ej5m7mmum0u5g0o0tuh6a4amqlv35",
        "bbsmisccookies": "%7B%22pv_count_for_insad%22%3A%7B0%3A-28%2C1%3A1708707658%7D%2C%22insad_views%22%3A%7B0%3A1%2C1%3A1708707658%7D%2C%22uisetting%22%3A%7B0%3A%22e%22%2C1%3A1708702306%7D%7D",
        "lastvisit": "1708702008",
    }
    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # 'Cookie': 'ngacn0comUserInfo=%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7%09%25E5%2585%25AD%25E6%25B0%25AF%25E8%258B%25AF%25E7%258E%25AF%0939%0939%09%0910%090%094%090%090%0922_60; ngacn0comUserInfoCheck=cc441e50eef8c3dfa3d575345532f1cb; ngacn0comInfoCheckTime=1708700726; ngaPassportUid=64006228; ngaPassportUrlencodedUname=%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7; ngaPassportCid=X9em6m8l9r3ej5m7mmum0u5g0o0tuh6a4amqlv35; bbsmisccookies=%7B%22pv_count_for_insad%22%3A%7B0%3A-28%2C1%3A1708707658%7D%2C%22insad_views%22%3A%7B0%3A1%2C1%3A1708707658%7D%2C%22uisetting%22%3A%7B0%3A%22e%22%2C1%3A1708702306%7D%7D; lastvisit=1708702008; lastpath=/thread.php?recommend=1&admin=1&fid=414&page=7',
        "Pragma": "no-cache",
        "Referer": "https://bbs.nga.cn/thread.php?recommend=1&admin=1&fid=414&page=3",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }

    params = {
        "fid": fid,
        "page": page_num,
    }
    retry_times = 10
    while retry_times > 0:
        try:
            response = requests.get(
                "https://bbs.nga.cn/thread.php",
                params=params,
                cookies=cookies,
                headers=headers,
                timeout=2,
            ).content.decode("gbk")
            break
        except Exception as e:
            print(e)
            retry_times -= 1
            if retry_times == 0:
                return True
            continue
    soup = BeautifulSoup(response, features="html.parser")
    nga_recommends = soup.find_all("td", attrs={"class": "c2"})
    if len(nga_recommends) < 10:
        return False
    for recommend_soup in nga_recommends:
        if "帖子发布或回复时间超过限制" in recommend_soup.text:
            return False
        NGARecommendUrls.add(recommend_soup.contents[1].attrs["href"].split("tid=")[1])
    NGARocksDict.put("url", NGARecommendUrls)
    print(len(NGARecommendUrls))
    time.sleep(random.random() * 1)
    return True


def requestNGAThread(tid):
    import requests

    cookies = {
        "ngacn0comUserInfo": "%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7%09%25E5%2585%25AD%25E6%25B0%25AF%25E8%258B%25AF%25E7%258E%25AF%0939%0939%09%0910%090%094%090%090%0922_60",
        "ngaPassportUid": "64006228",
        "ngaPassportUrlencodedUname": "%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7",
        "ngaPassportCid": "X9em6m8l9r3ej5m7mmum0u5g0o0tuh6a4amqlv35",
        "ngacn0comUserInfoCheck": "af5a3896278648d945ee23f618975e91",
        "ngacn0comInfoCheckTime": "1708704565",
        "bbsmisccookies": "%7B%22pv_count_for_insad%22%3A%7B0%3A-138%2C1%3A1708794046%7D%2C%22insad_views%22%3A%7B0%3A2%2C1%3A1708794046%7D%2C%22uisetting%22%3A%7B0%3A1%2C1%3A1709308958%7D%7D",
    }

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # 'Cookie': 'ngacn0comUserInfo=%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7%09%25E5%2585%25AD%25E6%25B0%25AF%25E8%258B%25AF%25E7%258E%25AF%0939%0939%09%0910%090%094%090%090%0922_60; ngaPassportUid=64006228; ngaPassportUrlencodedUname=%25C1%25F9%25C2%25C8%25B1%25BD%25BB%25B7; ngaPassportCid=X9em6m8l9r3ej5m7mmum0u5g0o0tuh6a4amqlv35; ngacn0comUserInfoCheck=af5a3896278648d945ee23f618975e91; ngacn0comInfoCheckTime=1708704565; lastvisit=1708706205; lastpath=/read.php?tid=39341339&_fp=3; bbsmisccookies=%7B%22pv_count_for_insad%22%3A%7B0%3A-138%2C1%3A1708794046%7D%2C%22insad_views%22%3A%7B0%3A2%2C1%3A1708794046%7D%2C%22uisetting%22%3A%7B0%3A1%2C1%3A1709308958%7D%7D',
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-GPC": "1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
    }
    page_num = 1
    decode_error_times = 0

    is_end_page = False
    thread_contents = []
    while not is_end_page:
        params = {"tid": tid, "page": f"{page_num}"}
        try:
            retry_times = 10
            while retry_times > 0:
                try:
                    response = func_timeout.func_timeout(
                        2,
                        requests.get,
                        ("https://bbs.nga.cn/read.php",),
                        {"params": params, "cookies": cookies, "headers": headers},
                    )
                    break
                except func_timeout.FunctionTimedOut:
                    retry_times -= 1
                    if retry_times == 0:
                        response = None
                        break
                    continue
                except Exception as e:
                    print(e)
                    retry_times -= 1
                    if retry_times == 0:
                        response = None
                        break
                    continue
            if response is None:
                continue
            page_num += 1
            response = response.content.decode("gbk")
            response = re.sub(r"<br/*>", "\n", response)
        except UnicodeDecodeError:
            decode_error_times += 1
            if decode_error_times > 3:
                is_end_page = True
            print("Decode Error")
            continue
        page_num += 1
        soup = BeautifulSoup(response, features="html.parser")
        if len(re.findall(r"发表回复.*下一页\([0-9]+\)", soup.text)) < 1:
            is_end_page = True
        try:
            new_text = remove_bbs_code_and_content(
                soup.find("p", attrs={"class": "postcontent ubbcode"}).text
            )
            if len(new_text) > 10 and calChinesePuncPercent(new_text) > 0.03:
                thread_contents.append(new_text)
        except AttributeError:
            pass
        try:
            for comment in soup.find_all(
                "span", attrs={"class": "postcontent ubbcode"}
            ):
                new_text = remove_bbs_code_and_content(comment.text)
                if len(new_text) > 10 and calChinesePuncPercent(new_text) > 0.03:
                    thread_contents.append(new_text)
        except AttributeError:
            pass
        except Exception as e:
            print(e)
    print(f"{tid, len((thread_contents))}")
    NGARocksDict.put(tid, thread_contents)


NGARocksDict = rocksdict.Rdict("./dataNGA", db_options())
NGARecommendUrls = NGARocksDict.get("url", default=set())
NGADoneUrls = NGARocksDict.get("done", default=set())
print(".")
# fidList = ["489","600","831","708","591","840","563","510397"]
# for targetFid in fidList:
#     continueToCrawl =True
#     pageNum = 1
#     while continueToCrawl:
#         continueToCrawl = requestNGARecommend(targetFid,pageNum)
#         pageNum += 1
#         print(f"{targetFid}  {pageNum} done")
NGATodoUrls = NGARecommendUrls - NGADoneUrls
NGATodoUrls = list(NGATodoUrls)
random.shuffle(NGATodoUrls)
print(len(NGATodoUrls))
for NGATid in NGATodoUrls:
    requestNGAThread(NGATid)
    print(f"{NGATid} Done")
    NGADoneUrls.add(NGATid)
    NGARocksDict.put("done", NGADoneUrls)
