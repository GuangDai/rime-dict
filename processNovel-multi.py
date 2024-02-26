import codecs
import gc
import multiprocessing
import re
import sqlite3
import sys
import time
from collections import defaultdict
from multiprocessing import Pipe, Process
import rocksdict
import diskcache
import numpy as np
import hanlp
import setproctitle
from pypinyin import lazy_pinyin
import torch
import random
import array


POSSIBILITY = 0.4
POSSIBILITY_TEN = POSSIBILITY * 10
PATTERN_1 = re.compile(r"^[\u4E00-\u9FFF\u3400-\u4DBF]+$")
PATTERN_2 = re.compile(r"[\u4E00-\u9FFF\u3400-\u4DBF]+")
patterns_3 = [
    "/run/media/hp/main/txt/《",
    ".txt",
    "》（校对版全本）",
    "》（实体版全本）",
    "作者：",
]
PATTERN_3 = re.compile("|".join(re.escape(p) for p in patterns_3))


class txtBookParser:
    def __init__(self, filelocation):
        self.__location__ = filelocation
        self.book = None
        self.bookParas = []
        self.paraNum = 0
        self.score = 0
        self.exist = False
        self.bookLens = 0

    def __readBook__(self):
        try:
            with codecs.open(
                self.__location__, encoding="gb18030", errors="replace"
            ) as file:
                self.book = file.read()
        except FileNotFoundError:
            print(self.__location__)
            with open(self.__location__, "r", encoding="utf-8") as file:
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
                processed_text = re.sub("\u3000", "  ", i)
                self.bookLens += len(processed_text)
                self.bookParas.append(processed_text)
        self.paraNum = len(self.bookParas)


def read_done_paths(file_path):
    with open(file_path, "r") as file:
        temp_paths = [line.strip() for line in file]
    done_paths = [i for i in temp_paths if len(temp_paths) > 3]
    return done_paths


def merge_dicts_with_count(dict_list):
    count_dict = defaultdict(int)
    for d in dict_list:
        # 将字典转换为可哈希的元组形式
        hashable = tuple(sorted(d.items()))
        count_dict[hashable] += 1

    # 将结果转换回字典形式，并附加出现次数
    result = [{"item": dict(key), "count": value} for key, value in count_dict.items()]
    return result


def read_scores_from_database(database_path, done_paths):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute("SELECT title, score FROM scores ORDER BY score DESC")
    rows = cursor.fetchall()
    conn.close()
    return [(row[0], row[1]) for row in rows if row[0] not in done_paths]


def build_mapping_with_pos(pinyins, words, pos_tags, rating, book_lens, book_path):
    mapping = []
    pinyin_index = 0

    for word, pos in zip(words, pos_tags):
        word_length = len(word)
        # 词对应的注音从pinyin_index开始
        start_index = pinyin_index
        # 结束索引是开始索引加上词长度（因为每个中文字对应一个注音）
        end_index = pinyin_index + word_length - 1
        # 将注音数组转换为字符串，多个注音用空格分隔
        pinyin_str = " ".join(pinyins[start_index : end_index + 1])
        # 记录映射关系，包括词性
        mapping.append(
            {
                "word": word,
                "pinyin": pinyin_str,
                "pos": pos,
                "rating": rating,
                "novelLen": book_lens,
                "novelPath": book_path,
            }
        )
        # 更新注音数组索引位置
        pinyin_index += word_length

    return mapping


def logistic_function(x, k=10, x0=0.5):
    return 1 / (1 + np.exp(-k * (x - x0)))


def calScore(word_count, score, book_lens):
    return (
        100000
        * pow(word_count, 1 / 2)
        * logistic_function(score)
        / (book_lens * POSSIBILITY)
    )


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


def processSQL(pipe_conn):
    gc.enable()
    setproctitle.setproctitle(f"{multiprocessing.current_process().name}")
    rockDict = rocksdict.Rdict("./rocksDict", db_options())
    while True:
        message = pipe_conn.recv()  # 接收数据
        words_to_put = []
        for word_item in message:
            word_info = word_item["item"]
            word_count = word_item["count"]
            cn_word = word_info["word"]
            cn_pinyin = word_info["pinyin"]
            cn_rating = (word_info["rating"] - 50) / 30
            cn_pos = word_info["pos"]
            cn_bookLen = word_info["novelLen"]
            cn_bookPath = word_info["novelPath"]

            word_key = f"{cn_word}:{cn_pinyin}"  # 创建唯一的键值
            # 转换布尔值为整数
            word_score = calScore(word_count, cn_rating, cn_bookLen)
            # 读取现有的单词信息
            existing_word_info: dict | None = rockDict.get(word_key, default=None)
            if existing_word_info is None:
                new_info = {
                    "times": word_score,
                    f"pos_{cn_pos}": True,
                    f"novel_{cn_bookPath}": True,
                }
                if "ner" in word_info:
                    new_info[f'ner_{word_info["ner"]}'] = word_score
                words_to_put.append((word_key, new_info))
            else:
                existing_word_info["times"] = (
                    existing_word_info.get("times", 0) + word_score
                )
                existing_word_info[f"novel_{cn_bookPath}"] = True
                existing_word_info[f"pos_{cn_pos}"] = True
                if "ner" in word_info:
                    existing_word_info[f'ner_{word_info["ner"]}'] = (
                        existing_word_info.get(f'ner_{word_info["ner"]}', 0)
                        + word_score
                    )
                words_to_put.append((word_key, existing_word_info))
        del message
        for word_key, word_info in words_to_put:
            rockDict.put(word_key, word_info)
        del words_to_put


def append_done_file(filepath, done_path):
    with open(filepath, "a") as file:
        file.write(done_path + "\n")


def find_longest_sequences_full(words_tags, ner_list):
    # 结果列表，用于存储所有满足条件的连续字符串及其拼音和其他相同属性
    results = []
    # 临时列表，用于存储当前正在检查的连续字符串的字典
    current_sequence = []
    # 遍历每个字典
    for item in words_tags:
        # 检查标签是否在目标标签列表中
        if item["pos"] in ["JJ", "VV", "NR", "NN", "VA", "AD"]:
            # 如果是，则添加到当前连续字符串列表中
            current_sequence.append(item)
        else:
            # 如果不是，检查当前连续字符串是否符合条件（长度不短于2）
            if (
                len(current_sequence) >= 2
                and current_sequence[0]["word"] not in ner_list
            ):
                # 构造结果字典，包括word、pinyin和其他相同属性
                result_dict = {
                    "word": "".join(d["word"] for d in current_sequence),
                    "pinyin": " ".join(d["pinyin"] for d in current_sequence),
                    "pos": "Str",
                }
                # 添加其他相同属性
                for key in current_sequence[0]:
                    if key not in ["word", "pinyin", "pos"]:
                        result_dict[key] = current_sequence[0][key]

                # 添加到结果列表中
                results.append(result_dict)
            # 重置当前连续字符串列表
            current_sequence = []

    # 循环结束后，检查并处理剩余的连续字符串
    if len(current_sequence) >= 2 and current_sequence[0]["word"] not in ner_list:
        result_dict = {
            "word": "".join(d["word"] for d in current_sequence),
            "pinyin": " ".join(d["pinyin"] for d in current_sequence),
            "pos": "Str",
        }
        # 添加其他相同属性
        for key in current_sequence[0]:
            if key not in ["word", "pinyin", "pos"]:
                result_dict[key] = current_sequence[0][key]

        results.append(result_dict)

    # 返回结果列表
    return results


def generate_dict_ngrams(dict_list, n):
    # 初始化n-grams列表
    ngrams = []
    # 生成n-grams
    for i in range(len(dict_list) - n + 1):
        # 从dict_list中提取n个连续的字典
        ngram_dicts = dict_list[i : i + n]
        # 合并word和pinyin，将pos改名为pos_Char
        NextGram = False
        for i in range(0, n):
            if ngram_dicts[i]["pos"] not in ["JJ", "VV", "NR", "NN", "VA", "AD"]:
                NextGram = True
                break
        if NextGram:
            continue
        word = "".join([d["word"] for d in ngram_dicts])
        pinyin = " ".join([d["pinyin"] for d in ngram_dicts])
        if PATTERN_1.match(word) and pinyin != "":
            # 使用第一个字典的其他字段初始化新的n-gram字典
            ngram_dict = {
                key: ngram_dicts[0][key]
                for key in ngram_dicts[0]
                if key not in ["word", "pinyin", "pos"]
            }
            # 更新n-gram字典
            ngram_dict.update({"word": word, "pinyin": pinyin, "pos": f"{n}Gram"})
            # 添加到结果列表
            ngrams.append(ngram_dict)
    return ngrams


def hanlpProcess(
    text_recv_coon,
    redis_send_coon,
    recv_text_from_pipe_lock,
    send_word_from_pipe_lock,
    process_text_semaphore,
):
    setproctitle.setproctitle(f"{multiprocessing.current_process().name}")
    time.sleep(random.randint(0, 10))
    HanLP = (
        hanlp.pipeline()
        .append(hanlp.utils.rules.split_sentence, output_key="sentences")
        .append(hanlp.load("COARSE_ELECTRA_SMALL_ZH"), output_key="tok")
        .append(hanlp.load("CTB9_POS_ELECTRA_SMALL"), output_key="pos")
        .append(
            hanlp.load("MSRA_NER_ELECTRA_SMALL_ZH"), output_key="ner", input_key="tok"
        )
    )
    count = 0
    while True:
        try:
            with recv_text_from_pipe_lock:
                text, rating, book_lens, book_path = text_recv_coon.recv()
                process_text_semaphore.release()

            book_path = PATTERN_3.sub("", book_path)

            hanlp_content = HanLP(text)
            temp_words = []
            for sentence_num in range(len(hanlp_content["sentences"])):
                if " " in hanlp_content["sentences"][sentence_num]:
                    continue
                if len(hanlp_content["sentences"][sentence_num]) < 2:
                    continue
                pinyin_list = lazy_pinyin(
                    hanlp_content["sentences"][sentence_num],
                    errors=lambda x: [None for _ in x],
                )
                word_list = build_mapping_with_pos(
                    pinyin_list,
                    hanlp_content["tok"][sentence_num],
                    hanlp_content["pos"][sentence_num],
                    rating,
                    book_lens,
                    book_path,
                )
                ner_list = []
                for ner in hanlp_content["ner"][sentence_num]:
                    if word_list[ner[2]]["word"] == ner[0]:
                        word_list[ner[2]]["ner"] = ner[1]
                        if ner[1] in [
                            "PERSON",
                            "LOCATION",
                            "ORGANIZATION",
                            "Person",
                            "Location",
                            "Organization",
                        ]:
                            ner_list.append(ner[2])
                temp_words.extend(find_longest_sequences_full(word_list, ner_list))
                for word_num in range(len(hanlp_content["sentences"][sentence_num])):
                    if PATTERN_1.match(
                        hanlp_content["sentences"][sentence_num][word_num]
                    ):
                        word_list.append(
                            {
                                "word": hanlp_content["sentences"][sentence_num][
                                    word_num
                                ],
                                "pinyin": pinyin_list[word_num],
                                "rating": rating,
                                "pos": "Char",
                                "novelLen": book_lens,
                                "novelPath": book_path,
                            }
                        )
                temp_words.extend(word_list)
            words = []
            for word_info in temp_words:
                if PATTERN_1.match(word_info["word"]) and word_info["pinyin"] != "":
                    words.append(word_info)
            with send_word_from_pipe_lock:
                redis_send_coon.send(words)
                count += 1
            if count % 2000 == 0:
                torch.cuda.empty_cache()
        except Exception as e:
            print(e)


def processRawWords(
    processtext_hanlp_pipe, processtext_redis_pipe, begin_process_semaphore
):
    setproctitle.setproctitle(f"{multiprocessing.current_process().name}")
    words_raw = []
    while True:
        new_words_raw = processtext_hanlp_pipe.recv()
        words_raw.extend(new_words_raw)
        if begin_process_semaphore.value == 0:
            begin_process_semaphore.value += 1
            print(f"前有{len(words_raw)}")
            words_raw = merge_dicts_with_count(words_raw)
            print(f"后有{len(words_raw)}")
            del new_words_raw
            processtext_redis_pipe.send(words_raw)
            del words_raw
            words_raw = []
            gc.collect()


if __name__ == "__main__":
    done_file_path = "done.txt"
    db_filepath = "database.db"  # 假设数据库文件名为database.db
    done_paths = read_done_paths(done_file_path)
    paths_scores = read_scores_from_database(db_filepath, done_paths)
    PROCESS_NUM = 3
    with multiprocessing.Manager() as manager:
        semaphore_Text_to_Process = manager.Semaphore(PROCESS_NUM * 10)
        Process_SQL_Semaphore = manager.Value("i", 1)
        main_Hanlp_Read_Text_Lock = manager.Lock()
        Hanlp_Redis_Send_Data_Lock = manager.Lock()

        main_Hanlp_conn, Hanlp_main_conn = Pipe()
        ProcessText_Redis_conn, Redis_ProcessText_Conn = Pipe()  # 创建一个管道
        Hanlp_ProcessText_conn, ProcessText_Hanlp_coon = Pipe()

        hanlp_processes = [
            Process(
                target=hanlpProcess,
                args=(
                    Hanlp_main_conn,
                    Hanlp_ProcessText_conn,
                    main_Hanlp_Read_Text_Lock,
                    Hanlp_Redis_Send_Data_Lock,
                    semaphore_Text_to_Process,
                ),
                name="python-hanlp-" + str(i),
            )
            for i in range(PROCESS_NUM)
        ]
        for i in hanlp_processes:
            i.start()

        process_raw_word = Process(
            target=processRawWords,
            args=(
                ProcessText_Hanlp_coon,
                ProcessText_Redis_conn,
                Process_SQL_Semaphore,
            ),
            name="python-processRawWords",
        )
        process_raw_word.start()

        redis_receiver = Process(
            target=processSQL,
            args=(Redis_ProcessText_Conn,),
            name="python-Cache",
        )
        redis_receiver.start()
        time.sleep(3)

        random_integers = array.array("i", (random.randint(0, 9) for _ in range(1000)))
        count = 0
        for path, score in paths_scores:
            # 这里可以根据路径和分数进行处理，示例仅使用路径
            book = txtBookParser(path)
            book.read()
            for i in book.bookParas:
                if random_integers[count] < POSSIBILITY_TEN:
                    main_Hanlp_conn.send((i, score, book.bookLens, path))
                    semaphore_Text_to_Process.acquire()
                count += 1
                count %= 1000
            while True:
                if Process_SQL_Semaphore.value == 1:
                    Process_SQL_Semaphore.value -= 1
                    break
                else:
                    time.sleep(1)
            append_done_file(done_file_path, path)
        redis_receiver.join()
