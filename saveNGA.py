import rocksdict
import re
import random

content_list = []
NGARocksDict = rocksdict.Rdict("./dataNGA/")
for key, value in NGARocksDict.items():
    if key != ("done" or "url"):
        if len(value) < 1:
            continue
        content_list.extend(value)


def merge_short_texts(texts, min_length=400000):
    """
    Merge short texts in a list of strings until each text has at least `min_length` characters.
    Texts are merged with their subsequent neighbors using '\n\n' as a separator.

    Parameters:
    - texts (list of str): The list of strings to be merged.
    - min_length (int): The minimum length of text after merging. Defaults to 800 characters.

    Returns:
    - list of str: A new list of strings where each text is at least `min_length` characters long.
    """
    merged_texts = []
    current_text = ""

    for text in texts:
        if not current_text:
            # If there's no current text, start with the first one
            current_text = text
        elif len(current_text) < min_length:
            # If current text is too short, merge with the next one
            current_text = f"{current_text}\n\n{text}"
        else:
            # If current text is long enough, add it to the result and start a new one
            merged_texts.append(current_text)
            current_text = text

    # Ensure the last text is added, even if it's short
    if current_text:
        # If the last merged text is still too short, try to merge with the previous one if possible
        if len(current_text) < min_length and merged_texts:
            merged_texts[-1] = f"{merged_texts[-1]}\n\n{current_text}"
        else:
            merged_texts.append(current_text)

    return merged_texts


contents = "\n\n".join(content_list)
contents = re.sub(r"\n[0-9]{6,}\n", "\n", contents)
contents = re.sub(r"Roll: d100=d100.+\n", "", contents)
content_list = re.split(r"\n\n+", contents)
random.shuffle(content_list)
ngaContents = merge_short_texts(content_list)
num = 0
for i in ngaContents:
    with open(f"./txt/nga_{num}.txt", "w") as f:
        num += 1
        f.write(i)
