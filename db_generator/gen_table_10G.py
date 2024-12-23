import random
import os
from shutil import copyfile

USERS_NUM = 10000
ARTICLES_NUM = 10000
READS_NUM = 1000000

uid_region = {}
aid_lang = {}

# 生成用户数据
def gen_an_user(i):
    timeBegin = 1506328859000
    user = [
        str(timeBegin + i),
        'u' + str(i),
        str(i),
        "user%d" % i,
        "male" if random.random() > 0.33 else "female",
        "email%d" % i,
        "phone%d" % i,
        "dept%d" % int(random.random() * 20),
        "grade%d" % int(random.random() * 4 + 1),
        "en" if random.random() > 0.8 else "zh",
        "Beijing" if random.random() > 0.4 else "Hong Kong",
        "role%d" % int(random.random() * 3),
        "tags%d" % int(random.random() * 50),
        str(int(random.random() * 100))
    ]

    uid_region[user[2]] = user[10]  # uid 和 region 的映射
    return user


# 生成文章数据
def gen_an_article(i):
    timeBegin = 1506000000000
    article = [
        str(timeBegin + i),
        'a' + str(i),
        str(i),
        "title%d" % i,
        "science" if random.random() > 0.55 else "technology",
        "abstract of article %d" % i,
        "tags%d" % int(random.random() * 50),
        "author%d" % int(random.random() * 2000),
        "en" if random.random() > 0.5 else "zh",
        f"text_a{i}.txt",
        ""
    ]

    # 创建文章文件夹
    path = f'./articles/article{i}'
    if not os.path.exists(path):
        os.makedirs(path)

    # 随机选择一个文本文件并复制
    categories = ['business', 'entertainment', 'sport', 'tech']
    random_category = categories[random.randint(0, 3)]
    files = os.listdir(f'./bbc_news_texts/{random_category}/')
    size = len(files)
    random_news = files[random.randint(0, size - 1)]
    copyfile(f'bbc_news_texts/{random_category}/{random_news}', f'{path}/text_a{i}.txt')

    # 生成图片
    image_num = random.randint(1, 2)
    image_str = ",".join([f'image_a{i}_{j}.jpg' for j in range(image_num)])
    article[10] = f'"{image_str}"'  # 用双引号括起来

    for j in range(image_num):
        copyfile(f'./image/{random.randint(0, 599)}.jpg', f'{path}/image_a{i}_{j}.jpg')

    # 生成视频
    if random.random() < 0.05:
        article.append(f'"video_a{i}_video.flv"')
        video_src = './video/video1.flv' if random.random() < 0.5 else './video/video2.flv'
        copyfile(video_src, f'{path}/video_a{i}_video.flv')
    else:
        article.append('""')

    aid_lang[article[2]] = article[8]  # aid 和 language 的映射
    return article


# 生成阅读数据
def gen_an_read(i):
    timeBegin = 1506332297000
    while True:
        read = [
            str(timeBegin + i * 10000),
            'r' + str(i),
            str(int(random.random() * USERS_NUM)),
            str(int(random.random() * ARTICLES_NUM)),
            "",
            "",
            "",
            "",
            ""
        ]

        region = uid_region[read[2]]
        lang = aid_lang[read[3]]
        ps = p[region + lang]

        if random.random() <= ps[0]:
            read[4] = str(int(random.random() * 100))  # readTimeLength
            read[5] = "1" if random.random() < ps[1] else "0"  # agreeOrNot
            read[6] = "1" if random.random() < ps[2] else "0"  # commentOrNot
            read[7] = "1" if random.random() < ps[3] else "0"  # shareOrNot
            read[8] = f'"comments to this article: ({read[2]},{read[3]})"'  # commentDetail
            return read


# 用户读取文章的概率
p = {
    "Beijingen": [0.6, 0.2, 0.2, 0.1],
    "Beijingzh": [1, 0.3, 0.3, 0.2],
    "Hong Kongen": [1, 0.3, 0.3, 0.2],
    "Hong Kongzh": [0.8, 0.2, 0.2, 0.1]
}

# 写入用户数据
with open("user.dat", "w") as f:
    for i in range(USERS_NUM):
        f.write(",".join(gen_an_user(i)) + "\n")

# 写入文章数据
if not os.path.exists('./articles'):
    os.makedirs('./articles')
with open("article.dat", "w") as f:
    for i in range(ARTICLES_NUM):
        f.write(",".join(gen_an_article(i)) + "\n")

# 写入阅读数据
with open("read.dat", "w") as f:
    for i in range(READS_NUM):
        f.write(",".join(gen_an_read(i)) + "\n")
