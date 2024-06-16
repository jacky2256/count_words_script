import psycopg2
from parsel import Selector
import re
import csv
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def count_descendants(data):
    children = {}

    for id, url, level, parent_id, content in data:
        if parent_id not in children:
            children[parent_id] = []
        children[parent_id].append(id) 

    def count_all_descendants(node_id):

        if node_id not in children: 
            return 0

        count = 0
        for child_id in children[node_id]:
            count += 1 + count_all_descendants(child_id)
        return count 
    
    def count_all_descendants_with_parents_id(node_id):
        if node_id not in children:
            return {"count": 0, "children_ids": []}

        count = 0
        children_ids = []
        for child_id in children[node_id]:
            result = count_all_descendants_with_parents_id(child_id)
            count += 1 + result["count"]
            children_ids.append(child_id)
            children_ids.extend(result["children_ids"])
        
        return {"count": count, "children_ids": children_ids}

    result = {}
    for id, url, level, parent_id, content in data:
        if parent_id == -1:
            result[id] = count_all_descendants_with_parents_id(id)
    
    return result


def get_data(html):
    count_keywords_list = []
    try:
        keywords_lists = read_keywords()
        for keywords_list in keywords_lists:
            cnt_item = count_keywords(html, keywords_list)
            count_keywords_list.append(cnt_item)
    except:
        count_keywords_list = []

    try:
        total_words = count_total_words(html)
    except:
        total_words = 0

    return count_keywords_list, total_words


def count_total_words(content):
    total_words = 0
    if not content.strip():
        return total_words
    
    try:
        selector = Selector(text=content)
        text = selector.get()
        html_content = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        html_content = re.sub(r'<script.*?>.*?</script>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<style.*?>.*?</style>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<[^>]+>', '', html_content)
        html_content = html_content.strip()
        html_content = re.sub(r"\s+", " ", html_content)
        words = html_content.split()
        total_words = len(words)
    except Exception as err:
        print(err)
    return total_words

def count_keywords(content, keywords):
    result = []
    if not content.strip():
        return ''
    try:
        selector = Selector(text=content)
        text = selector.get()
        html_content = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        html_content = re.sub(r'<script.*?>.*?</script>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<style.*?>.*?</style>', '', html_content, flags=re.DOTALL)
        html_content = re.sub(r'<[^>]+>', '', html_content)
        html_content = re.sub(r'[.,!?:]', ' ', html_content)

        count = 0
        for keyword in keywords:
            count_item = 0
            pattern = r'\s+' + re.escape(keyword) + r'\s+'
            count_item = len(re.findall(pattern, html_content, flags=re.IGNORECASE))
            count += count_item
            result.append(count_item) 
        result.append(count)     
    except Exception as err:
        print(err)
    return " | ".join(map(str, result))


def read_keywords():
    keywords_lists = []
    with open("data/in/keywords.txt") as _file:
        keywords_list = _file.readlines()
    keywords_list = [x.strip() for x in keywords_list]       
    for keywords in keywords_list:
        keywords = keywords.split(',')
        keywords = [x.strip() for x in keywords]
        keywords_lists.append(keywords)
    return keywords_lists

def read_count_keywords():
    keywords_lists = []
    count_keywords_list = []

    with open("data/in/keywords.txt") as _file:
        keywords_list = _file.readlines()

    keywords_list = [x.strip() for x in keywords_list if x.strip()]

    for keywords in keywords_list:
        keywords = keywords.split(',')
        keywords = [x.strip() for x in keywords]
        keywords_lists.append(keywords)

    for items in keywords_lists:
        if len(items) > 0:
            count = len(items) + 1
            result = ' | '.join(['0'] * count)
            count_keywords_list.append(result)

    return count_keywords_list

def create_data_dict(data):
    data_dict = {}
    for id, url, level, parent_id, content in data:
        data_dict[id] = {
            "level": level,
            "url": url,
            "parent_id": parent_id,
            "content": content
        }
    return data_dict

def parse_line(line):
    return list(map(int, line.split(' | ')))

def stringify_line(numbers):
    return ' | '.join(map(str, numbers))


def write_big_file(data, how='a+'):
    try:
        print(data)
        with open("data/out/other_urls.csv", how, newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(data)
    except Exception as err:
        print(err)



def sum_keyword_counts(parent_list, children_list):
    result_list = []
    
    for i in range(len(parent_list)):
        parent_split = parent_list[i].split(' | ')
        children_split = children_list[i].split(' | ')
        
        summed_values = []
        
        for j in range(len(parent_split)):
            parent_value = int(parent_split[j])
            children_value = int(children_split[j])
            summed_value = parent_value + children_value
            summed_values.append(summed_value)
        
        result_list.append(' | '.join(map(str, summed_values)))
    
    return result_list


try:
    headers_ = ['url']
    keywords_lists = read_keywords()
    for kewords in keywords_lists:
        headers_.append(' | '.join(kewords) + ' | total')
    headers_.append('total_words')
    with open("data/out/results.csv", 'w', newline='') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow(headers_)

    headers_big_file = headers_.copy()
    headers_big_file.append('source_link')
    write_big_file(headers_big_file, 'w')
    
    connection = psycopg2.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME
    )

    count_null_keywords_list = read_count_keywords()

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT l.id, l.url, l.level, l.parent_id, c.content
            FROM links AS l
            LEFT JOIN contents AS c ON (l.id = c.link_id)
            WHERE level < 2;
        """
        )
        data = cursor.fetchall()
        data_dict = create_data_dict(data)
        result = count_descendants(data)


        for parent_id, parent_value in result.items():
            count_parent_keywords_list = count_null_keywords_list
            count_parent_total_words = 0
            children_list = parent_value['children_ids']
            for children_id in children_list:
                count_children_keywords_list, count_children_total_words = get_data(data_dict.get(children_id, {}).get('content', 'Content not found'))
                children_data_to_write = [data_dict.get(children_id, {}).get('url', 'url not found')]

                for i in count_children_keywords_list:
                    children_data_to_write.append(i)

                children_data_to_write.append(count_children_total_words)
                children_data_to_write.append(data_dict.get(parent_id, {}).get('url', 'url not found'))
                write_big_file(children_data_to_write, 'a+')

                if len(count_children_keywords_list) > 0:
                    count_parent_keywords_list = sum_keyword_counts(count_parent_keywords_list, count_children_keywords_list)
                    count_parent_total_words += count_children_total_words

            results = [data_dict.get(parent_id, {}).get('url', 'url not found')]
            for i in count_parent_keywords_list:
                results.append(i)
            results.append(count_parent_total_words)
            with open("data/out/source_urls.csv", 'a+', newline='') as file_:
                _writer = csv.writer(file_, delimiter='\t')
                _writer.writerow(results)
            print(results)

            with open('logger.txt', '+a') as f:    
                f.writelines(f'{parent_id} - {count_parent_keywords_list} - {count_parent_total_words}\n')
                f.writelines(f'{parent_id} is READY\n')
                f.writelines(f'============================================\n')

except Exception as err:
    print(f"[ERROR] {err}") 
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
