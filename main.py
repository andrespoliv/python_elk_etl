import pandas as pd
import argparse
import os
import re
import uuid
import subprocess

parser = argparse.ArgumentParser(description="ETL tool")
parser.add_argument("-f", "--file", help="File to load")
parser.add_argument("-he", "--headers", help="Type -he yes to delete headers")
parser.add_argument("-fr", "--firstrow", help="Type -fr yes to delete file's first row")
parser.add_argument("-g", "--grouptype", help="Type of capture group, available one and two")
parser.add_argument("-i", "--index", help="ElasticSearch index")
parser.add_argument("-c", "--columns", help="Columns to keep, example: '1,2,3' or '2'")
parser.add_argument("-fn", "--function", help="Function to execute, type parse, build or load.")
parser = parser.parse_args()

capture_groups = {"one":"\((.*?),(.*?)\)", "two":"VALUES\((.*?),(.*?)\)", "three":"(.*?),(.*?)"}

def parse_cols(cols):
    match_pattern = re.search(capture_groups["three"], cols)
    result = []
    if match_pattern != None:
        result = cols.split(",")
        result = [int(elem) for elem in result]
    return result

def cols_to_keep(filename, cols):
    raw_file = open(filename, "r")
    result = ""
    for line in raw_file:
        new_list_of_strings = []
        list_of_strings = line.split(",")
        list_of_strings = [string for string in list_of_strings if list_of_strings.index(string) in cols]
        for string in list_of_strings:
            new_string = string.replace("\n", "")
            new_list_of_strings.append(new_string)
        result = result + ','.join(new_list_of_strings) + "\n"
    raw_file.close()

    file = open(filename, "w")
    file.write(result)
    file.close()
    return True

def delete_first_row(filename):
    raw_file = open(filename, "r")
    result = ""
    for line in raw_file:
        new_list_of_strings = []
        list_of_strings = line.split(",")
        list_of_strings = list_of_strings[1:]
        for string in list_of_strings:
            new_string = string.replace("\n", "")
            new_list_of_strings.append(new_string)
        result = result + ','.join(new_list_of_strings) + "\n"
    raw_file.close()

    file = open(filename, "w")
    file.write(result)
    file.close()
    return True

def delete_headers(filename):
    raw_file = open(filename, "r")
    lines = raw_file.readlines()
    raw_file.close()

    del lines[0]

    new_file = open(filename, "w+")
    for line in lines:
        new_file.write(line)
    
    new_file.close()
    return True

def check_file(filename):
    path = "./" + filename
    return os.path.exists(path)

def create_csv(text):
    if not check_file("./csv_files"):
        path = os.getcwd() + "/csv_files"
        os.mkdir(path)
    id = str(uuid.uuid4())
    filename = "./csv_files/" + id + ".csv"
    f = open(filename, "x")
    f.write(text)
    f.close()
    return [filename, id]

def create_conf(text):
    full_path = ""
    if not check_file("./pipelines"):
        full_path = os.getcwd() + "/pipelines"
        os.mkdir(full_path)
    id = str(uuid.uuid4())
    filename =  "pipeline_" + id + ".conf"
    path = "./pipelines/" + filename
    f = open(path, "x")
    f.write(text)
    f.close()
    return path

def check_path(filename):
    if filename.startswith("./"):
        full_path = os.getcwd().replace("\\", "/") + filename[1:]
        return full_path
    elif filename.startswith("/"):
        full_path = os.getcwd().replace("\\", "/") + filename
        return full_path
    else:
        return filename

def modify_csv(filename, delete_h=False, delete_first_r=False, cols=None):
    if delete_h == "yes":
        delete_headers(filename)
    if delete_first_r == "yes":
        delete_first_row(filename)
    if cols != None:
        columns = parse_cols(cols)
        cols_to_keep(filename, columns)
    return True

def open_json(filename):
    if not check_file("./csv_files"):
        path = os.getcwd() + "/csv_files"
        os.mkdir(path)
    df = pd.read_json(filename)
    id = str(uuid.uuid4())
    name = id + ".csv"
    new_filename = "./csv_files/" + name
    df.to_csv(new_filename)
    delete_first_row(new_filename)
    delete_headers(new_filename)
    return True

def open_sql(filename, type_of_group):
    file = open(filename, "r")
    result = ""
    for line in file:
        match = re.search(capture_groups[type_of_group], line)
        if match != None and not "`" in match.group():
            raw_list_of_items = re.findall('([^(,)]+)(?!.*\()', match.group())
            result = result + ','.join(raw_list_of_items) + "\n"
    return result

def read_file(filename, type_of_group=None, params=[]):
    if ".json" in filename:
        return open_json(filename)
    elif ".csv" in filename and len(params) > 0:
        return modify_csv(filename, params[0], params[1], params[2])
    elif ".sql" in filename and type_of_group != None:
        text = open_sql(filename, type_of_group)
        text = text.replace("\'", "")
        text = text.replace(" ", "")
        return create_csv(text)

def create_pipeline_file(path):
    pipeline_text= f'input {{\n\tfile {{\n\t\tpath =>"{path}"\n\t\tstart_position =>beginning\n\t\t}}\n\t}}\n\nfilter {{\n\tcsv {{\n\t\tcolumns =>["email","password"]}}\n\t}}\n\noutput {{\n\tstdout{{}}\n\nelasticsearch {{\n\t\tindex =>"exploitin"\n\t\t}}\n\t}}'
    result = create_conf(pipeline_text)
    return result

def call_logstash(config_file):
    try:
        command = f"logstash -f {config_file}"
        process = subprocess.Popen(command, shell=True)
        print("Pipeline executed successfully")
        return True
    except Exception as e:
        print(e)

def main():
    if parser.function == "parse":
        if parser.file and check_file(parser.file) and not parser.grouptype:
            read_file(parser.file, None, [parser.headers, parser.firstrow, parser.columns])   
            print("Done")    
        elif parser.file and parser.grouptype and check_file(parser.file):
            read_file(parser.file, parser.grouptype)
            print("Done")
        else:
            print("Something happened, try again.")
            return False
    elif parser.function == "build":
        if parser.file and check_file(parser.file):
            path = check_path(parser.file)
            create_pipeline_file(path)
            print("Done")
    elif parser.function == "load":
        if parser.file and check_file(parser.file):
            call_logstash(parser.file)
            print("Done")
    else:
        print("Please type -fn parse, build or load.")
   

if __name__ == "__main__":
    main()