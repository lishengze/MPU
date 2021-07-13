import json

def get_redis_config():
    file_name = "../redis_config.json"
    json_file = open(file_name,'r')
    json_dict = json.load(json_file)
    print(json_dict)
    return json_dict

if __name__ == '__main__':
    get_redis_config()