import yaml

with open("C:\\Users\\Jinson\\Desktop\\Degreed\\YAML\\data_types.yml") as f:
    yaml_contents = yaml.load_all(f,Loader=yaml.FullLoader)
    for content in yaml_contents:
        for k,v in content.items():
            print(k," : ",v)