import json


# For easy spreadsheet importing
def json_to_csv(file_gen):
    data_list = []
    with open(file_gen + '.json') as file:
        data = json.loads(file.read())

        for k, v in data.items():
            for k2, v2 in v.items():
                if v2 is not None:
                    data_list.append([k, k2] + v2)

    with open(file_gen + '.csv', 'w') as file:
        out = ''
        for lst in data_list:
            for item in lst:
                out += str(item) + ','
            out = out[:-1] + '\n'
        file.write(out)


json_to_csv('analyses/analysis_pg')
json_to_csv('analyses/analysis_ms')
json_to_csv('analyses/analysis_sl')
