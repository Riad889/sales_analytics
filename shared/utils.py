import json


class Utils:
    def __init__(self):
        pass

    def chunk_data(self, data, chunk_size):
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    def chunk_list(self, data_list, chunk_size):
        for i in range(0, len(data_list), chunk_size):
            yield data_list[i : i + chunk_size]

    def save_to_jsonl(self, data, output_file):
        with open(output_file, "a", encoding="utf-8") as file:
            file.writelines(json.dumps(record) + "\n" for record in data)
