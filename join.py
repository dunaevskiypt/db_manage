import json

# Путь к файлам
sprint_file_path = '/home/peter/Documents/store/sprintdata.json'
exdata_file_path = '/home/peter/Documents/store/exdata.json'
# Путь для сохранения объединенного файла
combined_file_path = '/home/peter/Documents/store/combined_data.json'

# Чтение данных из sprintdata.json и exdata.json
with open(sprint_file_path, 'r', encoding='utf-8') as sprint_file, \
        open(exdata_file_path, 'r', encoding='utf-8') as exdata_file:
    sprint_data = json.load(sprint_file)
    exdata = json.load(exdata_file)

# Создание словаря для объединения данных по id
combined_data = {}

# Добавляем данные из exdata
for item in exdata:
    combined_data[item["id"]] = item

# Объединяем с данными из sprintdata
for item in sprint_data:
    if item["id"] in combined_data:
        combined_data[item["id"]].update(item)
    else:
        combined_data[item["id"]] = item

# Сохранение объединенных данных в новый файл
with open(combined_file_path, 'w', encoding='utf-8') as combined_file:
    json.dump(list(combined_data.values()),
              combined_file, ensure_ascii=False, indent=4)

print(f"Данные успешно объединены и сохранены в {combined_file_path}")
