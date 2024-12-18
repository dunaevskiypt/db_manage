import json
import psycopg2
from psycopg2.extras import execute_values

# Параметры подключения к базе данных
conn = psycopg2.connect(
    dbname="autoscraper",
    user="postgres",
    password="42154298Skill",
    host="localhost",
    port="5432"
)

# Получаем данные из файла combined_data.json
combined_file_path = '/home/peter/Documents/store/combined_data.json'

with open(combined_file_path, 'r', encoding='utf-8') as file:
    combined_data = json.load(file)

# Разделяем данные на cars, owners и locations
cars_data = []
owners_data = []
locations_data = []

for record in combined_data:
    car = {
        "brand": record.get("brand"),
        "model": record.get("model"),
        "year": record.get("year"),
        "price_usd": record.get("price_usd"),
        "mileage": record.get("mileage"),
        "color": record.get("color"),
        "gearbox": record.get("gearbox"),
        "drive": record.get("drive"),
        "fuel_type": record.get("fuel_type"),
        "status": record.get("status"),
        "was_in_accident": record.get("was_in_accident"),
        "vin": record.get("vin"),
        "product_url": record.get("product_url"),
        "date_added": record.get("date_added")
    }

    car_id = None  # id автомобиля, который будет добавлен в cars

    # Вставка данных в таблицу cars
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO cars (brand, model, year, price_usd, mileage, color, gearbox, drive, fuel_type, status, was_in_accident, vin, product_url, date_added)
                VALUES (%(brand)s, %(model)s, %(year)s, %(price_usd)s, %(mileage)s, %(color)s, %(gearbox)s, %(drive)s, %(fuel_type)s, %(status)s, %(was_in_accident)s, %(vin)s, %(product_url)s, %(date_added)s)
                RETURNING id;
            """, car)
            car_id = cursor.fetchone(
            )[0]  # получаем id вставленного автомобиля

            # После вставки данных в cars, вставляем данные в owners и locations
            owner = {
                "car_id": car_id,
                "seller_name": record.get("seller_name"),
                "owners_count": record.get("owners_count"),
                "state_number": record.get("state_number")
            }

            location = {
                "car_id": car_id,
                "location": record.get("location")
            }

            owners_data.append(owner)
            locations_data.append(location)

        # Вставка данных в таблицу owners
        if owners_data:
            with conn.cursor() as cursor:
                execute_values(cursor, """
                    INSERT INTO owners (car_id, seller_name, owners_count, state_number)
                    VALUES %s
                """, [(d['car_id'], d['seller_name'], d['owners_count'], d['state_number']) for d in owners_data])

        # Вставка данных в таблицу locations
        if locations_data:
            with conn.cursor() as cursor:
                execute_values(cursor, """
                    INSERT INTO locations (car_id, location)
                    VALUES %s
                """, [(d['car_id'], d['location']) for d in locations_data])

    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        conn.rollback()
    else:
        conn.commit()

# Закрытие соединения с базой данных
conn.close()
