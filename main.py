import os
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from datetime import datetime, timedelta

# 🧪 Глобальные переменные окружения (используются GitHub Secrets)
SOURCE_SHEET_ID = os.environ.get("SOURCE_SHEET_ID")
TARGET_SHEET_ID = os.environ.get("TARGET_SHEET_ID")

# ⚙️ Подключение к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get("GOOGLE_CREDS_JSON")
if not creds_json:
    raise ValueError("GOOGLE_CREDS_JSON not set or empty!")

creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# 📑 Чтение токенов и кабинетов
source_sheet = client.open_by_key(SOURCE_SHEET_ID).sheet1
rows = source_sheet.get_all_values()[1:]
data = [{"token": row[0], "cabinet": row[1]} for row in rows if len(row) >= 2 and row[0].strip()]

# 📡 Функция для выгрузки заказов за последние N дней
def fetch_orders(token, days=14):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {
        "accept": "application/json",
        "Authorization": token,  # ⚠️ тут НЕ "Bearer", а просто токен
        "User-Agent": "Mozilla/5.0"
    }

    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    all_orders = []
    next_date_from = date_from

    while True:
        print(f"📡 Запрос заказов c {next_date_from} ...")
        params = {"dateFrom": next_date_from}
        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 401:
            print("❌ Неверный токен или нет доступа (401 Unauthorized).")
            break
        if response.status_code != 200:
            print(f"⚠️ Ошибка запроса {response.status_code}: {response.text}")
            break

        orders = response.json()
        if not orders:
            print("✅ Данные закончились, все заказы собраны.")
            break

        all_orders.extend(orders)
        print(f"📦 Получено заказов: {len(orders)}, всего собрано: {len(all_orders)}")

        # Готовим дату для следующего запроса
        next_date_from = orders[-1]["lastChangeDate"]

        # Лимит 1 запрос/мин
        print("⏳ Ждём 60 секунд...")
        time.sleep(60)

    return all_orders


# 📊 Запись заказов в Google Sheets
def write_orders_to_sheet(sheet_obj, cabinet_name, orders):
    try:
        # Создаём или очищаем лист
        try:
            worksheet = sheet_obj.worksheet(cabinet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet_obj.add_worksheet(title=cabinet_name, rows="1000", cols="20")

        if not orders:
            worksheet.update([["Нет данных за выбранный период"]])
            print(f"⚠️ Данных для '{cabinet_name}' нет.")
            return

        # Заголовки в том порядке, в котором они пришли
        headers = list(orders[0].keys())
        rows = [headers]

        for order in orders:
            row = [order.get(h, "") for h in headers]
            rows.append(row)

        # Обновляем таблицу одним запросом
        worksheet.update(rows)
        print(f"✅ Сохранено {len(orders)} заказов в лист '{cabinet_name}'")

    except Exception as e:
        print(f"🛑 Ошибка при записи в лист '{cabinet_name}': {e}")


# 🚀 Главная функция
def main():
    target_sheet = client.open_by_key(TARGET_SHEET_ID)

    for entry in data:
        cabinet = entry["cabinet"]
        token = entry["token"]
        print(f"\n🔄 Работаем с кабинетом: {cabinet}")

        orders = fetch_orders(token, days=14)
        write_orders_to_sheet(target_sheet, cabinet, orders)


if __name__ == "__main__":
    main()
