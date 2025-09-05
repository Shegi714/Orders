import os
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO

# 🧪 Глобальные переменные окружения (используются GitHub Secrets)
SOURCE_SHEET_ID = os.environ.get("SOURCE_SHEET_ID")
TARGET_SHEET_ID = os.environ.get("TARGET_SHEET_ID")
GOOGLE_CREDS_PATH = os.environ.get("GOOGLE_CREDS_PATH")

# ⚙️ Настройки подключения к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get("GOOGLE_CREDS_JSON")
if not creds_json:
    raise ValueError("GOOGLE_CREDS_JSON not set or empty!")

# 🧠 Преобразуем в файл-like объект
creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# 📑 Чтение данных токенов и кабинетов
source_sheet = client.open_by_key(SOURCE_SHEET_ID).sheet1
rows = source_sheet.get_all_values()[1:]
data = [{"token": row[0], "cabinet": row[1]} for row in rows if len(row) >= 2 and row[0].strip()]


# 📡 Параметры отчета
params = {
    "locale": "ru",
    "groupByBrand": "false",
    "groupBySubject": "false",
    "groupBySa": "true",
    "groupByNm": "true",
    "groupByBarcode": "true",
    "groupBySize": "true",
    "filterPics": "0",
    "filterVolume": "0"
}

# 🔁 Ретрий создания отчета
def create_report(token, cabinet_name, retries=3, delay=5):
    url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"📡 [{attempt}/{retries}] Создание отчета для {cabinet_name}...")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 401:
                print(f"❌ Токен для {cabinet_name} нерабочий (401 Unauthorized). Пропускаем без повторов.")
                return None

            response.raise_for_status()

            data = response.json()
            task_id = data.get("data", {}).get("taskId") or data.get("taskId")
            if task_id:
                print(f"✅ Получен taskId: {task_id}")
                return task_id
            else:
                print(f"⚠️ Нет taskId в ответе: {data}")

        except Exception as e:
            print(f"⚠️ Ошибка при создании отчета для {cabinet_name}: {e}")

        time.sleep(delay)

    print(f"❌ Превышено количество попыток для {cabinet_name}.")
    return None


# ⏳ Проверка готовности отчета (до 5 попыток)
def wait_for_report(token, task_id, cabinet_name, retries=20, delay=15):
    url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0"
    }

    print(f"⏳ Ждём 10 секунд перед первой проверкой taskId для кабинета {cabinet_name}...")
    time.sleep(delay)  # 💥 Принудительная первая задержка

    for attempt in range(1, retries + 1):
        try:
            print(f"🔁 [{attempt}/{retries}] Проверка готовности отчета для {cabinet_name}...")
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                print(f"✅ Отчет для {cabinet_name} готов.")
                return response.json()

            elif response.status_code == 401:
                print(f"❌ Токен для {cabinet_name} нерабочий (401 Unauthorized).")
                return None

            elif response.status_code == 429:
                print(f"⚠️ Слишком много запросов (429). Ждём {delay}s...")

            elif response.status_code == 404:
                print(f"ℹ️ Отчет ещё не готов (404). Ждём...")

            else:
                print(f"❗ Неожиданный код ответа {response.status_code}: {response.text}")

        except Exception as e:
            print(f"⚠️ Ошибка при проверке отчета для {cabinet_name}: {e}")

        time.sleep(delay)

    print(f"❌ Превышено количество попыток для {cabinet_name}. Отчет не получен.")
    return None


# 📊 Запись отчета в Google Sheets
from datetime import datetime

def write_report_to_sheet(sheet_obj, cabinet_name, report_data):
    try:
        today = datetime.now().strftime("%d-%m-%Y")

        try:
            worksheet = sheet_obj.worksheet(cabinet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet_obj.add_worksheet(title=cabinet_name, rows="1000", cols="10")

        # Заголовки
        headers = ["Дата", "nmId", "barcode", "", "", "В пути до получателей", "В пути возвраты на склад WB", "Всего находится на складах"]
        rows = [headers]

        for item in report_data:
            nm_id = item.get("nmId", "")
            barcode = item.get("barcode", "")
            warehouses = {w["warehouseName"]: w["quantity"] for w in item.get("warehouses", [])}

            row = [
                today,
                nm_id,
                barcode,
                "", "",  # D, E — пропущены по ТЗ
                warehouses.get("В пути до получателей", 0),
                warehouses.get("В пути возвраты на склад WB", 0),
                warehouses.get("Всего находится на складах", 0)
            ]
            rows.append(row)

        worksheet.update(rows)
        print(f"✅ Сохранено в лист '{cabinet_name}'")

    except Exception as e:
        print(f"🛑 Ошибка при записи в лист '{cabinet_name}': {e}")


# 🚀 Главная функция
def main():
    target_sheet = client.open_by_key(TARGET_SHEET_ID)

    for entry in data:
        cabinet = entry["cabinet"]
        token = entry["token"]
        print(f"\n🔄 Работаем с кабинетом: {cabinet}")

        task_id = create_report(token, cabinet)
        if not task_id:
            print(f"❌ Пропуск {cabinet}, не удалось создать отчет.")
            continue

        report = wait_for_report(token, task_id, cabinet)
        if not report:
            print(f"❌ Пропуск {cabinet}, отчет не готов.")
            continue

        write_report_to_sheet(target_sheet, cabinet, report)

if __name__ == "__main__":
    main()
