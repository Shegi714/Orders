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
        "Authorization": token,  # для WB statistics-api — без Bearer
        "User-Agent": "Mozilla/5.0 (compatible; WBFetcher/1.0; +https://github.com/yourrepo)"
    }

    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
    all_rows = []
    next_date_from = date_from
    page = 1

    # настройки повторов
    max_decode_retries = 3          # повторов при пустом/не-JSON ответе с 200
    base_sleep = 60                 # базовая пауза для лимита (1 req/min)
    max_http_retries = 3            # повторов при 429/5xx

    while True:
        attempt = 0
        while True:
            attempt += 1
            print(f"📡 [{page}] Запрос заказов: dateFrom={next_date_from} (попытка {attempt})")
            try:
                resp = requests.get(url, headers=headers, params={"dateFrom": next_date_from}, timeout=60)
            except Exception as e:
                print(f"❌ Ошибка сети: {e} — ждём {base_sleep}s и повторим.")
                time.sleep(base_sleep)
                if attempt >= max_http_retries:
                    print("🛑 Превышены повторы сети. Останавливаемся.")
                    return all_rows
                continue

            # хэндлинг лимитов/временных ошибок
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = base_sleep * min(attempt, 4)
                print(f"⚠️ HTTP {resp.status_code}. Ждём {wait}s и повторяем ту же страницу. Фрагмент: {resp.text[:200]}")
                time.sleep(wait)
                if attempt >= max_http_retries:
                    print("🛑 Превышены повторы при 429/5xx. Останавливаемся.")
                    return all_rows
                continue

            if resp.status_code == 401:
                print("❌ 401 Unauthorized — токен не подходит к statistics-api. Пропускаем кабинет.")
                return all_rows

            if resp.status_code != 200:
                print(f"⚠️ Неожиданный код {resp.status_code}. Тело (фрагмент): {resp.text[:300]}")
                # на всякий случай не двигаем dateFrom и пробуем повтор
                time.sleep(base_sleep)
                if attempt >= max_http_retries:
                    print("🛑 Превышены повторы при не-200. Останавливаемся.")
                    return all_rows
                continue

            # Проверим, что это JSON и не пустое тело
            ctype = (resp.headers.get("Content-Type") or "").lower()
            body = resp.text or ""
            if ("application/json" not in ctype) or (not body.strip()):
                # бывает HTML-страница WAF с 200
                print(f"⚠️ Ожидали JSON, получили Content-Type='{ctype}', len={len(body)}. Фрагмент: {body[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы декодирования JSON. Останавливаемся.")
                    return all_rows
                continue

            # Попробуем распарсить JSON
            try:
                chunk = resp.json()
            except Exception as e:
                print(f"⚠️ JSONDecodeError: {e}. Фрагмент тела: {body[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы декодирования JSON. Останавливаемся.")
                    return all_rows
                continue

            if not isinstance(chunk, list):
                print(f"⚠️ Ожидали массив, получили {type(chunk)}. Фрагмент: {str(chunk)[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы из-за неверного формата. Останавливаемся.")
                    return all_rows
                continue

            # валидный ответ — выходим из внутреннего цикла попыток
            break

        if not chunk:
            print("✅ Данные закончились, все записи собраны.")
            break

        all_rows.extend(chunk)
        print(f"📦 Получено записей: {len(chunk)}, всего: {len(all_rows)}")

        # Подготовим следующий dateFrom
        try:
            next_date_from = chunk[-1]["lastChangeDate"]
        except Exception:
            print("⚠️ В последней записи нет lastChangeDate — остановка пагинации.")
            break

        page += 1
        # соблюдаем лимит
        print(f"⏳ Пауза {base_sleep}s (лимит API 1 req/min)...")
        time.sleep(base_sleep)

    return all_rows


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
