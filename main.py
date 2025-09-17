import os
import time
import random
import json
from datetime import datetime, timedelta

import requests
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

# =========================
# Конфиг окружения
# =========================
SOURCE_SHEET_ID = os.environ.get("SOURCE_SHEET_ID")
TARGET_SHEET_ID = os.environ.get("TARGET_SHEET_ID")

if not SOURCE_SHEET_ID or not TARGET_SHEET_ID:
    raise ValueError("SOURCE_SHEET_ID и/или TARGET_SHEET_ID не заданы в переменных окружения.")

# =========================
# Авторизация Google Sheets
# =========================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_json = os.environ.get("GOOGLE_CREDS_JSON")
if not creds_json:
    raise ValueError("GOOGLE_CREDS_JSON not set or empty!")

creds_dict = json.loads(creds_json)
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# =========================
# Утилиты ретраев для Google API
# =========================
RETRYABLE_HTTP = {429, 500, 502, 503, 504}

def _sleep_backoff(attempt: int, base: float = 1.6, cap: float = 30.0):
    wait = min(cap, (base ** (attempt - 1))) + random.uniform(0, 0.9)
    print(f"⏳ Retry backoff: {wait:.1f}s")
    time.sleep(wait)

def _is_retryable_apierror(e: APIError) -> bool:
    try:
        code = getattr(e.response, "status_code", None)
        return code in RETRYABLE_HTTP
    except Exception:
        return False

def retry_call(fn, *args, retries: int = 5, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None)
            if _is_retryable_apierror(e) and attempt < retries:
                print(f"⚠️ Google API {code} — попытка {attempt}/{retries}, повторяем…")
                _sleep_backoff(attempt)
                continue
            print(f"🛑 Google API error (code={code}): {e}")
            raise
        except requests.RequestException as e:
            if attempt < retries:
                print(f"⚠️ Network error talking to Google API: {e} — попытка {attempt}/{retries}, повторяем…")
                _sleep_backoff(attempt)
                continue
            raise

def open_spreadsheet_by_key_safe(gc: gspread.Client, key: str):
    return retry_call(gc.open_by_key, key)

def get_worksheet_safe(spreadsheet: gspread.Spreadsheet, title: str):
    try:
        return retry_call(spreadsheet.worksheet, title)
    except WorksheetNotFound:
        return retry_call(spreadsheet.add_worksheet, title=title, rows="1000", cols="26")

def worksheet_clear_safe(ws: gspread.Worksheet):
    return retry_call(ws.clear)

def worksheet_update_safe(ws: gspread.Worksheet, rows):
    # При очень больших объёмах можно разбивать по чанкам, но update обычно справляется.
    return retry_call(ws.update, rows)

# =========================
# Чтение токенов/кабинетов
# =========================
source_book = open_spreadsheet_by_key_safe(client, SOURCE_SHEET_ID)
source_sheet = source_book.sheet1
rows = retry_call(source_sheet.get_all_values)
data = [{"token": row[0], "cabinet": row[1]} for row in rows[1:] if len(row) >= 2 and row[0].strip()]

# =========================
# Выгрузка заказов WB
# =========================
def fetch_orders(token: str, days: int = 14):
    """
    Пагинация по lastChangeDate. Соблюдаем лимит 1 req/min.
    Стабильно обрабатываем не-JSON ответы (WAF/503/HTML), не двигая курсор.
    """
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

    max_decode_retries = 3   # повторы при пустом/не-JSON ответе с 200
    base_sleep = 60          # базовая пауза (лимит 1 req/min)
    max_http_retries = 3     # повторы при 429/5xx

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
                    print("🛑 Превышены повторы сети. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            if resp.status_code in (429, 500, 502, 503, 504):
                wait = base_sleep * min(attempt, 4)
                print(f"⚠️ HTTP {resp.status_code}. Ждём {wait}s и повторяем ту же страницу. Фрагмент: {resp.text[:200]}")
                time.sleep(wait)
                if attempt >= max_http_retries:
                    print("🛑 Превышены повторы при 429/5xx. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            if resp.status_code == 401:
                print("❌ 401 Unauthorized — токен не подходит к statistics-api. Пропускаем кабинет.")
                return all_rows

            if resp.status_code != 200:
                print(f"⚠️ Неожиданный код {resp.status_code}. Тело (фрагмент): {resp.text[:300]}")
                time.sleep(base_sleep)
                if attempt >= max_http_retries:
                    print("🛑 Превышены повторы при не-200. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            ctype = (resp.headers.get("Content-Type") or "").lower()
            body = resp.text or ""
            if ("application/json" not in ctype) or (not body.strip()):
                print(f"⚠️ Ожидали JSON, получили Content-Type='{ctype}', len={len(body)}. Фрагмент: {body[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы декодирования JSON. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            try:
                chunk = resp.json()
            except Exception as e:
                print(f"⚠️ JSONDecodeError: {e}. Фрагмент тела: {body[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы декодирования JSON. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            if not isinstance(chunk, list):
                print(f"⚠️ Ожидали массив, получили {type(chunk)}. Фрагмент: {str(chunk)[:200]}")
                time.sleep(base_sleep)
                if attempt >= max_decode_retries:
                    print("🛑 Превышены повторы из-за неверного формата. Останавливаемся по текущему кабинету.")
                    return all_rows
                continue

            # валидный ответ
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
        print(f"⏳ Пауза {base_sleep}s (лимит API 1 req/min)…")
        time.sleep(base_sleep)

    return all_rows

# =========================
# Запись в Google Sheets
# =========================
def write_orders_to_sheet(sheet_obj: gspread.Spreadsheet, cabinet_name: str, orders: list):
    try:
        ws = get_worksheet_safe(sheet_obj, cabinet_name)

        if not orders:
            # Не затираем лист, если данных нет — просто лог.
            print(f"⚠️ Данных для '{cabinet_name}' нет. Лист не изменён.")
            return

        # Заголовки строго в порядке, как пришли с API
        headers = list(orders[0].keys())

        # Готовим все строки заранее
        rows = [headers]
        for order in orders:
            row = [order.get(h, "") for h in headers]
            rows.append(row)

        # Теперь очищаем и записываем атомарно
        worksheet_clear_safe(ws)
        worksheet_update_safe(ws, rows)
        print(f"✅ Сохранено {len(orders)} заказов в лист '{cabinet_name}'")

    except Exception as e:
        print(f"🛑 Ошибка при записи в лист '{cabinet_name}': {e}")

# =========================
# Точка входа
# =========================
def main():
    target_sheet = open_spreadsheet_by_key_safe(client, TARGET_SHEET_ID)

    for entry in data:
        cabinet = entry["cabinet"]
        token = entry["token"]
        print(f"\n🔄 Работаем с кабинетом: {cabinet}")

        orders = fetch_orders(token, days=14)
        write_orders_to_sheet(target_sheet, cabinet, orders)

        # Чуть разгрузим API между кабинетами
        time.sleep(2)

if __name__ == "__main__":
    main()
