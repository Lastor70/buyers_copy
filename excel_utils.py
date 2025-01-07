import openpyxl

def save_data_to_excel(catalog_w_leads, car_space_merged, catalog_cash, merged_ss, result_df, total_vykup, b, start_date, end_date):
    file_path = 'data/Рассчет для баеров_template.xlsx'  
    wb1 = openpyxl.load_workbook(file_path)
    sh_paste = wb1['Лист1']
    sh_vykup = wb1['Vykup']
    sh_catalog = wb1['Catalog']
    sh_car_space = wb1['CS']

    column_mapping = {
        'Название оффера': 'B',
        'offer_id(заказа)': 'C',
        'Кількість лідів': 'D',
        'Кількість чистих лідів': 'E',
        'Кількість аппрувів': 'G',
        'Средняя сумма в апрувах': 'J',
        'Лид до $': 'L',
        'Коэф. Апрува': 'M',
        'spend': 'N',
        'leads': 'O',
    }

    def paste_data(df, mapping, sheet):
        for df_column, excel_column in mapping.items():
            if df_column in df.columns:  # Перевірка на наявність стовпця в DataFrame
                column_data = df[df_column]
                for row_idx, value in enumerate(column_data, start=1):
                    cell = sheet[f"{excel_column}{row_idx+3}"]
                    cell.value = value
                    cell._style = sheet[f"{excel_column}4"]._style

    map_cash = {
        'offer_id': 'AB',
        'Рекл.спенд.': 'AC',
        'Лидов из ads': 'AD'
    }

    map_vykup = {
        'offer_id(заказа)': 'C',
        'Название оффера': 'B',
        'Количество выкупов': 'D',
        '% Аппрува': 'E',
        'Коэф. Апрува': 'F',
        'Коэф. Слож.': 'G',
        'Виплата баеру': 'H',
    }

    # Збереження даних у Excel
    if not catalog_w_leads.empty:
        paste_data(catalog_w_leads, column_mapping, sh_catalog)
    if car_space_merged is not None and not car_space_merged.empty:
        paste_data(car_space_merged, column_mapping, sh_car_space)
    if not catalog_cash.empty:
        paste_data(catalog_cash, map_cash, sh_catalog)
    if not result_df.empty:
        paste_data(result_df, map_cash, sh_paste)

    paste_data(merged_ss, column_mapping, sh_paste)
    paste_data(total_vykup, map_vykup, sh_vykup)

    filename = f'{b}-рассчет_{start_date}-{end_date}.xlsx'
    wb1.save(filename)

    return filename  # Повертаємо шлях до збереженого файлу
