import pandas as pd
from data_processing_main_req import *

def process_dataset(df, df_payment,df_grouped,combined_df,b, cash, df_appruv_range):
    # Всі замовлення без дублів та тестів
    df_new = df[~df['Статус'].isin(['testy', 'duplicate'])]

    # Табличка, яка містить кількість лідів
    leads = count_unique_orders(df_new, 'Кількість лідів')

    # Всі замовлення без треша, дублів, тестів
    df_new = df_new[~df_new['Статус'].isin(['trash'])]

    # Табличка, яка містить кількість чистих лідів
    clear_leads = count_unique_orders(df_new, 'Кількість чистих лідів')

    # Всі аппруви
    df_new = df_new[~df_new['Статус'].isin([
        'duplicate', 'testy', 'trash', 'new', 'perezvon-1', 'telegram', 'no-call',
        'cancel-other', 'peredumal', '1d-nedozvon', '2d-nedozvon', '3d-nedozvon'])]

    # Сума по замовленнях
    sum_per_order = sum_per_order_id(df_new)

    # Кількість аппрувів
    appruv = count_unique_orders(df_new, 'Кількість аппрувів')

    # З'єднання всього
    merged = merge_all_data(leads, clear_leads, sum_per_order, appruv)

    # Нові операції
    merged['% Аппрува'] = merged['Кількість аппрувів'] / merged['Кількість лідів'] * 100
    merged['Коэф. Апрува'] = merged['% Аппрува'].apply(lambda x: get_appruv_coefficient(x, df_appruv_range))
    merged['Средняя сумма в апрувах'] = merged['Сума'] / merged['Кількість аппрувів']
    try:
      merged['Лид от $'], merged['Лид до $'] = zip(*merged['Средняя сумма в апрувах'].map(lambda x: find_lead_range(x, df_payment)))
      merged['Лид до $'] = merged['Лид до $'].str.replace(',', '.').astype(float)
      merged['Коэф. Апрува'] = merged['Коэф. Апрува'].str.replace(',', '.').astype(float)
    except:
      pass

    #fb
    if cash == 1:
      merged = pd.merge(merged, df_grouped, how='left', left_on='offer_id(заказа)', right_on='offer_id')
      merged = pd.merge(merged, combined_df[['ID Оффера', 'Коэф. Слож.', 'Название оффера']], left_on='offer_id(заказа)', right_on='ID Оффера', how='left')
      merged = merged.dropna(subset=['offer_id(заказа)'])
    else:
      merged = merge_data(merged, df_grouped, b)
      # merged = pd.merge(merged, df_grouped, how='left', left_on='offer_id(заказа)', right_on='offer_id')
      merged = pd.merge(merged, combined_df[['ID Оффера', 'Коэф. Слож.', 'Название оффера']], left_on='offer_id(заказа)', right_on='ID Оффера', how='left')
      # merged = merged.dropna(subset=['offer_id(заказа)'])

    merged['Виплата баеру'] = merged['Средняя сумма в апрувах'] * 0.05 * 1000 * 0.000080
    
    return merged

def process_catalog(df, df_payment,df_grouped,combined_df,b, cash, df_appruv_range):
    df_catalog = df[df['offer_id(заказа)'].notna() & df['offer_id(заказа)'].str.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-[^0-9]{0,3}\d{0,3}[^0-9]{1,}$')]
    df_catalog = df_catalog[~df_catalog['Назва товару'].str.contains('оставка')]

    catalog_merged = process_dataset(df_catalog, df_payment,df_grouped,combined_df,b, cash, df_appruv_range)
    
    try:
        catalog_w_leads = catalog_merged[catalog_merged['offer_id'].str.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-[^0-9]{0,3}\d{0,3}[^0-9]{1,}$') & (~catalog_merged['offer_id'].isna())]
        catalog_cash = catalog_w_leads[(catalog_w_leads['Кількість лідів'].isna() & catalog_w_leads['spend'] !=0)] #без лідів зі спендом
        catalog_w_leads = catalog_w_leads.dropna(subset=['offer_id(заказа)'])
        catalog_cash = catalog_cash.rename(columns={'spend':'Рекл.спенд.'})
    except:
        catalog_w_leads = catalog_merged
        catalog_cash = pd.DataFrame()

    return catalog_w_leads, catalog_cash


def process_carspace(df, df_payment,df_grouped,combined_df,b, cash, df_appruv_range):
    df_car_space = df[df['offer_id(заказа)'].notna() & df['offer_id(заказа)'].str.contains('cs-')]
    df_car_space = df_car_space[~df_car_space['Назва товару'].str.contains('оставка')]
    if not df_car_space.empty:
        car_space_merged = process_dataset(df_car_space, df_payment,df_grouped,combined_df,b, cash, df_appruv_range)
        return car_space_merged
    else: 
       return df_car_space
    
