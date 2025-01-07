import numpy as np
import pandas as pd
from data_processing_main_req import *

def process_orders_data_vykup(df, combined_df, df_payment, df_appruv_range, df_grouped, b, merged_ss):
    """Обробляє отримані замовлення та форматує DataFrame."""
    
    mask = ['number', 'status', 'customFields', 'items']
    df_2 = df[mask]
    
    def get_item_data(items, key):
        data = []
        for item in items:
            if isinstance(item, dict) and 'offer' in item and key in item['offer']:
                data.append(item['offer'][key])
            else:
                data.append(None)
        return data

    df_items_expanded_1 = df_2.explode('items')

    df_items_expanded_1['price'] = df_items_expanded_1['items'].apply(lambda x: x['prices'][0]['price'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded_1['quantity'] = df_items_expanded_1['items'].apply(lambda x: x['prices'][0]['quantity'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded_1['externalId'] = df_items_expanded_1['items'].apply(lambda x: get_item_data([x], 'externalId')[0] if isinstance(x, dict) else None)
    df_items_expanded_1['name'] = df_items_expanded_1['items'].apply(lambda x: x['offer']['name'] if isinstance(x, dict) and 'offer' in x and 'name' in x['offer'] else None)
    df_items_expanded_1['item_buyer_id'] = df_items_expanded_1.apply(lambda x: x['customFields']['buyer_id'] if 'buyer_id' in x['customFields'] else None, axis=1)
    df_items_expanded_1['item_offer_id'] = df_items_expanded_1.apply(lambda x: x['customFields']['offer_id'] if 'offer_id' in x['customFields'] else None, axis=1)


    df_items_expanded_1 = df_items_expanded_1.rename(columns = {'number': 'Номер замовлення',
                        'status': 'Статус',
                        'externalId': 'Product_id',
                        'name': 'Назва товару',
                        'quantity': 'Кількість товару',
                        'price': 'Ціна товару',
                        'item_offer_id': 'offer_id(заказа)',
                        'item_buyer_id': 'buyer_id'})


    df_items_expanded_1.drop(['customFields', 'items'], axis = 1)

    df_v = df_items_expanded_1#[df_items_expanded_1['Статус'].isin(['payoff','complete', 'dostavlen-predvaritelno', 'given'])]
    df_v.dropna(subset=['Product_id'], inplace=True)
    df_v.dropna(subset=['buyer_id'], inplace=True)
    df_v['offer_id(товара)'] = df_v['Product_id'].apply(lambda x: '-'.join(x.split('-')[:3]))
    df_v['Загальна сума'] = df_v['Ціна товару'] * df_v['Кількість товару']

    df_v['Corresponding_Offer_Id_Found'] = df_v.apply(find_offer_id, args=(combined_df,), axis=1).fillna(0)
    if b != 'ph':
        df_v = df_v.loc[df_v['Corresponding_Offer_Id_Found'] == 1]

    avg_appruv_df = merged_ss[merged_ss['Кількість аппрувів'] >= 10]
    avg_appruv_value = avg_appruv_df['% Аппрува'].mean()

    df_vykup = df_v[df_v['Статус'].isin(['payoff'])]
    vykup_new = count_unique_orders(df_vykup, 'Количество выкупов')
    sum_vykup = sum_per_order_id(df_vykup)


    total_vykup = vykup_new.merge(sum_vykup, on='offer_id(заказа)', how='left')

    total_vykup = pd.merge(total_vykup, combined_df[['ID Оффера', 'Коэф. Слож.', 'Название оффера']], left_on='offer_id(заказа)', right_on='ID Оффера', how='left')
    total_vykup['% Аппрува'] = avg_appruv_value

    total_vykup['Коэф. Слож.'] = total_vykup['Коэф. Слож.'].fillna(1)

    try:
        total_vykup['Коэф. Апрува'] = total_vykup['% Аппрува'].apply(lambda x: get_appruv_coefficient(x, df_appruv_range)).str.replace(',', '.').astype(float)
        total_vykup['Виплата баеру'] = total_vykup['Сума'] * 0.05 * 1000 * 0.000080 * total_vykup['Коэф. Слож.'] * total_vykup['Коэф. Апрува']
    except:
        total_vykup['Коэф. Апрува'] = 0
        total_vykup['Виплата баеру'] = 0

    
    #catalog cs
    df_all_cs_catalog = df_items_expanded_1[df_items_expanded_1['offer_id(заказа)'].notna() & (df_items_expanded_1['offer_id(заказа)'].str.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-[^0-9]{0,3}\d{0,3}[^0-9]{1,}$') | df_items_expanded_1['offer_id(заказа)'].str.contains('cs'))]
    df_all_cs_catalog = df_all_cs_catalog.rename(columns = {'number': 'Номер замовлення',
                        'status': 'Статус',
                        'externalId': 'Product_id',
                        'name': 'Назва товару',
                        'quantity': 'Кількість товару',
                        'price': 'Ціна товару',
                        'item_offer_id': 'offer_id(заказа)',
                        'item_buyer_id': 'buyer_id'})


    df_all_cs_catalog = df_all_cs_catalog.drop(['customFields', 'items', 'Corresponding_Offer_Id_Found'], axis = 1)
    df_all_cs_catalog['offer_id(товара)'] = df_all_cs_catalog['Product_id'].apply(lambda x: '-'.join(x.split('-')[:3]))
    df_all_cs_catalog = df_all_cs_catalog[~df_all_cs_catalog['Назва товару'].str.contains('оставка')]
    df_all_cs_catalog['Загальна сума'] = df_all_cs_catalog['Ціна товару'] * df_all_cs_catalog['Кількість товару']

    return total_vykup, df_all_cs_catalog


def process_total_vykup(processed_vykups, df_all_cs_catalog, car_space_merged, catalog_w_leads, df_appruv_range):

    try:
        avg_appruv_df_cs = car_space_merged
        # avg_appruv_df_cs = car_space_merged[car_space_merged['Кількість аппрувів'] >= 10]
    except:
        avg_appruv_df_cs = car_space_merged

    avg_appruv_df_catalog = catalog_w_leads[catalog_w_leads['Кількість аппрувів'] >= 10]

    #окремі значення апрува дл кс та каталокжи
    try:
        avg_appruv_cs_value = avg_appruv_df_cs['% Аппрува'].mean()
    except:
        avg_appruv_cs_value = 0
    avg_appruv_catalog_value = avg_appruv_df_catalog['% Аппрува'].mean()

    df_vykup_cs_catalog = df_all_cs_catalog[df_all_cs_catalog['Статус'].isin(['payoff'])]
    vykup_new_cs_catalog = count_unique_orders(df_vykup_cs_catalog, 'Количество выкупов')
    sum_vykup_cs_catalog = sum_per_order_id(df_vykup_cs_catalog)

    total_vykup_cs_catalog = vykup_new_cs_catalog.merge(sum_vykup_cs_catalog, on='offer_id(заказа)', how='left')
    total_vykup_cs_catalog['Коэф. Слож.'] = 1.0
    #окремі значення % апрува дл кс та каталокжи
    total_vykup_cs_catalog['% Аппрува'] = np.where(
        total_vykup_cs_catalog['offer_id(заказа)'].str.startswith('cs'),
        avg_appruv_cs_value,
        avg_appruv_catalog_value
    )

    try:
        total_vykup_cs_catalog['Коэф. Апрува'] = total_vykup_cs_catalog['% Аппрува'].apply(lambda x: get_appruv_coefficient(x, df_appruv_range)).str.replace(',', '.').astype(float)
        total_vykup_cs_catalog['Виплата баеру'] = total_vykup_cs_catalog['Сума'] * 0.05 * 1000 * 0.000080 * total_vykup_cs_catalog['Коэф. Слож.'] * total_vykup_cs_catalog['Коэф. Апрува']
    except:
        total_vykup_cs_catalog['Коэф. Апрува'] = 0
        total_vykup_cs_catalog['Виплата баеру'] = 0

    total_vykup = pd.concat([processed_vykups, total_vykup_cs_catalog], ignore_index=True)
    return total_vykup