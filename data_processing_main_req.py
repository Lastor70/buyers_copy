import pandas as pd

def add_match_column(df, tovar_id, zakaz_id):
    df.reset_index(drop=True, inplace=True)
    df['Match'] = (df[tovar_id] == df[zakaz_id]).astype(int)

    return df

def find_offer_id(row, df):
    offer_id_order = row['offer_id(заказа)']
    offer_id_product = row['offer_id(товара)']

    if offer_id_product == offer_id_order:
        return 1
    else:
        if offer_id_order in df['ID Оффера'].values:
            corresponding_row = df[df['ID Оффера'] == offer_id_order]
            product_columns = corresponding_row.columns[1:]
            for column in product_columns:
                if offer_id_product == corresponding_row[column].values[0]:
                    return 1
            return 0
        else:
            return 0

def sum_per_order_id(df):
    sum_per_order = df.groupby('offer_id(заказа)').agg({'Загальна сума': 'sum', 'Назва товару': 'first'}).reset_index()
    sum_per_order.columns = ['offer_id(заказа)', 'Сума', 'Назва товару']
    return sum_per_order

def merge_all_data(leads, clear_leads, sum_per_order, appruv):
    merged_data = leads.merge(clear_leads, on='offer_id(заказа)', how='left') \
                      .merge(sum_per_order, on='offer_id(заказа)', how='left') \
                      .merge(appruv, on='offer_id(заказа)', how='left') \
                      # .merge(count_without_comparison, on='offer_id(заказа)', how='left') \
                      # .merge(count_with_comparison, on='offer_id(заказа)', how='left') \
                      # .merge(vykup, on='offer_id(заказа)', how='left')

    # merged_data['середня сума'] = merged_data['Сума'] / merged_data['Кількість чистих лідів']
    merged_data['% Аппрува'] = merged_data['Кількість аппрувів'] / merged_data['Кількість лідів'] *100

    return merged_data

def count_unique_orders(df, column_name):
    unique_orders_counts = df.groupby('offer_id(заказа)')['Номер замовлення'].nunique().reset_index(name=column_name)
    return unique_orders_counts

def get_appruv_coefficient(approval_percent, df_appruv_range):
    for range_str, coefficient in zip(df_appruv_range['Диапазон апрува'], df_appruv_range['Бонус/Вычет от чистой выплаты']):
        if not range_str:  # перевірка на порожній рядок
            continue
        if '<' in range_str:
            try:
                if approval_percent < float(range_str[1:]):
                    return coefficient
            except ValueError:
                continue
        elif '>' in range_str:
            try:
                if approval_percent > float(range_str[1:]):
                    return coefficient
            except ValueError:
                continue
        else:
            try:
                range_start, range_end = map(float, range_str.split('-'))
                if range_start <= approval_percent <= range_end:
                    return coefficient
            except ValueError:
                continue
    return None

def find_lead_range(average_sum, df_payment):
    if pd.isna(average_sum):
        first_row = df_payment.iloc[0]
        return first_row['Лид от $'], first_row['Лид до $']

    last_value = df_payment['Сумма по товарам(вкл.)'].iloc[-1]

    for index, row in df_payment.iterrows():
        if row['Сумма по товарам(вкл.)'] >= last_value:
            return row['Лид от $'], row['Лид до $']
        elif average_sum <= row['Сумма по товарам(вкл.)']:
            return row['Лид от $'], row['Лид до $']

    return None, None

def calculate_payout(order_avg_total, df_payment):
    for idx, payout_row in df_payment.iterrows():
        if order_avg_total < payout_row['Сумма по товарам(вкл.)']:
            return payout_row['Выплата за выкуп(ставка)']
    if order_avg_total >= df_payment.iloc[-1]['Сумма по товарам(вкл.)']:
        return df_payment.iloc[-1]['Выплата за выкуп(ставка)']
    return None

def merge_data(df, all_fb, user_prefix):
    try:
        if df.empty:
            print("Порожній датафрейм")
            return df
        filtered_all_fb = all_fb[all_fb['offer_id'].str[3:5] == user_prefix]
        df = df.merge(filtered_all_fb, how='outer', left_on='offer_id(заказа)', right_on='offer_id')
        df = df[~(df['Кількість лідів'].isna() & (df['Рекл.спенд.'] == 0))]

        return df
    except Exception as e:
        # print(f"Помилка при злитті датафреймів: {e}")
        return df


def process_orders_data(df, combined_df, df_payment, df_appruv_range, df_grouped, b):
    """Обробляє отримані замовлення та форматує DataFrame."""
    
    mask = ['number', 'status', 'createdAt', 'customFields', 'items']
    df2 = df[mask]

    def get_item_data(items, key):
        data = []
        for item in items:
            if isinstance(item, dict) and 'offer' in item and key in item['offer']:
                data.append(item['offer'][key])
            else:
                data.append(None)
        return data

    df_items_expanded = df2.explode('items')

    df_items_expanded['price'] = df_items_expanded['items'].apply(lambda x: x['prices'][0]['price'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded['quantity'] = df_items_expanded['items'].apply(lambda x: x['prices'][0]['quantity'] if isinstance(x, dict) and 'prices' in x and x['prices'] else None)
    df_items_expanded['externalId'] = df_items_expanded['items'].apply(lambda x: get_item_data([x], 'externalId')[0] if isinstance(x, dict) else None)
    df_items_expanded['name'] = df_items_expanded['items'].apply(lambda x: x['offer']['name'] if isinstance(x, dict) and 'offer' in x and 'name' in x['offer'] else None)
    df_items_expanded['item_buyer_id'] = df_items_expanded.apply(lambda x: x['customFields']['buyer_id'] if 'buyer_id' in x['customFields'] else None, axis=1)
    df_items_expanded['item_offer_id'] = df_items_expanded.apply(lambda x: x['customFields']['offer_id'] if 'offer_id' in x['customFields'] else None, axis=1)
    df_items_expanded = df_items_expanded.rename(columns={
        'number': 'Номер замовлення',
        'status': 'Статус',
        'createdAt': 'Дата создания',
        'externalId': 'Product_id',
        'name': 'Назва товару',
        'quantity': 'Кількість товару',
        'price': 'Ціна товару',
        'item_offer_id': 'offer_id(заказа)',
        'item_buyer_id': 'buyer_id'
    })

    df_items_expanded.drop(['customFields', 'items'], axis=1, inplace=True)

    df_items_expanded.dropna(subset=['Product_id'], inplace=True)
    df_items_expanded.dropna(subset=['buyer_id'], inplace=True)
    df_items_expanded['offer_id(товара)'] = df_items_expanded['Product_id'].apply(lambda x: '-'.join(x.split('-')[:3]))
    df_items_expanded['Загальна сума'] = df_items_expanded['Ціна товару'] * df_items_expanded['Кількість товару']

    desired_column_order = ['Номер замовлення', 'Статус', 'offer_id(товара)', 'Product_id', 'Назва товару', 'Кількість товару', 'Ціна товару', 'Загальна сума', 'offer_id(заказа)', 'buyer_id']

    df_items_expanded = df_items_expanded.reindex(columns=desired_column_order)

    ss_dataset = df_items_expanded

    ss_dataset = add_match_column(ss_dataset, 'offer_id(товара)', 'offer_id(заказа)')
    
    ss_dataset['Corresponding_Offer_Id_Found'] = ss_dataset.apply(find_offer_id, args=(combined_df,), axis=1).fillna(0)
    # ss_dataset = ss_dataset.loc[ss_dataset['Corresponding_Offer_Id_Found'] == 1]
    if  b == 'dn':
        ss_dataset = ss_dataset\
        .assign(cor_sum = lambda x: x.groupby('Номер замовлення')['Corresponding_Offer_Id_Found'].transform('sum'))
    else:
        ss_dataset = ss_dataset\
        .assign(cor_sum = lambda x: x.groupby('Номер замовлення')['Corresponding_Offer_Id_Found'].transform('sum'))\
        .query('cor_sum > 0')

    ss_new = ss_dataset[~ss_dataset['Статус'].isin(['testy','duplicate'])]

    #тута табличка яка містить кількість лідів
    leads_ss = count_unique_orders(ss_new, 'Кількість лідів')

    #тута всі закази без треша, дублів, тестів. І по цьому рахуємо чисті ліди
    ss_new = ss_new[~ss_new['Статус'].isin(['trash'])]

    #тута табличка яка містить кількість чистих лідів
    clear_leads_ss = count_unique_orders(ss_new, 'Кількість чистих лідів')

    #тута усі аппруви
    ss_new = ss_new[~ss_new['Статус'].isin(['duplicate',
    'testy', 'trash', 'new', 'perezvon-1', 'telegram', 'no-call', 'cancel-other', 'peredumal',
    '1d-nedozvon','2d-nedozvon','3d-nedozvon'])]

    #тута у нас сума по заказах
    sum_per_order_ss = sum_per_order_id(ss_new)

    #тута кількість аппрувів
    appruv_ss = count_unique_orders(ss_new, 'Кількість аппрувів')

    merged_ss = merge_all_data(leads_ss, clear_leads_ss, sum_per_order_ss, appruv_ss)

    merged_ss['% Аппрува'] = merged_ss['Кількість аппрувів'] / merged_ss['Кількість лідів'] * 100
    merged_ss['Коэф. Апрува'] = merged_ss['% Аппрува'].apply(lambda x: get_appruv_coefficient(x, df_appruv_range))
    merged_ss['Средняя сумма в апрувах'] = merged_ss['Сума'] / merged_ss['Кількість аппрувів']
    merged_ss['Лид от $'], merged_ss['Лид до $'] = zip(*merged_ss['Средняя сумма в апрувах'].map(lambda x: find_lead_range(x, df_payment)))
    merged_ss['Виплата баеру'] = merged_ss['Средняя сумма в апрувах'] * 0.05 * 1000 * 0.000080


    merged_ss = merge_data(merged_ss, df_grouped, b)

    merged_ss = pd.merge(merged_ss, combined_df[['ID Оффера', 'Коэф. Слож.', 'Название оффера']], left_on='offer_id(заказа)', right_on='ID Оффера', how='left')
    merged_ss['Лид до $'] = merged_ss['Лид до $'].str.replace(',', '.').astype(float)
    merged_ss['Коэф. Апрува'] = merged_ss['Коэф. Апрува'].str.replace(',', '.').astype(float)
    
    spend_wo_leads = merged_ss[merged_ss['offer_id(заказа)'].isna() & merged_ss['spend']>0]
    
    merged_ss = merged_ss[merged_ss['offer_id(заказа)'].str.match(r'^[a-zA-z]{2}-[a-zA-z]{2}-\d{4}$') & merged_ss['offer_id(заказа)'].notna()]  #прибираю категорії
    # print(merged_ss[merged_ss['offer_id(заказа)'] == 'ss-il-0071'])
    spend_wo_leads = spend_wo_leads[spend_wo_leads['offer_id'].str.match(r'^[a-zA-Z]{2}-[a-zA-Z]{2}-\d{4}$', na=False)]
    spend_wo_leads = spend_wo_leads.rename(columns = {'spend':'Рекл.спенд.','leads': 'Лидов из ads'})
    print(spend_wo_leads)


    merged_ss = merged_ss[merged_ss['offer_id(заказа)'].notna()]
    return merged_ss, spend_wo_leads, df_items_expanded