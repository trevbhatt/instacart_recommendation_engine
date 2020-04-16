import pandas as pd
import sqlite3

def suggest_combo(product_name,
            day_of_week,
            hour_of_day,
            source_db='instacart.db',
            n_recs=3,
            percent=True):
    '''
    product_name: str--name of product. must match record in database
    day of week: int--day of week, with 0 being sunday
    hour_of_day: int--hour of day, with 0 being midnight
    n_rec: int--number of recommendations to include
    percent: boolean--if true, uses percent (rather than absolute) to find recommendations
    '''


    # connect to source db
    conn = sqlite3.connect(source_db)
    print(f'\n Combined suggestions for {product_name}, day: {day_of_week} hour: {hour_of_day}')

    # get list of orders that include the product name
    orders = pd.read_sql_query(f"SELECT \
                                            order_products.product_id,\
                                            products.product_name,\
                                            COUNT(order_products.order_id) AS qty\
                                FROM order_products\
                                LEFT JOIN products\
                                    ON order_products.product_id = products.product_id\
                                LEFT JOIN orders \
                                    ON order_products.order_id = orders.order_id \
                                WHERE order_products.order_id IN \
                                            (SELECT \
                                                        order_products.order_id \
                                            FROM order_products \
                                            LEFT JOIN products \
                                                ON order_products.product_id = products.product_id \
                                            LEFT JOIN orders \
                                                ON order_products.order_id = orders.order_id \
                                            WHERE product_name LIKE '{product_name}' \
                                            OR (orders.order_dow = {day_of_week}  \
                                                AND orders.order_hour_of_day = {hour_of_day})) \
                                GROUP BY order_products.product_id \
                                ORDER BY qty DESC",
                               conn)

    # calclulate based on percent
    if percent:
            totals = pd.read_sql_query('SELECT product_id, \
                                               COUNT(order_id) AS total_qty \
                                        FROM order_products \
                                        GROUP BY product_id',
                                       conn)

            orders_totals = pd.merge(orders, totals, how='left', left_on='product_id', right_on='product_id')

            orders['qty'] = orders['qty'] / orders_totals['total_qty']
            orders = orders.sort_values('qty')

    # Remove duplicate suggestions
    orders = orders.drop(orders[orders['product_name'].str.contains(product_name, case=False)].index, axis=0)
    suggestion = orders.drop('qty', axis=1)
    # Print results
    print(suggestion.head(n_recs))

    # Close the connection
    conn.close()

    return suggestion.head(n_recs)

def suggest_similar(product_name,
            source_db='instacart.db',
            n_recs=3):
    '''
    product_name: str--name of product. must match record in database
    day of week: int--day of week, with 0 being sunday
    hour_of_day: int--hour of day, with 0 being midnight
    '''


    # connect to source db
    conn = sqlite3.connect(source_db)
    print(f'\n Similar suggestions for {product_name}')

    # get list of orders that include the product name
    orders = pd.read_sql_query(f"SELECT \
                                            order_products.product_id,\
                                            products.product_name,\
                                            COUNT(order_products.order_id) AS qty\
                                FROM order_products\
                                LEFT JOIN products\
                                    ON order_products.product_id = products.product_id\
                                LEFT JOIN orders \
                                    ON order_products.order_id = orders.order_id \
                                WHERE order_products.order_id IN \
                                            (SELECT \
                                                        order_products.order_id \
                                            FROM order_products \
                                            LEFT JOIN products \
                                ON order_products.product_id = products.product_id \
                                WHERE product_name LIKE '{product_name}') \
                                GROUP BY order_products.product_id \
                                ORDER BY qty DESC",
                               conn)


    # Remove duplicate suggestions
    orders = orders.drop(orders[orders['product_name'].str.contains(product_name, case=False)].index, axis=0)
    suggestion = orders.drop('qty', axis=1)
    # Print results
    print(suggestion.head(n_recs))

    # Close the connection
    conn.close()

    return suggestion.head(n_recs)

def suggest_time(product_name,
            day_of_week,
            hour_of_day,
            source_db='instacart.db',
            n_recs=3,
            percent=True):
    '''
    product_name: str--name of product. must match record in database
    day of week: int--day of week, with 0 being sunday
    hour_of_day: int--hour of day, with 0 being midnight
    '''


    # connect to source db
    conn = sqlite3.connect(source_db)
    print(f'\n Time-based suggestions for {product_name}, day: {day_of_week} hour: {hour_of_day}')

    # get list of orders that include the product name
    orders = pd.read_sql_query(f"SELECT \
                                            order_products.product_id,\
                                            products.product_name,\
                                            COUNT(order_products.order_id) AS qty\
                                FROM order_products\
                                LEFT JOIN products\
                                    ON order_products.product_id = products.product_id\
                                LEFT JOIN orders \
                                    ON order_products.order_id = orders.order_id \
                                WHERE order_products.order_id IN \
                                            (SELECT \
                                                        order_products.order_id \
                                            FROM order_products \
                                            LEFT JOIN products \
                                                ON order_products.product_id = products.product_id \
                                            LEFT JOIN orders \
                                                ON order_products.order_id = orders.order_id \
                                            WHERE orders.order_dow = {day_of_week}  \
                                                AND orders.order_hour_of_day = {hour_of_day}) \
                                GROUP BY order_products.product_id \
                                ORDER BY qty DESC",
                               conn)

    # calculate based on percent
    if percent:
        totals = pd.read_sql_query('SELECT product_id, \
                                                   COUNT(order_id) AS total_qty \
                                            FROM order_products \
                                            GROUP BY product_id',
                                   conn)

        orders_totals = pd.merge(orders, totals, how='left', left_on='product_id', right_on='product_id')

        orders['qty'] = orders['qty'] / orders_totals['total_qty']
        orders = orders.sort_values('qty')

    # Remove duplicate suggestions
    orders = orders.drop(orders[orders['product_name'].str.contains(product_name, case=False)].index, axis=0)
    suggestion = orders.drop('qty', axis=1)
    # Print results
    print(suggestion.head(n_recs))

    # Close the connection
    conn.close()

    return suggestion.head(n_recs)

def test_suggestions(product_name,
            day_of_week,
            hour_of_day,
            source_db='/Users/trevor/Documents/Earn/Job Applications/cascade_data_labs/technical interview/Instacart Prompt/instacart.db',
            n_recs=3,
            percent=True):
    '''
     product_name: str--name of product. must match record in database
     day of week: int--day of week, with 0 being sunday
     hour_of_day: int--hour of day, with 0 being midnight
     n_rec: int--number of recommendations to include
     percent: boolean--if true, uses percent (rather than absolute) to find recommendations
     '''

    suggest_time(product_name, day_of_week, hour_of_day, source_db, n_recs, percent)

    suggest_similar(product_name, source_db, n_recs)

    suggest_combo(product_name, day_of_week, hour_of_day, source_db, n_recs, percent)