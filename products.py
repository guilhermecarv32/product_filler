import requests
import psycopg2
from psycopg2 import sql

# requisição à API
# response = requests.get('https://apiv4.ordering.co/v400/en/demo/products')
# data = response.json()

# retornar resultado
# resultado = data.get('result', [])
# print(resultado)

def insert_product(cursor, product):
    insert_query = sql.SQL(""" 
        INSERT INTO produtos (
            id, name, price, description, images, sku, category_id,
            inventoried, quantity, featured, enabled, upselling,
            in_offer, offer_price, rank, offer_rate, offer_rate_type,
            offer_include_options, external_id, barcode, barcode_alternative,
            estimated_person, tax_id, fee_id, slug, seo_image, seo_title,
            seo_description, seo_keywords, cost_price, cost_offer_price,
            weight, calories, weight_unit, hide_special_instructions,
            maximum_per_order, minimum_per_order, duration, type,
            load_type, updated_at, created_at, deleted_at, is_hidden,
            is_alcohol, snooze_until, favorite
        ) VALUES (
            %(id)s, %(name)s, %(price)s, %(description)s, %(images)s, %(sku)s,
            %(category_id)s, %(inventoried)s, %(quantity)s, %(featured)s,
            %(enabled)s, %(upselling)s, %(in_offer)s, %(offer_price)s,
            %(rank)s, %(offer_rate)s, %(offer_rate_type)s,
            %(offer_include_options)s, %(external_id)s, %(barcode)s,
            %(barcode_alternative)s, %(estimated_person)s, %(tax_id)s,
            %(fee_id)s, %(slug)s, %(seo_image)s, %(seo_title)s,
            %(seo_description)s, %(seo_keywords)s, %(cost_price)s,
            %(cost_offer_price)s, %(weight)s, %(calories)s, %(weight_unit)s,
            %(hide_special_instructions)s, %(maximum_per_order)s,
            %(minimum_per_order)s, %(duration)s, %(type)s, %(load_type)s,
            %(updated_at)s, %(created_at)s, %(deleted_at)s, %(is_hidden)s,
            %(is_alcohol)s, %(snooze_until)s, %(favorite)s
        )
        ON CONFLICT (id) DO UPDATE
        SET 
            name = EXCLUDED.name,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            images = EXCLUDED.images,
            sku = EXCLUDED.sku,
            category_id = EXCLUDED.category_id,
            inventoried = EXCLUDED.inventoried,
            quantity = EXCLUDED.quantity,
            featured = EXCLUDED.featured,
            enabled = EXCLUDED.enabled,
            upselling = EXCLUDED.upselling,
            in_offer = EXCLUDED.in_offer,
            offer_price = EXCLUDED.offer_price,
            rank = EXCLUDED.rank,
            offer_rate = EXCLUDED.offer_rate,
            offer_rate_type = EXCLUDED.offer_rate_type,
            offer_include_options = EXCLUDED.offer_include_options,
            external_id = EXCLUDED.external_id,
            barcode = EXCLUDED.barcode,
            barcode_alternative = EXCLUDED.barcode_alternative,
            estimated_person = EXCLUDED.estimated_person,
            tax_id = EXCLUDED.tax_id,
            fee_id = EXCLUDED.fee_id,
            slug = EXCLUDED.slug,
            seo_image = EXCLUDED.seo_image,
            seo_title = EXCLUDED.seo_title,
            seo_description = EXCLUDED.seo_description,
            seo_keywords = EXCLUDED.seo_keywords,
            cost_price = EXCLUDED.cost_price,
            cost_offer_price = EXCLUDED.cost_offer_price,
            weight = EXCLUDED.weight,
            calories = EXCLUDED.calories,
            weight_unit = EXCLUDED.weight_unit,
            hide_special_instructions = EXCLUDED.hide_special_instructions,
            maximum_per_order = EXCLUDED.maximum_per_order,
            minimum_per_order = EXCLUDED.minimum_per_order,
            duration = EXCLUDED.duration,
            type = EXCLUDED.type,
            load_type = EXCLUDED.load_type,
            updated_at = EXCLUDED.updated_at,
            created_at = EXCLUDED.created_at,
            deleted_at = EXCLUDED.deleted_at,
            is_hidden = EXCLUDED.is_hidden,
            is_alcohol = EXCLUDED.is_alcohol,
            snooze_until = EXCLUDED.snooze_until,
            favorite = EXCLUDED.favorite;
    """)

    cursor.execute(insert_query, product)

def main():
    try:
        # conexão com o banco de dados
        conn = psycopg2.connect(
            host='localhost',
            database='products',
            user='postgres',
            password='123456'
        )
        cursor = conn.cursor()

        # requisição à API
        response = requests.get('https://apiv4.ordering.co/v400/en/demo/products')
        data = response.json()
        print(data)

        # insere cada produto
        for product in data['result']:
            print(f"Inserindo produto: {product.get('name', 'Unknown')}")
            try:
                insert_product(cursor, product)
            except Exception as e:
                print(f"Erro ao inserir produto {product.get('name', 'Unknown')}: {e}")

        conn.commit()

    except Exception as e:
        print(f"Erro na conexão ou na execução: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
