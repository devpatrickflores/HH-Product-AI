import pandas as pd
import re
import os
import glob

class MagentoParentProductCreator:
    SEARCH_DIRS = ['.', 'exports', 'data', 'csv', 'downloads']
    FILE_PATTERN = "export_catalog_product_*.csv"
    SIZE_SUFFIXES = ['SM', 'S-M', 'S/M', 'ML', 'M-L', 'M/L', 'LXL']
    SIZE_PRIORITY = ['SM', 'S-M', 'S/M', 'ML', 'M-L', 'M/L', 'LXL']

    RD_COLUMNS = [
        'rd_ca_angel_numbers', 'rd_ca_cat_name', 'rd_ca_collection', 'rd_ca_dept_name',
        'rd_ca_div_name', 'rd_ca_finish', 'rd_ca_gauge', 'rd_ca_gemstone',
        'rd_ca_horoscope', 'rd_ca_initials', 'rd_ca_material', 'rd_ca_metal',
        'rd_ca_plating', 'rd_ca_sub_category'
    ]

    def __init__(self):
        self.input_file = self._find_latest_magento_file()
        if not self.input_file:
            raise FileNotFoundError("No Magento CSV export file found.")
        print(f"📄 Loaded file: {self.input_file}")
        self.df = pd.read_csv(self.input_file, dtype=str).fillna("")
        self.output_file = 'rings-parent-to-import.xlsx'

    def _find_latest_magento_file(self):
        files = []
        for directory in self.SEARCH_DIRS:
            files.extend(glob.glob(os.path.join(directory, self.FILE_PATTERN)))
        if not files:
            return None
        return max(files, key=os.path.getmtime)

    def get_assigned_skus(self):
        assigned_skus = set()
        for row in self.df.itertuples(index=False):
            if row.product_type == 'configurable' and row.configurable_variations:
                matches = re.findall(r'sku=([^,|]+)', row.configurable_variations)
                assigned_skus.update(matches)
        return assigned_skus

    def extract_size(self, name):
        # Extract size suffix at the end of the product name, e.g. "GOLD RING - SM"
        for sfx in self.SIZE_SUFFIXES:
            if name.strip().endswith(f" {sfx}"):
                return sfx
        return ""

    def base_name(self, name):
        # Remove size suffix from the name
        size = self.extract_size(name)
        if size:
            # Remove ' - SIZE' or ' SIZE' at the end
            return re.sub(rf"[\s-]+{re.escape(size)}$", "", name.strip())
        return name.strip()

    def get_unassigned_simple_skus(self):
        assigned_skus = self.get_assigned_skus()
        simple = self.df[self.df['product_type'] == 'simple']
        filtered = simple[
            (simple['product_online'] == '1') &
            (~simple['sku'].str.endswith('-NS')) &
            (~simple['sku'].str.endswith('-Adjustable')) &
            (simple['visibility'] == 'Catalog, Search')
        ]
        unassigned = filtered[~filtered['sku'].isin(assigned_skus)].copy()

        # Add base name and size columns
        unassigned['size'] = unassigned['name'].apply(self.extract_size)
        unassigned['base_name'] = unassigned['name'].apply(self.base_name)
        return unassigned

    def create_parents(self, unassigned):
        parent_rows = []

        # Normalize sizes for reliable ranking and matching
        def normalize_size(s):
            if not s:
                return ''
            return s.strip().upper().replace('/', '-').replace(' ', '')

        SIZE_PRIORITY = [s.upper().replace('/', '-') for s in self.SIZE_PRIORITY]

        # Group by base_name
        grouped = unassigned.groupby('base_name')

        for base_name, group in grouped:
            if len(group) < 2:
                # Need at least 2 variants to create a parent
                continue

            group = group.copy()

            # Normalize sizes
            group['size'] = group['size'].apply(normalize_size)

            # Assign size rank for sorting
            group['size_rank'] = group['size'].apply(
                lambda x: SIZE_PRIORITY.index(x) if x in SIZE_PRIORITY else 999
            )
            group = group.sort_values('size_rank')

            smallest = group.iloc[0]

            # Remove size suffix from SKU, normalize suffix similarly
            def remove_size_suffix_from_sku(sku):
                sku_norm = sku.upper().strip()
                pattern = r'[-_]?(' + '|'.join(SIZE_PRIORITY) + r')$'
                return re.sub(pattern, '', sku_norm, flags=re.I)

            base_sku = remove_size_suffix_from_sku(smallest['sku'])
            parent_sku = f'P-{base_sku}'

            parent_name = base_name

            variations = [f"sku={row['sku']},size={row['size']}" for _, row in group.iterrows()]
            associated_skus = ','.join(group['sku'].tolist())

            rd_data = {col: smallest.get(col, '') for col in self.RD_COLUMNS}

            parent_row = {
                'sku': parent_sku,
                'store_view_code': smallest.get('store_view_code', ''),
                'attribute_set_code': smallest.get('attribute_set_code', ''),
                'product_type': 'configurable',
                'categories': smallest.get('categories', ''),
                'product_websites': smallest.get('product_websites', ''),
                'product_online': '1',
                'name': parent_name,
                'description': smallest.get('description', ''),
                'short_description': smallest.get('short_description', ''),
                'tax_class_name': smallest.get('tax_class_name', ''),
                'visibility': 'Catalog, Search',
                'price': smallest.get('price', ''),
                'special_price': smallest.get('special_price', ''),
                'special_price_from_date': smallest.get('special_price_from_date', ''),
                'special_price_to_date': smallest.get('special_price_to_date', ''),
                'meta_title': smallest.get('meta_title', ''),
                'meta_keywords': smallest.get('meta_keywords', ''),
                'meta_description': smallest.get('meta_description', ''),
                'base_image': smallest.get('base_image', ''),
                'base_image_label': smallest.get('base_image_label', ''),
                'small_image': smallest.get('small_image', ''),
                'small_image_label': smallest.get('small_image_label', ''),
                'thumbnail_image': smallest.get('thumbnail_image', ''),
                'thumbnail_image_label': smallest.get('thumbnail_image_label', ''),
                'swatch_image': smallest.get('swatch_image', ''),
                'swatch_image_label': smallest.get('swatch_image_label', ''),
                'hover': smallest.get('hover', ''),
                'created_at': smallest.get('created_at', ''),
                'updated_at': smallest.get('updated_at', ''),
                'additional_attributes': smallest.get('additional_attributes', ''),
                'qty': '0',
                'out_of_stock_qty': '0',
                'use_config_min_qty': '1',
                'is_qty_decimal': '0',
                'allow_backorders': '0',
                'use_config_backorders': '1',
                'min_cart_qty': '1',
                'use_config_min_sale_qty': '1',
                'max_cart_qty': '0',
                'use_config_max_sale_qty': '1',
                'is_in_stock': '1',
                'notify_on_stock_below': '1',
                'use_config_notify_stock_qty': '1',
                'manage_stock': '1',
                'use_config_manage_stock': '1',
                'use_config_qty_increments': '1',
                'qty_increments': '0',
                'use_config_enable_qty_inc': '0',
                'enable_qty_increments': '0',
                'is_decimal_divided': '0',
                'website_id': '1',
                'related_skus': '',
                'crosssell_skus': '',
                'upsell_skus': '',
                'additional_images': smallest.get('additional_images', ''),
                'additional_image_labels': smallest.get('additional_image_labels', ''),
                'configurable_variations': '|'.join(variations),
                'configurable_variation_labels': 'size=Size',
                'associated_skus': associated_skus,
            }

            parent_row.update(rd_data)

            parent_rows.append(parent_row)

        return pd.DataFrame(parent_rows)


    def run(self):
        unassigned = self.get_unassigned_simple_skus()
        if unassigned.empty:
            print("✅ No unassigned simple SKUs found.")
            return

        parent_df = self.create_parents(unassigned)

        # Drop helper columns before output
        unassigned = unassigned.drop(columns=['base_name', 'size'], errors='ignore')

        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            unassigned.to_excel(writer, index=False, sheet_name='Unassigned Variants')
            if not parent_df.empty:
                parent_df.to_excel(writer, index=False, sheet_name='Parent Products')
                print(f"✅ {len(parent_df)} parent products exported.")
            else:
                print("✅ No parent products generated.")

        print(f"📁 Excel file saved as: {self.output_file}")


if __name__ == '__main__':
    MagentoParentProductCreator().run()
