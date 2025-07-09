import pandas as pd
import re
import os
import glob

class MagentoUnassignedSKUExtractor:
    SEARCH_DIRS = ['.', 'exports', 'data', 'csv', 'downloads']
    FILE_PATTERN = "export_catalog_product_*.csv"

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
        latest_file = max(files, key=os.path.getmtime)
        return latest_file

    def get_assigned_skus(self):
        assigned_skus = set()
        for row in self.df.itertuples(index=False):
            if row.product_type == 'configurable' and row.configurable_variations:
                matches = re.findall(r'sku=([^,|]+)', row.configurable_variations)
                assigned_skus.update(matches)
        return assigned_skus
    
    def get_unassigned_simple_skus(self):
        assigned_skus = self.get_assigned_skus()
        simple_products = self.df[self.df['product_type'] == 'simple']

        filtered = simple_products[
            (simple_products['product_online'] == '1') &
            (~simple_products['sku'].str.endswith('-NS')) &
            (~simple_products['sku'].str.endswith('-Adjustable'))
        ]

        unassigned = filtered[~filtered['sku'].isin(assigned_skus)]

        return unassigned[['sku', 'name', 'price', 'product_online', 'visibility', 'categories']]

    def run(self):
        unassigned_df = self.get_unassigned_simple_skus()
        if not unassigned_df.empty:
            with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                unassigned_df.to_excel(writer, index=False, sheet_name='Unassigned Variants')
            print(f"✅ {len(unassigned_df)} unassigned simple SKUs exported to '{self.output_file}'")
        else:
            print("✅ No unassigned simple SKUs found.")

if __name__ == '__main__':
    extractor = MagentoUnassignedSKUExtractor()
    extractor.run()
