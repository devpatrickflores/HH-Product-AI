import pandas as pd
import glob
import os

class MagentoParentProductUpdater:
    SEARCH_DIRS = ['.', 'exports', 'data', 'csv', 'downloads']
    FILE_PATTERN = "export_catalog_product_*.csv"
    OUTPUT_FILE = "parent-rd-attributes-to-import.xlsx"

    RD_COLUMNS = [
        'rd_ca_angel_numbers', 'rd_ca_cat_name', 'rd_ca_collection', 'rd_ca_dept_name',
        'rd_ca_div_name', 'rd_ca_finish', 'rd_ca_gauge', 'rd_ca_gemstone',
        'rd_ca_horoscope', 'rd_ca_initials', 'rd_ca_material', 'rd_ca_metal',
        'rd_ca_plating', 'rd_ca_sub_category', 'hh_web_plating'
    ]

    # Suffixes to try for variant SKUs, in priority order
    VARIANT_SUFFIXES = ['-SM', '-S-M', '-NS', '-Adjustable']

    def __init__(self):
        self.input_file = self._find_latest_magento_file()
        if not self.input_file:
            raise FileNotFoundError("❌ No Magento CSV export file found.")
        print(f"📄 Loaded file: {self.input_file}")
        self.df = pd.read_csv(self.input_file, dtype=str).fillna("")
        self.output_file = self.OUTPUT_FILE

    def _find_latest_magento_file(self):
        files = []
        for directory in self.SEARCH_DIRS:
            files.extend(glob.glob(os.path.join(directory, self.FILE_PATTERN)))
        return max(files, key=os.path.getmtime) if files else None

    def run(self):
        all_rows = self.df.copy()

        # Parents with SKU starting with P- and empty rd_ca_div_name
        parents_empty_div = all_rows[
            (all_rows['product_type'] == 'configurable') &
            (all_rows['sku'].str.startswith('P-')) &
            ((all_rows['rd_ca_div_name'].isna()) | (all_rows['rd_ca_div_name'].str.strip() == ""))
        ].copy()

        # Build a lookup dict: simple SKU => rd_ attributes dict
        simple_rows = all_rows[all_rows['product_type'] == 'simple'].copy()
        simple_lookup = {
            sku: {col: row.get(col, "") for col in self.RD_COLUMNS}
            for sku, row in simple_rows.set_index('sku').iterrows()
        }

        updated_rows = []

        for _, parent in parents_empty_div.iterrows():
            parent_sku = parent['sku']
            base_sku = parent_sku[2:]  # Strip "P-"

            variant_rd_values = None
            matched_variant_sku = None

            # Try each suffix until we find a match
            for suffix in self.VARIANT_SUFFIXES:
                candidate_sku = base_sku + suffix
                if candidate_sku in simple_lookup:
                    variant_rd_values = simple_lookup[candidate_sku]
                    matched_variant_sku = candidate_sku
                    break

            updated_row = {
                'sku': parent_sku,
                'name': parent['name'],
                'variant_source_sku': matched_variant_sku or ""
            }

            if variant_rd_values:
                for col in self.RD_COLUMNS:
                    updated_row[col] = variant_rd_values.get(col, "").strip()
            else:
                for col in self.RD_COLUMNS:
                    updated_row[col] = ""

            updated_rows.append(updated_row)

        # Parents With Empty Columns tab: output original parents as is
        parents_with_empty_div = parents_empty_div[['sku', 'name'] + self.RD_COLUMNS].copy()

        with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
            pd.DataFrame(updated_rows).to_excel(writer, index=False, sheet_name='Updated Parents')
            parents_with_empty_div.to_excel(writer, index=False, sheet_name='Parents With Empty Columns')

        print(f"✅ Finished! Output saved to {self.output_file}")
        print(f"  - Updated Parents: {len(updated_rows)}")
        print(f"  - Parents With Empty Columns: {len(parents_with_empty_div)}")

if __name__ == '__main__':
    MagentoParentProductUpdater().run()
