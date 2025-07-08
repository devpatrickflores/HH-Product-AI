import pandas as pd
import re
import os
import glob
import sys

class MagentoProductProcessor:
    def __init__(self, file_path=None):
        self.file_path = file_path or self._find_latest_magento_file()
        self.df = None
        self.size_priority = ['SM', 'S-M', 'ML', 'M-L']
        self.size_pattern = r'-(SM|S-M|ML|M-L)$'

    def _find_latest_magento_file(self):
        pattern = "export_catalog_product_*.csv"
        search_dirs = ['.', 'exports', 'data', 'csv', 'downloads']
        files = []
        for directory in search_dirs:
            files += glob.glob(os.path.join(directory, pattern))

        if not files:
            raise FileNotFoundError("No Magento export files found.")

        files.sort(key=os.path.getmtime, reverse=True)
        print(f"📄 Using file: {files[0]}")
        return files[0]

    def load_csv(self):
        try:
            self.df = pd.read_csv(self.file_path)
            if 'sku' not in self.df.columns:
                raise ValueError("Missing 'sku' column in CSV.")
            return True
        except Exception as e:
            print(f"❌ Failed to load CSV: {e}")
            return False

    def extract_size(self, sku):
        match = re.search(self.size_pattern, str(sku))
        return match.group(1) if match else None

    def get_unassigned_variants(self):
        df = self.df

        if 'rd_' not in df.columns:
            df['rd_'] = ''

        # ✅ Exclude rows with product_online == 2
        if 'product_online' in df.columns:
            df = df[df['product_online'] != 2]

        size_mask = df['sku'].apply(lambda x: bool(re.search(self.size_pattern, str(x))) and not str(x).startswith('P-'))
        variants = df[size_mask].copy()

        assigned_skus = set()
        if 'configurable_variations' in df.columns:
            for row in df['configurable_variations'].dropna().astype(str):
                assigned_skus.update(re.findall(r'sku=([^,\s]+)', row))

        existing_skus = set(df['sku'].dropna().astype(str))

        def has_existing_parent(sku):
            base = str(sku).split('-')[0]
            return f'P-{base}' in existing_skus

        unassigned = variants[
            (~variants['sku'].isin(assigned_skus)) &
            (~variants['sku'].apply(has_existing_parent))
        ].copy()

        unassigned['base_sku'] = unassigned['sku'].apply(lambda x: str(x).split('-')[0])
        unassigned['size'] = unassigned['sku'].apply(self.extract_size)

        for col in ['name', 'visibility', 'base_image']:
            if col not in unassigned.columns:
                unassigned[col] = ''

        print(f"✅ Found {len(unassigned)} unassigned variant SKUs without parents")
        return unassigned

    def generate_parent_products(self, unassigned_df):
        parent_products = []
        grouped = unassigned_df.groupby('base_sku')

        for base_sku, group in grouped:
            if len(group) < 2:
                continue  # Skip if fewer than 2 variants

            group = group.copy()
            group['size_rank'] = group['size'].apply(
                lambda s: self.size_priority.index(s) if s in self.size_priority else 99
            )
            group_sorted = group.sort_values('size_rank')
            template = group_sorted.iloc[0].copy()

            parent_sku = f'P-{base_sku}'
            template['sku'] = parent_sku
            template['visibility'] = 'Catalog, Search'

            # Normalize name by removing size suffix
            template['name'] = re.sub(r'\s*[-–]?(SM|S-M|ML|M-L)\s*$', '', str(template['name']), flags=re.IGNORECASE).strip()

            # Include rd_ value from SM or S-M
            rd_values = group[group['size'].isin(['SM', 'S-M'])]['rd_'].dropna().unique()
            template['rd_'] = ','.join(rd_values) if len(rd_values) > 0 else ''

            # Combine all unique base images
            template['base_image'] = ','.join(group['base_image'].dropna().unique())

            parent_products.append(template.drop(['base_sku', 'size', 'size_rank'], errors='ignore'))

        print(f"🏗️ Created {len(parent_products)} parent products")
        return pd.DataFrame(parent_products)

    def export_to_excel(self, unassigned_df, parent_df, output_file="processed_output.xlsx"):
        with pd.ExcelWriter(output_file) as writer:
            unassigned_df[['sku', 'name', 'visibility', 'product_online', 'base_image']].to_excel(
                writer, sheet_name="Unassigned Variants", index=False
            )

            required_cols = ['sku', 'name', 'visibility', 'product_online', 'base_image']
            if parent_df.empty:
                print("⚠️ No parent products to export.")
                parent_df = pd.DataFrame(columns=required_cols)
            else:
                for col in required_cols:
                    if col not in parent_df.columns:
                        parent_df[col] = ''

            parent_df[required_cols].to_excel(
                writer, sheet_name="Generated Parents", index=False
            )

        print(f"📤 Exported results to '{output_file}'")

# ------------------------
# Main Execution
# ------------------------
if __name__ == "__main__":
    file_arg = sys.argv[1] if len(sys.argv) > 1 else None
    processor = MagentoProductProcessor(file_arg)
    if processor.load_csv():
        unassigned = processor.get_unassigned_variants()
        parents = processor.generate_parent_products(unassigned)
        processor.export_to_excel(unassigned, parents)
