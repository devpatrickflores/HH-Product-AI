import pandas as pd
import re
import os
import glob
import sys

class MagentoProductProcessor:
    def __init__(self, file_path=None):
        self.file_path = file_path or self._find_latest_magento_file()
        self.df = None
        self.size_priority = ['SM', 'S-M', 'S/M', 'ML', 'M-L', 'LXL']
        self.size_pattern = r'-(SM|S-M|S/M|ML|M-L|LXL)$'

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
            self.df = pd.read_csv(self.file_path, encoding='utf-8', dtype=str)
            if 'sku' not in self.df.columns:
                raise ValueError("Missing 'sku' column in CSV.")
            return True
        except Exception as e:
            print(f"❌ Failed to load CSV: {e}")
            return False

    def extract_size(self, sku):
        match = re.search(self.size_pattern, str(sku))
        return match.group(1) if match else None

    def normalize_name(self, name):
        name = str(name).lower()
        name = re.sub(r'[^a-z0-9 ]', '', name)  # remove punctuation
        name = re.sub(r'\s*[-–]?(sm|s-m|s/m|ml|m-l|lxl)\s*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name)
        return name.strip()

    def get_unassigned_variants(self):
        df = self.df.copy()
        allowed_suffixes = ('-SM', '-S-M', '-S/M', '-ML', '-M-L', '-LXL')

        excluded_base_skus = set(str(sku).strip() for sku in [
            '101762', '102230', '102199', '102342', '103142', '103468', '103475',
            '104076', '104097', '104545', '104546', '104928', '105468', '105491',
            '105492', '105495', '105575', '105751', '105720', '105867', '105868',
            '105875', '105876', '106260', '106222', '106248', '106335', '106328',
            '106409', '106447', '106524', '106525', '106603', '106606', '106620',
            '106634', '106727', '106132', '106288', '103469', '103471', '103472',
            '103473', '106545', '103470', '103467', '100133', '103278', '106452',
            '106453', '103474', '106242', '101772', '101774', '101775', '101776',
            '103476', '104169', '104173', '105768', '107264', '107086', '107600',
            '107601', '107602', '107597', '107598', '107599', '107604', '107619',
            '107629', '107635', '107696', '107709', '107710', '107720', '107683',
            '107715', '107800', '107859', '107888', '108028', '108029', '108030',
            '108032', '108096', '108097', '108098', '108099', '108102', '108103',
            '108107', '107664', '107261', '108322', '108326', '108404', '104094',
            '108439', '108923', '108924', '109138', '109241', '109164', '109171',
            'STERLING SILVER CROISSANT RING', '108993', '108998', '109015', '109017',
            '109019', '109021', '109072', '109078', '109080', '109043', '108960',
            '109372', '108860', '108861', '108854', '108853', '108846', '108842',
            '108833', '108831', '108845', '108896', '108890', '108880', '108907',
            '108655', '108894', '108849', '108840', '108834', '108822', '108823',
            '108818', '108819', '108815', '108812', '108809', '108805', '108806',
            '108797', '108792', '108788', '108784'
        ])

        # Filter products with product_online = '1'
        if 'product_online' in df.columns:
            df = df[df['product_online'].astype(str) == '1']

        df['sku'] = df['sku'].astype(str)

        # Identify variant SKUs ending with allowed size suffixes and NOT starting with 'P-'
        df['is_variant'] = df['sku'].str.endswith(allowed_suffixes) & ~df['sku'].str.startswith('P-')
        variants = df[df['is_variant']].copy()

        # Collect assigned SKUs from configurable_variations and associated_skus
        assigned_skus = set()
        pattern = re.compile(r'sku=([^,|]+)', re.IGNORECASE)

        if 'configurable_variations' in df.columns:
            for row in df['configurable_variations'].dropna():
                found_skus = pattern.findall(str(row))
                assigned_skus.update(s.strip() for s in found_skus)

        if 'associated_skus' in df.columns:
            for row in df['associated_skus'].dropna():
                for sku in str(row).split(','):
                    assigned_skus.add(sku.strip())

        # Exclude any variant SKU if it is in assigned_skus
        variants = variants[~variants['sku'].isin(assigned_skus)]

        # Remove variants with existing P- parents
        all_skus = set(df['sku'].dropna())
        variants = variants[~variants['sku'].apply(lambda x: f'P-{x.split("-")[0]}' in all_skus)].copy()

        # Normalize for grouping
        variants['base_sku'] = variants['sku'].apply(lambda x: x.split('-')[0])
        variants['size'] = variants['sku'].apply(self.extract_size)
        variants['normalized_name'] = variants['name'].apply(self.normalize_name)

        # Exclude variants if parent with normalized name exists
        existing_parents = set()
        if 'sku' in df.columns and 'name' in df.columns:
            parent_df = df[df['sku'].str.startswith('P-')].copy()
            parent_df['normalized_name'] = parent_df['name'].apply(self.normalize_name)
            existing_parents.update(parent_df['normalized_name'])

        variants = variants[~variants['normalized_name'].isin(existing_parents)]

        # Apply exclusion list by base sku
        variants = variants[~variants['base_sku'].isin(excluded_base_skus)]

        # Fill missing columns for export
        for col in ['name', 'visibility', 'base_image', 'product_online']:
            if col not in variants.columns:
                variants[col] = ''

        # Sort unassigned variants by 'name' ascending
        # variants = variants.sort_values(by='name', ascending=True)

        print("🔍 Debug Info:")
        print(f"   - Total rows: {len(self.df)}")
        print(f"   - Enabled products (product_online=1): {len(df)}")
        print(f"   - Assigned SKUs detected: {len(assigned_skus)}")
        print(f"   - Variant SKUs found (Unassigned): {len(variants)}")

        return variants, assigned_skus

    def generate_parent_products(self, unassigned_df):
        parent_products = []
        grouped = unassigned_df.groupby('normalized_name')

        for normalized_name, group in grouped:
            if len(group) < 2:
                continue

            group = group.copy()
            group['size_rank'] = group['size'].apply(
                lambda s: self.size_priority.index(s) if s in self.size_priority else 99
            )
            group_sorted = group.sort_values('size_rank')
            template = group_sorted.iloc[0].copy()

            parent_base_sku = sorted(group['base_sku'])[0]
            parent_sku = f'P-{parent_base_sku}'

            template['sku'] = parent_sku
            template['name'] = normalized_name.upper()
            template['visibility'] = 'Catalog, Search'
            template['base_image'] = ','.join(group['base_image'].dropna().unique())

            parent_products.append(template.drop([
                'base_sku', 'size', 'size_rank', 'normalized_name'
            ], errors='ignore'))

        print(f"🏗️ Created {len(parent_products)} parent products")
        return pd.DataFrame(parent_products)

    def export_to_excel(self, unassigned_df, parent_df, assigned_skus, output_file="processed_output.xlsx"):
        with pd.ExcelWriter(output_file) as writer:
            # Unassigned Variants tab
            cols_unassigned = ['sku', 'name', 'visibility', 'product_online', 'base_image']
            for col in cols_unassigned:
                if col not in unassigned_df.columns:
                    unassigned_df[col] = ''
            unassigned_df[cols_unassigned].to_excel(writer, sheet_name="Unassigned Variants", index=False)

            # Generated Parents tab
            cols_parent = ['sku', 'name', 'visibility', 'product_online', 'base_image']
            if parent_df.empty:
                print("⚠️ No parent products to export.")
                parent_df = pd.DataFrame(columns=cols_parent)
            else:
                for col in cols_parent:
                    if col not in parent_df.columns:
                        parent_df[col] = ''
            parent_df[cols_parent].to_excel(writer, sheet_name="Generated Parents", index=False)

            # Assigned SKUs tab
            assigned_list = list(assigned_skus)
            assigned_df = pd.DataFrame({'sku': assigned_list})
            if 'name' in self.df.columns:
                assigned_df = assigned_df.merge(self.df[['sku', 'name']], on='sku', how='left')
            else:
                assigned_df['name'] = ''
            assigned_df.to_excel(writer, sheet_name="Assigned SKUs", index=False)

        print(f"📤 Exported results to '{output_file}'")

# ------------------------
# Main Execution
# ------------------------
if __name__ == "__main__":
    file_arg = sys.argv[1] if len(sys.argv) > 1 else None
    processor = MagentoProductProcessor(file_arg)
    if processor.load_csv():
        unassigned, assigned_skus = processor.get_unassigned_variants()
        parents = processor.generate_parent_products(unassigned)
        processor.export_to_excel(unassigned, parents, assigned_skus)
