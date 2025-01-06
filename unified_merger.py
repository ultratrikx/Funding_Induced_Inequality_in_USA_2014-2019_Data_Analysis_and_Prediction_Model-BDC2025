import pandas as pd
import os
from pathlib import Path

class DistrictDataMerger:
    def __init__(self, base_path='./data'):
        self.base_path = Path(base_path)
        self.output_path = self.base_path / 'output'
        self.output_path.mkdir(exist_ok=True)
        self.staff_mapping = {
            'ELMGUI': 'Elementary School Counselors',
            'ELMTCH': 'Elementary Teachers',
            'TOTGUI': 'Guidance Counselors',
            'CORSUP': 'Instructional Coordinators and Supervisors to the Staff',
            'KGTCH': 'Kindergarten Teachers',
            'LEASUP': 'LEA Administrative Support Staff',
            'LEAADM': 'LEA Administrators',
            'LIBSPE': 'Librarians/media specialists',
            'LIBSUP': 'Library/Media Support Staff',
            'PARA': 'Paraprofessionals/Instructional Aides',
            'PKTCH': 'Pre-kindergarten Teachers',
            'SCHSUP': 'School Administrative Support Staff',
            'GUI': 'School Counselors',
            'STAFF': 'School Staff',
            'SCHADM': 'School administrators',
            'SECGUI': 'Secondary School Counselors',
            'SECTCH': 'Secondary Teachers',
            'STUSUP': 'Student Support Services Staff',
            'TOTTCH': 'Teachers',
            'UGTCH': 'Ungraded Teachers'
        }

        # Add column name mappings for old format files
        self.column_mapping = {
            'ELL': 'LEP_COUNT',
            'SPECED': 'IDEA_COUNT'
        }

    def convert_tab_to_csv(self, input_file, output_file):
        """Convert tab-delimited file to CSV"""
        try:
            # Add low_memory=False to avoid DtypeWarning
            df = pd.read_csv(input_file, sep='\t', low_memory=False)
            df.to_csv(output_file, index=False)
            print(f"Converted {input_file} to CSV format")
            return True
        except Exception as e:
            print(f"Error converting file: {str(e)}")
            return False

    def read_data_file(self, filepath):
        """Read data from either CSV or TXT file"""
        file_ext = filepath.suffix.lower()
        try:
            if file_ext == '.txt':
                df = pd.read_csv(filepath, sep='\t', low_memory=False)
            else:
                df = pd.read_csv(filepath, low_memory=False)
            
            # Rename old format columns if they exist
            df = df.rename(columns=self.column_mapping)
            
            # Convert numeric columns
            for col in df.columns:
                if col not in ['ST', 'LEA_NAME', 'UNION', 'NAME', 'STAFF']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        except Exception as e:
            print(f"Error reading file {filepath}: {str(e)}")
            return None

    def merge_district_data(self, year_folder):
        """Merge core district data (ELL, Disabilities, Directory)"""
        print("\nMerging district data...")
        
        district_path = self.base_path / year_folder / 'District'
        ell_file = next(district_path.glob('ELL.*'))
        disabilities_file = next(district_path.glob('Disability.*'))
        directory_file = next(district_path.glob('Directory District.*'))

        ell_df = self.read_data_file(ell_file)
        disabilities_df = self.read_data_file(disabilities_file)
        directory_df = self.read_data_file(directory_file)

        if all([ell_df is not None, disabilities_df is not None, directory_df is not None]):
            # Extract SCHOOL_YEAR and ST from any of the dataframes that has them
            school_year = None
            state = None
            
            # Try to get SCHOOL_YEAR and ST from each dataframe
            for df in [directory_df, ell_df, disabilities_df]:
                if 'SCHOOL_YEAR' in df.columns and school_year is None:
                    school_year = df['SCHOOL_YEAR'].iloc[0]
                if 'ST' in df.columns and state is None:
                    state = df['ST'].iloc[0]

            merged_df = pd.merge(ell_df, disabilities_df, on='LEAID', how='outer')
            final_df = pd.merge(merged_df, directory_df, on='LEAID', how='outer')
            
            # Ensure SCHOOL_YEAR and ST columns exist
            if 'SCHOOL_YEAR' not in final_df.columns and school_year is not None:
                final_df['SCHOOL_YEAR'] = school_year
            if 'ST' not in final_df.columns and state is not None:
                final_df['ST'] = state

            final_df = final_df.loc[:,~final_df.columns.duplicated()]
            return final_df
        else:
            raise Exception("Failed to read one or more district data files")

    def merge_staff_data(self, base_df, year_folder):
        """Merge staff data with existing merged data"""
        print("\nMerging staff data...")
        
        district_path = self.base_path / year_folder / 'District'
        staff_file = next(district_path.glob('Staff District.*'))
        staff_df = self.read_data_file(staff_file)

        if staff_df is not None:
            # Check if data is in old format (columns are ELMGUI, ELMTCH, etc.) or new format (STAFF column)
            old_format_cols = set(self.staff_mapping.keys())
            if any(col in old_format_cols for col in staff_df.columns):
                # Old format - rename columns using mapping and handle duplicates
                # Keep only the staff count columns and LEAID
                staff_count_cols = [col for col in staff_df.columns if col in old_format_cols]
                staff_df = staff_df[['LEAID'] + staff_count_cols]
                # Rename the columns
                staff_df = staff_df.rename(columns=self.staff_mapping)
                final_df = pd.merge(base_df, staff_df, on='LEAID', how='left')
            else:
                # New format - pivot and merge
                staff_df['STAFF'] = staff_df['STAFF'].map(
                    lambda x: self.staff_mapping.get(x, x)
                )
                staff_pivoted = staff_df.pivot(
                    index='LEAID',
                    columns='STAFF',
                    values='STAFF_COUNT'
                ).reset_index()
                final_df = pd.merge(base_df, staff_pivoted, on='LEAID', how='left')

            final_df = final_df.loc[:,~final_df.columns.duplicated()]
            return final_df
        else:
            raise Exception("Failed to read staff data file")

    def merge_fiscal_data(self, base_df, fiscal_file):
        """Merge fiscal data with existing merged data"""
        print("\nMerging fiscal data...")
        
        if fiscal_file.endswith('.txt'):
            csv_file = fiscal_file.replace('.txt', '.csv')
            self.convert_tab_to_csv(fiscal_file, csv_file)
            fiscal_file = csv_file

        fiscal_df = pd.read_csv(fiscal_file, low_memory=False)
        
        # Convert numeric columns in fiscal data
        for col in fiscal_df.columns:
            if col not in ['ST', 'LEA_NAME', 'NAME']:
                fiscal_df[col] = pd.to_numeric(fiscal_df[col], errors='coerce')
        
        overlapping = set(base_df.columns) & set(fiscal_df.columns) - {'LEAID'}
        fiscal_df = fiscal_df.drop(columns=overlapping, errors='ignore')
        
        final_df = pd.merge(base_df, fiscal_df, on='LEAID', how='left')
        return final_df

    def process_data(self, year_folder, fiscal_file):
        """Main processing function"""
        # 1. Merge district data
        merged_df = self.merge_district_data(year_folder)
        
        # 2. Add staff data
        merged_df = self.merge_staff_data(merged_df, year_folder)
        
        # 3. Add fiscal data
        final_df = self.merge_fiscal_data(merged_df, fiscal_file)
        
        # Define the columns to keep
        columns_to_keep = ['LEAID', 'LEP_COUNT', 'IDEA_COUNT', 'SCHOOL_YEAR', 'ST', 'LEA_NAME', 'UNION', 'ST_LEAID',
                           'Elementary School Counselors', 'Elementary Teachers',
                          'Guidance Counselors', 'Instructional Coordinators and Supervisors to the Staff',
                          'Kindergarten Teachers', 'LEA Administrative Support Staff', 'LEA Administrators',
                           'Librarians/media specialists', 'Library/Media Support Staff',
                          'Paraprofessionals/Instructional Aides',
                          'Pre-kindergarten Teachers', 'School Administrative Support Staff', 'School Counselors',
                          'School Staff', 'School administrators', 'Secondary School Counselors',
                          'Secondary Teachers', 'Student Support Services Staff', 'Teachers', 'Ungraded Teachers',
                          'CENSUSID', 'FIPST', 'CONUM', 'CSA', 'CBSA', 'NAME', 'AGCHRT', 'CCDNF', 'CENFILE',
                          'GSLO', 'GSHI', 'V33', 'MEMBERSCH', 'TOTALREV', 'TFEDREV', 'C14', 'C15', 'C16', 'C17',
                          'C19', 'B11', 'C20', 'C25', 'C36', 'B10', 'B12', 'B13', 'TSTREV', 'C01', 'C04', 'C05',
                          'C06', 'C07', 'C08', 'C09', 'C10', 'C11', 'C12', 'C13', 'C35', 'C38', 'C39', 'TLOCREV',
                          'T02', 'T06', 'T09', 'T15', 'T40', 'T99', 'D11', 'D23', 'A07', 'A08', 'A09', 'A11',
                          'A13', 'A15', 'A20', 'A40', 'U11', 'U22', 'U30', 'U50', 'U97', 'C24', 'TOTALEXP',
                          'TCURELSC', 'TCURINST', 'E13', 'V91', 'V92', 'TCURSSVC', 'E17', 'E07', 'E08', 'E09',
                          'V40', 'V45', 'V90', 'V85', 'TCUROTH', 'E11', 'V60', 'V65', 'TNONELSE', 'V70', 'V75',
                          'V80', 'TCAPOUT', 'F12', 'G15', 'K09', 'K10', 'K11', 'L12', 'M12', 'Q11', 'I86',
                          'Z32', 'Z33', 'Z35', 'Z36', 'Z37', 'Z38', 'V11', 'V13', 'V15', 'V17', 'V21', 'V23',
                          'V37', 'V29', 'Z34', 'V10', 'V12', 'V14', 'V16', 'V18', 'V22', 'V24', 'V38', 'V30',
                          'V32', 'V93', '_19H', '_21F', '_31F', '_41F', '_61V', '_66V', 'W01', 'W31', 'W61',
                          'V95', 'V02', 'K14', 'CE1', 'CE2', 'WEIGHT', 'FL_V33', 'FL_MEMBERSCH',
                          'FL_C14', 'FL_C15', 'FL_C16', 'FL_C17', 'FL_C19', 'FL_B11', 'FL_C20', 'FL_C25',
                          'FL_C36', 'FL_B10', 'FL_B12', 'FL_B13', 'FL_C01', 'FL_C04', 'FL_C05', 'FL_C06',
                          'FL_C07', 'FL_C08', 'FL_C09', 'FL_C10', 'FL_C11', 'FL_C12', 'FL_C13', 'FL_C35',
                          'FL_C38', 'FL_C39', 'FL_T02', 'FL_T06', 'FL_T09', 'FL_T15', 'FL_T40', 'FL_T99',
                          'FL_D11', 'FL_D23', 'FL_A07', 'FL_A08', 'FL_A09', 'FL_A11', 'FL_A13', 'FL_A15',
                          'FL_A20', 'FL_A40', 'FL_U11', 'FL_U22', 'FL_U30', 'FL_U50', 'FL_U97', 'FL_C24',
                          'FL_E13', 'FL_V91', 'FL_V92', 'FL_E17', 'FL_E07', 'FL_E08', 'FL_E09', 'FL_V40',
                          'FL_V45', 'FL_V90', 'FL_V85', 'FL_E11', 'FL_V60', 'FL_V65', 'FL_V70', 'FL_V75',
                          'FL_V80', 'FL_F12', 'FL_G15', 'FL_K09', 'FL_K10', 'FL_K11', 'FL_L12', 'FL_M12',
                          'FL_Q11', 'FL_I86', 'FL_Z32', 'FL_Z33', 'FL_Z35', 'FL_Z36', 'FL_Z37', 'FL_Z38',
                          'FL_V11', 'FL_V13', 'FL_V15', 'FL_V17', 'FL_V21', 'FL_V23', 'FL_V37', 'FL_V29',
                          'FL_Z34', 'FL_V10', 'FL_V12', 'FL_V14', 'FL_V16', 'FL_V18', 'FL_V22', 'FL_V24',
                          'FL_V38', 'FL_V30', 'FL_V32', 'FL_V93', 'FL_19H', 'FL_21F', 'FL_31F', 'FL_41F',
                          'FL_61V', 'FL_66V', 'FL_W01', 'FL_W31', 'FL_W61', 'FL_V95', 'FL_V02', 'FL_K14',
                          'FL_CE1', 'FL_CE2']

        # Filter columns
        final_df = final_df[columns_to_keep]
        
        # Save final output
        output_file = self.output_path / f'merged_district_data_{year_folder}.csv'
        final_df.to_csv(output_file, index=False)
        print(f"\nFinal dataset has {len(final_df.columns)} columns and {len(final_df)} rows")
        print(f"Saved to: {output_file}")
        return final_df

def main():
    # Configuration
    base_path = './data'
    year_folder = '15-16'  # Can be modified for different years
    fiscal_file = f'{base_path}/{year_folder}/fiscal.txt'

    # Initialize and run merger
    merger = DistrictDataMerger(base_path)
    merger.process_data(year_folder, fiscal_file)

if __name__ == '__main__':
    main()
