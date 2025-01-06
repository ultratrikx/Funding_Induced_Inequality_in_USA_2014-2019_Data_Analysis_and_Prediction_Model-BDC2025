import csv
import sys
import argparse
from pathlib import Path

def convert_tab_to_csv(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as fin:
            # Read tab-delimited file
            print(f"Reading {input_file}...")
            content = fin.read().strip()
            lines = content.split('\n')
            
        with open(output_file, 'w', newline='', encoding='utf-8') as fout:
            # Create CSV writer
            writer = csv.writer(fout)
            total_lines = len(lines)
            
            print(f"Converting {total_lines} lines to CSV format...")
            # Convert each line from tab-separated to CSV
            for i, line in enumerate(lines, 1):
                row = line.split('\t')
                writer.writerow(row)
                if i % 1000 == 0:
                    print(f"Processed {i}/{total_lines} lines...")
                    
        print(f"Successfully converted to {output_file}")
        return True
        
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        return False
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        return False

def main():
    input_path = './data/18-19/fiscal.txt'
    output_path ='./data/18-19/fiscal.csv'

    
    if convert_tab_to_csv(input_path, output_path):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()
