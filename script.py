"""
Generate Detailed Tables from the US Census ACS 5-Year Summary Files

usage: censusACS.py [-h] [-c config_json_file]

"""

import pandas as pd
import numpy as np
import requests
import json
import os
import sys
import zipfile
import argparse

states = ['al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga',
          'hi', 'id', 'il', 'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma',
          'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny',
          'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx',
          'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy', 'us', 'pr']

summary_levels = {
    'region' : '020',
    'division' : '030',
    'state' : '040',
    'county' : '050',
    'county_subdivision' : '060',
    'subminor_civil_division' : '067',
    'census_tract' : '140',
    'block_group' : '150',
    'place' : '160'
}

def stderr_print(*args, **kwargs):
    print(*args, **kwargs, file=sys.stderr, flush=True)

def get_config(config=None):
    """
    Return configuration dictionary read in from args
    """

    data = {}
    try:
        with open(config.config) as fp:
            data = json.load(fp)
    except:
        pass

    return {
        'year': '2019',
        'states': data.get('states', config.states),
        'tables': data.get('tables', config.tables),
        'level': data.get('level', config.level)
    }


def request_file(url):
    """
    requests.get with status check
    """
    try:
        response = requests.get(url, timeout=3.333)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        stderr_print(f'Error: Download from {url} failed. Reason: {e}')
        return None


def read_from_csv(file, names):
    """
    Customized call to pandas.read_csv for reading header-less summary files.
    """
    return pd.read_csv(file, encoding='ISO-8859-1', names=names,
                       header=None, na_values=['.', -1], dtype=str)


def read_summary_file(file, names):
    """
    Read summary estimates/margins file and return a massaged DataFrame
    ready for data extraction.
    """
    df = read_from_csv(file, names=names)
    return df.rename(columns={'SEQUENCE': 'seq', 'LOGRECNO': 'Logical Record Number'})


def get_appendix_data(df, table):
    """
    Given Appendix A DataFrame df and table name,
    return Sequence, Start, and End numbers as lists.
    """
    df = df[df['name'] == table]
    return df['seq'].tolist(), df['start'].tolist(), df['end'].tolist()


def get_by_summary_level(df, summary_level):
    """
    Given Geography file DataFrame df, return a subset DataFrame,
    filtered by Census geographic summary level.
    """
    return df[df['Summary Level'] == summary_level]

def convert_seq_int_to_str(seqint):
    """
    Given a sequence integer, return a 4 character string representation
    i.e. 1 >> '0001'
    """
    return(f'{seqint:04}')

def get_templates(templates_zip_archive):
    """
    Unzip the Summary File Templates archive file; generate and
    return a dictionary mapping 'geo' and seq # to corresponding
    file column name lists.
    """
    templates = dict()
    with zipfile.ZipFile(templates_zip_archive) as z:
        # Loop through all files in archive namelist
        for name in z.namelist():
            if 'seq' in name:
                # Generate 4-digit sequence number string
                index = name.index('seq')
                # Drop 'seq' and separate sequence number from file extension
                s = name[index + 3:].split('.')[0]
                # Generate number string
                key = s.zfill(4)
            elif 'Geo' in name:
                key = 'geo'
            else:
                # skip directories or other files
                continue
            with z.open(name) as f:
                df = pd.read_excel(f, engine='openpyxl')
                # Extract column names from data row 0
                templates[key] = df.loc[0].tolist()
    return templates


def get_logical_records(fp, names, summary_level):
    """
    Given a CSV geo file object fp, column-names list names,
    and geo summary level value, return a DataFrame of GEO IDS
    and Logical Record Numbers from the geo file, filtered by
    the geographic summary level.
    """
    gdf = read_from_csv(fp, names=names)
    summary_geo = get_by_summary_level(gdf, summary_level)
    return summary_geo[['Geographic Identifier', 'Logical Record Number']]


def progress_report(fraction):
    # Print the current progress, given as a fraction, as a percentage.
    print(f'\rProgress: {100*fraction:.0f}% ', end='')


def main(config=None):
    # Read config.json or default variables
    cfg = get_config(config)

    # ACS release year
    year = cfg['year']

    # Summary level
    summary_level = cfg['level'][0]
    

    # Make data directories, if necessary
    sourcedir = os.path.join(os.getcwd(), 'ACS_data_' + year)
    try:
        os.mkdir(sourcedir)
    except FileExistsError:
        pass

    outdir = os.path.join(sourcedir, 'ACS_tables')
    try:
        os.mkdir(outdir)
    except FileExistsError:
        pass

    # Assign variables

    if summary_level == '150' or summary_level == '140':
        summary_file_suffix = '_Tracts_Block_Groups_Only.zip'
    else:
        summary_file_suffix = '_All_Geographies_Not_Tracts_Block_Groups.zip'
    appendix_file = 'ACS_2019_SF_5YR_Appendices.xlsx'
    templates_file = '2019_5yr_Summary_FileTemplates.zip'

    acs_base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/2019'
    by_state_base_url = acs_base_url + '/data/5_year_by_state/'

    # Note: The summary files (e.g. 5-year by state) are multi-MB files
    states = cfg['states']
    state_urls = [by_state_base_url + state + summary_file_suffix for state in states]

    urls = [acs_base_url + '/documentation/tech_docs/' + appendix_file,
            acs_base_url + '/data/' + templates_file,
            ] + state_urls
    # Download files, as necessary
    for url in urls:
        basename, filename = os.path.split(url)
        p = os.path.join(sourcedir, filename)
        if not os.path.exists(p):
            print(f'Requesting file {url}')
            response = request_file(url)
            if response:
                try:
                    with open(p, 'wb') as w:
                        w.write(response.content)
                        print(f'File {p} downloaded successfully')
                except OSError as e:
                    stderr_print(f'Error {e}: File write on {p} failed')

    # Read ACS 5-year Appendix A for Table sequence numbers, start/end records
    pathname = os.path.join(sourcedir, appendix_file)
    with open(pathname, 'rb') as r:
        appx_A = pd.read_excel(r, converters={'Summary File Sequence Number': convert_seq_int_to_str}, engine='openpyxl')
        appx_A.columns = ['name', 'title', 'restr', 'seq', 'start_end', 'topics', 'universe']
        try:
            appx_A[['start', 'end']] = appx_A['start_end'].str.split('-', 1, expand=True)
            appx_A['start'] = pd.to_numeric(appx_A['start'])
            appx_A['end'] = pd.to_numeric(appx_A['end'])
        except ValueError as e:
            stderr_print(f'{e}')
            stderr_print(f'File {pathname} is corrupt or has invalid format')
            raise SystemExit(f'Exiting {__file__}')

    
    # Create Tables list
    tables = appx_A.drop(['restr', 'seq', 'start_end', 'start', 'end', 'topics', 'universe'], axis=1)
    pathname = os.path.join(outdir, 'ACS All Tables.csv')
    # Save table Names and Titles to CSV.
    tables.to_csv(pathname, index=False)
    # Now check for limited table list from input config file.
    all_tables = cfg['tables'] if cfg['tables'] else tables['name'].tolist()

    # Create the templates dictionary and rename duplicate columns
    pathname2 = os.path.join(sourcedir, templates_file)
    templates = get_templates(pathname2)
    for i in range(len(templates['geo'])):
        if templates['geo'][i] == 'Reserved Future Use':
            newcolname = templates['geo'][i] + str(i)
            templates['geo'][i] = newcolname

    # For each state and table name, generate output table
    for state in states:
        print(f'Building tables for {state}')
        # Unzip and open the summary files
        pathname = os.path.join(sourcedir, state + summary_file_suffix)
        try:
            with zipfile.ZipFile(pathname) as z:
                # Get Geography CSV file name
                geofile = [f for f in z.namelist()
                           if f.startswith('g') and f.endswith('csv')
                           ][0]
                # Open and read the Geography file
                try:
                    with z.open(geofile) as g:
                        # Get Geo IDs and Logical Record Numbers for this Summary Level
                        logi_recs = get_logical_records(g, templates['geo'], summary_level)
                except OSError as e:
                    stderr_print(f'Geofile error for {state}')
                    stderr_print(f'{e}')
                    continue

                # Get Estimate file names
                e = [f for f in z.namelist() if f.startswith('e')]
                # Pull sequence number from file name positions 8-11; use as dict key
                efiles = {f[8:12]: f for f in e}
                built = 0
                # Process all tables
                for n, table in enumerate(all_tables):
                    sequence_data = []
                    # For this table, get file sequence numbers, start/end record numbers (as strings)
                    seqs, starts, ends = get_appendix_data(appx_A, table)
                    for seq, start, end in zip(seqs, starts, ends):
                        # Get summary file based on sequence number
                        template = templates[seq]
                        try:
                            efile = efiles[seq]
                            with z.open(efile) as e:
                                edf = read_summary_file(e, names=template)
                        except OSError as e:
                            stderr_print(f'Estimates file {efile} error for {state}')
                            stderr_print(f'{e}')
                            break

                        # Merge the estimates with the logical records
                        edf = edf.merge(logi_recs).set_index('Geographic Identifier')
                        # Keep only data columns
                        use_col_nums = list(range(start - 1, end))
                        edf = edf.iloc[:, use_col_nums]
                        # Save DataFrame to list
                        sequence_data.append(edf)

                    # Guard rail against file errors above
                    if sequence_data:

                        # Concatenate multiple data frames column-wise
                        df = pd.concat(sequence_data, axis=1)

                        # Reset 'Geographic Identifier' from index to column
                        df.reset_index(inplace=True)

                        # Save non-empty table as CSV
                        if not df.drop('Geographic Identifier', axis=1).dropna().empty:
                            table_csv_pathname = os.path.join(outdir, state + table + '.csv')
                            df.to_csv(table_csv_pathname, index=False)
                            built += 1

                    # Print progress percentage
                    progress_report(n / len(all_tables))

                print(f'\n{state} tables: saved {built}, dropped {n + 1 - built} empty')

        except OSError as e:
            stderr_print(f'Summary file error for {pathname}')
            stderr_print(f'{e}')
            continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate US Census ACS Detailed Tables")
    parser.add_argument("-c", "--config")
    parser.add_argument("-l", "--level", nargs='+', help='Geographic level')
    parser.add_argument("-s", "--states", nargs='+', help='<Required> Set flag')
    parser.add_argument("-t", "--tables", nargs='+', help='<Required> Set flag')
    args = parser.parse_args()
    main(args)
