#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import json
import io
import sys
import logging
import tempfile
import os
import re
import requests
from hdfs import InsecureClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# WebHDFS configuration
HDFS_URL = "http://namenode:50070"
HDFS_USER = "nifi"

# API endpoint
API_BASE_URL = "https://data-observatoire.sandbox.gouv.tg"

def excel_to_csv(excel_data, separator="|"):
    """
    Convert Excel data from STDIN into CSV format (one per sheet).
    
    Args:
        excel_data (bytes): Binary Excel data from STDIN.
        separator (str): Separator for CSV files (default: "|").
    
    Returns:
        dict: Dictionary mapping sheet names to their CSV content.
    """
    try:
        logging.info("Reading Excel data from STDIN")
        xl = pd.ExcelFile(io.BytesIO(excel_data))
        logging.info(f"Available sheets: {xl.sheet_names}")
        
        csv_contents = {}
        
        for sheet_name in xl.sheet_names:
            try:
                df = pd.read_excel(xl, sheet_name=sheet_name, header=None)
            except Exception as e:
                logging.error(f"Failed to read sheet {sheet_name}: {e}")
                continue
            
            if df.empty:
                logging.warning(f"Sheet {sheet_name} is empty, skipping")
                continue
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=separator, index=False, header=False, na_rep="")
            csv_content = csv_buffer.getvalue()
            csv_contents[sheet_name] = csv_content
            
            logging.info(f"Converted sheet {sheet_name} to CSV with separator '{separator}'")
        
        if not csv_contents:
            logging.error("No sheets were converted to CSV")
            raise ValueError("No sheets were converted to CSV")
        
        return csv_contents
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

def save_csv_to_hdfs(csv_contents, hdfs_base_path):
    """
    Save CSV contents to HDFS under csv_output/<category>/<year>.csv.
    
    Args:
        csv_contents (dict): Dictionary mapping sheet names to CSV content.
        hdfs_base_path (str): Base HDFS path for saving CSVs.
    
    Returns:
        dict: Dictionary mapping sheet names to their HDFS paths.
    """
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    csv_output_base = os.path.join(hdfs_base_path, "csv_output")
    client.makedirs(csv_output_base)
    logging.info(f"Created base directory in HDFS: {csv_output_base}")

    saved_files = {}

    for sheet_name, content in csv_contents.items():
        parts = sheet_name.split('-')
        if len(parts) != 2:
            logging.warning(f"Invalid sheet name format {sheet_name}, skipping")
            continue

        category = parts[0].strip()
        year = parts[1].strip()

        if not category.startswith("C"):
            logging.warning(f"Sheet {sheet_name} does not start with 'C', skipping")
            continue

        category_dir = os.path.join(csv_output_base, category)
        client.makedirs(category_dir)
        logging.info(f"Created category directory in HDFS: {category_dir}")

        csv_filename = f"{year}.csv"
        csv_hdfs_path = os.path.join(category_dir, csv_filename)
        
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp_output:
            tmp_output_path = tmp_output.name
            tmp_output.write(content)
        
        client.upload(csv_hdfs_path, tmp_output_path, overwrite=True)
        logging.info(f"Saved CSV to HDFS: {csv_hdfs_path}")
        os.remove(tmp_output_path)

        saved_files[sheet_name] = csv_hdfs_path
    
    if not saved_files:
        logging.error("No CSV files were saved")
        raise ValueError("No CSV files were saved")
    
    return saved_files

def process_csvs_and_init_json(hdfs_base_path, separator="|"):
    """
    Process CSV files from HDFS, validate category code, and initialize the big JSON structure.
    
    Args:
        hdfs_base_path (str): Base HDFS path where CSVs are saved.
        separator (str): Separator used in CSV files.
    
    Returns:
        dict: Initialized big_json with category structure.
    """
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    csv_output_dir = os.path.join(hdfs_base_path, "csv_output")
    
    if not client.status(csv_output_dir, strict=False):
        logging.error(f"HDFS csv_output directory {csv_output_dir} does not exist")
        raise FileNotFoundError(f"HDFS csv_output directory {csv_output_dir} does not exist")
    
    big_json = {"categories": {}}
    
    category_dirs = client.list(csv_output_dir)
    
    for category_dir in category_dirs:
        category_path = os.path.join(csv_output_dir, category_dir)
        if client.status(category_path)['type'] != 'DIRECTORY':
            logging.warning(f"Skipping non-directory item: {category_dir}")
            continue
        
        category_code = category_dir
        if not category_code.startswith("C"):
            logging.warning(f"Category code {category_code} does not start with 'C', skipping")
            continue
        
        logging.info(f"Processing category folder: {category_dir}")
        
        csv_files = client.list(category_path)
        if not csv_files:
            logging.warning(f"No CSV files found in category directory {category_path}, skipping")
            continue
        
        for csv_file in csv_files:
            if not csv_file.endswith(".csv"):
                logging.warning(f"Skipping non-CSV file: {csv_file}")
                continue
            
            csv_hdfs_path = os.path.join(category_path, csv_file)
            logging.info(f"Processing CSV file: {csv_hdfs_path}")
            
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
                client.download(csv_hdfs_path, tmp_file_path, overwrite=True)
                
                try:
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                except Exception as e:
                    logging.error(f"Failed to read CSV file {csv_hdfs_path}: {e}")
                    os.remove(tmp_file_path)
                    continue
                
                cleaned_lines = [line.strip() for line in lines if line.strip()]
                if not cleaned_lines:
                    logging.error(f"CSV file {csv_hdfs_path} is empty after cleaning")
                    os.remove(tmp_file_path)
                    continue
                
                logging.info(f"Cleaned CSV file {csv_hdfs_path}: {len(cleaned_lines)} lines after removing empty lines")
                
                first_line = cleaned_lines[0]
                logging.info(f"First line of {csv_file}: {first_line}")
                
                columns = first_line.split(separator)
                cleaned_columns = [col for col in columns if "unnamed" not in col.lower()]
                logging.info(f"Columns after removing 'unnamed': {cleaned_columns}")
                
                if not cleaned_columns:
                    logging.error(f"No non-unnamed columns found in first line of {csv_file}")
                    os.remove(tmp_file_path)
                    continue
                
                first_non_unnamed = cleaned_columns[0]
                if first_non_unnamed.upper() != category_code.upper():
                    logging.warning(f"Category code mismatch in {csv_file}: expected {category_code}, found {first_non_unnamed}")
                    os.remove(tmp_file_path)
                    continue
                
                logging.info(f"Category code validation passed for {csv_file}: {first_non_unnamed} matches {category_code}")
                
                category_label = cleaned_columns[1] if len(cleaned_columns) > 1 else category_code
                
                big_json["categories"][category_code] = {
                    "level": "category",
                    "code": category_code,
                    "label": category_label,
                    "sources": {},
                    "indicators": {}  # Initialize indicators as empty dict for all categories
                }
                
                # Rewrite the cleaned CSV to HDFS
                with open(tmp_file_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(cleaned_lines))
                client.upload(csv_hdfs_path, tmp_file_path, overwrite=True)
                logging.info(f"Updated CSV file in HDFS with cleaned content: {csv_hdfs_path}")
                
                os.remove(tmp_file_path)
    
    if not big_json["categories"]:
        logging.error("No categories were processed")
        raise ValueError("No categories were processed")
    
    return big_json

def identify_columns_and_disaggregation(big_json, hdfs_base_path, separator="|"):
    """
    Identify columns and disaggregation levels from CSV lines 3 and 4, updating big_json.
    
    Args:
        big_json (dict): The big JSON object to update.
        hdfs_base_path (str): Base HDFS path where CSVs are saved.
        separator (str): Separator used in CSV files.
    
    Returns:
        dict: Updated big_json with disaggregation levels.
    """
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    csv_output_path = os.path.join(hdfs_base_path, "csv_output")
    
    if not client.status(csv_output_path, strict=False):
        logging.error(f"HDFS csv_output directory {csv_output_path} does not exist")
        raise FileNotFoundError(f"HDFS csv_output directory {csv_output_path} does not exist")
    
    for category_folder in client.list(csv_output_path):
        category_path = os.path.join(csv_output_path, category_folder)
        if client.status(category_path)['type'] != 'DIRECTORY':
            logging.warning(f"Skipping non-directory item: {category_folder}")
            continue
        
        category_code = category_folder
        if category_code not in big_json["categories"]:
            logging.warning(f"Category {category_code} not found in big_json, skipping")
            continue
        
        logging.info(f"Processing category folder: {category_folder}")
        
        for csv_file in client.list(category_path):
            if not csv_file.endswith(".csv"):
                logging.warning(f"Skipping non-CSV file: {csv_file}")
                continue
            
            csv_hdfs_path = os.path.join(category_path, csv_file)
            logging.info(f"Processing CSV file: {csv_hdfs_path}")
            
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
                client.download(csv_hdfs_path, tmp_file_path, overwrite=True)
                
                try:
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                except Exception as e:
                    logging.error(f"Failed to read CSV file {csv_hdfs_path}: {e}")
                    os.remove(tmp_file_path)
                    continue
                
                if len(lines) < 4:
                    logging.error(f"CSV file {csv_hdfs_path} has fewer than 4 lines")
                    os.remove(tmp_file_path)
                    continue
                
                line_3 = lines[2].strip().split(separator)
                line_4 = lines[3].strip().split(separator)
                
                if len(line_3) != len(line_4):
                    logging.error(f"Mismatch in column counts between line 3 ({len(line_3)}) and line 4 ({len(line_4)}) in {csv_hdfs_path}")
                    os.remove(tmp_file_path)
                    continue
                
                columns = []
                for i in range(len(line_3)):
                    main_header = line_3[i].strip()
                    sub_header = line_4[i].strip()
                    if sub_header:
                        columns.append(sub_header)  
                    else:
                        columns.append(main_header)  
                
                logging.info(f"Columns identified for {csv_file}: {columns}")
                
                try:
                    valeur_generale_index = columns.index("Valeur Générale")
                except ValueError:
                    logging.error(f"'Valeur Générale' column not found in {csv_file}")
                    os.remove(tmp_file_path)
                    continue
                
                fixed_columns = columns[:valeur_generale_index + 1]
                logging.info(f"Fixed columns: {fixed_columns}")
                
                disaggregation_columns = columns[valeur_generale_index + 1:]
                disaggregation_headers = line_3[valeur_generale_index + 1:]
                
                disaggregation_levels = {}
                current_level = None
                for i, header in enumerate(disaggregation_headers):
                    header = header.strip()
                    if header:  
                        current_level = header
                        disaggregation_levels[current_level] = []
                    if current_level and i < len(disaggregation_columns):
                        subcategory = disaggregation_columns[i].strip()
                        if subcategory:
                            disaggregation_levels[current_level].append(subcategory)
                
                disaggregation_levels = {k: v for k, v in disaggregation_levels.items() if v}
                
                logging.info(f"Disaggregation levels for {csv_file}: {disaggregation_levels}")
                
                big_json["categories"][category_code]["disaggregation_levels"] = disaggregation_levels
            
            os.remove(tmp_file_path)
    
    return big_json

def normalize_key(text):
    """Normalize a string to be used as a JSON key (lowercase, no special characters)."""
    text = text.lower()
    replacements = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'à': 'a', 'â': 'a', 'ä': 'a',
        'î': 'i', 'ï': 'i',
        'ô': 'o', 'ö': 'o',
        'û': 'u', 'ü': 'u',
        'ç': 'c',
        ' ': ''
    }
    for char, replacement in replacements.items():
        text = text.replace(char, replacement)
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

def process_sources_and_data(big_json, hdfs_base_path, separator="|"):
    """
    Process source lines and data rows in each CSV to extract indicators and their data,
    organizing them by source and duplicating indicators from 'Données exclusivement nationales'
    into a top-level 'indicators' object.
    
    Args:
        big_json (dict): The big JSON object to update.
        hdfs_base_path (str): Base HDFS path where CSVs are saved.
        separator (str): Separator used in CSV files.
    
    Returns:
        dict: Updated big_json with indicators grouped by source and duplicated in 'indicators'.
    """
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    csv_output_dir = os.path.join(hdfs_base_path, "csv_output")
    
    if not client.status(csv_output_dir, strict=False):
        logging.error(f"HDFS csv_output directory {csv_output_dir} does not exist")
        raise FileNotFoundError(f"HDFS csv_output directory {csv_output_dir} does not exist")
    
    source_markers = [
        "Données ITU",
        "Données CNUCED",
        "Données autres Organismes (GSMA, UNESCO, etc.)",
        "Données exclusivement nationales"
    ]
    
    for category_folder in client.list(csv_output_dir):
        category_path = os.path.join(csv_output_dir, category_folder)
        if client.status(category_path)['type'] != 'DIRECTORY':
            logging.warning(f"Skipping non-directory item: {category_folder}")
            continue
        
        category_code = category_folder
        if category_code not in big_json["categories"]:
            logging.warning(f"Category {category_code} not found in big_json, skipping")
            continue
        
        logging.info(f"Processing category folder: {category_folder}")
        
        for csv_file in client.list(category_path):
            if not csv_file.endswith(".csv"):
                logging.warning(f"Skipping non-CSV file: {csv_file}")
                continue
            
            csv_hdfs_path = os.path.join(category_path, csv_file)
            logging.info(f"Processing CSV file: {csv_hdfs_path}")
            
            year = os.path.splitext(csv_file)[0]
            
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
                client.download(csv_hdfs_path, tmp_file_path, overwrite=True)
                
                try:
                    with open(tmp_file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                except Exception as e:
                    logging.error(f"Failed to read CSV file {csv_hdfs_path}: {e}")
                    os.remove(tmp_file_path)
                    continue
                
                if len(lines) < 4:
                    logging.error(f"CSV file {csv_hdfs_path} has fewer than 4 lines")
                    os.remove(tmp_file_path)
                    continue
                
                line_3 = lines[2].strip().split(separator)
                line_4 = lines[3].strip().split(separator)
                columns = []
                for i in range(len(line_3)):
                    main_header = line_3[i].strip()
                    sub_header = line_4[i].strip()
                    if sub_header:
                        columns.append(sub_header)
                    else:
                        columns.append(main_header)
                
                # Ensure required columns are present, with fallback indices
                column_indices = {}
                required_columns = ["Code TG", "Code SI TG", "Définition", "intitulé de l'indicateur (FR)", "Unité de mesure", "Notes", "Valeur Générale"]
                for col in required_columns:
                    try:
                        column_indices[col] = columns.index(col)
                    except ValueError:
                        logging.warning(f"Column {col} not found in {csv_file}, using default index")
                        column_indices[col] = -1
                
                current_source = None
                for line_idx, line in enumerate(lines[4:], start=5):  # Start from line 5
                    line = line.strip()
                    if not line:
                        logging.debug(f"Skipping empty line at {line_idx} in {csv_file}")
                        continue
                
                    row = line.split(separator)
                    # Pad row with empty strings if shorter than columns
                    row = row + [""] * (len(columns) - len(row))
                
                    # Detect source
                    source_found = False
                    for marker in source_markers:
                        if marker in line:
                            current_source = marker
                            logging.info(f"Found source at line {line_idx}: {current_source}")
                            if current_source not in big_json["categories"][category_code]["sources"]:
                                big_json["categories"][category_code]["sources"][current_source] = {
                                    "level": "source",
                                    "name": current_source,
                                    "indicators": {}
                                }
                            source_found = True
                            break
                    if source_found:
                        continue
                
                    if not current_source:
                        logging.warning(f"No source defined for row at line {line_idx} in {csv_file}, skipping")
                        continue
                
                    # Check for indicator (first non-empty column starting with "SC")
                    code_tg = ""
                    for col in row:
                        if col.strip().startswith("SC"):
                            code_tg = col.strip()
                            break
                    if not code_tg:
                        logging.debug(f"Skipping non-indicator row at line {line_idx} in {csv_file}: {row[0]}")
                        continue
                
                    logging.info(f"Processing indicator row at line {line_idx} in {csv_file}: Code TG = {code_tg}")
                
                    # Extract data with fallback for missing indices
                    code_si_tg = row[column_indices["Code SI TG"]] if column_indices["Code SI TG"] >= 0 and column_indices["Code SI TG"] < len(row) else ""
                    definition = row[column_indices["Définition"]] if column_indices["Définition"] >= 0 and column_indices["Définition"] < len(row) else ""
                    intitule = row[column_indices["intitulé de l'indicateur (FR)"]] if column_indices["intitulé de l'indicateur (FR)"] >= 0 and column_indices["intitulé de l'indicateur (FR)"] < len(row) else ""
                    unite = row[column_indices["Unité de mesure"]] if column_indices["Unité de mesure"] >= 0 and column_indices["Unité de mesure"] < len(row) else ""
                    note = row[column_indices["Notes"]] if column_indices["Notes"] >= 0 and column_indices["Notes"] < len(row) else ""
                    valeur_generale = row[column_indices["Valeur Générale"]] if column_indices["Valeur Générale"] >= 0 and column_indices["Valeur Générale"] < len(row) else ""
                
                    if not code_tg:
                        logging.warning(f"Skipping row at line {line_idx} in {csv_file}: Missing Code TG")
                        continue
                
                    indicator_key = code_tg
                    sub_indicator_key = code_si_tg if code_si_tg else "NULL"
                    sub_indicator_code = f"{indicator_key}_{sub_indicator_key}" if code_si_tg else f"{indicator_key}_NULL"
                
                    label = intitule if intitule else definition
                
                    if not label:
                        logging.warning(f"Indicator at line {line_idx} in {csv_file} has no label (intitule or definition), skipping")
                        continue
                
                    if indicator_key not in big_json["categories"][category_code]["sources"][current_source]["indicators"]:
                        big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key] = {
                            "level": "indicator",
                            "code": indicator_key,
                            "label": label,
                            "notes": note,
                            "subindicators": {}
                        }
                        logging.info(f"Added indicator {indicator_key} to source {current_source} in category {category_code}")
                
                    if sub_indicator_key not in big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key]["subindicators"]:
                        big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key]["subindicators"][sub_indicator_key] = {
                            "level": "subindicator",
                            "code": sub_indicator_code,
                            "label": label,
                            "notes": note,
                            "years": {}
                        }
                        logging.info(f"Added subindicator {sub_indicator_key} to indicator {indicator_key} in source {current_source}")
                
                    year_key = year
                    year_code = f"{sub_indicator_code}_{year}"
                    if year_key not in big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"]:
                        big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key] = {
                            "level": "year",
                            "code": year_code,
                            "notes": current_source,
                            "value": {
                                "level": "region",
                                "code": f"{year_code}_value",
                                "value": valeur_generale,
                                "unit": unite,
                                "notes": ""
                            },
                            "regions": {
                                "level": "region",
                                "code": "region",
                                "values": {}
                            },
                            "desagregations": {}
                        }
                        logging.info(f"Added year {year_key} data for subindicator {sub_indicator_key}")
                
                    year_data = big_json["categories"][category_code]["sources"][current_source]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key]
                
                    regions = big_json["categories"][category_code]["disaggregation_levels"].get("Région", [])
                    for region in regions:
                        normalized_region = normalize_key(region)
                        try:
                            col_index = columns.index(region)
                            value = row[col_index].strip() if col_index < len(row) else ""
                            year_data["regions"]["values"][normalized_region] = {
                                "code": f"{year_code}_{normalized_region}",
                                "value": value,
                                "unit": unite,
                                "notes": ""
                            }
                        except ValueError:
                            year_data["regions"]["values"][normalized_region] = {
                                "code": f"{year_code}_{normalized_region}",
                                "value": "",
                                "unit": unite,
                                "notes": ""
                            }
                
                    for level, subcategories in big_json["categories"][category_code]["disaggregation_levels"].items():
                        if level == "Région":
                            continue
                        normalized_level = normalize_key(level)
                        year_data["desagregations"][normalized_level] = {
                            "level": "desagregation",
                            "code": normalized_level,
                            "values": {}
                        }
                        for subcategory in subcategories:
                            normalized_subcategory = normalize_key(subcategory)
                            try:
                                col_index = columns.index(subcategory)
                                value = row[col_index].strip() if col_index < len(row) else ""
                                year_data["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                    "code": f"{year_code}_{normalized_subcategory}",
                                    "value": value,
                                    "unit": unite,
                                    "notes": ""
                                }
                            except ValueError:
                                year_data["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                    "code": f"{year_code}_{normalized_subcategory}",
                                    "value": "",
                                    "unit": unite,
                                    "notes": ""
                                }
                
                    # Duplicate indicators from "Données exclusivement nationales" to top-level "indicators"
                    if current_source == "Données exclusivement nationales":
                        if indicator_key not in big_json["categories"][category_code]["indicators"]:
                            big_json["categories"][category_code]["indicators"][indicator_key] = {
                                "level": "indicator",
                                "code": indicator_key,
                                "label": label,
                                "notes": note,
                                "subindicators": {}
                            }
                            logging.info(f"Duplicated indicator {indicator_key} to 'indicators' for category {category_code}")
                        if sub_indicator_key not in big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"]:
                            big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key] = {
                                "level": "subindicator",
                                "code": sub_indicator_code,
                                "label": label,
                                "notes": note,
                                "years": {}
                            }
                            logging.info(f"Duplicated subindicator {sub_indicator_key} to 'indicators' for indicator {indicator_key}")
                        if year_key not in big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"]:
                            big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key] = {
                                "level": "year",
                                "code": year_code,
                                "notes": current_source,
                                "value": {
                                    "level": "region",
                                    "code": f"{year_code}_value",
                                    "value": valeur_generale,
                                    "unit": unite,
                                    "notes": ""
                                },
                                "regions": {
                                    "level": "region",
                                    "code": "region",
                                    "values": {}
                                },
                                "desagregations": {}
                            }
                            logging.info(f"Duplicated year {year_key} data to 'indicators' for subindicator {sub_indicator_key}")
                            year_data_indicators = big_json["categories"][category_code]["indicators"][indicator_key]["subindicators"][sub_indicator_key]["years"][year_key]
                            for region in regions:
                                normalized_region = normalize_key(region)
                                try:
                                    col_index = columns.index(region)
                                    value = row[col_index].strip() if col_index < len(row) else ""
                                    year_data_indicators["regions"]["values"][normalized_region] = {
                                        "code": f"{year_code}_{normalized_region}",
                                        "value": value,
                                        "unit": unite,
                                        "notes": ""
                                    }
                                except ValueError:
                                    year_data_indicators["regions"]["values"][normalized_region] = {
                                        "code": f"{year_code}_{normalized_region}",
                                        "value": "",
                                        "unit": unite,
                                        "notes": ""
                                    }
                            for level, subcategories in big_json["categories"][category_code]["disaggregation_levels"].items():
                                if level == "Région":
                                    continue
                                normalized_level = normalize_key(level)
                                year_data_indicators["desagregations"][normalized_level] = {
                                    "level": "desagregation",
                                    "code": normalized_level,
                                    "values": {}
                                }
                                for subcategory in subcategories:
                                    normalized_subcategory = normalize_key(subcategory)
                                    try:
                                        col_index = columns.index(subcategory)
                                        value = row[col_index].strip() if col_index < len(row) else ""
                                        year_data_indicators["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                            "code": f"{year_code}_{normalized_subcategory}",
                                            "value": value,
                                            "unit": unite,
                                            "notes": ""
                                        }
                                    except ValueError:
                                        year_data_indicators["desagregations"][normalized_level]["values"][normalized_subcategory] = {
                                            "code": f"{year_code}_{normalized_subcategory}",
                                            "value": "",
                                            "unit": unite,
                                            "notes": ""
                                        }
                
                os.remove(tmp_file_path)
    
    return big_json

def save_final_json(big_json, hdfs_output_file, processing_file_id, send_to_api):
    """
    Save the final big JSON to HDFS and send to API if specified using multipart/form-data.
    
    Args:
        big_json (dict): The big JSON object to save.
        hdfs_output_file (str): HDFS path to save the JSON file.
        processing_file_id (str): Unique ID for the processed file.
        send_to_api (bool): Flag indicating whether to send to API.
    
    Returns:
        dict: Updated big_json with metadata, and API call result.
    """
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    parent_dir = os.path.dirname(hdfs_output_file)
    client.makedirs(parent_dir)
    
    # Add metadata to the JSON
    big_json["processing_file_id"] = processing_file_id
    big_json["send_to_api"] = send_to_api
    
    # Save to HDFS
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp_output:
        tmp_output_path = tmp_output.name
        json.dump(big_json, tmp_output, ensure_ascii=False, indent=4)
    
    client.upload(hdfs_output_file, tmp_output_path, overwrite=True)
    logging.info(f"Saved final JSON to HDFS: {hdfs_output_file}")
    
    # Initialize API result
    api_result = {"api_call_made": False, "api_status": None, "api_message": ""}
    
    # Send to API if send_to_api is True using multipart/form-data
    if send_to_api:
        try:
            api_endpoint = f"{API_BASE_URL}/api/data/processed/upload"
            logging.info(f"Sending JSON to API endpoint as multipart/form-data: {api_endpoint}")
            # Open the temporary file in binary mode for the multipart upload
            with open(tmp_output_path, "rb") as f:
                files = {
                    "file": ("data.json", f, "application/json")
                }
                response = requests.post(api_endpoint, files=files, timeout=30)
                response.raise_for_status()
                api_result["api_call_made"] = True
                api_result["api_status"] = response.status_code
                api_result["api_message"] = "Successfully sent to API"
                logging.info(f"API call successful: Status {response.status_code}")
        except requests.exceptions.RequestException as e:
            api_result["api_call_made"] = True
            api_result["api_status"] = getattr(e.response, 'status_code', None)
            api_result["api_message"] = f"Failed to send to API: {str(e)}"
            logging.error(api_result["api_message"])
    
    # Remove the temporary file after both HDFS upload and API call
    os.remove(tmp_output_path)
    
    return big_json, api_result

if __name__ == "__main__":
    try:
        if len(sys.argv) < 4:
            error_msg = "Missing required arguments: <hdfs_base_path> <processing_file_id> <send_to_api>"
            logging.error(error_msg)
            print(json.dumps({"status": "failure", "message": error_msg}))
            sys.exit(1)
        
        hdfs_base_path = sys.argv[1]  # e.g., /data/${path}
        processing_file_id = sys.argv[2]  # e.g., 68125741c117df7d540d99c3
        send_to_api = sys.argv[3].lower() == "true"  # e.g., true/false
        
        hdfs_output_file = os.path.join(hdfs_base_path, "results", "data.json")
        
        excel_data = sys.stdin.buffer.read()
        
        csv_data = excel_to_csv(excel_data, separator="|")
        saved_files = save_csv_to_hdfs(csv_data, hdfs_base_path)
        big_json = process_csvs_and_init_json(hdfs_base_path, separator="|")
        big_json = identify_columns_and_disaggregation(big_json, hdfs_base_path, separator="|")
        updated_big_json = process_sources_and_data(big_json, hdfs_base_path, separator="|")
        final_json, api_result = save_final_json(updated_big_json, hdfs_output_file, processing_file_id, send_to_api)
        
        # Output success status for NiFi, including API result
        status_message = {
            "status": "success",
            "message": "Processing completed successfully",
            "hdfs_json_path": hdfs_output_file,
            "processing_file_id": processing_file_id,
            "send_to_api": send_to_api,
            "api_call_made": api_result["api_call_made"],
            "api_status": api_result["api_status"],
            "api_message": api_result["api_message"]
        }
        print(json.dumps(status_message))
        sys.exit(0)
        
    except Exception as e:
        error_msg = f"Failed to process Excel to final JSON: {str(e)}"
        logging.error(error_msg)
        status_message = {
            "status": "failure",
            "message": error_msg,
            "processing_file_id": processing_file_id if 'processing_file_id' in locals() else "unknown"
        }
        print(json.dumps(status_message))
        sys.exit(1)