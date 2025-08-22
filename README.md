Recon: Insurance Data Reconciliation Tool
Recon is a comprehensive, desktop-based application designed to streamline the complex process of reconciling insurance policy data. It compares an organization's internal records against the Management Information System (MIS) reports provided by various insurance companies. The tool features a user-friendly graphical interface, powerful data processing capabilities, and insightful reporting dashboards to identify discrepancies quickly and efficiently.

## Key Features 

Intuitive Graphical User Interface (GUI): Built with Flet, the application provides a clean and modern interface for uploading files, configuring comparisons, and viewing results without needing command-line skills.

Multi-Source Data Handling: Seamlessly loads and processes data from three key sources:

Internal Booked Data: Your organization's record of policies.

Insurer MIS Files: Reports from multiple insurance partners.

Offline Punching Data: Records for policies that are pending booking.

Two-Way Reconciliation: Identifies discrepancies in both directions:

Policies in your internal system but missing or mismatched in the insurer's MIS.

Policies present in the insurer's MIS but missing or mismatched in your internal system.

Insurer-Specific Logic: Highly configurable to handle the unique data formats of each insurance company:

Column Mapping: Maps dozens of fields to insurer-specific column names, including handling multiple possible names for a single field (coalescing).

Sheet Name Detection: Automatically finds the correct data sheet in multi-sheet Excel files.

Data Filtering: Applies predefined rules to clean raw MIS data by removing irrelevant records like endorsements, cancellations, or non-motor policies before comparison.

Advanced Comparison Engine: Employs sophisticated logic for accurate matching:

Fuzzy Matching: Uses fuzzywuzzy for comparing string fields like customer names to account for minor spelling differences.

Numeric Tolerance: Allows for minor floating-point differences (e.g., ±₹1.0) in premium amounts.

Date Standardization: Intelligently parses and compares dates from various formats (YYYY-MM-DD, DD/MM/YYYY, etc.).

Interactive Dashboard & Reporting:

Results Summary: Presents key performance indicators (KPIs) like mismatch counts, unbooked policies, premium differences, and more.

Full Dashboard: An interactive view with charts, filterable data tables, and search functionality for in-depth analysis.

Insurer-wise Report: Generates a comprehensive summary report that categorizes all MIS policies into Booked, Unbooked, and Pending buckets, providing a clear financial and operational overview.

Data Export: Allows users to export raw comparison results, filtered mismatch lists, or final summary reports to CSV and Excel for further analysis and record-keeping.

## Architecture & Core Components 
The application is built around a few key classes that separate the user interface from the core data processing logic.

InsuranceDataComparator
This is the brain of the application. It's a backend class that contains all the business logic for data reconciliation.

Configuration Dictionaries: Holds all insurer-specific rules:

column_mappings: The primary dictionary defining how to translate standard field names (like policy_number) to the unique column names used by each insurer (e.g., S_POLICYNO for SHRIRAM, PolicyNo for RELIANCE).

insurer_filters: Contains rules to preprocess and clean raw MIS data before comparison.

sheet_mapping: Maps insurers to their specific Excel sheet names.

insurer_name_map: Helps identify the correct insurer from variations in the "Insurance Company" column of the internal data.

Core Methods:

compare_datasets_async(): The main method that orchestrates the entire comparison process, including filtering, two-way matching, and generating results.

apply_insurer_filters(): Cleans the MIS data based on the predefined rules.

compare_values(): The low-level function that compares two individual values based on their data type (string, number, date).

InsuranceValidationApp
This is the main Flet application class that builds and manages the GUI.

UI Construction: Creates all the visual elements like file pickers, dropdowns, buttons, and progress bars.

State Management: Keeps track of loaded DataFrames (internal_df, mis_dfs, offline_df) and comparison results.

Event Handling: Manages user actions like button clicks and file uploads, triggering the InsuranceDataComparator to perform tasks in a separate thread to keep the UI responsive.

DashboardView & InsurerWiseReportView
These are dedicated view classes responsible for rendering the results.

DashboardView: Builds the interactive dashboard with KPIs, charts, and a paginated data table for drilling down into the comparison results.

InsurerWiseReportView: Builds the final summary report view, presenting the Booked, Unbooked, and Pending data in a clean, professional table format.

## How It Works: The Data Flow ⚙️
Upload Data: The user uploads three types of files:

Internal Data: One or more CSV/Excel files containing booked policies.

MIS Data: Multiple Excel/CSV files from different insurers. The application intelligently detects the insurer from the filename (e.g., Bajaj_MIS.xlsx, HDFC_Report.csv).

Offline Data: A file containing policies that have been punched but are not yet reflected in the main internal system.

Select & Configure: The user selects a specific insurer from a dropdown menu populated with the detected MIS files. They can then choose which data fields they want to compare.

Run Comparison: The user clicks "Run Comparison."

The app takes the corresponding MIS file and applies the insurer-specific filters to remove noise (endorsements, cancellations, etc.).

It then performs a two-way comparison between the cleaned MIS data and the internal data for that insurer.

View Results: The results are displayed instantly in the results panel, showing KPIs like:

Number of mismatches.

Number of unbooked policies (policies in MIS but not internal data).

Number of policies not found in MIS (policies in internal data but not MIS).

Total premium amount of booked vs. unbooked policies.

Analyze & Export: From the results screen, the user can:

Dive into the Dashboard for a visual analysis.

Generate the Insurer-wise Report which uses all three data sources to categorize every policy from the MIS file.

Export the detailed results or just the discrepancies to a CSV/Excel file.

