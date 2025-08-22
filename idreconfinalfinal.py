import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, timedelta
import json
import base64
import io
import threading
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import logging
from fuzzywuzzy import fuzz
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import tempfile
from pathlib import Path
import hashlib
import traceback
import os
import platform
import subprocess
import webbrowser
import flet as ft
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ComparisonResult:
    """Data class for comparison results"""
    policy_number: str
    field: str
    match_status: str
    match_score: float
    internal_value: Any
    mis_value: Any
    details: str

class MLColumnMapper:
    """Simple ML column mapper placeholder"""
    def __init__(self):
        self.is_trained = False

    def train(self, mappings):
        """Train the mapper with existing mappings"""
        self.is_trained = True

    def predict_mapping(self, column_name, threshold=0.6):
        """Predict mapping for a column"""
        return None

class InsuranceDataComparator:
    """Enhanced Insurance Data Comparator with two-way reconciliation"""
    def __init__(self):
        # Initialize ML mapper
        self.ml_mapper = MLColumnMapper()

        # Cache for performance
        self._cache = {}
        self._cache_size = 1000

        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=mp.cpu_count())
        
        # Dictionary to hold mappings resolved at runtime (e.g., after coalescing columns)
        self.resolved_mappings = {}

        # Insurer-specific sheet name mapping
        self.sheet_mapping = {
            'RELIANCE': 'Sheet1',
            'SBI': 'Sheet1',
            'RAHEJA': 'Raw',
            'TATA': 'Sheet1',
            'ZUNO': 'Sheet1',
            'UNITED': 'Sheet1',
            'DIGIT': 'Sheet1',
            'SOMPO': 'Sheet1',
            'MAGMA': 'Sheet1',
            'SHRIRAM': 'GNRDUMP31MAY2025-GNRDUMP"DDMonthYYYY"',
            'ROYAL_SUNDARAM': 'Sheet1',
            'ORIENTAL': 'Sheet1',
            'NATIONAL': 'SheetName',
            'LIBERTY': 'Sheet1',
            'KOTAK': ['Sheet1', 'Sheet3'],  # Can be either Sheet1 or Sheet3
            'IFFCO': 'Digital',
            'ICICI': 'Sheet1',
            'HDFC': 'Sheet1',
            'FGI': 'Sheet1',  # FGI = Future Generali Insurance
            'CHOLA': 'Sheet1',
            'BAJAJ': ['New', 'New Business']  # Bajaj uses "New" sheet, fallback to "New Business"
        }

        # Old to New column mappings - for converting old database format to new format
        self.old_to_new_mappings = {
            'Insurance Company': 'Insurance Company',
            'PolicyNumber': 'Policy Number',  # Note: space added in new format
            'Registration Number': 'Registration Number',
            'Request Id': 'Request Id',
            'Booked Time Stamp': 'Booked Time Stamp',
            'Policy Status': 'Policy Status',
            'Booking Type': 'Booking Type',
            'Ticket Type': 'Ticket Type',
            'Policy Source': 'Policy Source',
            'Policy Sub Source': 'Policy Sub Source',
            'Assigned to': 'Assigned to',
            'Payment Pickup Status': 'Payment Pickup Status',
            'Payment Clearance Status': 'Payment Clearance Status',
            'Case Type': 'Case Type',
            'Policy Type': 'Policy Type',
            'Policy Medium': 'Policy Medium',
            'Vehicle Sub Usage Type': 'Vehicle Sub Usage Type',
            'Vehicle Category': 'Vehicle Category',
            'Vehicle Sub Category': 'Vehicle Sub Category',
            'Vehicle Name': 'Vehicle Name',
            'Fuel Type': 'Fuel Type',
            'Engine Number': 'Engine Number',
            'Chassis No.': 'Chassis No.',
            'Gross Weight': 'Gross Weight',
            'Dealership Type': 'Dealership Type',
            'GCD Code': 'GCD Code',
            'Dealer Name': 'Dealer Name',
            'Dealer City': 'Dealer City',
            'Payment Mode': 'Payment Mode',
            'Premium': 'Premium',
            'Final Od Premium': 'Final Od Premium',
            'Final TP Premium': 'Final TP Premium',
            'Previous Policy Number': 'Previous Policy Number',
            'Policy Start Date': 'Policy Start Date',
            'Policy End Date': 'Policy End Date',
            'Policy Tenure': 'Policy Tenure',
            'CPA Cover Premium': 'CPA Cover Premium',
            'Customer Name': 'Customer Name',
            'Broker Name': 'Broker Name',
            'Created By': 'Created By',
            'Booked By': 'Booked By',
            'cKYC Number': 'cKYC Number',
            'Seating Capacity': 'Seating Capacity',
            'Business Manager (DSA)': 'Business Manager (DSA)',
            'Area Manager (DSA)': 'Area Manager (DSA)',
            'State Head (DSA)': 'State Head (DSA)',
            'Regional Head (DSA)': 'Regional Head (DSA)',
            'Zonal Head (DSA)': 'Zonal Head (DSA)',
            'Senior Relationship Manager (DSA)': 'Senior Relationship Manager (DSA)',
            'National Head (DSA)': 'National Head (DSA)',
            'Engine Cubic Capacity': 'Engine Cubic Capacity',
        }
        # Column mappings for each insurer
        self.column_mappings = {
            'SHRIRAM': {
                'policy_number': 'S_POLICYNO',
                'customer_name': 'S_INSUREDNAME',
                'policy_start_date': 'S_DOI',
                'policy_end_date': 'S_DOE',
                'registration_number': 'S_VEH_REG_NO',
                'engine_number': 'S_ENGNO',
                'chassis_number': 'S_CHNO',
                'total_premium': 'S_NET',
                'tp_premium': 'S_TP_TOTAL',
                'previous_policy_number': 'S_PREVIOUS_POL_NO',
                'broker_name': 'S_SOURCENAME',
                'broker_code': None,
                'sum_insured': None,
                'vehicle_make': 'S_MAKE_DESC',
                'vehicle_model': 'S_VEH_MODEL',
                'fuel_type': 'S_VEHFUEL_DESC',
                'gvw': 'S_GVW',
                'seating_capacity': 'S_VEH_SC',
                'ncb_percentage': 'S_NCB_PERCENTAGE',
                'manufacturing_year': None,
                'engine_cubic_capacity': 'S_VEH_CC',
            },
            'RELIANCE': {
                'policy_number': 'PolicyNo',
                'customer_name': 'CustomerName',
                'policy_start_date': 'PolicyStartDate',
                'policy_end_date': 'PolicyEndDate',
                'registration_number': 'VechileNo',
                'engine_number': 'EngineNo',
                'chassis_number': 'ChassisNo',
                'total_premium': 'GrossPremium',
                'tp_premium': 'TpPremium',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'IMDName',
                'broker_code': 'IMDCode',
                'sum_insured': 'SumInsured',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'FuelType',
                'gvw': 'GVW',
                'seating_capacity': 'SeatingCapacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'YearOfManufacture',
                'engine_cubic_capacity': 'CC',
            },
            'ROYAL': {
                'policy_number': 'Policy No',
                'customer_name': 'Client Full Name',
                'policy_start_date': 'Inception Date',
                'policy_end_date': 'Expiry Date',
                'registration_number': 'Reg No',
                'engine_number': 'Engine No',
                'chassis_number': 'Chasis No',
                'total_premium': 'Client Premium',
                'tp_premium': 'TP Premium',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Incremental SI',
                'vehicle_make': 'Make',
                'vehicle_model': 'Mfr_Model_GWP',
                'fuel_type': 'Fuel_Type',
                'gvw': 'GVW_Ton',
                'seating_capacity': 'SeatingCapacity',
                'ncb_percentage': 'NCB_Rate',
                'manufacturing_year': 'Year of Manufacturing',
                'engine_cubic_capacity': 'Cubic_Capcity',
            },
            'SBI': {
                'policy_number': ['PolicyNo'],
                'customer_name': 'CustomerName',
                'policy_start_date': 'PolicyStartDate',
                'policy_end_date': 'PolicyEndDate',
                'registration_number': None,
                'engine_number': None,
                'chassis_number': None,
                'total_premium': ['FinalPremium', 'GWP LACS', 'GWP Lacs','Gross Written Premium'],
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': ['IMD name', 'IMDName', 'Channel_Name'],
                'broker_code': 'IMDCode',
                'sum_insured': None,
                'vehicle_make': 'MakeModel',
                'vehicle_model': ['Product_Name', 'ProductName'],
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None
            },
            'ICICI': {
                'policy_number': 'POL_NUM_TXT',
                'customer_name': 'CUSTOMER_NAME',
                'policy_start_date': 'POL_START_DATE',
                'policy_end_date': 'POL_END_DATE',
                'registration_number': 'MOTOR_REGISTRATION_NUM',
                'engine_number': 'VEH_ENGINENO',
                'chassis_number': 'VEH_CHASSISNO',
                'total_premium': 'TOTAL_GROSS_PREMIUM',
                'tp_premium': 'MOTOR_TP_PREMIUM_AMT',
                'previous_policy_number': 'XC_PREV_POL_NO',
                'broker_name': 'AGENT_NAME',
                'broker_code': 'CHILD_ID',
                'sum_insured': 'POL_TOT_SUM_INSURED_AMT',
                'vehicle_make': 'MANUFACTURER_NAME',
                'vehicle_model': 'MODEL_NAME',
                'fuel_type': 'XC_FUEL_TYPE',
                'gvw': 'POL_GVW',
                'seating_capacity': 'XC_POL_SEATING_CAPACITY',
                'ncb_percentage': 'XC_POL_NCB_PCT',
                'manufacturing_year': 'MOTOR_MANUFACTURER_YEAR',
                'engine_cubic_capacity': 'MOTOR_ENGINE_CC',
            },
            'NATIONAL': {
                'policy_number': ['Policy Number', 'policy No'],
                'customer_name': 'Customer Name',
                'policy_start_date': 'Policy Effective Date',
                'policy_end_date': 'Policy Expiry Date',
                'registration_number': 'Vehicle Registration Number',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Premium',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': 'POSP Name',
                'broker_code': 'POSP Code',
                'sum_insured': None,
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'Gross Vehicle Weight',
                'seating_capacity': 'Seating Capacity',
                'ncb_percentage': 'NCB Percentage',
                'manufacturing_year': 'Manufacturing Year',
                'engine_cubic_capacity': 'Engine Cubic Capacity',
            },
            'DIGIT': {
                'policy_number': ['POLICY_NUMBER','Policy_Number'],
                'customer_name': 'INSURED_PERSON',
                'policy_start_date': 'RISK_INC_DATE',
                'policy_end_date': 'RISK_EXP_DATE',
                'registration_number': 'VEH_REG_NO',
                'engine_number': 'ENGINE_NO',
                'chassis_number': 'CHASSIS_NO',
                'total_premium': 'GROSS_PREMIUM',
                'tp_premium': 'TP_PREMIUM',
                'previous_policy_number': 'PREV_POLICY_NO',
                'broker_name': 'IMD_NAME',
                'broker_code': None,
                'sum_insured': 'SUM_INSURED',
                'vehicle_make': 'VEHICLE_MAKE',
                'vehicle_model': 'VEHICLE_MODEL',
                'fuel_type': 'FUEL_TYPE',
                'gvw': 'VEH_GVW',
                'seating_capacity': 'VEH_SEATING',
                'ncb_percentage': 'CURR_NCB',
                'manufacturing_year': 'VEHICLE_MANF_YEAR',
                'engine_cubic_capacity': 'VEH_CC',
            },
            'MAGMA': {
                'policy_number': 'PolicyNo',
                'customer_name': 'CustomerName',
                'policy_start_date': 'RiskStartDate',
                'policy_end_date': 'RiskEndDate',
                'registration_number': 'RegistrationNumber',
                'engine_number': 'EngineNumber',
                'chassis_number': 'ChassisNumber',
                'total_premium': 'PremiumAmount',
                'tp_premium': None,
                'previous_policy_number': 'PreviousYearPolicyNumber',
                'broker_name': 'AgentName',
                'broker_code': 'AgentCode',
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': 'ProductDesc',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'UNIVERSAL': {
                'policy_number': ['POLICY NO CHAR', 'USGIpos Policy Number','POLICY NO'],
                'customer_name': 'INSURED NAME',
                'policy_start_date': 'START DATE',
                'policy_end_date': 'EXPIRY DATE',
                'registration_number': 'MTOR Registration No',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'GROSS PREMIUM',
                'tp_premium': 'TOTAL TP PREMIUM',
                'previous_policy_number': 'Previous Yr Policy',
                'broker_name': 'INTERMEDIARY',
                'broker_code': 'INTERMEDIARY CODE',
                'sum_insured': 'TOTAL SUM INSURED',
                'vehicle_make': 'MAKE',
                'vehicle_model': 'MODEL',
                'fuel_type': None,
                'gvw': 'Gvw',
                'seating_capacity': 'Vehicle Seating Capacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'YEAR OF MANUFACTURING',
                'engine_cubic_capacity': None,
            },
            'UNITED': {
                'policy_number': 'Policy Number',
                'customer_name': 'Insured Name',
                'policy_start_date': 'Effect Date',
                'policy_end_date': 'Expiry Date',
                'registration_number': 'Registration Number',
                'engine_number': 'Engine Number',
                'chassis_number': 'Chassis Number',
                'total_premium': 'company_wise_own_share_premium_total',
                'tp_premium': 'TP Premium',
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': 'Agent/Br.Cd',
                'sum_insured': 'Sum Insured',
                'vehicle_make': None,
                'vehicle_model': None,
                'fuel_type': None,
                'gvw': 'NUM_GVW',
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'FGI': {
                'policy_number': 'Policy Number',
                'customer_name': 'Customer Name',
                'policy_start_date': 'Transaction',
                'policy_end_date': 'Policy Expiry Date',
                'registration_number': 'Registration Number',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Final Premium',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': 'Agent Code',
                'sum_insured': None,
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fueal Type',
                'gvw': 'GVW',
                'seating_capacity': 'Seating Capacity',
                'ncb_percentage': None,
                'manufacturing_year': 'Year Of Manufacturing',
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'SOMPO': {
                'policy_number': ['NUM_POLCIY_NO', 'POLICY_NO_CHAR', 'ILPOS_POLICY_NUMBER'],
                'customer_name': 'INSURED_NAME',
                'policy_start_date': 'START_DATE',
                'policy_end_date': 'EXPIRY_DATE',
                'registration_number': 'MOTOR_REGISTRATION_NO',
                'engine_number': 'ENGINE_NO',
                'chassis_number': 'CHASSIS_NO',
                'total_premium': 'GROSS_PREMIUM',
                'tp_premium': 'TOTAL_TP_PREMIUM',
                'previous_policy_number': 'PREVIOUS_POLICY_NUMBER',
                'broker_name': 'INTERMEDIARY',
                'broker_code': 'INTERMEDIARY_CODE',
                'sum_insured': 'TOTAL_SI',
                'vehicle_make': 'MAKE',
                'vehicle_model': 'MODEL',
                'fuel_type': 'FUEL_TYPE',
                'gvw': 'GVW',
                'seating_capacity': 'VEHICLE_SEATING_CAPACITY',
                'ncb_percentage': 'NCB_PERCENTAGE',
                'manufacturing_year': 'YR_OF_MANUFACTURING',
                'engine_cubic_capacity': 'CUBIC_CAPACITY',
            },
            'BAJAJ': {
                'policy_number': ['PolicyNo','P Policy Number'],
                'customer_name': ['CustomerName', 'Partner Desc'],
                'policy_start_date': 'Risk Start Date',
                'policy_end_date': 'Risk End Date',
                'registration_number': 'RegistrationNumber',
                'engine_number': 'EngineNumber',
                'chassis_number': 'ChassisNumber',
                'total_premium': ['R Rnwd Gross Prem', 'Gross Premium'],
                'tp_premium': 'R Rnwd Tp Prem',
                'previous_policy_number': 'Previous Year Policy Number',
                'broker_name': 'AgentName',
                'broker_code': 'AgentCode',
                'sum_insured': 'R Rnwd Sum Insured',
                'vehicle_make': None,
                'vehicle_model': 'Product Desc',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'CHOLA': {
                'policy_number': 'Policy No',
                'customer_name': 'Customer name',
                'policy_start_date': 'RSD',
                'policy_end_date': 'RED',
                'registration_number': 'Regist_NO',
                'engine_number': 'Engine No',
                'chassis_number': 'Chassis No',
                'total_premium': 'Gross Premium',
                'tp_premium': 'Third Party liability',
                'previous_policy_number': 'PREVIOUS_POLICY_NUMBER',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Total IDV',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'GROSS_VEHICLE_WEIGHT',
                'seating_capacity': 'TOTAL_SEATING_CAPACITY',
                'ncb_percentage': 'NCB_PERCENT',
                'manufacturing_year': 'MFG Year',
                'engine_cubic_capacity': 'CUBIC_CAPACITY',
            },
            'HDFC': {
                'policy_number': 'Policy No',
                'customer_name': 'Customer Name',
                'policy_start_date': 'Start Date',
                'policy_end_date': 'End Date',
                'registration_number': 'POL_MOT_VEH_REGISTRATION_NUM',
                'engine_number': 'ENGINE NUM',
                'chassis_number': 'CHASSIS NUM',
                'total_premium': 'Total GWP',
                'tp_premium': 'TP',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Sum Insured',
                'vehicle_make': 'POL_MOT_MANUFACTURER_NAME',
                'vehicle_model': 'POL_MOT_MODEL_NAME',
                'fuel_type': 'POL_FUEL_TYPE',
                'gvw': 'POL_GROSS_VEH_WT',
                'seating_capacity': None,
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'POL_MOT_MANUFACTURER_YEAR',
                'engine_cubic_capacity': None,
            },
            'IFFCO': {
                'policy_number': 'P400 policy',
                'customer_name': ['First Name', 'Last Name'],
                'policy_start_date': 'Inceptiondate',
                'policy_end_date': 'Expirydate',
                'registration_number': 'Registration No',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Total Premium after discount/loading',
                'tp_premium': 'TP Gross Premium',
                'previous_policy_number': 'PreviousInsurerPolicy Number',
                'broker_name': None,
                'broker_code': 'AgentNo',
                'sum_insured': 'Total Sum Insured',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'GVW',
                'seating_capacity': 'Carrying Capacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'Year',
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'KOTAK': {
                'policy_number': 'POLICY NO',
                'customer_name': 'CUSTOMER NAME',
                'policy_start_date': 'POLICY INCEPTION DATE',
                'policy_end_date': 'POLICY EXPIRY DATE',
                'registration_number': 'Registration Number',
                'engine_number': 'Engine Number',
                'chassis_number': 'Chassis Number',
                'total_premium': 'GWP',
                'tp_premium': 'ALL OTHER TP COVER PREMIUM',
                'previous_policy_number': None,
                'broker_name': 'IMD NAME',
                'broker_code': 'IMD CODE',
                'sum_insured': 'SYSTEMIDV',
                'vehicle_make': 'VehicleMake',
                'vehicle_model': 'VehicleModel',
                'fuel_type': 'FuelType',
                'gvw': 'VehicleWeight',
                'seating_capacity': None,
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'ManufacturerYear',
                'engine_cubic_capacity': 'VehicleCC',
            },
            'RAHEJA': {
                'policy_number': 'P400 Pol No.',
                'customer_name': 'CompanyName',
                'policy_start_date': 'policy_start_date',
                'policy_end_date': 'Expiry date',
                'registration_number': 'reg_no',
                'engine_number': 'engine_no',
                'chassis_number': 'chasis_no',
                'total_premium': 'final_premium',
                'tp_premium': 'TP Premium',
                'previous_policy_number': None,
                'broker_name': 'IMD Name',
                'broker_code': 'IMD Code',
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': 'Sub_Product',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'TATA': {
                'policy_number': 'policy_no',
                'customer_name': 'clientname',
                'policy_start_date': 'pol_incept_date',
                'policy_end_date': 'pol_exp_date',
                'registration_number': 'registration_no',
                'engine_number': 'veh_engine_no',
                'chassis_number': 'veh_chassis_no',
                'total_premium': 'total_premium',
                'tp_premium': 'basictp',
                'previous_policy_number': None,
                'broker_name': 'producername',
                'broker_code': 'producer_cd',
                'sum_insured': 'vehicleidv',
                'vehicle_make': 'veh_make',
                'vehicle_model': 'veh_model',
                'fuel_type': 'fuel_type',
                'gvw': 'veh_gr_wgt_cnt',
                'seating_capacity': 'seat_cap_cnt',
                'ncb_percentage': 'ncb_perctg',
                'manufacturing_year': 'mfg_year',
                'engine_cubic_capacity': 'kw_cc_no',
            },
            'ZUNO': {
                'policy_number': 'Policy_Number',
                'customer_name': 'Policy_Holder_Name',
                'policy_start_date': 'Policy_Start_Date',
                'policy_end_date': 'Policy_End_Date',
                'registration_number': 'Registration No',
                'engine_number': 'Engine No',
                'chassis_number': 'chasis No',
                'total_premium': 'Gross_Premium_Total',
                'tp_premium': 'Motor TP Premium',
                'previous_policy_number': 'Previous_Policy_No',
                'broker_name': 'Intermediary_Name',
                'broker_code': 'Intermediary_Code',
                'sum_insured': 'NUM_IDV',
                'vehicle_make': 'Make Code',
                'vehicle_model': 'Model Code',
                'fuel_type': 'Type of Fuel',
                'gvw': 'GVW',
                'seating_capacity': 'Capacity Type',
                'ncb_percentage': 'NCB_Percentage',
                'manufacturing_year': 'Manufacturing Year',
                'engine_cubic_capacity': 'Num_PCC CC',
            },
            'LIBERTY': {
                'policy_number': 'Policy No.',
                'customer_name': 'Insured Name',
                'policy_start_date': 'Policy Start Date',
                'policy_end_date': 'Policy End Date',
                'registration_number': 'Veh Reg No',
                'engine_number': 'Engine NO',
                'chassis_number': 'Chassis NO',
                'total_premium': 'Net Premium',
                'tp_premium': 'Net TP Premium / War & SRCC',
                'previous_policy_number': 'Previous Policy No.',
                'broker_name': 'Agent/ Broker Name',
                'broker_code': 'Agent/ Broker Code',
                'sum_insured': 'Sum Insured',
                'vehicle_make': 'Make Name',
                'vehicle_model': 'Model Name',
                'fuel_type': 'Fuel Type',
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': 'Current Year NCB (%)',
                'manufacturing_year': None,
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'ORIENTAL': {
                'policy_number': 'POLICY NO',
                'customer_name': None,
                'policy_start_date': None,
                'policy_end_date': None,
                'registration_number': None,
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'PREMIUM',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': None,
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': None,
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
        }

        # Internal dataset column mapping (new database columns)
        self.internal_columns = {
            'policy_number': 'Policy Number',
            'customer_name': 'Customer Name',
            'registration_number': 'Registration Number',
            'engine_number': 'Engine Number',
            'chassis_number': 'Chassis No.',
            'fuel_type': 'Fuel Type',
            'vehicle_name': 'Vehicle Name',
            'vehicle_sub_category': 'Vehicle Sub Category',
            'total_premium': 'Premium',
            'tp_premium': 'Final TP Premium',
            'policy_start_date': 'Policy Start Date',
            'policy_end_date': 'Policy End Date',
            'previous_policy_number': 'Previous Policy Number',
            'broker_name': 'Broker Name',
            'seating_capacity': 'Seating Capacity',
            'insurance_company': 'Insurance Company',
            'gvw': 'Gross Weight Category',
            'engine_cubic_capacity': 'Engine Cubic Capacity',
            'policy_type': 'Policy Type',
        }
        # Add insurer-specific filter configurations
        self.insurer_filters = {
            'CHOLA': [
                {
                    'field': 'LOB',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB records'
                },
                {
                    'field': 'POLICY_CASE',
                    'condition': 'not_in',
                    'values': ['Endorsement', 'CANCEL'],
                    'description': 'Remove Endorsement and Cancellation records'
                }
            ],
            'IFFCO': [
                {
                    'field': 'Status',
                    'condition': 'not_contains',
                    'values': ['Cancelled Policy'],
                    'description': 'Remove cancelled policies'
                },
                {
                    'field': 'Product',
                    'condition': 'not_contains',
                    'values': ['Other than motor codes'],
                    'description': 'Keep only motor products'
                }
            ],
            'KOTAK': [
                {
                    'field': 'TRANSACTION TYPE',
                    'condition': 'not_in',
                    'values': ['ENDORSEMENT', 'POLICY', 'CANCELLATION', 'ENDORSEMENTPOLICY'],
                    'description': 'Remove endorsement and cancellation transactions'
                },
                {
                    'field': 'LOB NAME',
                    'condition': 'not_contains',
                    'values': ['Other than MOTOR'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'LIBERTY': [
                {
                    'field': 'lob name',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                },
                {
                    'field': 'transaction type',
                    'condition': 'not_in',
                    'values': ['ENDORSEMENT', 'CANCELLATION'],
                    'description': 'Remove endorsement and cancellation transactions'
                }
            ],
            'ORIENTAL': [
                {
                    'field': 'POLICY NO',
                    'condition': 'not_null',
                    'values': [],
                    'description': 'Remove records with null policy numbers'
                }
            ],
            'RELIANCE': [
                {
                    'field': 'IMDName',
                    'condition': 'not_contains',
                    'values': ['GIRNAR INSURANCE BROKERS PVT LTD'],
                    'description': 'Remove specific broker records'
                },
                {
                    'field': 'BusinessType',
                    'condition': 'not_contains',
                    'values': ['Endorsement'],
                    'description': 'Remove endorsement business type'
                },
                {
                    'field': 'PolicyStatus',
                    'condition': 'not_contains',
                    'values': ['InActive'],
                    'description': 'Remove inactive policies'
                }
            ],
            'SHRIRAM': [
                {
                    'field': 'Category',
                    'condition': 'not_contains',
                    'values': ['NON MOTOR'],
                    'description': 'Keep only motor categories'
                },
                {
                    'field': 'S_ENDORSEMENT',
                    'condition': 'not_contains',
                    'values': ['IDX'],
                    'description': 'Remove endorsement records'
                },
                {
                    'field': 'S_ENDORSEMENT',
                    'condition': 'equals',
                    'values': [0],
                    'description': 'Keep only base policies (S_ENDORSEMENT = 0)'
                },
                {
                    'field': 'S_ENDORSEMENTIDX',
                    'condition': 'equals',
                    'values': [0],
                    'description': 'Only base policies (S_ENDORSEMENTIDX = 0)'
                }
            ],
            'TATA': [
                {
                    'field': 'record_type_desc',
                    'condition': 'not_in',
                    'values': ['ENDORSEMENT', 'CANCELLATION'],
                    'description': 'Remove endorsement and cancellation records'
                },
                {
                    'field': 'product_name',
                    'condition': 'not_contains',
                    'values': ['Other than Motor Products'],
                    'description': 'Keep only Motor products'
                }
            ],
            'UNITED': [
                {
                    'field': 'Endorsement Number',
                    'condition': 'equals',
                    'values': [0],
                    'description': 'Only Base Policies (Endorsement Number = 0)'
                },
                {
                    'field': 'Effect Date',
                    'condition': 'not_older_than_issue',
                    'values': [],
                    'description': 'Remove records where Effect Date is older than Issue Date'
                },
                {
                    'field': 'Department',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor department'
                },
                {
                    'field': 'Total',
                    'condition': 'positive',
                    'values': [],
                    'description': 'Remove negative premium records'
                }
            ],
            'UNIVERSAL': [
                {
                    'field': 'ENDORSMENT_NO',
                    'condition': 'equals',
                    'values': [0],
                    'description': 'Only base policies (Endorsement = 0)'
                },
                {
                    'field': 'LINE_OF_BUSINESS',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'ZUNO': [
                {
                    'field': 'Policy_Type',
                    'condition': 'not_in',
                    'values': ['ENDORSEMENT', 'CANCELLATION'],
                    'description': 'Remove endorsement and cancellation policies'
                },
                {
                    'field': 'Line_Of_Business',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'DIGIT': [
                {
                    'field': 'ENDORSEMENT_IND',
                    'condition': 'equals',
                    'values': ['G01'],
                    'description': 'ENDORSEMENT_IND = G01 (base policies only)'
                },
                {
                    'field': 'STATUS',
                    'condition': 'equals',
                    'values': ['A'],
                    'description': 'STATUS = A (active policies only)'
                },
                {
                    'field': 'STATUS',
                    'condition': 'not_equals',
                    'values': ['S'],
                    'description': 'Remove cancelled policies (POLICY_STATUS = S)'
                },
                {
                    'field': 'PRODUCT_LOB',
                    'condition': 'not_contains',
                    'values': ['Other than Motor Lob'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'HDFC': [
                {
                    'field': 'Endorsement No',
                    'condition': 'equals',
                    'values': ['000'],
                    'description': 'Only base policies (Endorsement No = 000)'
                },
                {
                    'field': 'Finance LOB',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                },
                {
                    'field': 'Pol Issue Date',
                    'condition': 'current_month_only',
                    'values': [],
                    'description': 'Keep only current month policies'
                },
                {
                    'field': 'Endorsement Type',
                    'condition': 'not_contains',
                    'values': ['Endorsement'],
                    'description': 'Remove endorsement types'
                }
            ],
            'FGI': [
                {
                    'field': 'Product Name',
                    'condition': 'not_contains',
                    'values': ['Other than Motor Products'],
                    'description': 'Keep only Motor products'
                }
            ],
            'ICICI': [
                {
                    'field': 'ENDORSEMENT_TYPE',
                    'condition': 'not_in',
                    'values': ['ENDORSED', 'CANCELLED'],
                    'description': 'Remove endorsed and cancelled policies'
                },
                {
                    'field': 'PRODUCT_NAME',
                    'condition': 'not_contains',
                    'values': ['Other than Motor Products'],
                    'description': 'Keep only Motor products'
                },
                {
                    'field': 'POL_ISSUE_DATE',
                    'condition': 'current_month_only',
                    'values': [],
                    'description': 'Keep only current month policies'
                },
                {
                    'field': 'TOTAL_GROSS_PREMIUM',
                    'condition': 'positive',
                    'values': [],
                    'description': 'Remove negative premium records'
                }
            ],
            'ROYAL': [
                {
                    'field': 'Endorsement No',
                    'condition': 'equals',
                    'values': ['000'],
                    'description': 'Only base policies (Endorsement No = 000)'
                },
                {
                    'field': 'Product',
                    'condition': 'not_contains',
                    'values': ['Other than Motor Products'],
                    'description': 'Keep only Motor products'
                }
            ],
            'MAGMA': [
                {
                    'field': 'LOBCode',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'SBI': [
                {
                    'field': 'Transaction Type',
                    'condition': 'not_in',
                    'values': ['Refund Endorsement', 'Policy Cancellation Endorsement', 'Extra Endorsement', 'Nil Endorsement'],
                    'description': 'Remove endorsement and cancellation transactions'
                },
                {
                    'field': 'OF LOB Name',
                    'condition': 'not_contains',
                    'values': ['Other than Motor'],
                    'description': 'Keep only Motor LOB'
                }
            ],
            'NATIONAL': {
                'field': 'Business Type',
                'condition': 'in',
                'values': ['New Business', 'Renewal'],
                'description': 'New Business and Renewal only (No Cancellation/Endorsement)'
            }
        }
        # Insurer name map for company detection
        self.insurer_name_map = {
            'BAJAJ': ['Bajaj Allianz', 'Bajaj Allianz General Insurance', 'Bajaj Allianz General Insurance Company Limited', 'Bajaj'],
            'DIGIT': ['Digit General Insurance Limited', 'Digit Insurance', 'Digit', 'Digit General Insurance'],
            'HDFC': ['HDFC ERGO', 'HDFC Ergo General Insurance', 'HDFC Ergo', 'HDFC Ergo General Insurance Company Limited'],
            'FUTURE': ['Future Generali','FGI', 'Future Generali India Insurance', 'Future Generali General Insurance'],
            'CHOLAMANDALAM': ['Cholamandalam MS General Insurance Company Ltd', 'Cholamandalam MS General Insurance', 'Cholamandalam MS', 'Cholamandalam MS General Insurance Company Limited'],
            'ICICI': ['ICICI Lombard', 'ICICI Lombard General Insurance', 'ICICI Lombard General Insurance Company Limited', 'ICICI'],
            'IFFCO': ['IFFCO Tokio', 'IFFCO Tokio General Insurance', 'IFFCO Tokio General Insurance Company Limited', 'IFFCO'],
            'KOTAK': ['Kotak Mahindra General Insurance Limited', 'Kotak General Insurance', 'Kotak', 'Kotak General Insurance Company Limited'],
            'LIBERTY': ['Liberty General Insurance Company Limited', 'Liberty Insurance', 'Liberty', 'Liberty Videocon General Insurance', 'Liberty General Insurance'],
            'MAGMA': ['Magma HDI General Insurance Company Ltd', 'Magma HDI', 'Magma', 'Magma HDI General Insurance'],
            'NATIONAL': ['National Insurance Company Limited', 'National Insurance', 'National', 'NIC'],
            'NEW_INDIA': ['New India', 'New India Assurance', 'New India General Insurance Company Limited'],
            'ORIENTAL': ['Oriental Insurance', 'Oriental', 'Oriental General Insurance Company Limited'],
            'RELIANCE': ['Reliance General Insurance', 'Reliance', 'Reliance General Insurance Company Limited'],
            'ROYAL': ['Royal Sundaram General Insurance', 'Royal Sundaram', 'Royal Sundaram General Insurance Company Limited'],
            'SBI': ['SBI General Insurance', 'SBI', 'SBI General Insurance Company Limited'],
            'SHRIRAM': ['Shriram General Insurance', 'Shriram', 'SGI'],
            'TATA': ['Tata AIG', 'Tata AIG General Insurance', 'Tata AIG General Insurance Company Limited', 'Tata'],
            'UNITED': ['United India Insurance Company Limited','UIIC', 'United India Insurance', 'United India', 'United', 'UIIC'],
            'ZUNO': ['Zuno General Insurance', 'Zuno Insurance', 'Zuno'],
            'UNIVERSAL': ['Universal Sompo','Sompo', 'Universal Sompo General Insurance', 'Universal Sompo General Insurance Company Limited']
        }
    
    def preprocess_mis_df(self, insurer: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocesses an MIS dataframe by coalescing or concatenating columns
        where the mapping is a list. It adds new columns to the dataframe and
        updates the internal resolved mapping for the comparator to use.
        """
        original_mapping = self.column_mappings.get(insurer)
        if not original_mapping:
            self.resolved_mappings[insurer] = {}
            return df

        resolved_mapping = original_mapping.copy()

        for field, mapped_cols in original_mapping.items():
            if isinstance(mapped_cols, list):
                existing_cols = [col for col in mapped_cols if col in df.columns]
                
                if not existing_cols:
                    resolved_mapping[field] = None
                    continue

                new_col_name = f"__generated_{field}"

                if field == 'customer_name':
                    df[new_col_name] = df[existing_cols].fillna('').astype(str).agg(' '.join, axis=1).str.strip()
                    logger.info(f"Concatenated {existing_cols} into '{new_col_name}' for {insurer}.")
                
                else:
                    if len(existing_cols) == 1:
                        resolved_mapping[field] = existing_cols[0]
                        continue
                    
                    series = df[existing_cols[0]].copy()
                    for col in existing_cols[1:]:
                        series.fillna(df[col], inplace=True)
                    df[new_col_name] = series
                    logger.info(f"Coalesced {existing_cols} into '{new_col_name}' for {insurer}.")

                resolved_mapping[field] = new_col_name

        self.resolved_mappings[insurer] = resolved_mapping
        return df

    def convert_old_to_new_format(self, df):
        """Convert old database format to new database format"""
        if 'PolicyNumber' in df.columns and 'Policy Number' not in df.columns:
            logger.info("Detected old database format. Converting to new format...")
            rename_dict = {old_col: new_col for old_col, new_col in self.old_to_new_mappings.items() if old_col in df.columns}
            df = df.rename(columns=rename_dict)
            logger.info(f"Converted {len(rename_dict)} columns from old to new format")
        return df

    def count_endorsement_cancellation_instances(self, df, insurer):
        """Count endorsement/cancellation records as non-unique policies from MIS data"""
        logger.info(f"=== COUNTING ENDORSEMENT/CANCELLATION FOR {insurer} ===")
        
        # Get policy number column with proper resolution
        policy_col = self.column_mappings.get(insurer, {}).get('policy_number')
        
        # Handle case where policy_number is a list of possible column names
        if isinstance(policy_col, list):
            # Find the first column that actually exists in the MIS data
            found_col = None
            for col_option in policy_col:
                if col_option in df.columns:
                    found_col = col_option
                    break
            
            if found_col:
                policy_col = found_col
                logger.info(f"Resolved policy column for {insurer}: {found_col}")
            else:
                logger.warning(f"Policy number column (tried {policy_col}) not found in MIS data for {insurer}")
                return 0
        elif not policy_col or policy_col not in df.columns:
            logger.warning(f"Policy number column '{policy_col}' not found for {insurer}")
            return 0
        
        # Count total records and unique policies
        total_records = len(df)
        unique_policies = df[policy_col].nunique()
        duplicate_records = total_records - unique_policies
        
        logger.info(f"Total MIS records: {total_records}")
        logger.info(f"Unique policies: {unique_policies}")
        logger.info(f"Duplicate records (endorsements/cancellations): {duplicate_records}")
        logger.info(f"=== END ENDORSEMENT/CANCELLATION COUNTING ===")
        
        return duplicate_records

    def count_cancellation_instances(self, df: pd.DataFrame, insurer: str) -> int:
        """Count cancellations per insurer based on explicit cancellation rules provided.

        The count is computed on the raw MIS dataframe (pre-filter) using
        insurer-specific column/value rules and is case-insensitive.
        """
        # Map of insurer -> (column_name, [cancellation_values])
        rules = {
            'CHOLA': ('POLICY_CASE', ['CANCEL']),
            'IFFCO': ('Status', ['Cancelled Policy']),
            'KOTAK': ('TRANSACTION TYPE', ['CANCELLATION']),
            'LIBERTY': ('transaction type', ['CANCELLATION']),
            'TATA': ('record_type_desc', ['CANCELLATION']),
            'ZUNO': ('Policy_Type', ['CANCELLATION']),
            'ICICI': ('ENDORSEMENT_TYPE', ['CANCELLED']),
            'SBI': ('Transaction Type', ['Policy Cancellation Endorsement']),
            'DIGIT': ('STATUS', ['S']),
        }

        rule = rules.get(insurer)
        if not rule:
            return 0

        column_name, cancel_values = rule
        if column_name not in df.columns:
            logger.warning(f"Cancellation column '{column_name}' not found for {insurer}")
            return 0

        series = df[column_name].astype(str).str.strip()
        # Compare case-insensitively
        cancel_set_upper = {str(v).strip().upper() for v in cancel_values}
        count = series.str.upper().isin(cancel_set_upper).sum()
        logger.info(f"Cancellation count for {insurer} using {column_name}: {int(count)}")
        return int(count)

    def apply_insurer_filters(self, df, insurer):
        """Apply insurer-specific filters to the dataframe"""
        if insurer not in self.insurer_filters:
            return df, None
        
        original_count = len(df)
        filtered_df = df.copy()
        filter_descriptions = []
        filters = self.insurer_filters[insurer]
        
        if not isinstance(filters, list):
            filters = [filters]
        
        for filter_config in filters:
            field = filter_config['field']
            condition = filter_config['condition']
            values = filter_config['values']
            
            # Skip if field doesn't exist in the dataframe
            if field not in filtered_df.columns:
                logger.warning(f"Filter field '{field}' not found in {insurer} dataset")
                continue
            
            initial_count = len(filtered_df)
            
            try:
                if condition == 'equals':
                    if any(isinstance(v, (int, float)) for v in values):
                        filtered_df = filtered_df[pd.to_numeric(filtered_df[field], errors='coerce').isin(values)]
                    else:
                        filtered_df = filtered_df[filtered_df[field].isin(values)]
                
                elif condition == 'not_equals':
                    if any(isinstance(v, (int, float)) for v in values):
                        filtered_df = filtered_df[~pd.to_numeric(filtered_df[field], errors='coerce').isin(values)]
                    else:
                        filtered_df = filtered_df[~filtered_df[field].isin(values)]
                
                elif condition == 'in':
                    filtered_df = filtered_df[filtered_df[field].isin(values)]
                
                elif condition == 'not_in':
                    filtered_df = filtered_df[~filtered_df[field].isin(values)]
                
                elif condition == 'contains':
                    mask = filtered_df[field].astype(str).str.contains('|'.join(values), case=False, na=False)
                    filtered_df = filtered_df[mask]
                
                elif condition == 'not_contains':
                    mask = filtered_df[field].astype(str).str.contains('|'.join(values), case=False, na=False)
                    filtered_df = filtered_df[~mask]
                
                elif condition == 'not_null':
                    filtered_df = filtered_df[filtered_df[field].notna()]
                
                elif condition == 'positive':
                    numeric_field = pd.to_numeric(filtered_df[field], errors='coerce')
                    filtered_df = filtered_df[numeric_field > 0]
                
                elif condition == 'not_older_than_issue':
                    # This requires comparing Effect Date with Issue Date
                    # Assuming there's an Issue Date column - adjust as needed
                    issue_date_col = 'Issue Date'  # Adjust column name as needed
                    if issue_date_col in filtered_df.columns:
                        effect_date = pd.to_datetime(filtered_df[field], errors='coerce')
                        issue_date = pd.to_datetime(filtered_df[issue_date_col], errors='coerce')
                        filtered_df = filtered_df[effect_date >= issue_date]
                    else:
                        logger.warning(f"Issue Date column not found for date comparison filter in {insurer}")
                
                elif condition == 'current_month_only':
                    # Keep only records from the current month
                    try:
                        from datetime import datetime
                        current_month = datetime.now().month
                        current_year = datetime.now().year
                        
                        date_col = pd.to_datetime(filtered_df[field], errors='coerce')
                        mask = (date_col.dt.month == current_month) & (date_col.dt.year == current_year)
                        filtered_df = filtered_df[mask]
                    except Exception as date_error:
                        logger.warning(f"Error applying current_month_only filter on {field}: {date_error}")
                
                else:
                    logger.warning(f"Unknown filter condition '{condition}' for {insurer}")
                    continue
                
                records_removed = initial_count - len(filtered_df)
                if records_removed > 0:
                    filter_descriptions.append(f"{filter_config['description']} (removed {records_removed:,} records)")
                else:
                    filter_descriptions.append(f"{filter_config['description']} (no records removed)")
                    
            except Exception as e:
                logger.error(f"Error applying filter {condition} on field {field} for {insurer}: {str(e)}")
                continue

        filtered_count = len(filtered_df)
        filtered_out_count = original_count - filtered_count
        
        return filtered_df, {
            'original_count': original_count,
            'filtered_count': filtered_count,
            'removed_count': filtered_out_count,
            'filtered_out_count': filtered_out_count,
            'filters_applied': filter_descriptions
        }

    def process_endorsements_cancellations(self, records, insurer, record_type):
        """Process endorsements and cancellations with numbering and maintain same format"""
        if not records:
            return []
        
        processed_records = []
        
        # Get policy number column for this insurer
        policy_col = self.column_mappings.get(insurer, {}).get('policy_number')
        if isinstance(policy_col, list):
            policy_col = policy_col[0] if policy_col else None
        
        # Group by policy number to number endorsements/cancellations sequentially
        policy_groups = {}
        for record in records:
            policy_num = record.get(policy_col, 'Unknown')
            if policy_num not in policy_groups:
                policy_groups[policy_num] = []
            policy_groups[policy_num].append(record)
        
        # Process each policy group
        for policy_num, group_records in policy_groups.items():
            # Sort by date if available, otherwise by order in original data
            date_col = None
            for col in ['policy_start_date', 'policy_end_date', 'start_date', 'end_date', 'transaction_date']:
                if col in group_records[0]:
                    date_col = col
                    break
            
            if date_col:
                try:
                    group_records.sort(key=lambda x: pd.to_datetime(x.get(date_col, '1900-01-01'), errors='coerce'))
                except:
                    pass
            
            # Number the records sequentially
            for idx, record in enumerate(group_records, 1):
                processed_record = record.copy()
                
                # Add numbering information
                if record_type == 'endorsement':
                    processed_record['Record_Type'] = 'Endorsement'
                    processed_record['Endorsement_Number'] = f"END-{policy_num}-{idx:03d}"
                    processed_record['Endorsement_Sequence'] = idx
                    processed_record['Description'] = f"Endorsement #{idx} for Policy {policy_num}"
                else:  # cancellation
                    processed_record['Record_Type'] = 'Cancellation'
                    processed_record['Cancellation_Number'] = f"CANCEL-{policy_num}-{idx:03d}"
                    processed_record['Cancellation_Sequence'] = idx
                    processed_record['Description'] = f"Cancellation #{idx} for Policy {policy_num}"
                
                # Ensure Request ID and Policy Number follow same format
                if 'Request Id' not in processed_record:
                    processed_record['Request Id'] = f"REQ-{insurer}-{policy_num}-{idx:03d}"
                
                # Add insurer information
                processed_record['Insurer'] = insurer
                processed_record['Processing_Date'] = datetime.now().strftime('%Y-%m-%d')
                
                processed_records.append(processed_record)
        
        logger.info(f"Processed {len(processed_records)} {record_type} records for {insurer}")
        return processed_records



    def clean_string(self, value):
        """Clean string for comparison - removes quotes, spaces, and converts to upper"""
        if pd.isna(value):
            return ''
        return str(value).strip().replace('"', '').replace("'", '').upper().replace('-', '').replace(' ', '')

    def clean_for_comparison(self, value):
        """Clean value for comparison while preserving readability"""
        if pd.isna(value):
            return ''
        return str(value).strip().replace('"', '').replace("'", '')

    def fuzzy_match_score(self, str1, str2, threshold=80):
        """Calculate fuzzy match score between two strings"""
        if pd.isna(str1) or pd.isna(str2):
            return 0
        return fuzz.ratio(str(str1).upper(), str(str2).upper())

    def standardize_date(self, date_value):
        """Standardize date formats"""
        if pd.isna(date_value):
            return None
        try:
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', '%Y%m%d']:
                try:
                    return pd.to_datetime(date_value, format=fmt).strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    continue
            return pd.to_datetime(date_value).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            return None

    def handle_offline_duplicates(self, offline_df):
        """
        Handle duplicate policy numbers in offline punching data by prioritizing 'booked' status.
        
        Priority order:
        1. booked (highest priority)
        2. case lost
        3. report pendency  
        4. ticket closed duplicate (lowest priority)
        
        Args:
            offline_df: DataFrame with offline punching data
            
        Returns:
            DataFrame with duplicates removed, keeping highest priority status
        """
        if 'Policy Number' not in offline_df.columns or 'Status' not in offline_df.columns:
            logger.warning("Required columns 'Policy Number' or 'Status' not found in offline data")
            return offline_df
        
        # Define status priority (lower number = higher priority)
        status_priority = {
            'booked': 1,
            'case lost': 2, 
            'report pendency': 3,
            'ticket closed duplicate': 4
        }
        
        # Standardize status values for comparison
        offline_df = offline_df.copy()
        offline_df['Status_Clean'] = offline_df['Status'].astype(str).str.strip().str.lower()
        
        # Add priority column
        offline_df['Status_Priority'] = offline_df['Status_Clean'].map(status_priority)
        
        # For unmapped statuses, assign lowest priority
        offline_df['Status_Priority'] = offline_df['Status_Priority'].fillna(999)
        
        # Clean policy numbers for grouping
        offline_df['Policy_Clean'] = offline_df['Policy Number'].astype(str).str.strip().str.upper()
        
        # Group by policy number and keep record with highest priority (lowest number)
        # Also keep the earliest record in case of same priority
        offline_df['Row_Index'] = range(len(offline_df))
        
        # Sort by policy number, priority, then row index to maintain deterministic results
        offline_df_sorted = offline_df.sort_values(['Policy_Clean', 'Status_Priority', 'Row_Index'])
        
        # Keep first record for each policy (highest priority)
        deduplicated_df = offline_df_sorted.groupby('Policy_Clean').first().reset_index()
        
        # Remove helper columns
        deduplicated_df = deduplicated_df.drop(['Status_Clean', 'Status_Priority', 'Policy_Clean', 'Row_Index'], axis=1)
        
        # Log duplicate handling summary
        original_count = len(offline_df)
        final_count = len(deduplicated_df)
        removed_count = original_count - final_count
        
        if removed_count > 0:
            logger.info(f"Offline duplicate handling: {original_count} -> {final_count} records ({removed_count} duplicates removed)")
            
            # Log status distribution of kept records
            status_counts = deduplicated_df['Status'].value_counts()
            logger.info(f"Final status distribution: {dict(status_counts)}")
        
        return deduplicated_df

    def compare_values(self, internal_value, mis_value, field_type):
        """Compare two values based on field type"""
        if pd.isna(internal_value) and pd.isna(mis_value):
            return True, 100, "Both null"
        if pd.isna(internal_value) or pd.isna(mis_value):
            return False, 0, "One value is null"

        if field_type in ['customer_name', 'broker_name']:
            clean_internal = self.clean_for_comparison(internal_value)
            clean_mis = self.clean_for_comparison(mis_value)
            score = self.fuzzy_match_score(clean_internal, clean_mis)
            return score >= 80, score, f"Fuzzy match score: {score}%"
        elif field_type in ['registration_number', 'engine_number', 'chassis_number', 'policy_number']:
            clean_internal = self.clean_string(internal_value)
            clean_mis = self.clean_string(mis_value)
            match = clean_internal == clean_mis
            return match, 100 if match else 0, "Exact match" if match else "No match"
        elif field_type in ['policy_start_date', 'policy_end_date']:
            date_internal = self.standardize_date(internal_value)
            date_mis = self.standardize_date(mis_value)
            match = date_internal is not None and date_internal == date_mis
            return match, 100 if match else 0, "Date match" if match else "Date mismatch"
        elif field_type in ['total_premium', 'tp_premium', 'sum_insured']:
            try:
                val_internal = float(str(internal_value).replace(',', ''))
                val_mis = float(str(mis_value).replace(',', ''))
                diff = abs(val_internal - val_mis)
                match = diff <= 1.0
                score = 100 if match else max(0, 100 - (diff / val_internal * 100)) if val_internal != 0 else 0
                return match, score, f"Difference: ₹{diff:.2f}"
            except (ValueError, TypeError):
                return False, 0, "Invalid numeric values"
        elif field_type == 'fuel_type':
            clean_internal = self.clean_for_comparison(internal_value).upper()
            clean_mis = self.clean_for_comparison(mis_value).upper()
            lpg_variations = ['LPG', 'LIQUID PETROLEUM GAS', 'LIQUID PETROL GAS', 'LIQUID PETROLEUM']
            if (clean_internal in lpg_variations and clean_mis in lpg_variations):
                return True, 100, "LPG match"
            match = clean_internal == clean_mis
            return match, 100 if match else 0, "Exact match" if match else "No match"
        else:
            clean_internal = self.clean_for_comparison(internal_value).upper()
            clean_mis = self.clean_for_comparison(mis_value).upper()
            match = clean_internal == clean_mis
            return match, 100 if match else 0, "Exact match" if match else "No match"

    def get_insurer_from_company_name(self, company_name):
        """Identify insurer from company name"""
        if pd.isna(company_name):
            return None
        company_upper = str(company_name).upper()
        exact_mappings = {
            'DIGIT': 'DIGIT', 'LIBERTY': 'LIBERTY', 'MAGMA': 'MAGMA',
            'NATIONAL': 'NATIONAL', 'UNITED': 'UNITED', 'SHRIRAM': 'SHRIRAM', 'FGI':'FGI','CHOLA':'CHOLA','HDFC':'HDFC',
        }
        for key, value in exact_mappings.items():
            if key in company_upper:
                return value
        for insurer, variations in self.insurer_name_map.items():
            for variation in variations:
                if variation.upper() in company_upper:
                    return insurer
        return None

    def compare_datasets_async(self, internal_df, mis_df, insurer, selected_fields=None, progress_callback=None):
        """Compare datasets with two-way reconciliation and async progress updates"""
        results = []
        
        # Merge current configured mappings with any runtime-resolved overrides
        base_mapping = self.column_mappings.get(insurer, {}) or {}
        resolved_overrides = self.resolved_mappings.get(insurer, {}) or {}
        insurer_mapping = base_mapping.copy()
        insurer_mapping.update(resolved_overrides)
        if not insurer_mapping:
            if progress_callback:
                progress_callback(1.0, f"Error: No column mapping found for insurer {insurer}")
            return pd.DataFrame(), {}

        logger.info(f"Starting comparison for {insurer}")
        logger.info(f"MIS DataFrame shape: {mis_df.shape}")
        logger.info(f"MIS DataFrame columns: {list(mis_df.columns)}")
        
        # Apply filters and get count of filtered out records (endorsements/cancellations)
        mis_df_filtered, filter_summary = self.apply_insurer_filters(mis_df, insurer)
        
        # Get processed endorsements and cancellations
        processed_endorsements = filter_summary.get('endorsements', []) if filter_summary else []
        processed_cancellations = filter_summary.get('cancellations', []) if filter_summary else []
        
        # Count ALL endorsement/cancellation instances (including duplicates)
        endorsement_cancellation_count = self.count_endorsement_cancellation_instances(mis_df, insurer)
        # Count explicit cancellations per insurer (raw MIS)
        explicit_cancellation_count = self.count_cancellation_instances(mis_df, insurer)
        logger.info(f"Total endorsement/cancellation instances for {insurer}: {endorsement_cancellation_count}")
        logger.info(f"Filtered out {filter_summary.get('removed_count', 0) if filter_summary else 0} records after deduplication")
        logger.info(f"Processed {len(processed_endorsements)} endorsements and {len(processed_cancellations)} cancellations")
        
        if filter_summary and progress_callback:
            progress_callback(0.05, f"Filters applied: {filter_summary['filtered_count']:,} records remain")

        if progress_callback: progress_callback(0.1, f"Filtering internal data for {insurer}...")
        
        insurance_col = self.internal_columns['insurance_company']
        if insurance_col in internal_df.columns and 'Detected_Insurer' not in internal_df.columns:
            internal_df['Detected_Insurer'] = internal_df[insurance_col].apply(self.get_insurer_from_company_name)
        
        internal_insurer_df = internal_df[internal_df['Detected_Insurer'] == insurer].copy()

        if selected_fields:
            insurer_mapping = {k: v for k, v in insurer_mapping.items() if k in selected_fields}
            # Auto-resolve any selected fields that are missing from mapping by scanning MIS columns
            missing_fields = [f for f in selected_fields if f not in insurer_mapping]
            if missing_fields:
                def normalize(name: str) -> str:
                    return str(name).lower().replace(' ', '').replace('_', '').replace('.', '')
                mis_norm_map = {normalize(c): c for c in mis_df.columns}
                for field in missing_fields:
                    candidates = [normalize(field)]
                    # Field-specific hints
                    if field == 'vehicle_sub_category':
                        candidates.extend([
                            'vehiclesubcategory', 'vehiclesubtype', 'vehsubtype', 'vehsubcat', 'subtype', 'sub_category', 'vehsub_type'
                        ])
                    # Try exact/substring matches against MIS columns
                    found_col = None
                    for norm_col, orig_col in mis_norm_map.items():
                        if any(token in norm_col for token in candidates):
                            found_col = orig_col
                            break
                    if found_col:
                        insurer_mapping[field] = found_col
                        # Persist into resolved mappings for this insurer for subsequent runs
                        if insurer not in self.resolved_mappings:
                            self.resolved_mappings[insurer] = {}
                        self.resolved_mappings[insurer][field] = found_col
                        logger.info(f"Auto-resolved MIS column for '{field}' to '{found_col}'")

        total_policies = len(internal_insurer_df)
        if progress_callback: progress_callback(0.2, f"Processing {total_policies:,} policies...")

        if filter_summary:
            results.append({
                'Policy Number': 'FILTER_INFO', 'Request Id': '', 'Field': 'Filter Applied', 'Match Status': 'Info',
                'Match Score': 100, 'Internal Value': f"Filters: {', '.join(filter_summary['filters_applied'])}",
                'MIS Value': f"Removed {filter_summary['removed_count']:,} records",
                'Details': f"MIS records: {filter_summary['original_count']:,} → {filter_summary['filtered_count']:,}"
            })
            
            # Add processed endorsements to results
            for endorsement in processed_endorsements:
                policy_col = insurer_mapping.get('policy_number')
                if isinstance(policy_col, list):
                    policy_col = policy_col[0] if policy_col else None
                
                if policy_col and policy_col in endorsement:
                    results.append({
                        'Policy Number': endorsement[policy_col],
                        'Request Id': endorsement.get('Request Id', 'N/A'),
                        'Field': 'Endorsement Record',
                        'Match Status': 'Endorsement',
                        'Match Score': 100,
                        'Internal Value': endorsement.get('Description', 'Endorsement Record'),
                        'MIS Value': f"Endorsement #{endorsement.get('Endorsement_Sequence', 'N/A')}",
                        'Details': f"Endorsement Number: {endorsement.get('Endorsement_Number', 'N/A')} | Insurer: {insurer}",
                        'Record_Type': 'Endorsement',
                        'Endorsement_Number': endorsement.get('Endorsement_Number', 'N/A'),
                        'Endorsement_Sequence': endorsement.get('Endorsement_Sequence', 'N/A')
                    })
            
            # Add processed cancellations to results
            for cancellation in processed_cancellations:
                policy_col = insurer_mapping.get('policy_number')
                if isinstance(policy_col, list):
                    policy_col = policy_col[0] if policy_col else None
                
                if policy_col and policy_col in cancellation:
                    results.append({
                        'Policy Number': cancellation[policy_col],
                        'Request Id': cancellation.get('Request Id', 'N/A'),
                        'Field': 'Cancellation Record',
                        'Match Status': 'Cancellation',
                        'Match Score': 100,
                        'Internal Value': cancellation.get('Description', 'Cancellation Record'),
                        'MIS Value': f"Cancellation #{cancellation.get('Cancellation_Sequence', 'N/A')}",
                        'Details': f"Cancellation Number: {cancellation.get('Cancellation_Number', 'N/A')} | Insurer: {insurer}",
                        'Record_Type': 'Cancellation',
                        'Cancellation_Number': cancellation.get('Cancellation_Number', 'N/A'),
                        'Cancellation_Sequence': cancellation.get('Cancellation_Sequence', 'N/A')
                    })

        mis_policy_col = insurer_mapping.get('policy_number')
        
        # Handle case where policy_number is a list of possible column names
        if isinstance(mis_policy_col, list):
            logger.info(f"Policy column is a list: {mis_policy_col}")
            logger.info(f"Available columns in filtered MIS: {list(mis_df_filtered.columns)[:10]}...")  # Show first 10 columns
            
            # Find the first column that actually exists in the MIS data
            found_col = None
            for col_option in mis_policy_col:
                logger.info(f"Checking if '{col_option}' exists in MIS columns...")
                if col_option in mis_df_filtered.columns:
                    found_col = col_option
                    logger.info(f"Found matching column: {found_col}")
                    break
                else:
                    logger.info(f"Column '{col_option}' not found in MIS data")
            
            if found_col:
                mis_policy_col = found_col
                # Update resolved mappings for future use
                if insurer not in self.resolved_mappings:
                    self.resolved_mappings[insurer] = insurer_mapping.copy()
                self.resolved_mappings[insurer]['policy_number'] = found_col
                logger.info(f"Successfully resolved policy column for {insurer}: {found_col}")
            else:
                original_list = insurer_mapping.get('policy_number', 'N/A')
                logger.error(f"Policy number column (tried {original_list}) not found in MIS data for {insurer}")
                logger.error(f"Available columns: {list(mis_df_filtered.columns)}")
                if progress_callback: progress_callback(1.0, f"Error: None of the policy columns {original_list} found in MIS file.")
                return pd.DataFrame(results), {}
        elif not mis_policy_col or mis_policy_col not in mis_df_filtered.columns:
            possible_cols = self.column_mappings.get(insurer, {}).get('policy_number', 'N/A')
            logger.error(f"Policy number column (tried {possible_cols}) not found in MIS data for {insurer}")
            if progress_callback: progress_callback(1.0, f"Error: Policy column for {insurer} not found in MIS file.")
            return pd.DataFrame(results), {}

        # Use sets for efficient tracking of matched policies
        mis_df_filtered['cleaned_policy'] = mis_df_filtered[mis_policy_col].apply(self.clean_string)
        internal_insurer_df['cleaned_policy'] = internal_insurer_df[self.internal_columns['policy_number']].apply(self.clean_string)
        
        matched_mis_policies_cleaned = set()
        
        mis_lookup_cleaned = {row['cleaned_policy']: row for _, row in mis_df_filtered.iterrows() if row['cleaned_policy']}

        processed = 0
        batch_size = 500
        
        for start_idx in range(0, total_policies, batch_size):
            end_idx = min(start_idx + batch_size, total_policies)
            batch_df = internal_insurer_df.iloc[start_idx:end_idx]
            
            batch_results, batch_matched_policies = self._process_batch_optimized(
                batch_df, mis_lookup_cleaned, insurer_mapping
            )
            results.extend(batch_results)
            matched_mis_policies_cleaned.update(batch_matched_policies)

            processed = end_idx
            if progress_callback:
                progress = 0.2 + (0.7 * processed / total_policies)
                progress_callback(progress, f"Processing: {processed:,}/{total_policies:,} policies")

        # Reverse check for MIS policies not in internal data
        if progress_callback: progress_callback(0.9, "Performing reverse check...")
        
        all_mis_policies_cleaned = set(mis_df_filtered['cleaned_policy'].unique())
        not_found_in_internal_policies = all_mis_policies_cleaned - matched_mis_policies_cleaned

        mis_premium_col = insurer_mapping.get('total_premium')
        
        # Handle case where total_premium is a list of possible column names
        if isinstance(mis_premium_col, list):
            # Find the first column that actually exists in the MIS data
            found_premium_col = None
            for col_option in mis_premium_col:
                if col_option in mis_df_filtered.columns:
                    found_premium_col = col_option
                    break
            
            if found_premium_col:
                mis_premium_col = found_premium_col
                # Update resolved mappings for future use
                if insurer not in self.resolved_mappings:
                    self.resolved_mappings[insurer] = insurer_mapping.copy()
                self.resolved_mappings[insurer]['total_premium'] = found_premium_col
                logger.info(f"Resolved premium column for {insurer}: {found_premium_col}")
            else:
                logger.warning(f"Premium column (tried {insurer_mapping.get('total_premium')}) not found in MIS data for {insurer}")
                mis_premium_col = None
        
        for policy in not_found_in_internal_policies:
            if not policy: continue
            mis_row = mis_lookup_cleaned.get(policy)
            if mis_row is not None:
                results.append({
                    'Policy Number': mis_row[mis_policy_col],
                    'Request Id': 'N/A',
                    'Field': 'Policy Number',
                    'Match Status': 'Not Found in Internal',
                    'Match Score': 0,
                    'Internal Value': 'Not Found in Internal Data',
                    'MIS Value': mis_row[mis_policy_col],
                    'Details': 'Policy exists in MIS but not in internal data',
                    'MIS Premium': pd.to_numeric(mis_row.get(mis_premium_col), errors='coerce') if mis_premium_col else 0
                })

        if progress_callback: progress_callback(1.0, f"Completed: {len(results):,} comparisons")
        
        # Calculate how many MIS policies were found in internal database
        results_df = pd.DataFrame(results)
        if not results_df.empty:
            # Count unique MIS policies that have any match (Match or Mismatch, not "Not Found in Internal")
            found_in_internal_count = len(results_df[
                results_df['Match Status'].isin(['Match', 'Mismatch'])
            ]['Policy Number'].unique())
        else:
            found_in_internal_count = 0
        
        logger.info(f"Found {found_in_internal_count} MIS policies in internal database for {insurer}")
        
        return results_df, {
            "internal_df": internal_insurer_df,
            "mis_df": mis_df_filtered,
            "endorsement_cancellation_count": endorsement_cancellation_count,
            "explicit_cancellation_count": explicit_cancellation_count,
            "found_in_internal_count": found_in_internal_count
        }

    def _process_batch_optimized(self, batch_df, mis_lookup_cleaned, resolved_mapping):
        """Process a batch of data with corrected 'Not Found' status."""
        batch_results = []
        batch_matched_policies = set()
        internal_policy_col = self.internal_columns['policy_number']

        for _, internal_row in batch_df.iterrows():
            policy_number = internal_row.get(internal_policy_col)
            if pd.isna(policy_number):
                continue

            request_id = internal_row.get('Request Id', 'N/A')
            cleaned_policy = internal_row['cleaned_policy']

            mis_row = mis_lookup_cleaned.get(cleaned_policy)

            if mis_row is None:
                batch_results.append({
                    'Policy Number': policy_number,
                    'Request Id': request_id,
                    'Field': 'Policy Number',
                    'Match Status': 'Not Found in MIS',
                    'Match Score': 0,
                    'Internal Value': policy_number,
                    'MIS Value': 'Not Found in MIS',
                    'Details': 'Policy from internal data not found in MIS data'
                })
                continue
            
            batch_matched_policies.add(cleaned_policy)
            # Get all fields to compare (both predefined internal columns and dynamically mapped fields)
            all_fields_to_compare = self.get_all_comparable_fields(resolved_mapping, internal_row)
            
            for field, internal_col_name in all_fields_to_compare.items():
                mis_col = resolved_mapping.get(field)
                if mis_col and internal_col_name in internal_row.index:
                    if field == 'policy_number':
                        continue
                    internal_value = internal_row[internal_col_name]
                    mis_value = mis_row.get(mis_col)
                    match, score, details = self.compare_values(internal_value, mis_value, field)
                    batch_results.append({
                        'Policy Number': policy_number,
                        'Request Id': request_id,
                        'Field': field.replace('_', ' ').title(),
                        'Match Status': 'Match' if match else 'Mismatch',
                        'Match Score': score,
                        'Internal Value': internal_value,
                        'MIS Value': mis_value,
                        'Details': details
                    })
        return batch_results, batch_matched_policies
    
    def get_all_comparable_fields(self, resolved_mapping, internal_row):
        """Get all fields that can be compared (predefined + dynamically mapped).
        Ensures each field maps to an actual column in the internal row, with fallback matching.
        """
        comparable_fields = {}
        
        # Start with predefined internal columns; verify existence and fallback via matcher
        for field, internal_col in self.internal_columns.items():
            effective_internal_col = internal_col if internal_col in internal_row.index else self.find_internal_column_for_field(field, internal_row)
            if effective_internal_col:
                comparable_fields[field] = effective_internal_col
        
        # Add dynamically mapped fields that exist in both MIS mapping and internal data
        for field, mis_col in resolved_mapping.items():
            if field not in comparable_fields:  # Don't override predefined mappings
                # Try to find a matching column in internal data
                internal_col = self.find_internal_column_for_field(field, internal_row)
                if internal_col:
                    comparable_fields[field] = internal_col
                    logger.info(f"Dynamically added field for comparison: {field} -> {internal_col}")
        
        return comparable_fields
    
    def find_internal_column_for_field(self, field, internal_row):
        """Find matching internal column for a dynamically mapped field."""
        # First try known synonyms to canonical internal columns
        synonyms_map = {
            # Vehicle / registration identifiers
            'vehicle_number': 'Registration Number',
            'vehicle_no': 'Registration Number',
            'veh_number': 'Registration Number',
            'veh_no': 'Registration Number',
            'veh_reg_no': 'Registration Number',
            'registration_no': 'Registration Number',
            'reg_no': 'Registration Number',
            # Insured / customer name
            'insured_name': 'Customer Name',
            'client_name': 'Customer Name',
            'policy_holder': 'Customer Name',
            # Policy number
            'policy_no': 'Policy Number',
            # Engine/chassis synonyms
            'chasis_number': 'Chassis No.',
            'chassis_no': 'Chassis No.',
            'engine_no': 'Engine Number',
            # Premium synonyms
            'gross_premium': 'Premium',
            'net_premium': 'Premium',
            # Vehicle sub category synonyms
            'vehicle_sub_category': 'Vehicle Sub Category',
            'veh_sub_category': 'Vehicle Sub Category',
            'vehicle_subcat': 'Vehicle Sub Category',
            'sub_category': 'Vehicle Sub Category',
            'vehicle_sub_type': 'Vehicle Sub Category',
            'veh_sub_type': 'Vehicle Sub Category',
        }
        normalized_field = str(field).strip().lower()
        if normalized_field in synonyms_map:
            target_internal_col = synonyms_map[normalized_field]
            if target_internal_col in internal_row.index:
                return target_internal_col

        # Convert field name to likely internal column name
        field_variations = [
            field.replace('_', ' ').title(),  # policy_type -> Policy Type
            field.replace('_', ' ').upper(),  # policy_type -> POLICY TYPE  
            field.replace('_', ' ').lower(),  # policy_type -> policy type
            field.title().replace('_', ' '),  # policy_type -> Policy Type
            field.upper(),                    # policy_type -> POLICY_TYPE
            field.lower(),                    # policy_type -> policy_type
            field,                            # policy_type -> policy_type
        ]
        
        # Check if any variation exists in internal data
        for variation in field_variations:
            if variation in internal_row.index:
                return variation
        
        # If no exact match, try partial matching
        field_lower = field.lower().replace('_', '')
        for col in internal_row.index:
            col_lower = str(col).lower().replace(' ', '').replace('_', '')
            if field_lower in col_lower or col_lower in field_lower:
                logger.info(f"Found partial match for {field}: {col}")
                return col
        
        return None
    
    def get_sheet_name_for_insurer(self, insurer: str) -> Optional[str]:
        """Get the sheet name for a specific insurer"""
        insurer_upper = insurer.upper()
        if insurer_upper in self.sheet_mapping:
            sheet_name = self.sheet_mapping[insurer_upper]
            return sheet_name[0] if isinstance(sheet_name, list) else sheet_name
        if 'ROYAL' in insurer_upper and 'SUNDARAM' in insurer_upper:
            return self.sheet_mapping.get('ROYAL_SUNDARAM', 'Sheet1')
        if 'FUTURE' in insurer_upper or 'GENERALI' in insurer_upper:
            return self.sheet_mapping.get('FGI', 'Sheet1')
        if 'CHOLAMANDALAM' in insurer_upper or 'CHOLA' in insurer_upper:
            return self.sheet_mapping.get('CHOLA', 'Sheet1')
        return 'Sheet1'

    def read_excel_with_sheet_detection(self, filepath: str, insurer: str) -> Optional[pd.DataFrame]:
        """Read Excel file with specific sheet detection for insurer"""
        try:
            expected_sheet = self.get_sheet_name_for_insurer(insurer)
            logger.info(f"Attempting to read {insurer} file: {filepath} with expected sheet: {expected_sheet}")
            try:
                return pd.read_excel(filepath, sheet_name=expected_sheet)
            except Exception as e:
                logger.warning(f"Could not read expected sheet '{expected_sheet}': {e}")
                excel_file = pd.ExcelFile(filepath)
                available_sheets = excel_file.sheet_names
                logger.info(f"Available sheets: {available_sheets}")
                sheet_options = self.sheet_mapping.get(insurer.upper(), [])
                if isinstance(sheet_options, list):
                    for sheet in sheet_options:
                        if sheet in available_sheets:
                            return pd.read_excel(filepath, sheet_name=sheet)
                common_sheets = ['Sheet1', 'Data', 'Raw', 'Digital', 'New', 'New Business']
                for sheet in common_sheets:
                    if sheet in available_sheets:
                        return pd.read_excel(filepath, sheet_name=sheet)
                return pd.read_excel(filepath, sheet_name=0)
        except Exception as e:
            logger.error(f"Error reading Excel file {filepath}: {e}")
            return None

    def debug_insurer_processing(self, insurer: str, filepath: str = None):
        """Debug method to help troubleshoot insurer processing issues"""
        logger.info(f"=== DEBUGGING {insurer} PROCESSING ===")
        expected_sheet = self.get_sheet_name_for_insurer(insurer)
        logger.info(f"Expected sheet for {insurer}: {expected_sheet}")
        column_mapping = self.column_mappings.get(insurer, {})
        logger.info(f"Column mapping for {insurer}: {column_mapping}")
        has_filters = insurer in self.insurer_filters
        logger.info(f"Has filters: {has_filters}")
        if has_filters:
            logger.info(f"Filters: {self.insurer_filters[insurer]}")
        logger.info(f"=== END DEBUGGING {insurer} ===")

class DashboardView:
    """Dashboard view integrated into the main app"""
    def __init__(self, page: ft.Page, main_app):
        self.page = page
        self.main_app = main_app
        self.df = None
        self.filtered_df = None
        self.datatable_page_number = 1
        self.rows_per_page = 50
        # UI Control References
        self.kpi_cards = None
        self.status_filter = None
        self.field_filter = None
        self.policy_search = None
        self.bar_chart = None
        self.pie_chart = None
        self.data_table = None
        self.pagination_controls = None
        self.dashboard_content = None

    def build_dashboard(self, results_df):
        """Build the dashboard with the results data"""
        self.df = results_df.copy()
        # Clean the data
        self.df = self.df[self.df['Policy Number'] != 'FILTER_INFO'] if 'Policy Number' in self.df.columns else self.df
        self.df['Match Score'] = pd.to_numeric(self.df['Match Score'], errors='coerce')
        self.df['Internal Value'] = self.df['Internal Value'].astype(str)
        self.df['MIS Value'] = self.df['MIS Value'].astype(str)
        self.filtered_df = self.df.copy()
        return self._create_dashboard_layout()

    def _create_dashboard_layout(self):
        """Create the dashboard layout"""
        self.kpi_cards = ft.Container(
            content=ft.Row(alignment=ft.MainAxisAlignment.CENTER),
            padding=ft.padding.symmetric(vertical=10, horizontal=20)
        )
        self.status_filter = ft.Dropdown(
            label="Filter by Match Status",
            options=[ft.dropdown.Option(status) for status in sorted(self.df['Match Status'].unique())],
            on_change=self.apply_filters,
            width=250
        )
        self.field_filter = ft.Dropdown(
            label="Filter by Field",
            options=[ft.dropdown.Option(field) for field in sorted(self.df['Field'].unique())],
            on_change=self.apply_filters,
            width=250
        )
        self.policy_search = ft.TextField(
            label="Search by Policy Number",
            on_change=self.apply_filters,
            prefix_icon=ft.Icons.SEARCH,
            width=250
        )
        filter_section = ft.Container(
            content=ft.Column([
                ft.Text("Filters", size=18, weight=ft.FontWeight.BOLD),
                self.status_filter,
                self.field_filter,
                self.policy_search,
            ]),
            padding=15,
            border=ft.border.all(1, ft.Colors.GREY_300),
            border_radius=10,
            bgcolor=ft.Colors.WHITE
        )
        self.bar_chart = ft.Container(height=400, bgcolor=ft.Colors.WHITE, border_radius=10)
        self.pie_chart = ft.Container(height=400, bgcolor=ft.Colors.WHITE, border_radius=10)
        self.data_table = ft.DataTable(
            columns=[],
            rows=[],
            expand=True,
            border=ft.border.all(1, ft.Colors.GREY_300),
            border_radius=ft.border_radius.all(10),
            vertical_lines=ft.border.BorderSide(1, ft.Colors.GREY_300),
            horizontal_lines=ft.border.BorderSide(1, ft.Colors.GREY_200),
            heading_row_color=ft.Colors.GREY_50,
        )
        self.pagination_controls = ft.Row(alignment=ft.MainAxisAlignment.CENTER)
        self.dashboard_content = ft.Column([
            ft.Row([
                ft.IconButton(
                    icon=ft.Icons.ARROW_BACK,
                    on_click=lambda _: self.main_app.switch_to_validation(),
                    tooltip="Back to Validation"
                ),
                ft.Text("Insurance Data Dashboard", size=24, weight=ft.FontWeight.BOLD),
            ]),
            ft.Divider(),
            self.kpi_cards,
            ft.Divider(height=20, color=ft.Colors.TRANSPARENT),
            ft.Row(
                [
                    ft.Column([filter_section], width=300),
                    ft.Column(
                        [
                            ft.Text("Visual Analysis", size=18, weight=ft.FontWeight.BOLD),
                            ft.Row([self.bar_chart, self.pie_chart], expand=True),
                            ft.Divider(height=10, color=ft.Colors.TRANSPARENT),
                            ft.Text("Detailed Data", size=18, weight=ft.FontWeight.BOLD),
                            ft.Container(
                                content=self.data_table,
                                height=400,
                                border_radius=10,
                                bgcolor=ft.Colors.WHITE
                            ),
                            self.pagination_controls
                        ],
                        expand=True,
                    ),
                ],
                vertical_alignment=ft.CrossAxisAlignment.START,
                expand=True
            ),
        ], scroll=ft.ScrollMode.AUTO)
        self.update_all_visuals()
        return self.dashboard_content

    def apply_filters(self, e):
        """Apply filters to the data"""
        self.filtered_df = self.df.copy()
        if self.status_filter.value:
            self.filtered_df = self.filtered_df[self.filtered_df['Match Status'] == self.status_filter.value]
        if self.field_filter.value:
            self.filtered_df = self.filtered_df[self.filtered_df['Field'] == self.field_filter.value]
        if self.policy_search.value:
            self.filtered_df = self.filtered_df[
                self.filtered_df['Policy Number'].str.contains(
                    self.policy_search.value, case=False, na=False
                )
            ]
        self.datatable_page_number = 1
        self.update_all_visuals()

    def update_all_visuals(self):
        """Update all dashboard components"""
        self.update_kpis()
        self.update_charts()
        self.update_datatable()
        self.page.update()

    def update_kpis(self):
        """Update KPI cards"""
        total_records = len(self.filtered_df)
        mismatches = len(self.filtered_df[self.filtered_df['Match Status'] == 'Mismatch'])
        matches = len(self.filtered_df[self.filtered_df['Match Status'] == 'Match'])
        not_found_internal = len(self.filtered_df[self.filtered_df['Match Status'] == 'Not Found in Internal'])
        not_found_mis = len(self.filtered_df[self.filtered_df['Match Status'] == 'Not Found in MIS'])
        
        match_rate = (matches / total_records * 100) if total_records > 0 else 0
        
        premium_mismatches = self.filtered_df[
            (self.filtered_df['Match Status'] == 'Mismatch') &
            (self.filtered_df['Field'].isin(['Total Premium', 'Tp Premium', 'Final Tp Premium']))
        ]
        total_mismatch_value = 0
        if not premium_mismatches.empty:
            for _, row in premium_mismatches.iterrows():
                details = row['Details']
                if isinstance(details, str) and '₹' in details:
                    try:
                        amount_str = details.split('₹')[1].split()[0].replace(',', '')
                        total_mismatch_value += float(amount_str)
                    except:
                        pass
        # Helper function to format large numbers compactly
        def format_number(num):
            if num >= 1_000_000:
                return f"{num/1_000_000:.1f}M"
            elif num >= 1_000:
                return f"{num/1_000:.1f}K"
            else:
                return f"{num:,}"
        
        def format_currency(amount):
            if amount >= 10_000_000:  # 1 crore
                return f"₹{amount/10_000_000:.1f}Cr"
            elif amount >= 100_000:  # 1 lakh
                return f"₹{amount/100_000:.1f}L"
            elif amount >= 1_000:
                return f"₹{amount/1_000:.1f}K"
            else:
                return f"₹{amount:,.0f}"
        
        # Organize KPI cards in two rows for better layout
        top_row_cards = [
            self.create_kpi_card("Total Records", format_number(total_records), ft.Icons.TABLE_ROWS, ft.Colors.BLUE),
            self.create_kpi_card("Mismatches", format_number(mismatches), ft.Icons.WARNING, ft.Colors.ORANGE),
            self.create_kpi_card("Match Rate", f"{match_rate:.1f}%", ft.Icons.CHECK_CIRCLE, ft.Colors.GREEN),
        ]
        
        bottom_row_cards = [
            self.create_kpi_card("Not Found (Internal)", format_number(not_found_internal), ft.Icons.ERROR, ft.Colors.RED),
            self.create_kpi_card("Not Found (MIS)", format_number(not_found_mis), ft.Icons.HELP, ft.Colors.PURPLE),
            self.create_kpi_card("Premium Diff", format_currency(total_mismatch_value), ft.Icons.MONETIZATION_ON, ft.Colors.TEAL),
        ]
        
        self.kpi_cards.content.controls = [
            ft.Column([
                ft.Row(top_row_cards, alignment=ft.MainAxisAlignment.SPACE_EVENLY, spacing=10),
                ft.Container(height=10),  # Spacer between rows
                ft.Row(bottom_row_cards, alignment=ft.MainAxisAlignment.SPACE_EVENLY, spacing=10),
            ], spacing=0)
        ]

    def create_kpi_card(self, title, value, icon, color):
        """Create a KPI card widget"""
        return ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(icon, color=color, size=24),
                    ft.Text(title, size=12, color=ft.Colors.GREY_700, weight=ft.FontWeight.W_500)
                ], alignment=ft.MainAxisAlignment.START, spacing=8),
                ft.Container(height=5),  # Small spacer
                ft.Text(
                    value, 
                    size=16, 
                    weight=ft.FontWeight.BOLD, 
                    color=color,
                    text_align=ft.TextAlign.CENTER,
                    overflow=ft.TextOverflow.ELLIPSIS
                )
            ], 
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=2
            ),
            padding=ft.padding.all(15),
            border_radius=8,
            border=ft.border.all(1, ft.Colors.GREY_300),
            bgcolor=ft.Colors.WHITE,
            width=180,
            height=85
        )

    def update_charts(self):
        """Update the bar and pie charts"""
        # Bar Chart
        mismatch_data = self.filtered_df[self.filtered_df['Match Status'] == 'Mismatch']
        if not mismatch_data.empty:
            field_counts = mismatch_data['Field'].value_counts().head(10)
            bar_content = ft.Column([
                ft.Text("Top Mismatched Fields", size=16, weight=ft.FontWeight.BOLD),
                ft.Divider(),
            ])
            max_count = field_counts.max() if not field_counts.empty else 1
            for field, count in field_counts.items():
                bar_width = (count / max_count) * 300 if max_count > 0 else 0
                bar_content.controls.append(
                    ft.Row([
                        ft.Text(field[:20], width=120, size=12),
                        ft.Container(
                            bgcolor=ft.Colors.ORANGE_400,
                            width=bar_width,
                            height=20,
                            border_radius=5
                        ),
                        ft.Text(str(count), size=12)
                    ])
                )
            self.bar_chart.content = bar_content
        else:
            self.bar_chart.content = ft.Text("No mismatches to display", size=14, color=ft.Colors.GREY_600)
        
        # Pie Chart
        status_counts = self.filtered_df['Match Status'].value_counts()
        pie_content = ft.Column([
            ft.Text("Match Status Distribution", size=16, weight=ft.FontWeight.BOLD),
            ft.Divider(),
        ])
        Colors = {
            'Match': ft.Colors.GREEN_400,
            'Mismatch': ft.Colors.ORANGE_400,
            'Not Found in Internal': ft.Colors.RED_400,
            'Not Found in MIS': ft.Colors.PURPLE_400,
            'Endorsement': ft.Colors.BLUE_400,
            'Cancellation': ft.Colors.RED_600
        }
        total = status_counts.sum()
        for status, count in status_counts.items():
            percentage = (count / total * 100) if total > 0 else 0
            pie_content.controls.append(
                ft.Row([
                    ft.Container(
                        bgcolor=Colors.get(status, ft.Colors.GREY_400),
                        width=20,
                        height=20,
                        border_radius=5
                    ),
                    ft.Text(f"{status}: {count:,} ({percentage:.1f}%)", size=12)
                ])
            )
        self.pie_chart.content = pie_content

    def update_datatable(self, e=None, direction=0):
        """Update the data table with pagination"""
        if e:
            self.datatable_page_number += direction
        total_pages = (len(self.filtered_df) - 1) // self.rows_per_page + 1
        if total_pages == 0: total_pages = 1
        self.datatable_page_number = max(1, min(self.datatable_page_number, total_pages))
        start_index = (self.datatable_page_number - 1) * self.rows_per_page
        end_index = start_index + self.rows_per_page
        page_df = self.filtered_df.iloc[start_index:end_index]
        self.data_table.columns = [
            ft.DataColumn(ft.Text("Policy Number", size=12)),
            ft.DataColumn(ft.Text("Request Id", size=12)),
            ft.DataColumn(ft.Text("Field", size=12)),
            ft.DataColumn(ft.Text("Status", size=12)),
            ft.DataColumn(ft.Text("Internal", size=12)),
            ft.DataColumn(ft.Text("MIS", size=12)),
        ]
        self.data_table.rows = []
        for _, row in page_df.iterrows():
            status_color = {
                'Match': ft.Colors.GREEN_700,
                'Mismatch': ft.Colors.ORANGE_700,
                'Not Found in Internal': ft.Colors.RED_700,
                'Not Found in MIS': ft.Colors.PURPLE_700,
                'Endorsement': ft.Colors.BLUE_700,
                'Cancellation': ft.Colors.RED_800
            }.get(row['Match Status'], ft.Colors.BLACK)

            self.data_table.rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(str(row['Policy Number'])[:20], size=11)),
                        ft.DataCell(ft.Text(str(row.get('Request Id', ''))[:20], size=11)),
                        ft.DataCell(ft.Text(row['Field'], size=11)),
                        ft.DataCell(ft.Text(row['Match Status'], size=11, color=status_color, weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(str(row['Internal Value'])[:30], size=11, tooltip=str(row['Internal Value']))),
                        ft.DataCell(ft.Text(str(row['MIS Value'])[:30], size=11, tooltip=str(row['MIS Value']))),
                    ]
                )
            )
        self.pagination_controls.controls = [
            ft.IconButton(icon=ft.Icons.ARROW_BACK, on_click=lambda e: self.update_datatable(e, -1), disabled=(self.datatable_page_number == 1)),
            ft.Text(f"Page {self.datatable_page_number} of {total_pages}"),
            ft.IconButton(icon=ft.Icons.ARROW_FORWARD, on_click=lambda e: self.update_datatable(e, 1), disabled=(self.datatable_page_number == total_pages))
        ]

class InsurerWiseReportView:
    """Insurer-wise summary report view with a custom, aligned table layout."""
    def __init__(self, page: ft.Page, main_app, summary_df):
        self.page = page
        self.main_app = main_app
        self.summary_df = summary_df

    def build(self):
        """Build the insurer-wise report view."""
        
        def format_full_premium(num):
            """Formats a number in the Indian numbering system with ₹ prefix."""
            if not isinstance(num, (int, float, np.number)) or pd.isna(num):
                return '₹ 0'
            s = f"{num:,.2f}"
            integer_part, decimal_part = s.split('.')
            integer_part = integer_part.replace(',', '')
            if len(integer_part) <= 3:
                return f"₹ {integer_part}" 
            last_three = integer_part[-3:]
            other_digits = integer_part[:-3]
            formatted_other = re.sub(r'(\d)(?=(\d{2})+(?!\d))', r'\1,', other_digits)
            return f"₹ {formatted_other},{last_three}"

        # Define styles
        header_style = {"size": 13, "weight": ft.FontWeight.BOLD, "color": ft.Colors.WHITE}
        sub_header_style = {"size": 12, "weight": ft.FontWeight.BOLD, "color": ft.Colors.BLACK87}
        cell_style = {"size": 12}
        numeric_cell_style = {"size": 12, "font_family": "monospace", "text_align": ft.TextAlign.RIGHT}
        
        col_ratios = {"insurer": 4, "nop": 2, "premium": 3}

        def create_cell(text, expand_ratio, bgcolor=None, is_numeric=False, is_bold=False, is_cr=False, full_value=None):
            final_style = numeric_cell_style.copy() if (is_numeric or is_cr) else cell_style.copy()
            if is_bold:
                final_style['weight'] = ft.FontWeight.BOLD
            
            tooltip = str(full_value) if full_value is not None else str(text)
            formatted_text = str(text)

            if is_cr:
                try:
                    num = float(text)
                    if num == 0:
                        formatted_text = "₹0"
                    else:
                        formatted_text = f"{num:,.2f} Cr"
                    tooltip = format_full_premium(full_value)
                except (ValueError, TypeError):
                    formatted_text = 'Premium'
            elif is_numeric:
                try:
                    num = int(text)
                    formatted_text = f"{num:,}"
                except (ValueError, TypeError):
                     formatted_text = '0'

            return ft.Container(
                content=ft.Text(value=formatted_text, tooltip=tooltip, **final_style),
                expand=expand_ratio, bgcolor=bgcolor,
                padding=ft.padding.symmetric(vertical=10, horizontal=8),
                alignment=ft.alignment.center_right if (is_numeric or is_cr) else ft.alignment.center_left
            )

        # Header Row 1
        header_row_1 = ft.Row(
            controls=[
                ft.Container(expand=col_ratios["insurer"]),
                ft.Container(content=ft.Text("Booked", **header_style, text_align=ft.TextAlign.CENTER), expand=col_ratios["nop"] + col_ratios["premium"], bgcolor="#2F4F4F"),
                ft.Container(content=ft.Text("Unbooked", **header_style, text_align=ft.TextAlign.CENTER), expand=col_ratios["nop"] + col_ratios["premium"], bgcolor="#FF8C00"),
                ft.Container(content=ft.Text("Pending For Booking", **header_style, text_align=ft.TextAlign.CENTER), expand=col_ratios["nop"] + col_ratios["premium"], bgcolor="#20B2AA"),
                ft.Container(content=ft.Text("Grand Total", **header_style, text_align=ft.TextAlign.CENTER), expand=col_ratios["nop"] + col_ratios["premium"], bgcolor="#4682B4"),
            ], spacing=1
        )

        # Header Row 2
        sub_header_bgcolor = ft.Colors.GREY_200
        header_row_2 = ft.Row(
            controls=[
                create_cell("Insurer", col_ratios["insurer"], bgcolor=sub_header_bgcolor, is_bold=True),
                # Booked
                create_cell("NOP", col_ratios["nop"], bgcolor=sub_header_bgcolor, is_bold=True),
                create_cell("Premium (Cr)", col_ratios["premium"], bgcolor=sub_header_bgcolor, is_cr=True, is_bold=True),
                # Unbooked
                create_cell("NOP", col_ratios["nop"], bgcolor=sub_header_bgcolor, is_bold=True),
                create_cell("Premium (Cr)", col_ratios["premium"], bgcolor=sub_header_bgcolor, is_cr=True, is_bold=True),
                # Pending
                create_cell("NOP", col_ratios["nop"], bgcolor=sub_header_bgcolor, is_bold=True),
                create_cell("Premium (Cr)", col_ratios["premium"], bgcolor=sub_header_bgcolor, is_cr=True, is_bold=True),
                # Grand Total
                create_cell("Total NOP", col_ratios["nop"], bgcolor=sub_header_bgcolor, is_bold=True),
                create_cell("Total Premium (Cr)", col_ratios["premium"], bgcolor=sub_header_bgcolor, is_cr=True, is_bold=True),
            ], spacing=1
        )
        
        # Data Rows
        data_rows = []
        for _, row in self.summary_df.iterrows():
            data_rows.append(ft.Row(
                controls=[
                    create_cell(row['Insurer'], col_ratios["insurer"]),
                    create_cell(str(row['Booked NOP']), col_ratios["nop"], is_numeric=True),
                    create_cell(row['Booked Premium (Cr)'], col_ratios["premium"], is_cr=True, full_value=row['Booked Premium']),
                    create_cell(str(row['Unbooked NOP']), col_ratios["nop"], is_numeric=True),
                    create_cell(row['Unbooked Premium (Cr)'], col_ratios["premium"], is_cr=True, full_value=row['Unbooked Premium']),
                    create_cell(str(row['Pending NOP']), col_ratios["nop"], is_numeric=True),
                    create_cell(row['Pending Premium (Cr)'], col_ratios["premium"], is_cr=True, full_value=row['Pending Premium']),
                    create_cell(str(row['Total NOP']), col_ratios["nop"], is_numeric=True),
                    create_cell(row['Total Premium (Cr)'], col_ratios["premium"], is_cr=True, full_value=row['Total Premium']),
                ], spacing=1
            ))

        # Total Row - Calculate totals properly
        total_row_data = {}
        for col in ['Booked NOP', 'Unbooked NOP', 'Pending NOP', 'Total NOP', 
                   'Booked Premium', 'Unbooked Premium', 'Pending Premium', 'Total Premium',
                   'Booked Premium (Cr)', 'Unbooked Premium (Cr)', 'Pending Premium (Cr)', 'Total Premium (Cr)']:
            if col in self.summary_df.columns:
                total_row_data[col] = self.summary_df[col].sum()
        
        # Verify total calculation
        calculated_total_nop = total_row_data.get('Booked NOP', 0) + total_row_data.get('Unbooked NOP', 0) + total_row_data.get('Pending NOP', 0)
        logger.info(f"=== GRAND TOTAL VERIFICATION ===")
        logger.info(f"Booked Total: {total_row_data.get('Booked NOP', 0)}")
        logger.info(f"Unbooked Total: {total_row_data.get('Unbooked NOP', 0)}")
        logger.info(f"Pending Total: {total_row_data.get('Pending NOP', 0)}")
        logger.info(f"Sum: {calculated_total_nop}")
        logger.info(f"Total NOP from sum: {total_row_data.get('Total NOP', 0)}")
        
        # Ensure the total is correct
        total_row_data['Total NOP'] = calculated_total_nop
        total_row_bgcolor = ft.Colors.BLUE_GREY_50
        total_row = ft.Row(
            controls=[
                create_cell("Total", col_ratios["insurer"], bgcolor=total_row_bgcolor, is_bold=True),
                create_cell(str(total_row_data['Booked NOP']), col_ratios["nop"], bgcolor=total_row_bgcolor, is_numeric=True, is_bold=True),
                create_cell(total_row_data['Booked Premium (Cr)'], col_ratios["premium"], bgcolor=total_row_bgcolor, is_cr=True, is_bold=True, full_value=total_row_data['Booked Premium']),
                create_cell(str(total_row_data['Unbooked NOP']), col_ratios["nop"], bgcolor=total_row_bgcolor, is_numeric=True, is_bold=True),
                create_cell(total_row_data['Unbooked Premium (Cr)'], col_ratios["premium"], bgcolor=total_row_bgcolor, is_cr=True, is_bold=True, full_value=total_row_data['Unbooked Premium']),
                create_cell(str(total_row_data['Pending NOP']), col_ratios["nop"], bgcolor=total_row_bgcolor, is_numeric=True, is_bold=True),
                create_cell(total_row_data['Pending Premium (Cr)'], col_ratios["premium"], bgcolor=total_row_bgcolor, is_cr=True, is_bold=True, full_value=total_row_data['Pending Premium']),
                create_cell(str(total_row_data['Total NOP']), col_ratios["nop"], bgcolor=total_row_bgcolor, is_numeric=True, is_bold=True),
                create_cell(total_row_data['Total Premium (Cr)'], col_ratios["premium"], bgcolor=total_row_bgcolor, is_cr=True, is_bold=True, full_value=total_row_data['Total Premium']),
            ], spacing=1
        )

        custom_table = ft.Column([
            header_row_1, header_row_2,
            ft.Container(
                content=ft.Column(controls=data_rows, scroll=ft.ScrollMode.ADAPTIVE, spacing=1),
                expand=True, bgcolor=ft.Colors.GREY_300
            ),
            total_row,
        ], spacing=1, expand=True)

        return ft.Column([
            ft.Row([
                ft.IconButton(icon=ft.Icons.ARROW_BACK, on_click=lambda _: self.main_app.switch_to_validation(), tooltip="Back to Validation"),
                ft.Text(f"Motor Report", size=24, weight=ft.FontWeight.BOLD),
                ft.Container(expand=True),
                ft.ElevatedButton("Download Report", icon=ft.Icons.DOWNLOAD, on_click=self.download_report, bgcolor=ft.Colors.BLUE_700, color=ft.Colors.WHITE)
            ]),
            ft.Divider(),
            ft.Container(
                content=custom_table, expand=True, border=ft.border.all(1, ft.Colors.GREY_300), 
                border_radius=8, clip_behavior=ft.ClipBehavior.HARD_EDGE
            ),
        ], expand=True)

    def download_report(self, e):
        """Handler to call the main app's export function."""
        self.main_app.export_summary_report(self.summary_df)

class InsuranceValidationApp:
    """Main Flet Application for ID Recon - Insurance Data Reconciliation"""
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = "ID Recon"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.padding = 0
        
        self.internal_df = None
        self.mis_dfs = {}
        # ### FIX ###: Initialized self.offline_df here.
        self.offline_df = None
        self.comparison_results = {}
        self.current_results_df = None
        self.df_to_export = None
        
        self.comparator = InsuranceDataComparator()
        self.dashboard_view = DashboardView(page, self)
        
        self.progress_ring = None
        self.progress_text = None
        self.results_container = None
        self.main_container = None
        
        self.build_ui()

    def switch_to_dashboard(self):
        """Switch to dashboard view"""
        if self.current_results_df is not None and not self.current_results_df.empty:
            dashboard_content = self.dashboard_view.build_dashboard(self.current_results_df)
            self.main_container.content = dashboard_content
            self.page.update()

    def switch_to_validation(self):
        """Switch back to validation view"""
        self.main_container.content = self.validation_content
        self.page.update()

    def switch_to_insurer_report(self, summary_df):
        """Switch to the new insurer-wise report view"""
        insurer_report_view = InsurerWiseReportView(self.page, self, summary_df)
        report_content = insurer_report_view.build()
        self.main_container.content = report_content
        self.page.update()

    def build_ui(self):
        """Build the main UI"""
        try:
            header = ft.Container(
                content=ft.Row([
                    ft.Icon(ft.Icons.SHIELD, size=40, color=ft.Colors.BLUE_700),
                    ft.Text("ID Recon", size=28, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_700),
                    ft.Text("Enterprise Edition", size=16, color=ft.Colors.GREY_600)
                ], spacing=10),
                padding=20, bgcolor=ft.Colors.GREY_100, border_radius=10
            )
            self.file_upload_section = self.create_file_upload_section()
            self.control_panel = self.create_control_panel()
            self.results_container = ft.Container(
                padding=20, border_radius=10, bgcolor=ft.Colors.WHITE,
                content=ft.Column([
                    ft.Text("Upload files and run comparison to see results", size=16, color=ft.Colors.GREY_600, text_align=ft.TextAlign.CENTER)
                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER)
            )
            self.validation_content = ft.Column([
                header,
                ft.Row([
                    ft.Column([self.file_upload_section, self.control_panel], width=400),
                    ft.VerticalDivider(width=1),
                    ft.Column([self.results_container], expand=True)
                ], expand=True)
            ], scroll=ft.ScrollMode.AUTO)
            self.main_container = ft.Container(
                content=self.validation_content, padding=20, expand=True
            )
            self.page.add(self.main_container)
        except Exception as ex:
            logger.error(f"Error building UI: {str(ex)}\n{traceback.format_exc()}")
            self.page.add(ft.Text(f"Fatal Error Initializing Application: {str(ex)}", color=ft.Colors.RED_700))

    def create_file_upload_section(self):
        """Create file upload section"""
        self.internal_file_picker = ft.FilePicker(on_result=self.handle_internal_file)
        self.mis_file_picker = ft.FilePicker(on_result=self.handle_mis_files)
        # ### FIX ###: Ensure the picker for the offline file is created and linked.
        self.offline_file_picker = ft.FilePicker(on_result=self.handle_offline_file)
        self.save_file_picker = ft.FilePicker(on_result=self.handle_save_file_result)
        
        self.page.overlay.extend([
            self.internal_file_picker, 
            self.mis_file_picker, 
            self.offline_file_picker, 
            self.save_file_picker
        ])
        
        self.internal_file_info = ft.Column()
        self.mis_file_info = ft.Column()
        self.offline_file_info = ft.Column()

        return ft.Container(
            content=ft.Column([
                ft.Text("Data Upload", size=20, weight=ft.FontWeight.BOLD), ft.Divider(),
                
                ft.Text("1. Internal Booked Data", weight=ft.FontWeight.W_500),
                ft.ElevatedButton("Upload Internal Dataset", icon=ft.Icons.UPLOAD_FILE, width=350, on_click=lambda _: self.internal_file_picker.pick_files(allowed_extensions=["xlsx", "csv"], allow_multiple=True)),
                self.internal_file_info, ft.Divider(),
                
                ft.Text("2. Insurer MIS Data", weight=ft.FontWeight.W_500),
                ft.ElevatedButton("Upload MIS Files (Multiple)", icon=ft.Icons.UPLOAD_FILE, width=350, bgcolor=ft.Colors.GREEN_700, color=ft.Colors.WHITE, on_click=lambda _: self.mis_file_picker.pick_files(allowed_extensions=["xlsx", "csv"], allow_multiple=True)),
                self.mis_file_info, ft.Divider(),

                ft.Text("3. Offline Punching Data", weight=ft.FontWeight.W_500),
                ft.ElevatedButton("Upload Offline Punching Dataset", icon=ft.Icons.EDIT_DOCUMENT, width=350, bgcolor=ft.Colors.ORANGE_700, color=ft.Colors.WHITE, on_click=lambda _: self.offline_file_picker.pick_files(allowed_extensions=["xlsx", "csv"], allow_multiple=True)),
                self.offline_file_info, ft.Divider(),
                
                ft.Text("4. Configuration", weight=ft.FontWeight.W_500),
                ft.ElevatedButton("Manage Column Mappings", icon=ft.Icons.SETTINGS, width=350, bgcolor=ft.Colors.PURPLE_700, color=ft.Colors.WHITE, on_click=self.debug_mapping_manager_click)
            ], spacing=10),
            padding=20, border_radius=10, bgcolor=ft.Colors.WHITE, border=ft.border.all(1, ft.Colors.GREY_300)
        )

    def create_control_panel(self):
        """Create control panel for comparison settings"""
        self.insurer_dropdown = ft.Dropdown(label="Select Insurer", width=350, options=[], on_change=self.update_field_options)
        self.field_checkboxes = ft.Column(scroll=ft.ScrollMode.AUTO, height=200, horizontal_alignment=ft.CrossAxisAlignment.START)
        self.date_filter_switch = ft.Switch(label="Apply Date Filter", value=False, on_change=self.toggle_date_filter)
        self.start_date_picker = ft.TextField(label="Start Date", read_only=True, value=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"), disabled=True, width=160)
        self.end_date_picker = ft.TextField(label="End Date", read_only=True, value=datetime.now().strftime("%Y-%m-%d"), disabled=True, width=160)
        self.run_button = ft.ElevatedButton("Run Comparison", icon=ft.Icons.PLAY_ARROW, on_click=self.run_comparison, disabled=True, width=350, height=50, bgcolor=ft.Colors.BLUE_700, color=ft.Colors.WHITE)
        self.progress_ring = ft.ProgressRing(visible=False)
        self.progress_text = ft.Text("", visible=False)
        self.progress_bar = ft.ProgressBar(visible=False, width=350)
        return ft.Container(
            content=ft.Column([
                ft.Text("Comparison Settings", size=20, weight=ft.FontWeight.BOLD), ft.Divider(),
                self.insurer_dropdown,
                ft.Text("Select Fields to Compare", weight=ft.FontWeight.W_500),
                ft.Row([
                    ft.TextButton("Select All", on_click=self.select_all_fields),
                    ft.TextButton("Clear All", on_click=self.clear_all_fields),
                    ft.TextButton("Test Button", on_click=self.test_button_click),
                    ft.TextButton("Test Save Dialog", on_click=self.test_save_dialog, tooltip="Test save file dialog"),
                    ft.TextButton("Show Mapping", on_click=self.show_column_mapping_debug, tooltip="Show column mapping for debugging"),
                    ft.TextButton("Show Filters", on_click=self.show_insurer_filters_debug, tooltip="Show applied filters for this insurer"),
                ], scroll=ft.ScrollMode.AUTO),
                self.field_checkboxes, ft.Divider(),
                self.date_filter_switch,
                ft.Row([self.start_date_picker, self.end_date_picker], scroll=ft.ScrollMode.AUTO), ft.Divider(),
                self.run_button, self.progress_ring, self.progress_bar, self.progress_text
            ], spacing=10),
            padding=20, border_radius=10, bgcolor=ft.Colors.WHITE, border=ft.border.all(1, ft.Colors.GREY_300)
        )

    def handle_internal_file(self, e: ft.FilePickerResultEvent):
        """Handle internal file upload (multiple files)"""
        if not e.files: return
        self.internal_file_info.controls.clear()
        dfs = []
        total_files = len(e.files)
        self.progress_bar.visible = True
        self.progress_bar.value = 0
        self.progress_text.value = "Loading internal files..."
        self.progress_text.visible = True
        self.page.update()
        for idx, file in enumerate(e.files):
            try:
                self.progress_text.value = f"Loading {file.name}..."
                self.page.update()
                df = pd.read_csv(file.path, low_memory=False, on_bad_lines='skip') if file.name.endswith('.csv') else pd.read_excel(file.path)
                df.columns = df.columns.str.strip()
                df = self.comparator.convert_old_to_new_format(df)
                self.internal_file_info.controls.append(ft.Text(f"✓ Loaded: {file.name} ({len(df):,} rows)", color=ft.Colors.GREEN_700))
                dfs.append(df)
            except Exception as ex:
                self.internal_file_info.controls.append(ft.Text(f"❌ Error loading {file.name}: {ex}", color=ft.Colors.RED_700))
                logger.error(f"Error loading {file.name}: {traceback.format_exc()}")
            self.progress_bar.value = (idx + 1) / total_files
            self.page.update()
        if dfs:
            self.progress_text.value = "Merging files..."
            self.page.update()
            self.internal_df = pd.concat(dfs, ignore_index=True, sort=False)
            self.internal_file_info.controls.append(ft.Text(f"Total records: {len(self.internal_df):,}", weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_700))
        self.progress_bar.visible = False
        self.progress_text.visible = False
        self.check_enable_run_button()
        self.page.update()

    # ### FIX 2: ADD THE MISSING FILE HANDLER ###
    # This entire method was missing, causing the 'offline_df' variable to never be set.
    def handle_offline_file(self, e: ft.FilePickerResultEvent):
        """Handle 'Offline Punching Data' file upload."""
        if not e.files: return
        self.offline_file_info.controls.clear()
        dfs = []
        total_files = len(e.files)
        self.progress_bar.visible = True
        self.progress_bar.value = 0
        self.progress_text.value = "Loading offline files..."
        self.progress_text.visible = True
        self.page.update()
        
        for idx, file in enumerate(e.files):
            try:
                self.progress_text.value = f"Loading {file.name}..."
                self.page.update()
                df = pd.read_csv(file.path, low_memory=False, on_bad_lines='skip', encoding='latin1') if file.name.endswith('.csv') else pd.read_excel(file.path)
                df.columns = df.columns.str.strip()
                df = self.comparator.convert_old_to_new_format(df)
                
                # Check for the crucial 'Status' column as requested.
                if 'Status' not in df.columns:
                    # If 'Status' is missing, maybe the old format conversion created 'Policy Status'.
                    if 'Policy Status' in df.columns:
                        df.rename(columns={'Policy Status': 'Status'}, inplace=True)
                    else:
                        self.offline_file_info.controls.append(ft.Text(f"❌ Error in {file.name}: 'Status' column not found.", color=ft.Colors.RED_700))
                        continue

                self.offline_file_info.controls.append(ft.Text(f"✓ Loaded: {file.name} ({len(df):,} rows)", color=ft.Colors.GREEN_700))
                dfs.append(df)
            except Exception as ex:
                self.offline_file_info.controls.append(ft.Text(f"❌ Error loading {file.name}: {ex}", color=ft.Colors.RED_700))
                logger.error(f"Error loading offline file {file.name}: {traceback.format_exc()}")
            self.progress_bar.value = (idx + 1) / total_files
            self.page.update()

        if dfs:
            self.progress_text.value = "Merging offline files..."
            self.page.update()
            self.offline_df = pd.concat(dfs, ignore_index=True, sort=False)
            
            # Handle duplicate policy numbers by prioritizing 'booked' status
            self.progress_text.value = "Processing duplicates..."
            self.page.update()
            original_count = len(self.offline_df)
            self.offline_df = self.comparator.handle_offline_duplicates(self.offline_df)
            duplicate_count = original_count - len(self.offline_df)
            
            self.offline_file_info.controls.append(ft.Text(f"Total offline records: {len(self.offline_df):,}", weight=ft.FontWeight.BOLD, color=ft.Colors.ORANGE_800))
            if duplicate_count > 0:
                self.offline_file_info.controls.append(ft.Text(f"Removed {duplicate_count:,} duplicate records (prioritized 'booked' status)", color=ft.Colors.BLUE_700))
        
        self.progress_bar.visible = False
        self.progress_text.visible = False
        self.check_enable_run_button()
        self.page.update()

    def handle_mis_files(self, e: ft.FilePickerResultEvent):
        """Handle MIS files upload with insurer-specific sheet detection and preprocessing."""
        if not e.files:
            return
        self.mis_file_info.controls.clear()
        loading_text = ft.Text("Loading and preprocessing MIS files...", color=ft.Colors.BLUE_700)
        self.mis_file_info.controls.append(loading_text)
        self.page.update()
        
        for file in e.files:
            insurer = None
            filename_upper = file.name.upper()
            insurer_patterns = {
                'DIGIT': ['DIGIT'], 'LIBERTY': ['LIBERTY'], 'MAGMA': ['MAGMA'], 'NATIONAL': ['NATIONAL'],
                'UNITED': ['UNITED'], 'SHRIRAM': ['SHRIRAM'], 'ICICI': ['ICICI'], 'IFFCO': ['IFFCO'],
                'BAJAJ': ['BAJAJ'], 'SBI': ['SBI'], 'RELIANCE': ['RELIANCE'], 'TATA': ['TATA'], 'HDFC': ['HDFC'],
                'KOTAK': ['KOTAK'], 'ROYAL': ['ROYAL'], 'CHOLA': ['CHOLA'],
                'FGI': ['FGI'], 'ZUNO': ['ZUNO'], 'UNIVERSAL': ['UNIVERSAL', 'SOMPO'],
                'NEW_INDIA': ['NEW_INDIA', 'NEWINDIA'], 'ORIENTAL': ['ORIENTAL'], 'RAHEJA': ['RAHEJA']
            }
            for ins_key, patterns in insurer_patterns.items():
                if any(pattern in filename_upper for pattern in patterns):
                    insurer = ins_key
                    break
            
            if not insurer:
                self.mis_file_info.controls.append(
                    ft.Text(f"⚠️ Could not detect insurer from {file.name}. Please rename file.", color=ft.Colors.ORANGE_700)
                )
                continue

            try:
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file.path, low_memory=False, on_bad_lines='skip', encoding='utf-8', errors='replace')
                else:
                    df = self.comparator.read_excel_with_sheet_detection(file.path, insurer)
                
                if df is None:
                    raise Exception("Failed to read file.")

                df.columns = df.columns.str.strip()
                
                df = self.comparator.preprocess_mis_df(insurer, df)
                
                if insurer in self.mis_dfs:
                    self.mis_dfs[insurer] = pd.concat([self.mis_dfs[insurer], df], ignore_index=True, sort=False)
                else:
                    self.mis_dfs[insurer] = df
                
                info_text = f"✓ {insurer}: Loaded {len(df):,} records"
                if insurer in self.comparator.insurer_filters:
                    info_text += " (filters will apply)"
                self.mis_file_info.controls.append(ft.Text(info_text, color=ft.Colors.GREEN_700))

            except Exception as ex:
                error_msg = f"❌ Error with {file.name}: {ex}"
                self.mis_file_info.controls.append(ft.Text(error_msg, color=ft.Colors.RED_700))
                logger.error(f"Error loading {file.name}: {traceback.format_exc()}")

        if loading_text in self.mis_file_info.controls:
            self.mis_file_info.controls.remove(loading_text)
            
        self.insurer_dropdown.options = [ft.dropdown.Option(ins) for ins in sorted(self.mis_dfs.keys())]
        if self.insurer_dropdown.options:
            self.insurer_dropdown.value = self.insurer_dropdown.options[0].key
            self.update_field_options(None)
            
        self.check_enable_run_button()
        self.page.update()

    def update_field_options(self, e):
        """Update field selection based on selected insurer"""
        if not self.insurer_dropdown.value: return
        insurer = self.insurer_dropdown.value
        fields = list(self.comparator.column_mappings.get(insurer, {}).keys())
        # Ensure internal-only fields like vehicle_sub_category are present in selection UI
        if 'vehicle_sub_category' not in fields:
            fields.append('vehicle_sub_category')
        self.field_checkboxes.controls.clear()
        for i, field in enumerate(fields):
            self.field_checkboxes.controls.append(ft.Checkbox(label=field.replace('_', ' ').title(), value=True, data=field))
        self.page.update()

    def show_column_mapping_debug(self, e):
        """Debug wrapper for show_column_mapping"""
        try:
            logger.info("Show Mapping button clicked")
            print("DEBUG: Show Mapping button clicked!")  # Console debug
            
            # Simple test first
            if not self.insurer_dropdown.value:
                self.show_error("Please select an insurer first")
                return
                
            insurer = self.insurer_dropdown.value
            self.show_info(f"Showing mapping for {insurer}")
            
            # Try to call the actual function
            self.show_column_mapping(e)
        except Exception as ex:
            logger.error(f"Error in show_column_mapping_debug: {ex}")
            print(f"DEBUG ERROR: {ex}")  # Console debug
            self.show_error(f"Button error: {str(ex)}")

    def show_insurer_filters_debug(self, e):
        """Debug wrapper for show_insurer_filters"""
        try:
            logger.info("Show Filters button clicked")
            print("DEBUG: Show Filters button clicked!")  # Console debug
            
            # Simple test first
            if not self.insurer_dropdown.value:
                self.show_error("Please select an insurer first")
                return
                
            insurer = self.insurer_dropdown.value
            self.show_info(f"Showing filters for {insurer}")
            
            # Try to call the actual function
            self.show_insurer_filters(e)
        except Exception as ex:
            logger.error(f"Error in show_insurer_filters_debug: {ex}")
            print(f"DEBUG ERROR: {ex}")  # Console debug
            self.show_error(f"Button error: {str(ex)}")

    def show_column_mapping(self, e):
        """Show column mapping including sheet information and resolved names."""
        try:
            if not self.insurer_dropdown.value:
                self.show_error("Please select an insurer first")
                return
            
            insurer = self.insurer_dropdown.value
            print(f"DEBUG: Showing mapping for {insurer}")
            
            # Simple dialog test first
            def close_test_dialog(e):
                self.page.dialog.open = False
                self.page.update()
            
            dlg = ft.AlertDialog(
                title=ft.Text(f"Test Dialog - {insurer}"),
                content=ft.Text(f"This is a test dialog for {insurer}"),
                actions=[ft.TextButton("Close", on_click=close_test_dialog)]
            )
            self.page.dialog = dlg
            dlg.open = True
            self.page.update()
            print("DEBUG: Dialog should be showing now")
            
        except Exception as ex:
            logger.error(f"Error showing column mapping: {ex}")
            print(f"DEBUG: Exception in show_column_mapping: {ex}")
            self.show_error(f"Error showing column mapping: {str(ex)}")

    def show_insurer_filters(self, e):
        """Show the filters that will be applied for the selected insurer"""
        try:
            if not self.insurer_dropdown.value:
                self.show_error("Please select an insurer first")
                return
                
            insurer = self.insurer_dropdown.value
            print(f"DEBUG: Showing filters for {insurer}")
            
            # Simple dialog test first
            def close_test_dialog(e):
                self.page.dialog.open = False
                self.page.update()
            
            dlg = ft.AlertDialog(
                title=ft.Text(f"Test Filters Dialog - {insurer}"),
                content=ft.Text(f"This is a test filters dialog for {insurer}"),
                actions=[ft.TextButton("Close", on_click=close_test_dialog)]
            )
            self.page.dialog = dlg
            dlg.open = True
            self.page.update()
            print("DEBUG: Filters dialog should be showing now")
            
        except Exception as ex:
            logger.error(f"Error showing insurer filters: {ex}")
            print(f"DEBUG: Exception in show_insurer_filters: {ex}")
            self.show_error(f"Error showing insurer filters: {str(ex)}")

    def test_button_click(self, e):
        """Test button to verify event handling works"""
        print("TEST BUTTON CLICKED!")
        
        # Test endorsement and cancellation processing
        if hasattr(self, 'comparator'):
            test_records = [
                {'PolicyNo': 'POL001', 'CustomerName': 'Test Customer 1', 'TransactionType': 'Endorsement'},
                {'PolicyNo': 'POL001', 'CustomerName': 'Test Customer 1', 'TransactionType': 'Endorsement'},
                {'PolicyNo': 'POL002', 'CustomerName': 'Test Customer 2', 'TransactionType': 'Cancellation'},
                {'PolicyNo': 'POL003', 'CustomerName': 'Test Customer 3', 'TransactionType': 'Endorsement'}
            ]
            
            endorsements = self.comparator.process_endorsements_cancellations(test_records, 'TEST', 'endorsement')
            cancellations = self.comparator.process_endorsements_cancellations(test_records, 'TEST', 'cancellation')
            
            test_result = f"Test Results:\nEndorsements: {len(endorsements)}\nCancellations: {len(cancellations)}"
            print(test_result)
            self.show_success(f"Test completed! {test_result}")
        else:
            self.show_success("Test button works! (Comparator not available)")
        
        # Test export functionality
        if hasattr(self, 'current_results_df') and self.current_results_df is not None:
            print(f"DEBUG: Testing export with current_results_df shape: {self.current_results_df.shape}")
            print(f"DEBUG: Current results columns: {list(self.current_results_df.columns)}")
            print(f"DEBUG: Current results columns: {list(self.current_results_df.columns)}")
            print(f"DEBUG: Match Status values: {self.current_results_df['Match Status'].value_counts().to_dict()}")
            
            # Test the export method directly
            self.export_endorsements_cancellations(self.current_results_df)
        else:
            print("DEBUG: No current_results_df available for export test")
        
    def test_save_dialog(self, e):
        """Test the save file dialog functionality"""
        print("DEBUG: Test Save Dialog button clicked!")
        
        # Create a simple test DataFrame
        test_data = [
            {'Test': 'Data 1', 'Value': 100},
            {'Test': 'Data 2', 'Value': 200}
        ]
        test_df = pd.DataFrame(test_data)
        
        # Store it and trigger save dialog
        self.df_to_export = test_df
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Test_Export_{timestamp}.csv"
        
        print(f"DEBUG: Triggering save dialog with filename: {filename}")
        
        # Trigger the save file dialog
        self.save_file_picker.save_file(
            dialog_title="Test Save Dialog",
            file_name=filename,
            allowed_extensions=["csv"]
        )
        
        self.show_success("Test save dialog triggered!")
        
    def select_all_fields(self, e):
        """Select all fields"""
        for control in self.field_checkboxes.controls:
            if isinstance(control, ft.Checkbox):
                control.value = True
        self.page.update()

    def clear_all_fields(self, e):
        """Clear all fields"""
        for control in self.field_checkboxes.controls:
            if isinstance(control, ft.Checkbox):
                control.value = False
        self.page.update()

    def toggle_date_filter(self, e):
        """Toggle date filter controls"""
        enabled = self.date_filter_switch.value
        self.start_date_picker.disabled = not enabled
        self.end_date_picker.disabled = not enabled
        self.page.update()

    def check_enable_run_button(self):
        """Check if run button should be enabled"""
        self.run_button.disabled = not (self.internal_df is not None and self.mis_dfs and self.insurer_dropdown.value)
        self.page.update()

    def run_comparison(self, e):
        """Run the comparison"""
        if not self.insurer_dropdown.value: return
        selected_fields = [control.data for control in self.field_checkboxes.controls if isinstance(control, ft.Checkbox) and control.value]
        if not selected_fields:
            self.show_error("Please select at least one field to compare")
            return
        self.progress_ring.visible = True
        self.progress_bar.visible = True
        self.progress_text.visible = True
        self.progress_bar.value = 0
        self.run_button.disabled = True
        self.page.update()
        threading.Thread(target=self.run_comparison_thread, args=(self.insurer_dropdown.value, selected_fields)).start()

    def run_comparison_thread(self, insurer, selected_fields):
        """Run comparison in separate thread - uses resolved mappings."""
        try:
            logger.info(f"[DEBUG] Starting comparison for insurer: {insurer}")
            if self.internal_df is None:
                self.show_error("Internal dataset not loaded")
                return

            internal_df_to_compare = self.internal_df
            if self.date_filter_switch.value:
                date_col = self.comparator.internal_columns['policy_start_date']
                if date_col in internal_df_to_compare.columns:
                    internal_df_to_compare = internal_df_to_compare.copy()
                    internal_df_to_compare[date_col] = pd.to_datetime(internal_df_to_compare[date_col], errors='coerce')
                    start_date = self.start_date_picker.value
                    end_date = self.end_date_picker.value
                    mask = (internal_df_to_compare[date_col] >= pd.Timestamp(start_date)) & (internal_df_to_compare[date_col] <= pd.Timestamp(end_date))
                    internal_df_to_compare = internal_df_to_compare[mask].copy()

            def progress_callback(progress, message):
                self.progress_text.value = message
                self.progress_bar.value = progress
                self.page.update()

            results_df, context_dfs = self.comparator.compare_datasets_async(
                internal_df_to_compare, self.mis_dfs[insurer], insurer, selected_fields, progress_callback
            )
            
            self.current_results_df = results_df
            self.comparison_results[insurer] = results_df
            self.display_results(results_df, context_dfs)

        except Exception as ex:
            logger.error(f"Error during comparison: {traceback.format_exc()}")
            self.show_error(f"Error during comparison: {ex}")
        finally:
            self.progress_ring.visible = False
            self.progress_bar.visible = False
            self.progress_text.visible = False
            self.run_button.disabled = False
            self.page.update()

    def run_full_report(self, e):
        """Handles the 'Insurer-wise Report' button click, now requiring all three files."""
        if self.internal_df is None or not self.mis_dfs or self.offline_df is None:
            self.show_error("Please load Internal, MIS, and Offline Punching datasets first.")
            return

        self.progress_ring.visible = True
        self.progress_text.value = "Generating full insurer-wise report..."
        self.progress_text.visible = True
        self.progress_bar.visible = True
        self.progress_bar.value = 0
        self.page.update()

        threading.Thread(target=self.run_full_report_thread).start()

    def run_full_report_thread(self):
        """Generates the summary data for all insurers using the correct 3-bucket logic with proper priority handling."""
        try:
            summary_data = []
            total_insurers = len(self.mis_dfs.keys())
            
            # --- Step 1: Preparation and Initial Grouping ---
            internal_policy_col = self.comparator.internal_columns['policy_number']
            internal_premium_col = self.comparator.internal_columns['total_premium']
            insurance_col = self.comparator.internal_columns['insurance_company']
            
            # Prepare internal data
            if 'cleaned_policy' not in self.internal_df.columns:
                self.internal_df['cleaned_policy'] = self.internal_df[internal_policy_col].apply(self.comparator.clean_string)
            if 'Detected_Insurer' not in self.internal_df.columns:
                self.internal_df['Detected_Insurer'] = self.internal_df[insurance_col].apply(self.comparator.get_insurer_from_company_name)

            # Prepare offline data with proper duplicate handling
            offline_status_col = 'Status'
            if offline_status_col not in self.offline_df.columns:
                self.show_error(f"Offline Punching Data must contain a '{offline_status_col}' column.")
                return

            # Clean and prepare offline data
            if 'cleaned_policy' not in self.offline_df.columns:
                self.offline_df['cleaned_policy'] = self.offline_df[internal_policy_col].apply(self.comparator.clean_string)
            if 'Detected_Insurer' not in self.offline_df.columns:
                self.offline_df['Detected_Insurer'] = self.offline_df[insurance_col].apply(self.comparator.get_insurer_from_company_name)

            # Standardize status column
            self.offline_df[offline_status_col] = self.offline_df[offline_status_col].astype(str).str.strip().str.lower()

            # --- Step 2: Handle Offline Data Duplicates with Priority Logic ---
            def get_status_priority(status):
                """Return priority for status (lower number = higher priority)"""
                status = str(status).lower().strip()
                if 'booked' in status:
                    return 1  # Highest priority
                elif status in ['new', 'ticket closed duplicate']:
                    return 2  # Pending priority
                else:
                    return 3  # Unbooked priority

            # Group offline data by policy and insurer, keep highest priority status
            offline_deduplicated = []
            for insurer in self.mis_dfs.keys():
                offline_insurer = self.offline_df[self.offline_df['Detected_Insurer'] == insurer].copy()
                if offline_insurer.empty:
                    continue
                
                # Group by policy and keep the record with highest priority status
                for policy, group in offline_insurer.groupby('cleaned_policy'):
                    if len(group) > 1:
                        # Multiple records for same policy - prioritize by status
                        group['status_priority'] = group[offline_status_col].apply(get_status_priority)
                        best_record = group.loc[group['status_priority'].idxmin()]
                    else:
                        best_record = group.iloc[0]
                    offline_deduplicated.append(best_record)

            offline_deduplicated_df = pd.DataFrame(offline_deduplicated)

            # --- Step 3: Main loop for each insurer ---
            for i, insurer in enumerate(self.mis_dfs.keys()):
                self.progress_text.value = f"Processing {insurer} ({i+1}/{total_insurers})..."
                self.progress_bar.value = (i + 1) / total_insurers
                self.page.update()

                # Get insurer-specific data
                internal_insurer_df = self.internal_df[self.internal_df['Detected_Insurer'] == insurer].copy()
                mis_insurer_df = self.mis_dfs[insurer].copy()
                offline_insurer_df = offline_deduplicated_df[offline_deduplicated_df['Detected_Insurer'] == insurer].copy() if not offline_deduplicated_df.empty else pd.DataFrame()

                logger.info(f"Raw MIS data for {insurer}: {len(mis_insurer_df)} records")

                # Prepare MIS data
                mis_policy_col = self.comparator.resolved_mappings.get(insurer, self.comparator.column_mappings.get(insurer, {})).get('policy_number')
                if not mis_policy_col or mis_policy_col not in mis_insurer_df.columns:
                    logger.warning(f"Skipping {insurer}: MIS policy column '{mis_policy_col}' not found.")
                    continue
                
                # Clean policy numbers but keep all records
                mis_insurer_df['cleaned_policy'] = mis_insurer_df[mis_policy_col].apply(self.comparator.clean_string)
                
                # Count records with valid policy numbers
                valid_policy_records = mis_insurer_df[mis_insurer_df['cleaned_policy'].notna() & (mis_insurer_df['cleaned_policy'] != '')]
                invalid_policy_records_df = mis_insurer_df[mis_insurer_df['cleaned_policy'].isna() | (mis_insurer_df['cleaned_policy'] == '')]
                invalid_policy_records = len(invalid_policy_records_df)
                
                if invalid_policy_records > 0:
                    logger.warning(f"{insurer}: {invalid_policy_records} records have null/empty policy numbers (will be counted as unbooked)")
                    
                    # Debug: Show details of invalid policy records
                    logger.info(f"=== DEBUG: Invalid Policy Records for {insurer} ===")
                    for idx, row in invalid_policy_records_df.head(10).iterrows():  # Show first 10 invalid records
                        original_policy = row.get(mis_policy_col, 'N/A')
                        cleaned_policy = row.get('cleaned_policy', 'N/A')
                        logger.info(f"Row {idx}: Original='{original_policy}', Cleaned='{cleaned_policy}', Type={type(original_policy)}")
                    
                    if len(invalid_policy_records_df) > 10:
                        logger.info(f"... and {len(invalid_policy_records_df) - 10} more invalid records")
                    
                    # Show unique values of invalid policy numbers
                    unique_invalid = invalid_policy_records_df[mis_policy_col].value_counts().head(10)
                    logger.info(f"Top invalid policy values: {dict(unique_invalid)}")
                    logger.info(f"=== END DEBUG ===")
                else:
                    logger.info(f"{insurer}: All {len(mis_insurer_df)} records have valid policy numbers")

                # Create policy sets (only from valid policy numbers for matching)
                internal_policies = set(internal_insurer_df['cleaned_policy'].dropna())
                
                # Debug: Track where policies are lost
                logger.info(f"=== DEBUGGING POLICY LOSS FOR {insurer} ===")
                logger.info(f"Step 1 - Raw MIS records: {len(mis_insurer_df)}")
                logger.info(f"Step 2 - Valid policy records: {len(valid_policy_records)}")
                
                # Check for duplicates in cleaned policy numbers
                all_cleaned_policies = valid_policy_records['cleaned_policy'].dropna()
                unique_cleaned_policies = all_cleaned_policies.drop_duplicates()
                
                logger.info(f"Step 3 - All cleaned policies: {len(all_cleaned_policies)}")
                logger.info(f"Step 4 - Unique cleaned policies: {len(unique_cleaned_policies)}")
                
                if len(all_cleaned_policies) != len(unique_cleaned_policies):
                    duplicates_count = len(all_cleaned_policies) - len(unique_cleaned_policies)
                    logger.warning(f"Found {duplicates_count} duplicate policy numbers in MIS data!")
                    
                    # Show ALL duplicate policies with their counts
                    duplicate_policies_series = all_cleaned_policies.value_counts()
                    duplicate_policies_only = duplicate_policies_series[duplicate_policies_series > 1]
                    
                    logger.info(f"=== ALL DUPLICATE POLICIES IN {insurer} MIS ===")
                    for policy, count in duplicate_policies_only.items():
                        logger.info(f"Policy '{policy}': appears {count} times")
                    logger.info(f"=== END DUPLICATE POLICIES LIST ===")
                    
                    # Show sample duplicate examples
                    duplicate_policies = all_cleaned_policies[all_cleaned_policies.duplicated()].head(10)
                    logger.info(f"Sample duplicate policies: {list(duplicate_policies)}")
                
                mis_policies = set(unique_cleaned_policies)
                
                # Total MIS count includes ALL records (even those with invalid policy numbers)
                total_mis_count = len(mis_insurer_df)
                
                logger.info(f"Step 5 - Final unique MIS policies set: {len(mis_policies)}")
                logger.info(f"=== END DEBUGGING ===")
                logger.info(f"MIS breakdown for {insurer}: Total={total_mis_count}, Valid policies={len(mis_policies)}, Invalid policies={invalid_policy_records}")

                # --- Step 4: Apply CORRECT 3-Bucket Logic (MIS as Source of Truth) ---
                
                # Start with MIS as the universe of policies
                logger.info(f"Processing {insurer}: MIS policies = {len(mis_policies)}, Internal policies = {len(internal_policies)}")
                
                # STEP 1: BOOKED = MIS policies that exist in Internal Database
                logger.info(f"=== BOOKED CALCULATION DEBUG FOR {insurer} ===")
                logger.info(f"MIS policies (unique): {len(mis_policies)}")
                logger.info(f"Internal policies (unique): {len(internal_policies)}")
                
                final_booked_policies = mis_policies.intersection(internal_policies)
                logger.info(f"Intersection (booked): {len(final_booked_policies)}")
                logger.info(f"Calculation: MIS ∩ Internal = {len(mis_policies)} ∩ {len(internal_policies)} = {len(final_booked_policies)}")
                
                # Show some sample booked policies
                if len(final_booked_policies) > 0:
                    sample_booked = list(final_booked_policies)[:10]
                    logger.info(f"Sample booked policies: {sample_booked}")
                
                # VALIDATION: Booked cannot exceed MIS policies
                if len(final_booked_policies) > len(mis_policies):
                    logger.error(f"ERROR: Booked policies ({len(final_booked_policies)}) cannot exceed MIS policies ({len(mis_policies)})")
                    logger.error(f"This indicates a logic error in the intersection calculation")
                    # Force correct the booked count
                    final_booked_policies = final_booked_policies.intersection(mis_policies)
                    logger.info(f"Corrected booked count: {len(final_booked_policies)}")
                
                logger.info(f"=== END BOOKED CALCULATION DEBUG ===")
                
                # STEP 2: PENDING = MIS policies that exist in Offline Punching with pending status
                logger.info(f"=== PENDING CALCULATION DEBUG FOR {insurer} ===")
                
                final_pending_policies = set()
                offline_policies_checked = 0
                offline_policies_in_mis = 0
                offline_policies_with_pending_status = 0
                
                if not offline_insurer_df.empty:
                    logger.info(f"Offline records for {insurer}: {len(offline_insurer_df)}")
                    
                    for _, row in offline_insurer_df.iterrows():
                        policy = row['cleaned_policy']
                        status = str(row[offline_status_col]).lower().strip()
                        offline_policies_checked += 1
                        
                        # Check if policy exists in MIS
                        if policy in mis_policies:
                            offline_policies_in_mis += 1
                            
                            # Check if status is pending
                            if status in ['new', 'ticket closed duplicate']:
                                offline_policies_with_pending_status += 1
                                final_pending_policies.add(policy)
                                logger.debug(f"Added to pending: {policy} (status: {status})")
                
                logger.info(f"Offline policies checked: {offline_policies_checked}")
                logger.info(f"Offline policies found in MIS: {offline_policies_in_mis}")
                logger.info(f"Offline policies with pending status: {offline_policies_with_pending_status}")
                logger.info(f"Final pending policies: {len(final_pending_policies)}")
                
                # Show some sample pending policies
                if len(final_pending_policies) > 0:
                    sample_pending = list(final_pending_policies)[:10]
                    logger.info(f"Sample pending policies: {sample_pending}")
                else:
                    logger.info(f"No offline records for {insurer}")
                
                logger.info(f"=== END PENDING CALCULATION DEBUG ===")
                
                # STEP 3: UNBOOKED = Remaining MIS policies (not booked, not pending) + invalid policy records
                final_unbooked_policies = mis_policies - final_booked_policies - final_pending_policies
                
                logger.info(f"=== UNBOOKED CALCULATION DEBUG FOR {insurer} ===")
                logger.info(f"Starting MIS policies (unique): {len(mis_policies)}")
                logger.info(f"Minus Booked policies: {len(final_booked_policies)}")
                logger.info(f"Minus Pending policies: {len(final_pending_policies)}")
                logger.info(f"Equals Unbooked policies: {len(final_unbooked_policies)}")
                logger.info(f"Calculation: {len(mis_policies)} - {len(final_booked_policies)} - {len(final_pending_policies)} = {len(final_unbooked_policies)}")
                
                # Show some sample unbooked policies
                if len(final_unbooked_policies) > 0:
                    sample_unbooked = list(final_unbooked_policies)[:10]
                    logger.info(f"Sample unbooked policies: {sample_unbooked}")
                
                # Verify no overlap between categories
                booked_pending_overlap = final_booked_policies.intersection(final_pending_policies)
                booked_unbooked_overlap = final_booked_policies.intersection(final_unbooked_policies)
                pending_unbooked_overlap = final_pending_policies.intersection(final_unbooked_policies)
                
                if booked_pending_overlap:
                    logger.warning(f"OVERLAP WARNING: {len(booked_pending_overlap)} policies in both Booked and Pending")
                if booked_unbooked_overlap:
                    logger.warning(f"OVERLAP WARNING: {len(booked_unbooked_overlap)} policies in both Booked and Unbooked")
                if pending_unbooked_overlap:
                    logger.warning(f"OVERLAP WARNING: {len(pending_unbooked_overlap)} policies in both Pending and Unbooked")
                
                logger.info(f"=== END UNBOOKED CALCULATION DEBUG ===")
                
                # Use the correct unique count (after removing duplicates)
                unbooked_count = len(final_unbooked_policies)
                unique_total = len(final_booked_policies) + len(final_pending_policies) + unbooked_count
                
                logger.info(f"{insurer} breakdown: Booked={len(final_booked_policies)}, Pending={len(final_pending_policies)}, Unbooked={unbooked_count}, Total={unique_total}")
                logger.info(f"Note: {total_mis_count - unique_total} duplicate records were removed from MIS data")

                # --- Step 5: Calculate NOP and Premiums ---
                
                # CORRECT: Count unique policies only, not total records
                booked_nop = len(final_booked_policies)  # This is already unique count from set
                pending_nop = len(final_pending_policies)  # This is already unique count from set
                
                logger.info(f"=== VERIFICATION FOR {insurer} ===")
                logger.info(f"Unique MIS policies available: {len(mis_policies)}")
                logger.info(f"Booked (unique): {booked_nop}")
                logger.info(f"Pending (unique): {pending_nop}")
                logger.info(f"Unbooked (unique): {len(final_unbooked_policies)}")
                logger.info(f"Sum of unique categories: {booked_nop + pending_nop + len(final_unbooked_policies)}")
                logger.info(f"=== END VERIFICATION ===")
                
                # Premium calculations
                booked_df = internal_insurer_df[internal_insurer_df['cleaned_policy'].isin(final_booked_policies)]
                booked_premium = pd.to_numeric(booked_df[internal_premium_col], errors='coerce').sum()

                pending_df = offline_insurer_df[offline_insurer_df['cleaned_policy'].isin(final_pending_policies)] if not offline_insurer_df.empty else pd.DataFrame()
                pending_premium = pd.to_numeric(pending_df[internal_premium_col], errors='coerce').sum() if not pending_df.empty else 0

                # Unbooked calculations (unique policies only)
                unbooked_nop = unbooked_count
                unbooked_premium = 0
                
                # Premium from MIS for unbooked policies
                mis_premium_col = self.comparator.resolved_mappings.get(insurer, self.comparator.column_mappings.get(insurer, {})).get('total_premium')
                if mis_premium_col and mis_premium_col in mis_insurer_df.columns:
                    unbooked_valid_df = valid_policy_records[valid_policy_records['cleaned_policy'].isin(final_unbooked_policies)]
                    unbooked_premium = pd.to_numeric(unbooked_valid_df[mis_premium_col], errors='coerce').sum()

                # Ensure total is exactly the sum of the three categories
                total_nop = booked_nop + pending_nop + unbooked_nop
                total_premium = booked_premium + pending_premium + unbooked_premium
                
                # Log the calculation for verification
                logger.info(f"=== FINAL TOTALS FOR {insurer} ===")
                logger.info(f"Booked NOP: {booked_nop}")
                logger.info(f"Pending NOP: {pending_nop}")
                logger.info(f"Unbooked NOP: {unbooked_nop}")
                logger.info(f"Total NOP: {total_nop} (should equal {booked_nop} + {pending_nop} + {unbooked_nop})")
                logger.info(f"Verification: {booked_nop + pending_nop + unbooked_nop} = {total_nop}")

                summary_data.append({
                    "Insurer": insurer,
                    "Booked NOP": int(booked_nop),  # Ensure integer
                    "Booked Premium": float(booked_premium),
                    "Unbooked NOP": int(unbooked_nop),  # Ensure integer
                    "Unbooked Premium": float(unbooked_premium),
                    "Pending NOP": int(pending_nop),  # Ensure integer
                    "Pending Premium": float(pending_premium),
                    "Total NOP": int(total_nop),  # Ensure integer
                    "Total Premium": float(total_premium),
                    "Booked Premium (Cr)": float(booked_premium / 1_00_00_000),
                    "Unbooked Premium (Cr)": float(unbooked_premium / 1_00_00_000),
                    "Pending Premium (Cr)": float(pending_premium / 1_00_00_000),
                    "Total Premium (Cr)": float(total_premium / 1_00_00_000)
                })

                # Store detailed metrics for the report
                detailed_metrics = {
                    'insurer': insurer,
                    'raw_mis_count': total_mis_count,
                    'duplicate_count': total_mis_count - len(mis_policies),
                    'unique_mis_count': len(mis_policies),
                    'filtered_out_count': 0,  # Filters not applied in insurer-wise report
                    'internal_count': len(internal_policies),
                    'offline_count': len(offline_insurer_df) if not offline_insurer_df.empty else 0,
                    'booked_count': len(final_booked_policies),
                    'pending_count': len(final_pending_policies),
                    'unbooked_count': len(final_unbooked_policies),
                    'duplicate_policies': list(all_cleaned_policies[all_cleaned_policies.duplicated()].unique()) if len(all_cleaned_policies) != len(unique_cleaned_policies) else [],
                    'filter_descriptions': []  # No filters applied in insurer-wise report
                }
                
                # Add detailed metrics to summary data
                summary_data[-1]['detailed_metrics'] = detailed_metrics
                
                logger.info(f"Processed {insurer}: Booked={booked_nop}, Unbooked={unbooked_nop}, Pending={pending_nop}, Total={total_nop}")

            # Generate detailed calculation report
            self.generate_calculation_report(summary_data)
            
            summary_df = pd.DataFrame(summary_data)
            self.switch_to_insurer_report(summary_df)

        except Exception as ex:
            logger.error(f"Error during full report generation: {traceback.format_exc()}")
            self.show_error(f"Error generating report: {ex}")
        finally:
            self.progress_ring.visible = False
            self.progress_bar.visible = False
            self.progress_text.visible = False
            self.page.update()
    
    def generate_calculation_report(self, summary_data):
        """Generate a detailed calculation report with all numbers and formulas"""
        try:
            from datetime import datetime
            import os
            
            # Create report filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"ID_Recon_Calculation_Report_{timestamp}.txt"
            
            # Get the directory where the script is running
            script_dir = os.path.dirname(os.path.abspath(__file__))
            report_path = os.path.join(script_dir, report_filename)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("ID RECON - DETAILED CALCULATION REPORT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Insurers Processed: {len(summary_data)}\n")
                f.write("=" * 80 + "\n\n")
                
                # Overall summary
                total_booked = sum(item['Booked NOP'] for item in summary_data)
                total_unbooked = sum(item['Unbooked NOP'] for item in summary_data)
                total_pending = sum(item['Pending NOP'] for item in summary_data)
                total_policies = sum(item['Total NOP'] for item in summary_data)
                total_premium = sum(item['Total Premium'] for item in summary_data)
                
                f.write("OVERALL SUMMARY\n")
                f.write("-" * 40 + "\n")
                f.write(f"Total Booked Policies: {total_booked:,}\n")
                f.write(f"Total Unbooked Policies: {total_unbooked:,}\n")
                f.write(f"Total Pending Policies: {total_pending:,}\n")
                f.write(f"Grand Total Policies: {total_policies:,}\n")
                f.write(f"Grand Total Premium: ₹{total_premium:,.2f}\n")
                f.write(f"Overall Booking Rate: {(total_booked/total_policies*100):.2f}%\n")
                f.write("\n" + "=" * 80 + "\n\n")
                
                # Detailed breakdown for each insurer
                for item in summary_data:
                    insurer = item['Insurer']
                    metrics = item.get('detailed_metrics', {})
                    
                    f.write(f"INSURER: {insurer}\n")
                    f.write("=" * 50 + "\n")
                    
                    # Data Processing Pipeline
                    f.write("DATA PROCESSING PIPELINE:\n")
                    f.write(f"  1. Raw MIS Records: {metrics.get('raw_mis_count', 'N/A'):,}\n")
                    f.write(f"  2. Duplicate Records Found: {metrics.get('duplicate_count', 0):,}\n")
                    f.write(f"  3. Unique MIS Records: {metrics.get('unique_mis_count', 'N/A'):,}\n")
                    f.write(f"  4. Final MIS Universe: {item['Total NOP']:,}\n")
                    f.write(f"  Pipeline: {metrics.get('raw_mis_count', 0):,} → {metrics.get('unique_mis_count', 0):,} → {item['Total NOP']:,}\n")
                    f.write(f"  Note: Insurer-specific filters are applied during individual comparisons, not in this report\n")
                    
                    # Data Sources
                    f.write("\nDATA SOURCES:\n")
                    f.write(f"  Internal Database Records: {metrics.get('internal_count', 'N/A'):,}\n")
                    f.write(f"  MIS Records (after processing): {item['Total NOP']:,}\n")
                    f.write(f"  Offline Punching Records: {metrics.get('offline_count', 'N/A'):,}\n")
                    
                    # Final Policy Counts
                    f.write("\nFINAL POLICY CATEGORIZATION:\n")
                    f.write(f"  Booked NOP: {item['Booked NOP']:,}\n")
                    f.write(f"  Unbooked NOP: {item['Unbooked NOP']:,}\n")
                    f.write(f"  Pending NOP: {item['Pending NOP']:,}\n")
                    f.write(f"  Total NOP: {item['Total NOP']:,}\n")
                    f.write(f"  Verification: {item['Booked NOP']} + {item['Unbooked NOP']} + {item['Pending NOP']} = {item['Booked NOP'] + item['Unbooked NOP'] + item['Pending NOP']}\n")
                    
                    # Data Quality Details
                    if metrics.get('duplicate_count', 0) > 0:
                        f.write(f"\nDATA QUALITY - DUPLICATES:\n")
                        f.write(f"  Total Duplicate Records: {metrics.get('duplicate_count', 0):,}\n")
                        f.write(f"  Duplicate Rate: {(metrics.get('duplicate_count', 0) / metrics.get('raw_mis_count', 1) * 100):.2f}%\n")
                        
                        duplicate_policies = metrics.get('duplicate_policies', [])
                        if duplicate_policies:
                            f.write(f"  Sample Duplicate Policies: {', '.join(duplicate_policies[:10])}\n")
                            if len(duplicate_policies) > 10:
                                f.write(f"  ... and {len(duplicate_policies) - 10} more duplicate policies\n")
                    
                    # Note about filters
                    f.write(f"\nFILTER INFORMATION:\n")
                    f.write(f"  Note: Insurer-specific filters (endorsements, cancellations, non-Motor) are applied\n")
                    f.write(f"  during individual policy comparisons, not during this insurer-wise report generation.\n")
                    f.write(f"  The numbers shown here represent the raw MIS data after duplicate removal only.\n")
                    
                    # Premium amounts
                    f.write("\nPREMIUM AMOUNTS:\n")
                    f.write(f"  Booked Premium: ₹{item['Booked Premium']:,.2f} ({item['Booked Premium (Cr)']:.2f} Cr)\n")
                    f.write(f"  Unbooked Premium: ₹{item['Unbooked Premium']:,.2f} ({item['Unbooked Premium (Cr)']:.2f} Cr)\n")
                    f.write(f"  Pending Premium: ₹{item['Pending Premium']:,.2f} ({item['Pending Premium (Cr)']:.2f} Cr)\n")
                    f.write(f"  Total Premium: ₹{item['Total Premium']:,.2f} ({item['Total Premium (Cr)']:.2f} Cr)\n")
                    
                    # Calculations and percentages
                    booking_rate = (item['Booked NOP'] / item['Total NOP'] * 100) if item['Total NOP'] > 0 else 0
                    pending_rate = (item['Pending NOP'] / item['Total NOP'] * 100) if item['Total NOP'] > 0 else 0
                    unbooked_rate = (item['Unbooked NOP'] / item['Total NOP'] * 100) if item['Total NOP'] > 0 else 0
                    
                    f.write("\nPERFORMANCE METRICS:\n")
                    f.write(f"  Booking Rate: {booking_rate:.2f}% ({item['Booked NOP']:,} / {item['Total NOP']:,})\n")
                    f.write(f"  Pending Rate: {pending_rate:.2f}% ({item['Pending NOP']:,} / {item['Total NOP']:,})\n")
                    f.write(f"  Unbooked Rate: {unbooked_rate:.2f}% ({item['Unbooked NOP']:,} / {item['Total NOP']:,})\n")
                    
                    # Premium per policy
                    avg_booked_premium = item['Booked Premium'] / item['Booked NOP'] if item['Booked NOP'] > 0 else 0
                    avg_unbooked_premium = item['Unbooked Premium'] / item['Unbooked NOP'] if item['Unbooked NOP'] > 0 else 0
                    avg_pending_premium = item['Pending Premium'] / item['Pending NOP'] if item['Pending NOP'] > 0 else 0
                    avg_total_premium = item['Total Premium'] / item['Total NOP'] if item['Total NOP'] > 0 else 0
                    
                    f.write("\nAVERAGE PREMIUM PER POLICY:\n")
                    f.write(f"  Booked: ₹{avg_booked_premium:,.2f}\n")
                    f.write(f"  Unbooked: ₹{avg_unbooked_premium:,.2f}\n")
                    f.write(f"  Pending: ₹{avg_pending_premium:,.2f}\n")
                    f.write(f"  Overall: ₹{avg_total_premium:,.2f}\n")
                    
                    f.write("\n" + "-" * 50 + "\n\n")
                
                # Overall data processing summary
                total_raw_records = sum(item.get('detailed_metrics', {}).get('raw_mis_count', 0) for item in summary_data)
                total_duplicates = sum(item.get('detailed_metrics', {}).get('duplicate_count', 0) for item in summary_data)
                f.write("OVERALL DATA PROCESSING SUMMARY\n")
                f.write("=" * 50 + "\n")
                f.write(f"Total Raw MIS Records: {total_raw_records:,}\n")
                f.write(f"Total Duplicate Records Removed: {total_duplicates:,}\n")
                f.write(f"Final Unique Records Processed: {total_policies:,}\n")
                f.write(f"Duplicate Removal Rate: {(total_duplicates/total_raw_records*100):.2f}%\n")
                f.write(f"Data Retention Rate: {(total_policies/total_raw_records*100):.2f}%\n\n")
                
                # Data sources and methodology
                f.write("DATA SOURCES & METHODOLOGY\n")
                f.write("=" * 50 + "\n")
                f.write("1. INTERNAL DATABASE: Policies successfully booked in the system\n")
                f.write("2. MIS DATA: Official insurer data (source of truth for policy universe)\n")
                f.write("3. OFFLINE PUNCHING: Manually entered policies with status tracking\n\n")
                
                f.write("DATA PROCESSING PIPELINE:\n")
                f.write("Step 1: Load Raw MIS Data\n")
                f.write("Step 2: Remove Duplicate Policy Numbers\n")
                f.write("Step 3: Apply Insurer-Specific Filters (Remove Endorsements/Cancellations/Non-Motor)\n")
                f.write("Step 4: Create Final Policy Universe\n")
                f.write("Step 5: Categorize into Booked/Pending/Unbooked\n\n")
                
                f.write("CALCULATION METHODOLOGY:\n")
                f.write("- BOOKED = MIS policies ∩ Internal Database policies\n")
                f.write("- PENDING = MIS policies ∩ Offline policies (status: 'New' or 'Ticket closed duplicate')\n")
                f.write("- UNBOOKED = MIS policies - BOOKED - PENDING\n")
                f.write("- TOTAL = BOOKED + PENDING + UNBOOKED (equals processed MIS policies)\n\n")
                
                f.write("DATA QUALITY CONTROLS:\n")
                f.write("- Automatic duplicate removal using policy number matching\n")
                f.write("- Insurer-specific filters for endorsements, cancellations, and non-Motor policies\n")
                f.write("- Policy number standardization (remove spaces, convert to uppercase)\n")
                f.write("- Data validation and overlap detection between categories\n")
                f.write("- Mathematical verification of all calculations\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")
            
            logger.info(f"Detailed calculation report generated: {report_path}")
            self.show_success(f"Calculation report saved to: {report_filename}")
            
        except Exception as ex:
            logger.error(f"Error generating calculation report: {ex}")
            self.show_error(f"Failed to generate calculation report: {str(ex)}")

    def display_results(self, results_df, context_dfs):
        """Display comparison results with a neatly organized KPI grid and accurate metrics."""
        if results_df is None or results_df.empty:
            content = [ft.Text("No results found.", size=16, color=ft.Colors.GREY_600)]
            if results_df is not None:
                content.extend([
                    ft.Text("This could mean:", size=14),
                    ft.Text("• No matching records for this insurer", size=12),
                    ft.Text("• Company name mismatch in internal data", size=12),
                    ft.Text("• Date filter excluded all records", size=12),
                    ft.Text("• All records were filtered out by insurer-specific rules", size=12)
                ])
            self.results_container.content = ft.Column(content)
            self.page.update()
            return

        filter_info_df = results_df[results_df['Policy Number'] == 'FILTER_INFO']
        filter_details = filter_info_df.iloc[0]['Details'] if not filter_info_df.empty else None
        results_df = results_df[results_df['Policy Number'] != 'FILTER_INFO'].copy()

        # --- Accurate Metric Calculations ---
        results_df['Policy Number'] = results_df['Policy Number'].astype(str)

        compared_policies_df = results_df[results_df['Match Status'].isin(['Match', 'Mismatch'])]
        policies_compared = compared_policies_df['Policy Number'].unique()
        total_policies_compared = len(policies_compared)

        # Only count policies with 3 or more field mismatches
        mismatch_df = compared_policies_df[compared_policies_df['Match Status'] == 'Mismatch']
        policy_mismatch_counts = mismatch_df.groupby('Policy Number').size()
        policies_with_3plus_mismatches = policy_mismatch_counts[policy_mismatch_counts >= 3].index
        mismatches_count = len(policies_with_3plus_mismatches)
        
        logger.info(f"Mismatch analysis: Total policies with any mismatch: {len(mismatch_df['Policy Number'].unique())}")
        logger.info(f"Policies with 3+ field mismatches: {mismatches_count}")
        
        # Show breakdown of mismatch counts
        mismatch_breakdown = policy_mismatch_counts.value_counts().sort_index()
        logger.info(f"Mismatch breakdown: {dict(mismatch_breakdown)}")

        matched_policies_set = set(policies_compared) - set(policies_with_3plus_mismatches)
        matches_count = len(matched_policies_set)

        match_rate = (matches_count / total_policies_compared * 100) if total_policies_compared > 0 else 0

        # Fix the logic: "Not Found in Internal" means MIS policies not found in internal DB
        not_found_in_internal_count = len(results_df[results_df['Match Status'] == 'Not Found in Internal']['Policy Number'].unique())
        # "Not Found in MIS" means Internal policies not found in MIS
        not_found_in_mis_count = len(results_df[results_df['Match Status'] == 'Not Found in MIS']['Policy Number'].unique())

        internal_df = context_dfs.get("internal_df")
        booked_premium_in_mis = 0
        if internal_df is not None:
            booked_and_found_df = internal_df[internal_df['Policy Number'].isin(policies_compared)]
            premium_col = self.comparator.internal_columns['total_premium']
            if premium_col in booked_and_found_df.columns:
                booked_premium_in_mis = pd.to_numeric(booked_and_found_df[premium_col], errors='coerce').sum()

        # Calculate unbooked premium correctly: only from MIS policies not found in internal database
        try:
            if 'MIS Premium' in results_df.columns:
                unbooked_premium_from_mis = results_df[results_df['Match Status'] == 'Not Found in Internal']['MIS Premium'].sum()
            else:
                # Fallback: calculate from context_dfs if MIS Premium column doesn't exist
                unbooked_premium_from_mis = 0
                logger.warning("MIS Premium column not found in results, using 0 for unbooked premium calculation")
        except Exception as premium_error:
            logger.error(f"Error calculating unbooked premium: {premium_error}")
            unbooked_premium_from_mis = 0

        if total_policies_compared == 0 and (not_found_in_internal_count > 0 or not_found_in_mis_count > 0):
            self.show_policy_mismatch_debug()
        
        # Get endorsement/cancellation count and found in internal count from context
        # endorsement_cancellation_count is computed as (total MIS records - unique policies)
        # which aligns with the business definition: repeats beyond the base policy are
        # endorsements or cancellations.
        endorsement_cancellation_count = context_dfs.get("endorsement_cancellation_count", 0)
        explicit_cancellation_count = context_dfs.get("explicit_cancellation_count", 0)
        found_in_internal_count = context_dfs.get("found_in_internal_count", 0)

        # KPI logic: show duplicates as Endorsements count, since insurer-specific
        # filter rules already strip base policies and we may not always have a clean
        # split between endorsements vs cancellations at this stage.
        endorsement_count = endorsement_cancellation_count
        # Show explicit cancellation count based on insurer-specific rules
        cancellation_count = explicit_cancellation_count
        
        # --- UI Components ---
        kpi_grid = ft.GridView(
            expand=False, runs_count=4, max_extent=190,
            child_aspect_ratio=2.1, spacing=10, run_spacing=10,
            controls=[
                self.create_metric_card("Endorsements", self.format_number(endorsement_count), ft.Colors.BLUE_700),
                self.create_metric_card("Cancellations", self.format_number(cancellation_count), ft.Colors.RED_700),
                self.create_metric_card("Mismatched Policies", self.format_number(mismatches_count), ft.Colors.ORANGE_700),
                self.create_metric_card("Unbooked Policies", self.format_number(not_found_in_internal_count), ft.Colors.RED_700),
                self.create_metric_card("Not Found in MIS", self.format_number(not_found_in_mis_count), ft.Colors.PURPLE_700),
                self.create_metric_card("Booked Premium", self.format_currency(booked_premium_in_mis), ft.Colors.TEAL_700),
                self.create_metric_card("Unbooked Premium", self.format_currency(unbooked_premium_from_mis), ft.Colors.BROWN_700),
                self.create_metric_card("Total Policies Compared", self.format_number(total_policies_compared), ft.Colors.BLUE_700),
            ]
        )
        
        field_analysis = results_df.groupby('Field').agg(
            Matches=('Match Status', lambda x: (x == 'Match').sum()),
            Total=('Policy Number', 'count'),
            Avg_Match_Score=('Match Score', 'mean')
        ).rename(columns={'Avg_Match_Score': 'Avg Match Score'})
        field_analysis['Match Rate %'] = (field_analysis['Matches'] / field_analysis['Total'] * 100).round(1)
        
        field_table = ft.DataTable(
            columns=[ft.DataColumn(ft.Text("Field")), ft.DataColumn(ft.Text("Match Rate %"), numeric=True), ft.DataColumn(ft.Text("Avg Score"), numeric=True), ft.DataColumn(ft.Text("Total Checks"), numeric=True)],
            rows=[ft.DataRow(cells=[ft.DataCell(ft.Text(field)), ft.DataCell(ft.Text(f"{row['Match Rate %']:.1f}%")), ft.DataCell(ft.Text(f"{row['Avg Match Score']:.1f}")), ft.DataCell(ft.Text(f"{row['Total']:,}"))]) for field, row in field_analysis.iterrows()],
            border_radius=10, width=800
        )
        
        export_buttons = ft.Row([
            ft.ElevatedButton("Export All Results", icon=ft.Icons.DOWNLOAD, on_click=lambda _: self.simple_export(results_df, "all")),
            ft.ElevatedButton("Export Mismatches", icon=ft.Icons.WARNING, on_click=lambda _: self.simple_export(results_df[results_df['Match Status'].isin(['Mismatch', 'Not Found in Internal', 'Not Found in MIS'])], "mismatches"), bgcolor=ft.Colors.ORANGE_700, color=ft.Colors.WHITE),
            ft.ElevatedButton("Export Endorsements/Cancellations", icon=ft.Icons.EDIT_DOCUMENT, on_click=lambda e: self.handle_export_endorsements_click(e, results_df), bgcolor=ft.Colors.PURPLE_700, color=ft.Colors.WHITE),
            ft.ElevatedButton("View Dashboard", icon=ft.Icons.DASHBOARD, on_click=lambda _: self.switch_to_dashboard(), bgcolor=ft.Colors.INDIGO_700, color=ft.Colors.WHITE),
            ft.ElevatedButton("Insurer-wise Report", icon=ft.Icons.ASSESSMENT, on_click=self.run_full_report, bgcolor=ft.Colors.BLUE_GREY_700, color=ft.Colors.WHITE),
        ], spacing=10, scroll=ft.ScrollMode.AUTO)
        
        # Debug: Print button click handler
        print("DEBUG: Export Endorsements/Cancellations button created with on_click handler")

        content_controls = [ft.Text("Comparison Results", size=24, weight=ft.FontWeight.BOLD), ft.Divider()]
        if filter_details:
            content_controls.append(ft.Container(content=ft.Row([ft.Icon(ft.Icons.FILTER_ALT, color=ft.Colors.BLUE_700), ft.Text(f"Filter Applied: {filter_details}", color=ft.Colors.BLUE_700)]), padding=10, border_radius=5, bgcolor=ft.Colors.BLUE_50))
        
        content_controls.extend([
            kpi_grid,
            ft.Divider(),
        ])
        
        # Add endorsement and cancellation summary if any exist
        if endorsement_count > 0 or cancellation_count > 0:
            summary_content = []
            if endorsement_count > 0:
                summary_content.append(ft.Text(f"📋 Endorsements: {endorsement_count} records found", size=14, color=ft.Colors.BLUE_700, weight=ft.FontWeight.W_500))
            if cancellation_count > 0:
                summary_content.append(ft.Text(f"❌ Cancellations: {cancellation_count} records found", size=14, color=ft.Colors.RED_700, weight=ft.FontWeight.W_500))
            
            if summary_content:
                content_controls.append(ft.Container(
                    content=ft.Column(summary_content, spacing=5),
                    padding=15,
                    border_radius=10,
                    bgcolor=ft.Colors.GREY_50,
                    border=ft.border.all(1, ft.Colors.GREY_300)
                ))
                content_controls.append(ft.Divider())
        
        content_controls.extend([
            ft.Text("Field-wise Analysis", size=18, weight=ft.FontWeight.W_500),
            ft.Container(content=field_table, border_radius=10, padding=10, bgcolor=ft.Colors.GREY_50),
            ft.Divider(),  
            export_buttons
        ])
        self.results_container.content = ft.Column(content_controls, spacing=15, scroll=ft.ScrollMode.AUTO, horizontal_alignment=ft.CrossAxisAlignment.START)
        self.page.update()

    def create_metric_card(self, title, value, color):
        """Create a metric card that adapts to its container size."""
        return ft.Container(
            content=ft.Column(
                [
                    ft.Text(title, size=12, color=ft.Colors.GREY_600, text_align=ft.TextAlign.CENTER, weight=ft.FontWeight.W_500),
                    ft.Text(
                        value, 
                        size=16, 
                        weight=ft.FontWeight.BOLD, 
                        color=color, 
                        text_align=ft.TextAlign.CENTER,
                        overflow=ft.TextOverflow.ELLIPSIS
                    )
                ], 
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8
            ),
            padding=ft.padding.all(12), 
            border_radius=8, 
            bgcolor=ft.Colors.WHITE,
            border=ft.border.all(1, color),
            width=180,
            height=85
        )
    
    def format_number(self, num):
        """Format large numbers compactly"""
        if num >= 1_000_000:
            return f"{num/1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num/1_000:.1f}K"
        else:
            return f"{num:,}"
    
    def format_currency(self, amount):
        """Format currency amounts compactly"""
        if amount >= 10_000_000:  # 1 crore
            return f"₹{amount/10_000_000:.1f}Cr"
        elif amount >= 100_000:  # 1 lakh
            return f"₹{amount/100_000:.1f}L"
        elif amount >= 1_000:
            return f"₹{amount/1_000:.1f}K"
        else:
            return f"₹{amount:,.0f}"
    
    def export_summary_report(self, summary_df):
        """Exports the insurer-wise summary report to Excel."""
        if summary_df is None or summary_df.empty:
            self.show_error("No summary data to export.")
            return
        
        self.df_to_export = summary_df.copy()
        
        # Use the save_file_picker to let the user choose a location
        self.save_file_picker.save_file(
            dialog_title="Save Insurer-wise Report",
            file_name=f"Insurer_Wise_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

    def simple_export(self, df, export_type="all"):
        """Triggers the save file dialog for detailed results."""
        if df is None or df.empty:
            self.show_error("No data to export")
            return
        
        self.df_to_export = df
        
        insurer_name = self.insurer_dropdown.value or "Unknown"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{insurer_name}_{export_type}_results_{timestamp}.csv"
        
        self.save_file_picker.save_file(
            dialog_title="Save Results As...",
            file_name=filename,
            allowed_extensions=["csv"]
        )

    def handle_save_file_result(self, e: ft.FilePickerResultEvent):
        """Saves the stored DataFrame to the path chosen by the user."""
        if not e.path:
            self.show_info("Export cancelled.")
            return

        if self.df_to_export is None:
            self.show_error("No data was available to export.")
            return

        try:
            # Differentiate between report (xlsx) and detailed results (csv)
            if e.path.endswith(".xlsx"):
                # Make column names more Excel-friendly
                df_to_save = self.df_to_export.copy()
                df_to_save.columns = [col.replace(' (Cr)', '_Cr') for col in df_to_save.columns]
                df_to_save.to_excel(e.path, index=False)
                self.show_success(f"Report exported successfully!\nSaved to:\n{e.path}")
                logger.info(f"Summary export successful: {e.path}")
            else:
                self.df_to_export.to_csv(e.path, index=False, encoding='utf-8-sig')
                self.show_success(f"File exported successfully!\nSaved to:\n{e.path}")
                logger.info(f"Detailed export successful: {e.path}")
        except Exception as ex:
            self.show_error(f"Export failed: {ex}")
            logger.error(f"Save file result failed: {traceback.format_exc()}")
        finally:
            self.df_to_export = None

    def export_policy_wise_mismatch(self, df):
        """Export data in policy-wise format with internal and MIS values side by side."""
        try:
            if df is None or df.empty:
                self.show_error("No data to export")
                return
            mismatch_df = df[df['Match Status'].isin(['Mismatch', 'Not Found in Internal', 'Not Found in MIS'])].copy()
            if mismatch_df.empty:
                self.show_info("No mismatches or 'Not Found' records to export.")
                return
            
            pivot_df = mismatch_df.pivot_table(
                index=['Policy Number', 'Request Id'],
                columns='Field',
                values=['Internal Value', 'MIS Value'],
                aggfunc='first'
            )
            
            pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
            pivot_df.reset_index(inplace=True)
            self.simple_export(pivot_df, "policy_wise_mismatch")
        except Exception as ex:
            self.show_error(f"Policy-wise export failed: {ex}")
            logger.error(f"Policy-wise export failed: {traceback.format_exc()}")

    def handle_export_endorsements_click(self, e, df):
        """Handle the export endorsements/cancellations button click"""
        print("DEBUG: Export Endorsements/Cancellations button clicked!")
        self.show_info("Processing export request...")
        self.export_endorsements_cancellations(df)

    def export_endorsements_cancellations(self, df):
        """Export endorsements and cancellations with numbering information"""
        try:
            print("DEBUG: export_endorsements_cancellations method called!")
            logger.info(f"Starting endorsement/cancellation export with DataFrame shape: {df.shape if df is not None else 'None'}")
            
            if df is None or df.empty:
                self.show_error("No data to export")
                return
            
            # Check if required columns exist
            required_columns = ['Match Status', 'Policy Number', 'Request Id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.show_error(f"Missing required columns: {', '.join(missing_columns)}")
                logger.error(f"Missing required columns: {missing_columns}")
                return
                
            # Filter for endorsements and cancellations
            endorsement_df = df[df['Match Status'] == 'Endorsement'].copy()
            cancellation_df = df[df['Match Status'] == 'Cancellation'].copy()
            
            logger.info(f"Found {len(endorsement_df)} endorsements and {len(cancellation_df)} cancellations")
            
            if endorsement_df.empty and cancellation_df.empty:
                self.show_info("No endorsements or cancellations found to export.")
                logger.info("No endorsements or cancellations found to export")
                return
            
            # Create combined export
            export_data = []
            
            # Add endorsements
            for _, row in endorsement_df.iterrows():
                try:
                    export_data.append({
                        'Record_Type': 'Endorsement',
                        'Policy_Number': str(row.get('Policy Number', 'N/A')),
                        'Request_Id': str(row.get('Request Id', 'N/A')),
                        'Endorsement_Number': str(row.get('Endorsement_Number', 'N/A')),
                        'Endorsement_Sequence': str(row.get('Endorsement_Sequence', 'N/A')),
                        'Description': str(row.get('Details', '')),
                        'Insurer': str(row.get('Insurer', 'N/A')),
                        'Processing_Date': str(row.get('Processing_Date', 'N/A'))
                    })
                except Exception as row_error:
                    logger.error(f"Error processing endorsement row: {row_error}")
                    continue
            
            # Add cancellations
            for _, row in cancellation_df.iterrows():
                try:
                    export_data.append({
                        'Record_Type': 'Cancellation',
                        'Policy_Number': str(row.get('Policy Number', 'N/A')),
                        'Request_Id': str(row.get('Request Id', 'N/A')),
                        'Cancellation_Number': str(row.get('Cancellation_Number', 'N/A')),
                        'Cancellation_Sequence': str(row.get('Cancellation_Sequence', 'N/A')),
                        'Description': str(row.get('Details', '')),
                        'Insurer': str(row.get('Insurer', 'N/A')),
                        'Processing_Date': str(row.get('Processing_Date', 'N/A'))
                    })
                except Exception as row_error:
                    logger.error(f"Error processing cancellation row: {row_error}")
                    continue
            
            if export_data:
                export_df = pd.DataFrame(export_data)
                logger.info(f"Created export DataFrame with {len(export_data)} records")
                
                # Store the data for export and trigger save file dialog
                self.df_to_export = export_df
                logger.info("Stored DataFrame in df_to_export")
                
                # Generate filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"Endorsements_Cancellations_{timestamp}.csv"
                logger.info(f"Generated filename: {filename}")
                
                # Trigger the save file dialog
                logger.info("Triggering save file picker...")
                self.save_file_picker.save_file(
                    dialog_title="Save Endorsements and Cancellations",
                    file_name=filename,
                    allowed_extensions=["csv"]
                )
                
                self.show_success(f"Prepared {len(export_data)} endorsement/cancellation records for export")
                logger.info("Save file dialog triggered successfully")
            else:
                self.show_info("No data to export")
                logger.warning("No export data created")
                
        except Exception as ex:
            error_msg = f"Export failed: {str(ex)}"
            self.show_error(error_msg)
            logger.error(f"Export endorsements/cancellations failed: {traceback.format_exc()}")
            print(f"DEBUG: Export error: {ex}")
                
            
    def show_policy_mismatch_debug(self):
        """Show debug information when all policies are 'Not Found'"""
        try:
            insurer = self.insurer_dropdown.value
            internal_sample = self.internal_df.head(3)[['Policy Number', 'Insurance Company']].to_string(index=False)
            resolved_mapping = self.comparator.resolved_mappings.get(insurer, {})
            policy_col = resolved_mapping.get('policy_number')
            mis_sample = "Could not find resolved policy column."
            if policy_col and policy_col in self.mis_dfs[insurer].columns:
                mis_sample = self.mis_dfs[insurer].head(3)[[policy_col]].to_string(index=False)
            
            content = ft.Column([
                ft.Text("All policies were 'Not Found'. Possible causes:", size=16, weight=ft.FontWeight.BOLD, color=ft.Colors.RED_700),
                ft.Text("1. Policy Number mismatch (formatting, extra spaces, case)."),
                ft.Text("2. Wrong insurer selected or detected."),
                ft.Text("3. Date filter is too restrictive."),
                ft.Text("4. Insurer-specific filters removed all relevant MIS records."),
                ft.Divider(),
                ft.Text("INTERNAL DATA SAMPLE (Policy Number, Insurer):", weight=ft.FontWeight.W_500),
                ft.Container(content=ft.Text(internal_sample, selectable=True), padding=10, bgcolor=ft.Colors.GREY_100),
                ft.Divider(),
                ft.Text(f"MIS DATA SAMPLE (Resolved Policy Column: '{policy_col}'):", weight=ft.FontWeight.W_500),
                ft.Container(content=ft.Text(mis_sample, selectable=True), padding=10, bgcolor=ft.Colors.GREY_100),
            ], scroll=ft.ScrollMode.AUTO, spacing=10)
            
            dlg = ft.AlertDialog(
                title=ft.Text("Debug: All Policies Not Found"),
                content=ft.Container(content=content, width=600, height=500),
                actions=[ft.TextButton("Close", on_click=lambda _: self.close_dialog(dlg))]
            )
            self.page.dialog = dlg
            dlg.open = True
            self.page.update()
        except Exception as ex:
            logger.error(f"Error showing policy mismatch debug: {ex}")

    def close_dialog(self, dlg):
        """Close a dialog"""
        dlg.open = False
        self.page.update()

    def show_error(self, message):
        """Show error message"""
        self.page.snack_bar = ft.SnackBar(content=ft.Text(message, color=ft.Colors.WHITE), bgcolor=ft.Colors.RED_700)
        self.page.snack_bar.open = True
        self.page.update()

    def show_success(self, message):
        """Show success message"""
        self.page.snack_bar = ft.SnackBar(content=ft.Text(message, color=ft.Colors.WHITE), bgcolor=ft.Colors.GREEN_700)
        self.page.snack_bar.open = True
        self.page.update()

    def show_info(self, message):
        """Show info message"""
        self.page.snack_bar = ft.SnackBar(content=ft.Text(message, color=ft.Colors.WHITE), bgcolor=ft.Colors.BLUE_700)

    def debug_mapping_manager_click(self, e):
        """Debug wrapper for mapping manager button click"""
        try:
            logger.info("Mapping manager button clicked!")
            print("Mapping manager button clicked!")  # Console output for immediate feedback
            self.show_mapping_manager(e)
        except Exception as ex:
            logger.error(f"Error in mapping manager button click: {str(ex)}")
            print(f"Error in mapping manager button click: {str(ex)}")
            self.show_error(f"Button click error: {str(ex)}")

    def show_mapping_manager(self, e):
        """Show column mapping management dialog"""
        try:
            logger.info("Opening mapping manager dialog...")
            
            # Check if comparator exists
            if not hasattr(self, 'comparator') or not self.comparator:
                logger.warning("Comparator not initialized, creating default mappings")
                # Create a simple dialog with default mappings
                self.show_simple_mapping_dialog()
                return
            
            # Create mapping content for each insurer
            mapping_content = ft.Column(scroll=ft.ScrollMode.AUTO, height=500, spacing=5)
            
            # Get all insurers from column mappings
            insurers = list(self.comparator.column_mappings.keys()) if hasattr(self.comparator, 'column_mappings') else []
            
            if not insurers:
                logger.warning("No insurers found in column mappings")
                self.show_simple_mapping_dialog()
                return
            
            logger.info(f"Found {len(insurers)} insurers: {insurers}")
            
            # Add summary at the top
            summary_text = ft.Container(
                content=ft.Text(
                    f"Managing column mappings for {len(insurers)} insurers. Each section shows current mappings vs backend defaults.",
                    size=14,
                    color=ft.Colors.BLUE_700,
                    text_align=ft.TextAlign.CENTER
                ),
                bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.BLUE_700),
                border_radius=5,
                padding=10,
                margin=ft.margin.only(bottom=10)
            )
            mapping_content.controls.append(summary_text)
            
            for insurer in insurers:
                # Get current mappings
                current_mappings = self.comparator.column_mappings.get(insurer, {})
                resolved_mappings = self.comparator.resolved_mappings.get(insurer, {}) if hasattr(self.comparator, 'resolved_mappings') else {}
                
                logger.info(f"Creating section for {insurer} with {len(current_mappings)} mappings")
                
                # Calculate mapping status
                default_mappings = self.get_default_mappings_for_insurer(insurer)
                matches = sum(1 for field in current_mappings if current_mappings.get(field) == default_mappings.get(field))
                total_defaults = len(default_mappings)
                
                status_text = f"{matches}/{total_defaults} match defaults" if total_defaults > 0 else f"{len(current_mappings)} custom mappings"
                status_color = ft.Colors.GREEN if matches == total_defaults else ft.Colors.ORANGE if matches > 0 else ft.Colors.RED
                
                # Create insurer section with better styling
                insurer_section = ft.ExpansionTile(
                    title=ft.Text(f"{insurer} Mappings", weight=ft.FontWeight.BOLD, size=16),
                    subtitle=ft.Text(status_text, color=status_color, weight=ft.FontWeight.W_500),
                    controls=[self.create_insurer_mapping_controls(insurer, current_mappings, resolved_mappings)],
                    initially_expanded=False,
                    bgcolor=ft.Colors.with_opacity(0.05, ft.Colors.BLUE_700),
                    collapsed_bgcolor=ft.Colors.with_opacity(0.02, ft.Colors.BLUE_700),
                    text_color=ft.Colors.BLUE_900,
                    icon_color=ft.Colors.BLUE_700
                )
                mapping_content.controls.append(insurer_section)
            
            # Create dialog content with buttons at top
            dialog_content = ft.Container(
                content=ft.Column([
                    # Header with close button
                    ft.Row([
                        ft.Text("Column Mapping Manager", size=18, weight=ft.FontWeight.BOLD),
                        ft.IconButton(ft.Icons.CLOSE, on_click=self.close_mapping_dialog)
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    
                    # Action buttons at the top
                    ft.Container(
                        content=ft.Row([
                            ft.ElevatedButton("Save to Config", on_click=self.save_mappings_to_config, bgcolor=ft.Colors.BLUE_700, color=ft.Colors.WHITE, tooltip="Save current mappings to config file", width=140, height=35, data="save_config_btn"),
                            ft.ElevatedButton("Load from Config", on_click=self.load_mappings_from_config, bgcolor=ft.Colors.PURPLE_700, color=ft.Colors.WHITE, tooltip="Load mappings from config file", width=140, height=35, data="load_config_btn"),
                            ft.ElevatedButton("Reset to Defaults", on_click=self.reset_mappings, bgcolor=ft.Colors.ORANGE_700, color=ft.Colors.WHITE, tooltip="Reset all mappings to backend defaults", width=140, height=35, data="reset_btn"),
                            ft.ElevatedButton("Apply Changes", on_click=self.apply_changes_with_visual_feedback, bgcolor=ft.Colors.GREEN_700, color=ft.Colors.WHITE, tooltip="Apply changes to current session", width=140, height=35, data="apply_btn")
                        ], alignment=ft.MainAxisAlignment.CENTER, spacing=8),
                        bgcolor=ft.Colors.with_opacity(0.05, ft.Colors.BLUE_700),
                        border_radius=5,
                        padding=8
                    ),
                    
                    # Mapping content
                    ft.Container(
                        content=mapping_content,
                        height=500,
                        border=ft.border.all(1, ft.Colors.GREY_300),
                        border_radius=5,
                        padding=10
                    )
                ], spacing=8),
                width=1000,
                height=600,
                bgcolor=ft.Colors.WHITE,
                border_radius=10,
                padding=15,
                border=ft.border.all(2, ft.Colors.BLUE_700)
            )
            
            # Create modal overlay
            self.mapping_overlay = ft.Container(
                content=ft.Stack([
                    # Background overlay
                    ft.Container(
                        width=self.page.window_width or 1200,
                        height=self.page.window_height or 800,
                        bgcolor=ft.Colors.with_opacity(0.5, ft.Colors.BLACK),
                        on_click=self.close_mapping_dialog
                    ),
                    # Dialog content centered
                    ft.Container(
                        content=dialog_content,
                        alignment=ft.alignment.center,
                        width=self.page.window_width or 1200,
                        height=self.page.window_height or 800
                    )
                ]),
                visible=True
            )
            
            # Add overlay to page
            if not hasattr(self.page, 'overlay'):
                self.page.overlay = []
            self.page.overlay.append(self.mapping_overlay)
            self.page.update()
            
            # Auto-scroll to bottom to show buttons immediately
            try:
                # Small delay to ensure dialog is rendered
                import time
                time.sleep(0.1)
                
                # Try to scroll the mapping content to bottom
                if hasattr(mapping_content, 'scroll_to'):
                    mapping_content.scroll_to(offset=mapping_content.height or 1000, duration=100)
                    self.page.update()
            except Exception as scroll_ex:
                logger.warning(f"Could not auto-scroll: {scroll_ex}")
            
            logger.info("Mapping manager dialog opened successfully")
            
        except Exception as ex:
            logger.error(f"Error showing mapping manager: {str(ex)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            self.show_error(f"Error opening mapping manager: {str(ex)}")

    def show_simple_mapping_dialog(self):
        """Show a simple mapping dialog when comparator is not initialized"""
        try:
            # Default mappings for display
            default_mappings = {
                'DIGIT': {'policy_number': 'POLICY_NO', 'total_premium': 'TOTAL_PREMIUM', 'insured_name': 'INSURED_NAME', 'vehicle_number': 'VEHICLE_NO'},
                'CHOLA': {'policy_number': 'Policy No', 'total_premium': 'Total Premium', 'insured_name': 'Insured Name', 'vehicle_number': 'Registration No'},
                'IFFCO': {'policy_number': 'Policy Number', 'total_premium': 'Premium', 'insured_name': 'Customer Name', 'vehicle_number': 'Vehicle Registration Number'},
                'KOTAK': {'policy_number': 'POLICY_NO', 'total_premium': 'TOTAL_PREMIUM', 'insured_name': 'INSURED_NAME', 'vehicle_number': 'VEHICLE_NO'},
                'LIBERTY': {'policy_number': 'policy no', 'total_premium': 'total premium', 'insured_name': 'insured name', 'vehicle_number': 'vehicle no'}
            }
            
            mapping_content = ft.Column(scroll=ft.ScrollMode.AUTO, height=500)
            
            for insurer, mappings in default_mappings.items():
                insurer_section = ft.ExpansionTile(
                    title=ft.Text(f"{insurer} Mappings", weight=ft.FontWeight.BOLD),
                    subtitle=ft.Text(f"{len(mappings)} fields mapped"),
                    controls=[self.create_insurer_mapping_controls(insurer, mappings, {})],
                    initially_expanded=False
                )
                mapping_content.controls.append(insurer_section)
            
            # Create dialog content
            dialog_content = ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Text("Column Mapping Manager (Default View)", size=20, weight=ft.FontWeight.BOLD),
                        ft.IconButton(ft.Icons.CLOSE, on_click=self.close_mapping_dialog)
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    ft.Divider(),
                    ft.Container(
                        content=mapping_content,
                        height=500,
                        border=ft.border.all(1, ft.Colors.GREY_300),
                        border_radius=5,
                        padding=10
                    ),
                    ft.Divider(),
                    ft.Row([
                        ft.ElevatedButton("Close", on_click=self.close_mapping_dialog, bgcolor=ft.Colors.GREY_700, color=ft.Colors.WHITE)
                    ], alignment=ft.MainAxisAlignment.END)
                ], spacing=10),
                width=900,
                height=650,
                bgcolor=ft.Colors.WHITE,
                border_radius=10,
                padding=20,
                border=ft.border.all(2, ft.Colors.BLUE_700)
            )
            
            # Create modal overlay
            self.mapping_overlay = ft.Container(
                content=ft.Stack([
                    # Background overlay
                    ft.Container(
                        width=self.page.window_width or 1200,
                        height=self.page.window_height or 800,
                        bgcolor=ft.Colors.with_opacity(0.5, ft.Colors.BLACK),
                        on_click=self.close_mapping_dialog
                    ),
                    # Dialog content centered
                    ft.Container(
                        content=dialog_content,
                        alignment=ft.alignment.center,
                        width=self.page.window_width or 1200,
                        height=self.page.window_height or 800
                    )
                ]),
                visible=True
            )
            
            # Add overlay to page
            if not hasattr(self.page, 'overlay'):
                self.page.overlay = []
            self.page.overlay.append(self.mapping_overlay)
            self.page.update()
            
        except Exception as ex:
            logger.error(f"Error showing simple mapping dialog: {str(ex)}")
            self.show_error(f"Error: {str(ex)}")

    def create_insurer_mapping_controls(self, insurer, current_mappings, resolved_mappings):
        """Create mapping controls for a specific insurer with side-by-side comparison"""
        
        # Create header row
        header_row = ft.Row([
            ft.Container(ft.Text("Internal Field", weight=ft.FontWeight.BOLD, size=14), width=250),
            ft.Container(ft.Text("Current MIS Mapping", weight=ft.FontWeight.BOLD, size=14), width=300),
            ft.Container(ft.Text("Edit MIS Column Name", weight=ft.FontWeight.BOLD, size=14), width=300)
        ], spacing=15)
        
        # Create data rows
        data_rows = []
        
        # Define standard internal fields that we map to
        internal_fields = {
            'policy_number': 'Policy Number',
            'total_premium': 'Total Premium', 
            'insured_name': 'Insured Name',
            'vehicle_number': 'Vehicle Number',
            'policy_start_date': 'Policy Start Date',
            'policy_end_date': 'Policy End Date',
            'broker_name': 'Broker Name',
            'broker_code': 'Broker Code',
            'chassis_number': 'Chassis Number',
            'engine_number': 'Engine Number',
            'fuel_type': 'Fuel Type',
            'vehicle_make': 'Vehicle Make',
            'vehicle_model': 'Vehicle Model',
            'vehicle_variant': 'Vehicle Variant',
            'vehicle_sub_category': 'Vehicle Sub Category',
            'manufacturing_year': 'Manufacturing Year',
            'seating_capacity': 'Seating Capacity',
            'cubic_capacity': 'Cubic Capacity',
            'gross_vehicle_weight': 'Gross Vehicle Weight',
            'policy_type': 'Policy Type',
            'product_name': 'Product Name',
            'customer_name': 'Customer Name'
        }
        
        # Get all fields from current mappings and internal fields
        all_fields = set(current_mappings.keys())
        all_fields.update(internal_fields.keys())
        
        # Sort fields for consistent display
        sorted_fields = sorted(all_fields)
        
        for field in sorted_fields:
            current_value = current_mappings.get(field, '')
            internal_field_name = internal_fields.get(field, field.replace('_', ' ').title())
            resolved_value = resolved_mappings.get(field, current_value)
            
            # Determine status color
            status_color = ft.Colors.GREEN if current_value else ft.Colors.RED
            
            # Create row
            row = ft.Row([
                # Internal field name
                ft.Container(
                    ft.Text(internal_field_name, size=12, weight=ft.FontWeight.W_500),
                    width=250,
                    bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.BLUE_700),
                    border_radius=5,
                    padding=8
                ),
                # Current MIS mapping
                ft.Container(
                    ft.Text(
                        str(current_value) if current_value else "Not Mapped",
                        size=12,
                        color=status_color,
                        weight=ft.FontWeight.W_500 if current_value else ft.FontWeight.NORMAL
                    ),
                    width=300,
                    bgcolor=ft.Colors.with_opacity(0.1, status_color),
                    border_radius=5,
                    padding=8
                ),
                # Edit field
                ft.Container(
                    ft.TextField(
                        value=self.format_mapping_value_for_display(resolved_value),
                        hint_text=f"Enter MIS column name for {internal_field_name}",
                        dense=True,
                        text_size=12,
                        data=f"{insurer}_{field}",
                        border_color=status_color
                    ),
                    width=300
                )
            ], spacing=15)
            
            data_rows.append(row)
        
        # Create scrollable content for this insurer
        scrollable_content = ft.Column([
            header_row,
            ft.Divider(height=1),
            ft.Container(
                content=ft.Column(
                    data_rows, 
                    spacing=5,
                    scroll=ft.ScrollMode.AUTO  # Enable scrolling
                ),
                height=200,  # Fixed height with scroll
                border=ft.border.all(1, ft.Colors.GREY_300),
                border_radius=5,
                padding=10
            )
        ], spacing=10)
        
        return scrollable_content

    def format_mapping_value_for_display(self, value):
        """Format mapping value for display in text field"""
        if value is None:
            return ''
        elif isinstance(value, list):
            # Join list items with comma and space for user-friendly display
            return ', '.join(str(item) for item in value)
        else:
            return str(value)

    def parse_mapping_value_from_display(self, display_value):
        """Parse mapping value from text field display back to proper format"""
        if not display_value or not display_value.strip():
            return None
        
        # Check if it looks like a list (contains commas)
        if ',' in display_value:
            # Split by comma and clean up each item
            items = [item.strip() for item in display_value.split(',') if item.strip()]
            return items if len(items) > 1 else items[0] if items else None
        else:
            return display_value.strip()

    def get_all_comparable_fields(self, resolved_mapping, internal_row):
        """Get all fields that can be compared (predefined + dynamically mapped)"""
        comparable_fields = {}
        
        # Start with predefined internal columns
        for field, internal_col in self.internal_columns.items():
            comparable_fields[field] = internal_col
        
        # Add dynamically mapped fields that exist in both MIS mapping and internal data
        for field, mis_col in resolved_mapping.items():
            if field not in comparable_fields:  # Don't override predefined mappings
                # Try to find a matching column in internal data
                internal_col = self.find_internal_column_for_field(field, internal_row)
                if internal_col:
                    comparable_fields[field] = internal_col
                    logger.info(f"Dynamically added field for comparison: {field} -> {internal_col}")
        
        return comparable_fields
    
    def find_internal_column_for_field(self, field, internal_row):
        """Find matching internal column for a dynamically mapped field"""
        # Convert field name to likely internal column name
        field_variations = [
            field.replace('_', ' ').title(),  # policy_type -> Policy Type
            field.replace('_', ' ').upper(),  # policy_type -> POLICY TYPE  
            field.replace('_', ' ').lower(),  # policy_type -> policy type
            field.title().replace('_', ' '),  # policy_type -> Policy Type
            field.upper(),                    # policy_type -> POLICY_TYPE
            field.lower(),                    # policy_type -> policy_type
            field,                           # policy_type -> policy_type
        ]
        
        # Check if any variation exists in internal data
        for variation in field_variations:
            if variation in internal_row.index:
                return variation
        
        # If no exact match, try partial matching
        field_lower = field.lower().replace('_', '')
        for col in internal_row.index:
            col_lower = str(col).lower().replace(' ', '').replace('_', '')
            if field_lower in col_lower or col_lower in field_lower:
                logger.info(f"Found partial match for {field}: {col}")
                return col
        
        return None

    def get_default_mappings_for_insurer(self, insurer):
        """Get original hardcoded default mappings for a specific insurer"""
        # Use the same comprehensive defaults as reset function
        all_defaults = self.get_comprehensive_default_mappings()
        return all_defaults.get(insurer, {})

    def apply_changes_with_visual_feedback(self, e):
        """Apply changes with visual feedback - refresh dialog without closing"""
        try:
            # First apply the changes
            self.save_mapping_changes(e)
            
            # Refresh the dialog content in place
            self.refresh_mapping_dialog()
            
            # Also refresh the selectable fields for the currently selected insurer
            if self.insurer_dropdown.value:
                self.update_field_options(None)
                self.check_enable_run_button()
            
        except Exception as ex:
            logger.error(f"Error applying changes with visual feedback: {str(ex)}")
            self.show_error(f"Error applying changes: {str(ex)}")

    def refresh_mapping_dialog(self):
        """Refresh the mapping dialog content without closing it"""
        try:
            # Find the mapping content container and refresh it
            if hasattr(self, 'mapping_overlay'):
                # Get current insurers and recreate content
                insurers = list(self.comparator.column_mappings.keys()) if hasattr(self.comparator, 'column_mappings') else []
                
                # Create new mapping content
                new_mapping_content = ft.Column(scroll=ft.ScrollMode.AUTO, height=500, spacing=5)
                
                # Add summary
                summary_text = ft.Container(
                    content=ft.Text(
                        f"Managing column mappings for {len(insurers)} insurers. Changes applied successfully!",
                        size=14,
                        color=ft.Colors.GREEN_700,
                        text_align=ft.TextAlign.CENTER
                    ),
                    bgcolor=ft.Colors.with_opacity(0.1, ft.Colors.GREEN_700),
                    border_radius=5,
                    padding=10,
                    margin=ft.margin.only(bottom=10)
                )
                new_mapping_content.controls.append(summary_text)
                
                # Recreate insurer sections
                for insurer in insurers:
                    current_mappings = self.comparator.column_mappings.get(insurer, {})
                    resolved_mappings = self.comparator.resolved_mappings.get(insurer, {}) if hasattr(self.comparator, 'resolved_mappings') else {}
                    
                    # Calculate mapping status
                    default_mappings = self.get_default_mappings_for_insurer(insurer)
                    matches = sum(1 for field in current_mappings if current_mappings.get(field) == default_mappings.get(field))
                    total_defaults = len(default_mappings)
                    
                    status_text = f"{matches}/{total_defaults} match defaults" if total_defaults > 0 else f"{len(current_mappings)} custom mappings"
                    status_color = ft.Colors.GREEN if matches == total_defaults else ft.Colors.ORANGE if matches > 0 else ft.Colors.RED
                    
                    insurer_section = ft.ExpansionTile(
                        title=ft.Text(f"{insurer} Mappings", weight=ft.FontWeight.BOLD, size=16),
                        subtitle=ft.Text(status_text, color=status_color, weight=ft.FontWeight.W_500),
                        controls=[self.create_insurer_mapping_controls(insurer, current_mappings, resolved_mappings)],
                        initially_expanded=False,
                        bgcolor=ft.Colors.with_opacity(0.05, ft.Colors.BLUE_700),
                        collapsed_bgcolor=ft.Colors.with_opacity(0.02, ft.Colors.BLUE_700),
                        text_color=ft.Colors.BLUE_900,
                        icon_color=ft.Colors.BLUE_700
                    )
                    new_mapping_content.controls.append(insurer_section)
                
                # Find and update the mapping content container
                self.update_mapping_content_container(new_mapping_content)
                self.page.update()
                
        except Exception as ex:
            logger.error(f"Error refreshing mapping dialog: {str(ex)}")

    def update_mapping_content_container(self, new_content):
        """Update the mapping content container with new content"""
        def find_and_update_container(control):
            if isinstance(control, ft.Container):
                # Check if this is the mapping content container
                if (hasattr(control, 'height') and control.height == 500 and
                    hasattr(control, 'border') and control.border):
                    control.content = new_content
                    return True
            
            # Recursively check child controls
            if hasattr(control, 'controls'):
                for child in control.controls:
                    if find_and_update_container(child):
                        return True
            elif hasattr(control, 'content'):
                return find_and_update_container(control.content)
            
            return False
        
        if hasattr(self, 'mapping_overlay'):
            find_and_update_container(self.mapping_overlay)

    def save_mapping_changes(self, e):
        """Apply changes made to column mappings to current session"""
        try:
            # Collect all text field values from the dialog
            updated_mappings = self.collect_mappings_from_dialog()
            
            # Update the comparator's mappings
            for insurer, mappings in updated_mappings.items():
                if insurer in self.comparator.column_mappings:
                    self.comparator.column_mappings[insurer].update(mappings)
                else:
                    self.comparator.column_mappings[insurer] = mappings
            
            # Clear resolved mappings to force re-resolution
            self.comparator.resolved_mappings.clear()
            
            self.show_success("Column mappings applied to current session!")
            logger.info(f"Applied mappings for {len(updated_mappings)} insurers")
            
            # Immediately refresh the selectable fields to include any new custom fields
            if self.insurer_dropdown.value:
                self.update_field_options(None)
                self.check_enable_run_button()
            
        except Exception as ex:
            logger.error(f"Error applying mapping changes: {str(ex)}")
            self.show_error(f"Error applying changes: {str(ex)}")

    def reset_mappings(self, e):
        """Reset mappings to original hardcoded backend default values"""
        try:
            # Get original hardcoded default mappings
            original_defaults = self.get_comprehensive_default_mappings()
            
            # Reset each insurer to ONLY its original default mappings
            # This will remove any extra fields that were added later
            for insurer in self.comparator.column_mappings.keys():
                if insurer in original_defaults:
                    # Replace with exact original mappings (removes extra fields)
                    self.comparator.column_mappings[insurer] = original_defaults[insurer].copy()
                    logger.info(f"Reset {insurer} to {len(original_defaults[insurer])} original fields")
                else:
                    # If insurer not in original defaults, clear it completely
                    self.comparator.column_mappings[insurer] = {}
                    logger.info(f"Cleared {insurer} mappings (not in original defaults)")
            
            # Clear resolved mappings to force re-resolution
            self.comparator.resolved_mappings.clear()
            
            # Refresh dialog in place
            self.refresh_mapping_dialog()
            
            # Refresh selectable fields as well
            if self.insurer_dropdown.value:
                self.update_field_options(None)
                self.check_enable_run_button()
            
            self.show_success("Mappings reset to original hardcoded backend defaults!")
            logger.info("All mappings reset to original hardcoded state - extra fields removed")
            
        except Exception as ex:
            logger.error(f"Error resetting mappings: {str(ex)}")
            self.show_error(f"Error resetting mappings: {str(ex)}")

    def get_comprehensive_default_mappings(self):
        """Get the exact original hardcoded mappings from self.column_mappings (true backend defaults)"""
        return {
            'SHRIRAM': {
                'policy_number': 'S_POLICYNO',
                'customer_name': 'S_INSUREDNAME',
                'policy_start_date': 'S_DOI',
                'policy_end_date': 'S_DOE',
                'registration_number': 'S_VEH_REG_NO',
                'engine_number': 'S_ENGNO',
                'chassis_number': 'S_CHNO',
                'total_premium': 'S_NET',
                'tp_premium': 'S_TP_TOTAL',
                'previous_policy_number': 'S_PREVIOUS_POL_NO',
                'broker_name': 'S_SOURCENAME',
                'broker_code': None,
                'sum_insured': None,
                'vehicle_make': 'S_MAKE_DESC',
                'vehicle_model': 'S_VEH_MODEL',
                'fuel_type': 'S_VEHFUEL_DESC',
                'gvw': 'S_GVW',
                'seating_capacity': 'S_VEH_SC',
                'ncb_percentage': 'S_NCB_PERCENTAGE',
                'manufacturing_year': None,
                'engine_cubic_capacity': 'S_VEH_CC',
            },
            'RELIANCE': {
                'policy_number': 'PolicyNo',
                'customer_name': 'CustomerName',
                'policy_start_date': 'PolicyStartDate',
                'policy_end_date': 'PolicyEndDate',
                'registration_number': 'VechileNo',
                'engine_number': 'EngineNo',
                'chassis_number': 'ChassisNo',
                'total_premium': 'GrossPremium',
                'tp_premium': 'TpPremium',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'IMDName',
                'broker_code': 'IMDCode',
                'sum_insured': 'SumInsured',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'FuelType',
                'gvw': 'GVW',
                'seating_capacity': 'SeatingCapacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'YearOfManufacture',
                'engine_cubic_capacity': 'CC',
            },
            'ROYAL': {
                'policy_number': 'Policy No',
                'customer_name': 'Client Full Name',
                'policy_start_date': 'Inception Date',
                'policy_end_date': 'Expiry Date',
                'registration_number': 'Reg No',
                'engine_number': 'Engine No',
                'chassis_number': 'Chasis No',
                'total_premium': 'Client Premium',
                'tp_premium': 'TP Premium',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Incremental SI',
                'vehicle_make': 'Make',
                'vehicle_model': 'Mfr_Model_GWP',
                'fuel_type': 'Fuel_Type',
                'gvw': 'GVW_Ton',
                'seating_capacity': 'SeatingCapacity',
                'ncb_percentage': 'NCB_Rate',
                'manufacturing_year': 'Year of Manufacturing',
                'engine_cubic_capacity': 'Cubic_Capcity',
            },
            'SBI': {
                'policy_number': ['PolicyNo'],
                'customer_name': 'CustomerName',
                'policy_start_date': 'PolicyStartDate',
                'policy_end_date': 'PolicyEndDate',
                'registration_number': None,
                'engine_number': None,
                'chassis_number': None,
                'total_premium': ['FinalPremium', 'GWP LACS', 'GWP Lacs','Gross Written Premium'],
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': ['IMD name', 'IMDName', 'Channel_Name'],
                'broker_code': 'IMDCode',
                'sum_insured': None,
                'vehicle_make': 'MakeModel',
                'vehicle_model': ['Product_Name', 'ProductName'],
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None
            },
            'ICICI': {
                'policy_number': 'POL_NUM_TXT',
                'customer_name': 'CUSTOMER_NAME',
                'policy_start_date': 'POL_START_DATE',
                'policy_end_date': 'POL_END_DATE',
                'registration_number': 'MOTOR_REGISTRATION_NUM',
                'engine_number': 'VEH_ENGINENO',
                'chassis_number': 'VEH_CHASSISNO',
                'total_premium': 'TOTAL_GROSS_PREMIUM',
                'tp_premium': 'MOTOR_TP_PREMIUM_AMT',
                'previous_policy_number': 'XC_PREV_POL_NO',
                'broker_name': 'AGENT_NAME',
                'broker_code': 'CHILD_ID',
                'sum_insured': 'POL_TOT_SUM_INSURED_AMT',
                'vehicle_make': 'MANUFACTURER_NAME',
                'vehicle_model': 'MODEL_NAME',
                'fuel_type': 'XC_FUEL_TYPE',
                'gvw': 'POL_GVW',
                'seating_capacity': 'XC_POL_SEATING_CAPACITY',
                'ncb_percentage': 'XC_POL_NCB_PCT',
                'manufacturing_year': 'MOTOR_MANUFACTURER_YEAR',
                'engine_cubic_capacity': 'MOTOR_ENGINE_CC',
            },
            'NATIONAL': {
                'policy_number': ['Policy Number', 'policy No'],
                'customer_name': 'Customer Name',
                'policy_start_date': 'Policy Effective Date',
                'policy_end_date': 'Policy Expiry Date',
                'registration_number': 'Vehicle Registration Number',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Premium',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': 'POSP Name',
                'broker_code': 'POSP Code',
                'sum_insured': None,
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'Gross Vehicle Weight',
                'seating_capacity': 'Seating Capacity',
                'ncb_percentage': 'NCB Percentage',
                'manufacturing_year': 'Manufacturing Year',
                'engine_cubic_capacity': 'Engine Cubic Capacity',
            },
            'DIGIT': {
                'policy_number': ['POLICY_NUMBER','Policy_Number'],
                'customer_name': 'INSURED_PERSON',
                'policy_start_date': 'RISK_INC_DATE',
                'policy_end_date': 'RISK_EXP_DATE',
                'registration_number': 'VEH_REG_NO',
                'engine_number': 'ENGINE_NO',
                'chassis_number': 'CHASSIS_NO',
                'total_premium': 'GROSS_PREMIUM',
                'tp_premium': 'TP_PREMIUM',
                'previous_policy_number': 'PREV_POLICY_NO',
                'broker_name': 'IMD_NAME',
                'broker_code': None,
                'sum_insured': 'SUM_INSURED',
                'vehicle_make': 'VEHICLE_MAKE',
                'vehicle_model': 'VEHICLE_MODEL',
                'fuel_type': 'FUEL_TYPE',
                'gvw': 'VEH_GVW',
                'seating_capacity': 'VEH_SEATING',
                'ncb_percentage': 'CURR_NCB',
                'manufacturing_year': 'VEHICLE_MANF_YEAR',
                'engine_cubic_capacity': 'VEH_CC',
            },
            'MAGMA': {
                'policy_number': 'PolicyNo',
                'customer_name': 'CustomerName',
                'policy_start_date': 'RiskStartDate',
                'policy_end_date': 'RiskEndDate',
                'registration_number': 'RegistrationNumber',
                'engine_number': 'EngineNumber',
                'chassis_number': 'ChassisNumber',
                'total_premium': 'PremiumAmount',
                'tp_premium': None,
                'previous_policy_number': 'PreviousYearPolicyNumber',
                'broker_name': 'AgentName',
                'broker_code': 'AgentCode',
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': 'ProductDesc',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'UNIVERSAL': {
                'policy_number': ['POLICY NO CHAR', 'USGIpos Policy Number','POLICY NO'],
                'customer_name': 'INSURED NAME',
                'policy_start_date': 'START DATE',
                'policy_end_date': 'EXPIRY DATE',
                'registration_number': 'MTOR Registration No',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'GROSS PREMIUM',
                'tp_premium': 'TOTAL TP PREMIUM',
                'previous_policy_number': 'Previous Yr Policy',
                'broker_name': 'INTERMEDIARY',
                'broker_code': 'INTERMEDIARY CODE',
                'sum_insured': 'TOTAL SUM INSURED',
                'vehicle_make': 'MAKE',
                'vehicle_model': 'MODEL',
                'fuel_type': None,
                'gvw': 'Gvw',
                'seating_capacity': 'Vehicle Seating Capacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'YEAR OF MANUFACTURING',
                'engine_cubic_capacity': None,
            },
            'UNITED': {
                'policy_number': 'Policy Number',
                'customer_name': 'Insured Name',
                'policy_start_date': 'Effect Date',
                'policy_end_date': 'Expiry Date',
                'registration_number': 'Registration Number',
                'engine_number': 'Engine Number',
                'chassis_number': 'Chassis Number',
                'total_premium': 'company_wise_own_share_premium_total',
                'tp_premium': 'TP Premium',
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': 'Agent/Br.Cd',
                'sum_insured': 'Sum Insured',
                'vehicle_make': None,
                'vehicle_model': None,
                'fuel_type': None,
                'gvw': 'NUM_GVW',
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'FGI': {
                'policy_number': 'Policy Number',
                'customer_name': 'Customer Name',
                'policy_start_date': 'Transaction',
                'policy_end_date': 'Policy Expiry Date',
                'registration_number': 'Registration Number',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Final Premium',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': 'Agent Code',
                'sum_insured': None,
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fueal Type',
                'gvw': 'GVW',
                'seating_capacity': 'Seating Capacity',
                'ncb_percentage': None,
                'manufacturing_year': 'Year Of Manufacturing',
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'SOMPO': {
                'policy_number': ['NUM_POLCIY_NO', 'POLICY_NO_CHAR', 'ILPOS_POLICY_NUMBER'],
                'customer_name': 'INSURED_NAME',
                'policy_start_date': 'START_DATE',
                'policy_end_date': 'EXPIRY_DATE',
                'registration_number': 'MOTOR_REGISTRATION_NO',
                'engine_number': 'ENGINE_NO',
                'chassis_number': 'CHASSIS_NO',
                'total_premium': 'GROSS_PREMIUM',
                'tp_premium': 'TOTAL_TP_PREMIUM',
                'previous_policy_number': 'PREVIOUS_POLICY_NUMBER',
                'broker_name': 'INTERMEDIARY',
                'broker_code': 'INTERMEDIARY_CODE',
                'sum_insured': 'TOTAL_SI',
                'vehicle_make': 'MAKE',
                'vehicle_model': 'MODEL',
                'fuel_type': 'FUEL_TYPE',
                'gvw': 'GVW',
                'seating_capacity': 'VEHICLE_SEATING_CAPACITY',
                'ncb_percentage': 'NCB_PERCENTAGE',
                'manufacturing_year': 'YR_OF_MANUFACTURING',
                'engine_cubic_capacity': 'CUBIC_CAPACITY',
            },
            'BAJAJ': {
                'policy_number': ['PolicyNo','P Policy Number'],
                'customer_name': ['CustomerName', 'Partner Desc'],
                'policy_start_date': 'Risk Start Date',
                'policy_end_date': 'Risk End Date',
                'registration_number': 'RegistrationNumber',
                'engine_number': 'EngineNumber',
                'chassis_number': 'ChassisNumber',
                'total_premium': ['R Rnwd Gross Prem', 'Gross Premium'],
                'tp_premium': 'R Rnwd Tp Prem',
                'previous_policy_number': 'Previous Year Policy Number',
                'broker_name': 'AgentName',
                'broker_code': 'AgentCode',
                'sum_insured': 'R Rnwd Sum Insured',
                'vehicle_make': None,
                'vehicle_model': 'Product Desc',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'CHOLA': {
                'policy_number': 'Policy No',
                'customer_name': 'Customer name',
                'policy_start_date': 'RSD',
                'policy_end_date': 'RED',
                'registration_number': 'Regist_NO',
                'engine_number': 'Engine No',
                'chassis_number': 'Chassis No',
                'total_premium': 'Gross Premium',
                'tp_premium': 'Third Party liability',
                'previous_policy_number': 'PREVIOUS_POLICY_NUMBER',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Total IDV',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'GROSS_VEHICLE_WEIGHT',
                'seating_capacity': 'TOTAL_SEATING_CAPACITY',
                'ncb_percentage': 'NCB_PERCENT',
                'manufacturing_year': 'MFG Year',
                'engine_cubic_capacity': 'CUBIC_CAPACITY',
            },
            'HDFC': {
                'policy_number': 'Policy No',
                'customer_name': 'Customer Name',
                'policy_start_date': 'Start Date',
                'policy_end_date': 'End Date',
                'registration_number': 'POL_MOT_VEH_REGISTRATION_NUM',
                'engine_number': 'ENGINE NUM',
                'chassis_number': 'CHASSIS NUM',
                'total_premium': 'Total GWP',
                'tp_premium': 'TP',
                'previous_policy_number': 'Previous Policy No',
                'broker_name': 'Agent Name',
                'broker_code': 'Agent Code',
                'sum_insured': 'Sum Insured',
                'vehicle_make': 'POL_MOT_MANUFACTURER_NAME',
                'vehicle_model': 'POL_MOT_MODEL_NAME',
                'fuel_type': 'POL_FUEL_TYPE',
                'gvw': 'POL_GROSS_VEH_WT',
                'seating_capacity': None,
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'POL_MOT_MANUFACTURER_YEAR',
                'engine_cubic_capacity': None,
            },
            'IFFCO': {
                'policy_number': 'P400 policy',
                'customer_name': ['First Name', 'Last Name'],
                'policy_start_date': 'Inceptiondate',
                'policy_end_date': 'Expirydate',
                'registration_number': 'Registration No',
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'Total Premium after discount/loading',
                'tp_premium': 'TP Gross Premium',
                'previous_policy_number': 'PreviousInsurerPolicy Number',
                'broker_name': None,
                'broker_code': 'AgentNo',
                'sum_insured': 'Total Sum Insured',
                'vehicle_make': 'Make',
                'vehicle_model': 'Model',
                'fuel_type': 'Fuel Type',
                'gvw': 'GVW',
                'seating_capacity': 'Carrying Capacity',
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'Year',
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'KOTAK': {
                'policy_number': 'POLICY NO',
                'customer_name': 'CUSTOMER NAME',
                'policy_start_date': 'POLICY INCEPTION DATE',
                'policy_end_date': 'POLICY EXPIRY DATE',
                'registration_number': 'Registration Number',
                'engine_number': 'Engine Number',
                'chassis_number': 'Chassis Number',
                'total_premium': 'GWP',
                'tp_premium': 'ALL OTHER TP COVER PREMIUM',
                'previous_policy_number': None,
                'broker_name': 'IMD NAME',
                'broker_code': 'IMD CODE',
                'sum_insured': 'SYSTEMIDV',
                'vehicle_make': 'VehicleMake',
                'vehicle_model': 'VehicleModel',
                'fuel_type': 'FuelType',
                'gvw': 'VehicleWeight',
                'seating_capacity': None,
                'ncb_percentage': 'NCB',
                'manufacturing_year': 'ManufacturerYear',
                'engine_cubic_capacity': 'VehicleCC',
            },
            'RAHEJA': {
                'policy_number': 'P400 Pol No.',
                'customer_name': 'CompanyName',
                'policy_start_date': 'policy_start_date',
                'policy_end_date': 'Expiry date',
                'registration_number': 'reg_no',
                'engine_number': 'engine_no',
                'chassis_number': 'chasis_no',
                'total_premium': 'final_premium',
                'tp_premium': 'TP Premium',
                'previous_policy_number': None,
                'broker_name': 'IMD Name',
                'broker_code': 'IMD Code',
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': 'Sub_Product',
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
            'TATA': {
                'policy_number': 'policy_no',
                'customer_name': 'clientname',
                'policy_start_date': 'pol_incept_date',
                'policy_end_date': 'pol_exp_date',
                'registration_number': 'registration_no',
                'engine_number': 'veh_engine_no',
                'chassis_number': 'veh_chassis_no',
                'total_premium': 'total_premium',
                'tp_premium': 'basictp',
                'previous_policy_number': None,
                'broker_name': 'producername',
                'broker_code': 'producer_cd',
                'sum_insured': 'vehicleidv',
                'vehicle_make': 'veh_make',
                'vehicle_model': 'veh_model',
                'fuel_type': 'fuel_type',
                'gvw': 'veh_gr_wgt_cnt',
                'seating_capacity': 'seat_cap_cnt',
                'ncb_percentage': 'ncb_perctg',
                'manufacturing_year': 'mfg_year',
                'engine_cubic_capacity': 'kw_cc_no',
            },
            'ZUNO': {
                'policy_number': 'Policy_Number',
                'customer_name': 'Policy_Holder_Name',
                'policy_start_date': 'Policy_Start_Date',
                'policy_end_date': 'Policy_End_Date',
                'registration_number': 'Registration No',
                'engine_number': 'Engine No',
                'chassis_number': 'chasis No',
                'total_premium': 'Gross_Premium_Total',
                'tp_premium': 'Motor TP Premium',
                'previous_policy_number': 'Previous_Policy_No',
                'broker_name': 'Intermediary_Name',
                'broker_code': 'Intermediary_Code',
                'sum_insured': 'NUM_IDV',
                'vehicle_make': 'Make Code',
                'vehicle_model': 'Model Code',
                'fuel_type': 'Type of Fuel',
                'gvw': 'GVW',
                'seating_capacity': 'Capacity Type',
                'ncb_percentage': 'NCB_Percentage',
                'manufacturing_year': 'Manufacturing Year',
                'engine_cubic_capacity': 'Num_PCC CC',
            },
            'LIBERTY': {
                'policy_number': 'Policy No.',
                'customer_name': 'Insured Name',
                'policy_start_date': 'Policy Start Date',
                'policy_end_date': 'Policy End Date',
                'registration_number': 'Veh Reg No',
                'engine_number': 'Engine NO',
                'chassis_number': 'Chassis NO',
                'total_premium': 'Net Premium',
                'tp_premium': 'Net TP Premium / War & SRCC',
                'previous_policy_number': 'Previous Policy No.',
                'broker_name': 'Agent/ Broker Name',
                'broker_code': 'Agent/ Broker Code',
                'sum_insured': 'Sum Insured',
                'vehicle_make': 'Make Name',
                'vehicle_model': 'Model Name',
                'fuel_type': 'Fuel Type',
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': 'Current Year NCB (%)',
                'manufacturing_year': None,
                'engine_cubic_capacity': 'Cubic Capacity',
            },
            'ORIENTAL': {
                'policy_number': 'POLICY NO',
                'customer_name': None,
                'policy_start_date': None,
                'policy_end_date': None,
                'registration_number': None,
                'engine_number': None,
                'chassis_number': None,
                'total_premium': 'PREMIUM',
                'tp_premium': None,
                'previous_policy_number': None,
                'broker_name': None,
                'broker_code': None,
                'sum_insured': None,
                'vehicle_make': None,
                'vehicle_model': None,
                'fuel_type': None,
                'gvw': None,
                'seating_capacity': None,
                'ncb_percentage': None,
                'manufacturing_year': None,
                'engine_cubic_capacity': None,
            },
        }

    def save_mappings_to_config(self, e):
        """Save current mappings to a config file"""
        try:
            import json
            import os
            
            # Collect current mappings from dialog
            updated_mappings = self.collect_mappings_from_dialog()
            
            # Create config directory if it doesn't exist
            config_dir = "config"
            if not os.path.exists(config_dir):
                os.makedirs(config_dir)
            
            # Save to config file
            config_file = os.path.join(config_dir, "column_mappings.json")
            config_data = {
                "version": "1.0",
                "timestamp": datetime.now().isoformat(),
                "mappings": updated_mappings
            }
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
            
            self.show_success(f"Mappings saved to {config_file}")
            logger.info(f"Mappings saved to config file: {config_file}")
            
        except Exception as ex:
            logger.error(f"Error saving mappings to config: {str(ex)}")
            self.show_error(f"Error saving to config: {str(ex)}")

    def load_mappings_from_config(self, e):
        """Load mappings from config file"""
        try:
            import json
            import os
            
            config_file = os.path.join("config", "column_mappings.json")
            
            if not os.path.exists(config_file):
                self.show_error("No config file found. Please save mappings first.")
                return
            
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            loaded_mappings = config_data.get("mappings", {})
            
            # Update comparator mappings
            for insurer, mappings in loaded_mappings.items():
                if insurer in self.comparator.column_mappings:
                    self.comparator.column_mappings[insurer].update(mappings)
                else:
                    self.comparator.column_mappings[insurer] = mappings
            
            # Clear resolved mappings to force re-resolution
            self.comparator.resolved_mappings.clear()
            
            # Close and reopen dialog to refresh display
            self.close_mapping_dialog(e)
            self.show_mapping_manager(e)
            
            timestamp = config_data.get("timestamp", "Unknown")
            self.show_success(f"Mappings loaded from config (saved: {timestamp[:19]})")
            logger.info(f"Mappings loaded from config file: {config_file}")
            
            # Refresh selectable fields for the current insurer
            if self.insurer_dropdown.value:
                self.update_field_options(None)
                self.check_enable_run_button()
            
        except Exception as ex:
            logger.error(f"Error loading mappings from config: {str(ex)}")
            self.show_error(f"Error loading from config: {str(ex)}")

    def collect_mappings_from_dialog(self):
        """Collect all mappings from the dialog text fields"""
        updated_mappings = {}
        
        def collect_mappings(control):
            if isinstance(control, ft.TextField) and hasattr(control, 'data') and control.data:
                insurer_field = control.data.split('_', 1)
                if len(insurer_field) == 2:
                    insurer, field = insurer_field
                    if insurer not in updated_mappings:
                        updated_mappings[insurer] = {}
                    if control.value and control.value.strip():  # Only save non-empty values
                        # Parse the value properly (handles lists)
                        parsed_value = self.parse_mapping_value_from_display(control.value)
                        if parsed_value is not None:
                            updated_mappings[insurer][field] = parsed_value
            
            # Recursively check child controls
            if hasattr(control, 'controls'):
                for child in control.controls:
                    collect_mappings(child)
            elif hasattr(control, 'content'):
                collect_mappings(control.content)
        
        # Collect mappings from dialog content
        if hasattr(self, 'mapping_overlay'):
            collect_mappings(self.mapping_overlay)
        
        return updated_mappings

    def set_mapping_buttons_enabled(self, enabled=True):
        """Enable or disable mapping dialog buttons"""
        try:
            def update_buttons(control):
                if isinstance(control, ft.ElevatedButton) and hasattr(control, 'data'):
                    if control.data in ['save_config_btn', 'load_config_btn', 'reset_btn', 'apply_btn']:
                        control.disabled = not enabled
                        if not enabled:
                            control.bgcolor = ft.Colors.GREY_400
                            control.tooltip = "Disabled during comparison"
                        else:
                            # Restore original colors
                            if control.data == 'save_config_btn':
                                control.bgcolor = ft.Colors.BLUE_700
                            elif control.data == 'load_config_btn':
                                control.bgcolor = ft.Colors.PURPLE_700
                            elif control.data == 'reset_btn':
                                control.bgcolor = ft.Colors.ORANGE_700
                            elif control.data == 'apply_btn':
                                control.bgcolor = ft.Colors.GREEN_700
                            control.tooltip = control.tooltip.replace("Disabled during comparison", "").strip()
                
                # Recursively check child controls
                if hasattr(control, 'controls'):
                    for child in control.controls:
                        update_buttons(child)
                elif hasattr(control, 'content'):
                    update_buttons(control.content)
            
            if hasattr(self, 'mapping_overlay'):
                update_buttons(self.mapping_overlay)
                self.page.update()
                
        except Exception as ex:
            logger.error(f"Error updating button states: {str(ex)}")

    def close_mapping_dialog(self, e):
        """Close the mapping management dialog"""
        try:
            if hasattr(self, 'mapping_overlay') and self.mapping_overlay in self.page.overlay:
                self.page.overlay.remove(self.mapping_overlay)
                self.page.update()
                logger.info("Mapping dialog closed")
        except Exception as ex:
            logger.error(f"Error closing mapping dialog: {str(ex)}")
            # Fallback - clear all overlays
            if hasattr(self.page, 'overlay'):
                self.page.overlay.clear()
                self.page.update()
        self.page.snack_bar.open = True
        self.page.update()

def main(page: ft.Page):
    """Main entry point for the Flet app"""
    page.window_width = 1600
    page.window_height = 900
    app = InsuranceValidationApp(page)

if __name__ == "__main__":
    # To ensure the app runs correctly when packaged with PyInstaller
    # This check prevents re-execution in child processes on Windows
    if mp.get_start_method(allow_none=True) != 'fork':
        mp.set_start_method('spawn', force=True)
    ft.app(target=main)
    