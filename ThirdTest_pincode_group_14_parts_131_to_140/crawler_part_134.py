"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 134 / 400
================================================================================
- Group: ThirdTest_pincode_group_14_parts_131_to_140
- Assigned PIN Codes: 48 (Range: 416311 to 416520)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_134.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_134.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_134"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-134] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "416311",
  "416312",
  "416313",
  "416314",
  "416315",
  "416316",
  "416401",
  "416402",
  "416403",
  "416404",
  "416405",
  "416406",
  "416407",
  "416408",
  "416409",
  "416410",
  "416411",
  "416412",
  "416413",
  "416414",
  "416415",
  "416416",
  "416417",
  "416418",
  "416419",
  "416420",
  "416436",
  "416437",
  "416501",
  "416502",
  "416503",
  "416504",
  "416505",
  "416506",
  "416507",
  "416508",
  "416509",
  "416510",
  "416511",
  "416512",
  "416513",
  "416514",
  "416515",
  "416516",
  "416517",
  "416518",
  "416519",
  "416520"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "416311": {
    "pincode": "416311",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Savlaj S.O",
      "Anjani B.O",
      "Banur B.O",
      "Bastawade B.O",
      "Dahiwadi B.O",
      "Dongarsoni B.O",
      "Jarandi B.O",
      "Khujgaon B.O",
      "Siddhewadi Savalaj B.O",
      "Vadgaon B.O"
    ]
  },
  "416312": {
    "pincode": "416312",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Tasgaon M.D.G S.O",
      "Arawade B.O",
      "Balagavade B.O",
      "Bendri B.O",
      "Biranwadi B.O",
      "Borgaon (T) B.O",
      "Chinchani (T) B.O",
      "Dhavali (T) B.O",
      "Gaurgaon B.O",
      "Karanje B.O",
      "Limb B.O",
      "Lode B.O",
      "Manjarde B.O",
      "Nimani B.O",
      "Nimblak B.O",
      "Ped B.O",
      "Punadi (T) B.O",
      "Turchi B.O",
      "Vasumbe (T) B.O",
      "Waiphale B.O",
      "Yamgarwadi B.O",
      "Guruwar Peth,tasgaon S.O"
    ]
  },
  "416313": {
    "pincode": "416313",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Walwa S.O",
      "Junekhed B.O",
      "Nagthane B.O"
    ]
  },
  "416314": {
    "pincode": "416314",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Visapur S.O (Sangli)",
      "Hatnoli B.O",
      "Hatnur B.O",
      "Khalsa Dhamni B.O",
      "Padali B.O"
    ]
  },
  "416315": {
    "pincode": "416315",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Malwadi B.O",
      "Dudhgaon S.O"
    ]
  },
  "416316": {
    "pincode": "416316",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Ankalkhop S.O"
    ]
  },
  "416401": {
    "pincode": "416401",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Arag S.O",
      "Khatav B.O",
      "Lingnoor B.O",
      "Shindewadi B.O"
    ]
  },
  "416402": {
    "pincode": "416402",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Daphalapur S.O",
      "Ankale B.O",
      "Baj B.O",
      "Jirgyal B.O",
      "Khalati B.O",
      "Washan B.O"
    ]
  },
  "416403": {
    "pincode": "416403",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Dhalgaon S.O",
      "Agalgaon B.O",
      "Arewadi B.O",
      "Chorochi B.O",
      "Dhavadwadi B.O",
      "Dorli B.O",
      "Ghorpadi B.O",
      "Kerewadi B.O",
      "Kosari B.O",
      "Kumbhari B.O",
      "Nagaj B.O"
    ]
  },
  "416404": {
    "pincode": "416404",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Jath S.O",
      "Antral B.O",
      "Avandhi B.O",
      "Banali B.O",
      "Basargi B.O",
      "Bevnur B.O",
      "Billur B.O",
      "Gugwad B.O",
      "Kunikonnur B.O",
      "Mendhigiri B.O",
      "Muchandi B.O",
      "Nigdi Khurd B.O",
      "Revnal B.O",
      "Sanmadi B.O",
      "Shegaon B.O",
      "Shinganhalli B.O",
      "Sindur B.O",
      "Umarani B.O",
      "Vajrawad B.O",
      "Valsang B.O",
      "Walekhindi B.O",
      "Yelvi B.O"
    ]
  },
  "416405": {
    "pincode": "416405",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Kavathe Mahankal S.O",
      "Dhalewadi B.O",
      "Ghatnandre B.O",
      "Hingangaon B.O",
      "Irali B.O",
      "Jakhapur B.O",
      "Karoli  T B.O",
      "Kokale B.O",
      "Kuchi B.O",
      "Kundalapur B.O",
      "Langarpeth B.O",
      "Nangole B.O",
      "Shinganapur B.O",
      "Tisangi B.O"
    ]
  },
  "416406": {
    "pincode": "416406",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Madhavnagar S.O"
    ]
  },
  "416407": {
    "pincode": "416407",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Malgaon S.O",
      "Dongarwadi B.O",
      "Gundewadi B.O"
    ]
  },
  "416408": {
    "pincode": "416408",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Gavan B.O",
      "Karoli (M) B.O",
      "Savarde B.O",
      "Manerajuri S.O"
    ]
  },
  "416409": {
    "pincode": "416409",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Mhaisal S.O",
      "Narwad B.O"
    ]
  },
  "416410": {
    "pincode": "416410",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Miraj H.O",
      "Brahmanpuri Miraj S.O",
      "Govt.Medical College Miraj S.O",
      "Khanbhag,sangli At Midc Miraj S.O",
      "Miraj Audyogik Vasahat S.O",
      "Miraj Station Road S.O",
      "Shaniwar Peth Miraj S.O"
    ]
  },
  "416411": {
    "pincode": "416411",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Ranjani S.O (Sangli)",
      "Agran Dhulgaon B.O",
      "Alkud  (S) B.O",
      "Lonarwadi B.O"
    ]
  },
  "416412": {
    "pincode": "416412",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Sankh S.O",
      "Ankalagi B.O",
      "Asangi J B.O",
      "Asangi K B.O",
      "Boblad K B.O",
      "Daribadachi B.O",
      "Sidhnath B.O",
      "Sordi B.O",
      "Tikundi B.O"
    ]
  },
  "416413": {
    "pincode": "416413",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Umadi S.O",
      "Balgaon B.O",
      "Bellondagi B.O",
      "Boblad Jadar B.O",
      "Borgi(Khurd) B.O",
      "Girgaon B.O",
      "Madgyal B.O",
      "Sonyal B.O",
      "Suslad B.O",
      "Utagi B.O",
      "Vhaspet B.O"
    ]
  },
  "416414": {
    "pincode": "416414",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Wanlsswadi S.O",
      "Deshing B.O",
      "Banewadi BO",
      "Bedag B.O",
      "Belanki B.O",
      "Khanderajuri B.O",
      "Kalambi B.O",
      "Kharshing B.O",
      "Shipur B.O",
      "Payappachiwadi B.O",
      "Tanang B.O",
      "Yerandoli B.O",
      "Vaddi B.O",
      "Siddhewadi B.O",
      "Bolwad B.O",
      "Mallewadi B.O",
      "Dhavali B.O"
    ]
  },
  "416415": {
    "pincode": "416415",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Willingdon College Sangli S.O",
      "Nandre B.O",
      "Mouje Digraj B.O",
      "Brahmanal B.O",
      "Vasgade B.O",
      "Ankali B.O",
      "Haripur B.O",
      "Inam Dhamni B.O",
      "Padmale B.O",
      "Samdolii B.O",
      "Karnal B.O",
      "Yelavi B.O",
      "Govt. Colony S.O",
      "Sangaliwadi B.O"
    ]
  },
  "416416": {
    "pincode": "416416",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Sangli H.O",
      "Factory Area Sangli S.O",
      "Gajanan Mills Sangli S.O",
      "Ganpati Peth Sangli S.O",
      "Market Yard Sangli S.O",
      "R K Extension Sangli S.O",
      "S S K Sangli S.O",
      "Sangli City S.O"
    ]
  },
  "416417": {
    "pincode": "416417",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Kavathe Piran S.O"
    ]
  },
  "416418": {
    "pincode": "416418",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Salgare S.O",
      "Kognoli B.O",
      "Kuktoli B.O"
    ]
  },
  "416419": {
    "pincode": "416419",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Shirdhon S.O",
      "Alkud (M) B.O",
      "Borgaon B.O",
      "Jaigavan B.O",
      "Landgewadi B.O",
      "Malangaon B.O"
    ]
  },
  "416420": {
    "pincode": "416420",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Soni S.O",
      "Bhose B.O",
      "Dhulgaon B.O"
    ]
  },
  "416436": {
    "pincode": "416436",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Bamnoli B.O",
      "Kupwad B.O",
      "Savali B.O.",
      "Kupwad MIDC Area S.O"
    ]
  },
  "416437": {
    "pincode": "416437",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Subhashnagar S.O"
    ]
  },
  "416501": {
    "pincode": "416501",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Dundage S.O",
      "Hanimnal B.O",
      "Hasur Champu B.O",
      "Hebbal B.O",
      "Hitni B.O",
      "Mutnal B.O",
      "Nilji B.O"
    ]
  },
  "416502": {
    "pincode": "416502",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Gadhinglaj S.O",
      "Atyal B.O",
      "Badyachiwadi B.O",
      "Bhadgaon B.O",
      "Channe Kuppi B.O",
      "Gajargaon B.O",
      "Gijawane B.O",
      "Harali BK B.O",
      "Inchnal B.O",
      "Kadgaon Gadhinglaj B.O",
      "Vadarage Majare B.O",
      "Gadhinglaj Shivaji Chowk S.O",
      "Gadhinglaj Tilakpath S.O"
    ]
  },
  "416503": {
    "pincode": "416503",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Mahagaon (Kolhapur) S.O",
      "Batkangale B.O",
      "Hebbal Jaldyal B.O",
      "Kanoli B.O",
      "Kolindre B.O",
      "Kowade B.O",
      "Lakudwadi B.O",
      "Maligre B.O",
      "Mangnoor T/F Sawantwadi B.O",
      "Mungurwadi B.O",
      "Sarambalwadi B.O",
      "Sule B.O"
    ]
  },
  "416504": {
    "pincode": "416504",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Nesari S.O",
      "Arjunwadi B.O",
      "Hadalage B.O",
      "Kanadewadi B.O",
      "Kine B.O",
      "Sambre B.O",
      "Shippur T/F Nesari B.O",
      "Watangi B.O"
    ]
  },
  "416505": {
    "pincode": "416505",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Ajara S.O",
      "Bhadvan B.O",
      "Gavase B.O",
      "Hativade B.O",
      "Kasar Kandgaon B.O",
      "Kitawade B.O",
      "Madilage B.O",
      "Pernoli B.O",
      "Sohale B.O",
      "Vite B.O",
      "Yerandol B.O"
    ]
  },
  "416506": {
    "pincode": "416506",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Halkarni S.O",
      "Basarge BK B.O",
      "Bugadikatti B.O",
      "Hasur Sasgiri B.O",
      "Kadalage B.O",
      "Kawlikatti B.O",
      "Manwad B.O",
      "Nandanwad B.O",
      "Narewadi B.O",
      "Navkud B.O",
      "Terni B.O",
      "Yenechavandi B.O"
    ]
  },
  "416507": {
    "pincode": "416507",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Basarge B.O",
      "Dholgarwadi B.O",
      "Dukkarwadi B.O",
      "Hajgoli B.O",
      "Jangamhatti B.O",
      "Kanur Khurd B.O",
      "Mangaon B.O",
      "Mhalunge Khalsa B.O",
      "Shinoli KH B.O",
      "Shivanage Lakkikatte B.O",
      "Sundi B.O",
      "Surute B.O",
      "Tudiye B.O",
      "Turkewadi B.O",
      "Karve S.O"
    ]
  },
  "416508": {
    "pincode": "416508",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Hosur B.O",
      "Kowad S.O",
      "Kalkundri B.O",
      "Kini(CHD) B.O",
      "Kudnur B.O",
      "Mhalewadi B.O",
      "Malatwadi B.O",
      "Nittur B.O",
      "Rajagoli KH B.O",
      "Teurwadi B.O"
    ]
  },
  "416509": {
    "pincode": "416509",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Chandgad S.O",
      "Adkur B.O",
      "Amroli B.O",
      "Gavase B.O",
      "Hera B.O",
      "Hindgaon B.O",
      "Ibrahimpur B.O",
      "Isapur B.O",
      "Keravade B.O",
      "Kurani Buzawade B.O",
      "Naganwadi B.O",
      "Nagave B.O",
      "Nandavade B.O",
      "Satavane B.O",
      "Umgaon B.O"
    ]
  },
  "416510": {
    "pincode": "416510",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Sawantwadi H.O"
    ]
  },
  "416511": {
    "pincode": "416511",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Banda S.O (Sindhudurg)",
      "Otavane B.O",
      "Insuli B.O",
      "CHARATHA B.O",
      "Bhalaval B.O",
      "Degave B.O",
      "Kalane B.O",
      "Kolzar B.O",
      "Kumbral B.O",
      "Morgaon B.O",
      "Netarde B.O",
      "Sarmale B.O",
      "Sasoli B.O",
      "Talkat B.O",
      "Tambuli B.O",
      "Vilvade B.O",
      "Wafoli B.O",
      "Zolambe B.O",
      "Dingane B.O"
    ]
  },
  "416512": {
    "pincode": "416512",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Dodamarg S.O",
      "Ayee B.O",
      "Ghotage B.O",
      "Kudase B.O",
      "Maneri B.O",
      "Mangeli B.O",
      "Parme B.O",
      "Pikule B.O",
      "Sateli Traf Bhedshi B.O",
      "Shirange B.O",
      "Talekhol B.O",
      "Usap B.O",
      "Vazare B.O",
      "Zarebambar Vasahat B.O"
    ]
  },
  "416513": {
    "pincode": "416513",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Aronda S.O",
      "Talavane B.O"
    ]
  },
  "416514": {
    "pincode": "416514",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Satarda S.O",
      "Aros B.O",
      "Kas B.O",
      "Kavathani B.O",
      "Nigude B.O",
      "MADURA B.O",
      "Satose B.O",
      "Sherla B.O"
    ]
  },
  "416515": {
    "pincode": "416515",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Tulas S.O"
    ]
  },
  "416516": {
    "pincode": "416516",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Vengurla S.O",
      "Nemale B.O",
      "Adeli B.O",
      "Ansur B.O",
      "Asolipal B.O",
      "Dabholi B.O",
      "Khanoli B.O",
      "Math B.O",
      "Vajarat B.O",
      "Vetore B.O",
      "Wainganiwadi B.O",
      "Vengurla Camp S.O",
      "Ubhadanda B.O"
    ]
  },
  "416517": {
    "pincode": "416517",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Redi S.O"
    ]
  },
  "416518": {
    "pincode": "416518",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Shiroda S.O (Sindhudurg)",
      "Malgaon B.O",
      "Sonurli B.O",
      "Kunkeri B.O",
      "Niravade B.O",
      "Majgaon B.O",
      "Nhaveli B.O",
      "Malewad B.O",
      "Ajgaon B.O",
      "Aravli B.O",
      "Asoli B.O",
      "Dhakore B.O",
      "Tak B.O",
      "Tiravade B.O",
      "Wadkhol B.O"
    ]
  },
  "416519": {
    "pincode": "416519",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Ambegaon B.O",
      "Mangaon S.O (Sindhudurg)",
      "Kolgaon B.O",
      "Zarap B.O",
      "Akeri B.O",
      "Salgaon B.O",
      "Dukanwad B.O",
      "Gothos B.O",
      "Kaleli B.O",
      "Mahadevache Kervade B.O",
      "Naneli B.O",
      "Narur B.O",
      "Nileli B.O",
      "Nivaje B.O",
      "Shivapur B.O",
      "Vados B.O",
      "Vasoli B.O"
    ]
  },
  "416520": {
    "pincode": "416520",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kudal S.O (Sindhudurg)",
      "Andurla B.O",
      "Bambuli B.O",
      "Bav B.O",
      "Bibavane B.O",
      "Ghavanale B.O",
      "Goveri B.O",
      "Hirlok B.O",
      "Kelus B.O",
      "Keravade B.O",
      "Khutvalwadi B.O",
      "Madyachiwadi B.O",
      "Mandkuli B.O",
      "Mulde B.O",
      "Pawashi B.O",
      "Sarambal B.O",
      "Sonavade B.O",
      "Tendoli B.O",
      "Terse Bambarde B.O",
      "Tulsuli B.O",
      "Vadivaravde B.O",
      "Vetal Bambarde B.O"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
