"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 135 / 400
================================================================================
- Group: ThirdTest_pincode_group_14_parts_131_to_140
- Assigned PIN Codes: 48 (Range: 416521 to 416804)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_135.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_135.csv & .json
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

PART_ID = "part_135"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-135] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "416521",
  "416522",
  "416523",
  "416524",
  "416525",
  "416526",
  "416527",
  "416528",
  "416529",
  "416531",
  "416534",
  "416549",
  "416550",
  "416551",
  "416552",
  "416601",
  "416602",
  "416603",
  "416604",
  "416605",
  "416606",
  "416608",
  "416609",
  "416610",
  "416611",
  "416612",
  "416613",
  "416614",
  "416615",
  "416616",
  "416620",
  "416623",
  "416626",
  "416628",
  "416630",
  "416632",
  "416701",
  "416702",
  "416703",
  "416704",
  "416705",
  "416707",
  "416709",
  "416712",
  "416713",
  "416801",
  "416803",
  "416804"
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
  "416521": {
    "pincode": "416521",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kadaval S.O",
      "Avalegaon B.O",
      "Bhadgaon Budruk B.O",
      "Digas B.O",
      "Nirukhe B.O",
      "Pangrad B.O",
      "Varde B.O"
    ]
  },
  "416522": {
    "pincode": "416522",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Pat S.O",
      "Khavane B.O",
      "Kochara B.O",
      "Mhapan B.O",
      "Niwatiwadi B.O"
    ]
  },
  "416523": {
    "pincode": "416523",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Parula S.O",
      "Bhogave B.O",
      "Karliwadi B.O",
      "Terawalewadi B.O"
    ]
  },
  "416524": {
    "pincode": "416524",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Walaval S.O",
      "Chendavan B.O",
      "Kavathi B.O"
    ]
  },
  "416525": {
    "pincode": "416525",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Deulwada Nerur S.O",
      "Nerur B.O"
    ]
  },
  "416526": {
    "pincode": "416526",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Ainapur B.O",
      "Hiralge B.O",
      "Karambali B.O",
      "Ningudage B.O",
      "Koulage S.O"
    ]
  },
  "416527": {
    "pincode": "416527",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Tillarinagar S.O"
    ]
  },
  "416528": {
    "pincode": "416528",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Industrial Area Pinguli S.O",
      "Pinguli B.O"
    ]
  },
  "416529": {
    "pincode": "416529",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Talavade S.O",
      "Hodavade B.O",
      "Matond B.O",
      "Pendhryachiwadi B.O"
    ]
  },
  "416531": {
    "pincode": "416531",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kalmist S.O",
      "Madkhol B.O",
      "Karivade B.O",
      "Sangeli B.O",
      "Shirshinge B.O",
      "Verle B.O"
    ]
  },
  "416534": {
    "pincode": "416534",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Sukalwadi S.O",
      "Gavrai B.O",
      "Padve B.O",
      "Talgaon B.O"
    ]
  },
  "416549": {
    "pincode": "416549",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Konalkatta Colony S.O",
      "Ghotagewadi B.O",
      "Ker B.O",
      "Konal B.O",
      "Tervan Medhe B.O"
    ]
  },
  "416550": {
    "pincode": "416550",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Amboli B.O",
      "Kudal Audyogik Samuha S.O",
      "Danoli B.O",
      "Devsu B.O",
      "Choukul B.O"
    ]
  },
  "416551": {
    "pincode": "416551",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Nool S.O",
      "Jarali B.O",
      "Khandal B.O",
      "Mugali B.O",
      "Nangnoor B.O"
    ]
  },
  "416552": {
    "pincode": "416552",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "DSF Halkarni S.O",
      "Ambewadi B.O",
      "Date B.O",
      "Halkarni (Chandgad) B.O",
      "Patne B.O"
    ]
  },
  "416601": {
    "pincode": "416601",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Phondaghat S.O",
      "Belnekhurd B.O",
      "Damre B.O",
      "Ghonsari B.O",
      "Harkul Khurd B.O",
      "Karul B.O",
      "Kurli B.O",
      "Lore i B.O",
      "Lore II B.O",
      "Tondavali B.O",
      "Wagheri B.O"
    ]
  },
  "416602": {
    "pincode": "416602",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kankavli S.O",
      "Asrondi B.O",
      "Aynal B.O",
      "Bharni B.O",
      "Bidwadi B.O",
      "Gopuri B.O",
      "Halval B.O",
      "Harkul Budruk B.O",
      "Humbrat B.O",
      "Janavali B.O",
      "Karanje B.O",
      "Kasaral B.O",
      "Kasavan B.O",
      "Main B.O",
      "Nagve B.O",
      "Nandgaon B.O",
      "Otav B.O",
      "Sakedi B.O",
      "Savdav B.O",
      "Shirvande B.O",
      "Tarandale B.O",
      "Kalmath B.O",
      "Varavade B.O"
    ]
  },
  "416603": {
    "pincode": "416603",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kasal S.O",
      "Bordave B.O",
      "Hedul B.O",
      "Hiwale B.O",
      "Khotale B.O",
      "Osargaon B.O",
      "Ovaliye B.O",
      "Waingavde B.O"
    ]
  },
  "416604": {
    "pincode": "416604",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Katta S.O",
      "Chafekhol B.O",
      "Golvan B.O",
      "Kunkavale B.O",
      "Mogarne B.O",
      "Nandos B.O",
      "Pendur B.O",
      "Tiravade B.O",
      "Varad B.O"
    ]
  },
  "416605": {
    "pincode": "416605",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Tarkarli B.O",
      "Kumbharmath B.O",
      "Devli B.O",
      "Chouke S.O",
      "Devbag B.O",
      "Wairy Bhutnath B.O",
      "Amberi B.O",
      "Dhamapur B.O",
      "Kalse B.O",
      "Nandrukh B.O",
      "Salel B.O"
    ]
  },
  "416606": {
    "pincode": "416606",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Malvan H.O"
    ]
  },
  "416608": {
    "pincode": "416608",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kandalgaon B.O",
      "Khaida B.O",
      "Bilwas B.o",
      "Masura S.O",
      "Malgaon B.O",
      "Bandivade B.O",
      "Deulwada B.O",
      "Malond B.O",
      "Masde B.O",
      "Poip B.O",
      "Veral B.O"
    ]
  },
  "416609": {
    "pincode": "416609",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Sangve S.O",
      "Bhirvande B.O",
      "Dariste B.O",
      "Digavle B.O",
      "Kumbhavade B.O",
      "Nardave B.O",
      "Natal B.O"
    ]
  },
  "416610": {
    "pincode": "416610",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Shirgaon S.O",
      "Asalda B.O",
      "Hadpid B.O",
      "Koloshi B.O",
      "Kuvale B.O",
      "Salshi B.O"
    ]
  },
  "416611": {
    "pincode": "416611",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Talebazar S.O",
      "Are B.O",
      "Gadhitamhane B.O",
      "Khudi B.O",
      "Kinjavade B.O",
      "Kotkamte B.O",
      "Talavade B.O",
      "Torsole B.O",
      "Valivande B.O"
    ]
  },
  "416612": {
    "pincode": "416612",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Jamsande S.O",
      "Dabhole B.O",
      "Ilye B.O",
      "Katta B.O",
      "Kunkeshwar B.O",
      "Mithmumbari B.O",
      "Tembavli B.O"
    ]
  },
  "416613": {
    "pincode": "416613",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Devgad S.O",
      "Baparde B.O",
      "Taramumbari B.O",
      "Waniwade B.O"
    ]
  },
  "416614": {
    "pincode": "416614",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Achara S.O",
      "Adavli B.O",
      "Chindar B.O",
      "Chindar Bazar B.O",
      "Gaudwadi B.O",
      "Hirlewadi B.O",
      "Palsamb B.O",
      "Pirawadi B.O",
      "Trimbak B.O",
      "Waingani B.O"
    ]
  },
  "416615": {
    "pincode": "416615",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Mithbav S.O",
      "Tambaldeg B.O"
    ]
  },
  "416616": {
    "pincode": "416616",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Ramgad S.O",
      "Budhavale B.O",
      "Gothane B.O",
      "Kirlos B.O",
      "Math Budruk B.O",
      "Nirom B.O",
      "Rathivade B.O",
      "Sandave B.O",
      "Shravan B.O"
    ]
  },
  "416620": {
    "pincode": "416620",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kalsuli S.O",
      "Shirval B.O",
      "Shivdav B.O"
    ]
  },
  "416623": {
    "pincode": "416623",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Mond S.O"
    ]
  },
  "416626": {
    "pincode": "416626",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kolamb B.O",
      "Revandi B.O",
      "Sarjekot Miryabanda B.O",
      "Hadi S.O",
      "Tondavali B.O"
    ]
  },
  "416628": {
    "pincode": "416628",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Ambrad S.O",
      "Kunde B.O",
      "Pokharan B.O"
    ]
  },
  "416630": {
    "pincode": "416630",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Naringre S.O",
      "Dahibav B.O",
      "Hindale B.O",
      "Munge B.O",
      "Poyare B.O"
    ]
  },
  "416632": {
    "pincode": "416632",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Jambhavade S.O",
      "Bharni B.O",
      "Ghotage B.O",
      "Kupavade B.O",
      "Sonavade B.O"
    ]
  },
  "416701": {
    "pincode": "416701",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Lanja S.O",
      "Bapere B.O",
      "Bhambed B.O",
      "Golawashi B.O",
      "Govil B.O",
      "Hardakhale B.O",
      "Indavati B.O",
      "Isavali B.O",
      "Javade B.O",
      "Kelambe B.O",
      "Khavadi B.O",
      "Khorninko B.O",
      "Kondye B.O",
      "Korle B.O",
      "Kuve B.O",
      "Majal B.O",
      "Palu B.O",
      "Prabhanvalli B.O",
      "Roon B.O",
      "Satavali B.O",
      "Vadgaon B.O",
      "Vangule B.O",
      "Veravali Budruk B.O",
      "Veravali Khurd B.O",
      "Zapade B.O"
    ]
  },
  "416702": {
    "pincode": "416702",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Rajapur S.O (Ratnagiri)",
      "Angale B.O",
      "Ansure B.O",
      "Bhalavali B.O",
      "Burambe Wadi B.O",
      "Devache Gothane B.O",
      "Dhaulwali B.O",
      "Dhopeshwar B.O",
      "Dongar B.O",
      "Donivade B.O",
      "Goval B.O",
      "Hasol Tarf Saundal B.O",
      "Juwathi B.O",
      "Kaneri B.O",
      "Kelavali B.O",
      "Kodavali B.O",
      "Kondye B.O",
      "Kotapur B.O",
      "Kumbhavade B.O",
      "Kuveshi B.O",
      "Madban B.O",
      "Mithgavane B.O",
      "Moroshi B.O",
      "Mosam B.O",
      "Nanar B.O",
      "Niveli B.O",
      "Padave B.O",
      "Pangare B.O",
      "Panhale Tarf Saundal B.O",
      "Prindavan B.O",
      "Sakhar B.O",
      "Sasale B.O",
      "Shivane Khurd B.O",
      "Solgaon B.O",
      "Talgaon B.O",
      "Taral B.O",
      "Upale B.O",
      "Vilye B.O",
      "Walye B.O"
    ]
  },
  "416703": {
    "pincode": "416703",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Kharepatan S.O",
      "Korla B.O",
      "Kunkavan B.O",
      "Kurangavane B.O",
      "Nadgive B.O",
      "Nanivade B.O",
      "Tithavli B.O"
    ]
  },
  "416704": {
    "pincode": "416704",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Pachal S.O (Ratnagiri)",
      "Ajiwali B.O",
      "Bagave Wadi B.O",
      "Haral B.O",
      "Hatade B.O",
      "Jawalethar B.O",
      "Kajirda B.O",
      "Karak B.O",
      "Karavali B.O",
      "Kolamb B.O",
      "Kolwan Khadi B.O",
      "Miland B.O",
      "Moor B.O",
      "Oshivale B.O",
      "Parule B.O",
      "Raypatan B.O",
      "Saundal B.O",
      "Savdav B.O",
      "Soliwade B.O",
      "Talavade B.O",
      "Tamhane B.O",
      "Yelvan B.O",
      "Yerdav B.O"
    ]
  },
  "416705": {
    "pincode": "416705",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Oni S.O",
      "Gothane Doniwade B.O",
      "Kalsavali B.O",
      "Kharvate B.O",
      "Kolwan Chuna B.O",
      "Ozare B.O",
      "Tiware B.O",
      "Wadavali B.O"
    ]
  },
  "416707": {
    "pincode": "416707",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Adivare S.O",
      "Kasheli B.O",
      "Kondsur Budruk B.O",
      "Rajwadi B.O"
    ]
  },
  "416709": {
    "pincode": "416709",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Sagave S.O",
      "Katali Sagave B.O"
    ]
  },
  "416712": {
    "pincode": "416712",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Devadhe S.O",
      "Asage B.O",
      "Beni Budruk B.O",
      "Bhade B.O",
      "Gavane B.O",
      "Harche B.O",
      "Harcheri B.O",
      "Ibrahimpattan B.O",
      "Khanavali B.O",
      "Koldhe B.O",
      "Kot B.O",
      "Kurne B.O",
      "Kurtade B.O",
      "Lavgan B.O",
      "Punas B.O",
      "Talavade B.O",
      "Tike B.O",
      "Upale B.O",
      "Veral B.O",
      "Waghrat B.O"
    ]
  },
  "416713": {
    "pincode": "416713",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "RATNAGIRI DIVISION",
    "offices": [
      "Bhoo S.O",
      "Dasur B.O",
      "Devihasol B.O",
      "Pendkhale B.O",
      "Shivane Budruk B.O",
      "Vadad Hasol B.O"
    ]
  },
  "416801": {
    "pincode": "416801",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Talera S.O (Sindhudurg)",
      "Gavane B.O",
      "Kasarde B.O",
      "Nad B.O",
      "Ozaram B.O",
      "Shidavane B.O",
      "Waghivare B.O",
      "Wargaon B.O"
    ]
  },
  "416803": {
    "pincode": "416803",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Mutat S.O"
    ]
  },
  "416804": {
    "pincode": "416804",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SINDHUDURG DIVISION",
    "offices": [
      "Padel S.O",
      "Soundale B.O",
      "Thakurwadi B.O",
      "Waghotan B.O"
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
