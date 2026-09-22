"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 133 / 400
================================================================================
- Group: ThirdTest_pincode_group_14_parts_131_to_140
- Assigned PIN Codes: 48 (Range: 416116 to 416310)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_133.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_133.csv & .json
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

PART_ID = "part_133"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-133] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "416116",
  "416118",
  "416119",
  "416120",
  "416121",
  "416122",
  "416138",
  "416143",
  "416144",
  "416146",
  "416201",
  "416202",
  "416203",
  "416204",
  "416205",
  "416206",
  "416207",
  "416208",
  "416209",
  "416210",
  "416211",
  "416212",
  "416213",
  "416214",
  "416215",
  "416216",
  "416218",
  "416219",
  "416220",
  "416221",
  "416223",
  "416229",
  "416230",
  "416231",
  "416232",
  "416234",
  "416235",
  "416236",
  "416301",
  "416302",
  "416303",
  "416304",
  "416305",
  "416306",
  "416307",
  "416308",
  "416309",
  "416310"
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
  "416116": {
    "pincode": "416116",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Ganganagar Ichalkaranji S.O"
    ]
  },
  "416118": {
    "pincode": "416118",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Rukadi S.O",
      "Atigre B.O",
      "Chokak B.O",
      "Male B.O",
      "Mangaon B.O"
    ]
  },
  "416119": {
    "pincode": "416119",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Gandhinagar S.O (Kolhapur)",
      "Chinchwad B.O",
      "Gadmudshingi B.O",
      "Valivade B.O"
    ]
  },
  "416120": {
    "pincode": "416120",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Dattanagar S.O",
      "Arjunwad B.O",
      "Chinchwad B.O"
    ]
  },
  "416121": {
    "pincode": "416121",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "R K Nagar Ichalkaranji S.O",
      "Jambhali B.O",
      "Shahapur B.O",
      "Shirdhon B.O",
      "Takawade B.O",
      "Tardal B.O"
    ]
  },
  "416122": {
    "pincode": "416122",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "MIDC Shiroli S.O",
      "Jatharwadi B.O",
      "Nagaon B.O",
      "Shiye B.O",
      "Tasgaon B.O",
      "Top B.O",
      "P. Shiroli B.O"
    ]
  },
  "416138": {
    "pincode": "416138",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Chandur B.O",
      "Rui B.O",
      "Sajani B.O",
      "Tilwani B.O",
      "Kabnur SO"
    ]
  },
  "416143": {
    "pincode": "416143",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "ICO Spinning Mills S.O",
      "Lat B.O",
      "Rangoli B.O",
      "Shiradwad B.O"
    ]
  },
  "416144": {
    "pincode": "416144",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Akiwate Industrial Estate S.O"
    ]
  },
  "416146": {
    "pincode": "416146",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Parvati Industrial Estate S.O",
      "Yadrav B.O"
    ]
  },
  "416201": {
    "pincode": "416201",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Panhala S.O",
      "Ambavade B.O",
      "Rakshi B.O"
    ]
  },
  "416202": {
    "pincode": "416202",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Pattankodoli S.O",
      "Halsawade B.O",
      "Ingali B.O",
      "Sangavade B.O",
      "Vasagade B.O"
    ]
  },
  "416203": {
    "pincode": "416203",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Hupari S.O",
      "Randivewadi B.O",
      "Rendal B.O"
    ]
  },
  "416204": {
    "pincode": "416204",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Sangrul B.O",
      "Wakare B.O",
      "Kuditre S.O",
      "Amshi Dumala B.O",
      "Bhamate B.O",
      "Khatangale B.O",
      "Koparde B.O"
    ]
  },
  "416205": {
    "pincode": "416205",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Kale S.O (Kolhapur)",
      "Bajar Bhogaon B.O",
      "Dhundwade B.O",
      "Karanjphen B.O",
      "Kasba Thane B.O",
      "Kate Bhogaon B.O",
      "Khupire B.O",
      "Kisrul B.O",
      "Kolik B.O",
      "Majgaon B.O",
      "Malapude B.O",
      "Mhasurli B.O",
      "Padal B.O",
      "Panutre B.O",
      "Parkhandale B.O",
      "Pat Panhala B.O",
      "Pisatri B.O",
      "Pohale T/F Borgaon B.O",
      "Punal B.O",
      "Sule B.O",
      "Vetawade B.O",
      "Yavluj B.O"
    ]
  },
  "416206": {
    "pincode": "416206",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Gaganbavada S.O",
      "Asandoli B.O",
      "Aslaj B.O",
      "Borbet B.O",
      "Gariwade B.O",
      "Katali B.O",
      "Mandukali B.O",
      "Salvan B.O",
      "Sangashi B.O",
      "Shenawade B.O",
      "Tisangi B.O"
    ]
  },
  "416207": {
    "pincode": "416207",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Ispurli S.O",
      "Chuye B.O",
      "Dindnerli B.O",
      "Kavane B.O",
      "Mhalunge B.O",
      "Nandgaon B.O",
      "Nigave Khalasa B.O",
      "DARYACHE VADGAON"
    ]
  },
  "416208": {
    "pincode": "416208",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Bidri S.O",
      "Arjunwada B.O",
      "Baradwadi B.O",
      "Boravade B.O",
      "Mudhal B.O",
      "Talashi B.O",
      "Thikpurli B.O",
      "Titave B.O",
      "Turambe B.O",
      "Undarwadi B.O"
    ]
  },
  "416209": {
    "pincode": "416209",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Gargoti S.O",
      "Barve B.O",
      "Gangapur B.O",
      "Kalnakwadi B.O",
      "Karadwadi B.O",
      "Koor B.O",
      "Madilage BK B.O",
      "Manawale B.O",
      "Mangnoor B.O",
      "Nadhavade B.O",
      "Pal B.O",
      "Palshivane B.O",
      "Phanaswadi B.O",
      "Pimpalgaon B.O",
      "Shengaon B.O",
      "Sonarwadi B.O",
      "Tikkewadi B.O",
      "Waghapur B.O"
    ]
  },
  "416210": {
    "pincode": "416210",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Kadgaon Gargoti S.O",
      "Adoli B.O",
      "Anap Khurd B.O",
      "Kondoshi B.O",
      "Mathgaon B.O",
      "Mhasrang B.O",
      "Navale B.O",
      "Patgaon B.O",
      "Sheloli B.O",
      "Shivdav B.O",
      "Tambale B.O",
      "Tambyachiwadi B.O",
      "Vengrul B.O",
      "Vesarde B.O"
    ]
  },
  "416211": {
    "pincode": "416211",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Parite S.O",
      "Awali BK B.O",
      "Awali KH B.O",
      "Dhamod B.O",
      "Durgamanwad B.O",
      "Gudal B.O",
      "Kaulav B.O",
      "Kodavade B.O",
      "Piral B.O",
      "Pungaon B.O",
      "Rashivade B.O",
      "Shirgaon B.O",
      "Shirse B.O",
      "Tarale B.O",
      "Tarsambale B.O"
    ]
  },
  "416212": {
    "pincode": "416212",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Radhanagari S.O",
      "Akanur B.O",
      "Dajipur B.O",
      "Kasarwada B.O",
      "Nartavade B.O",
      "Padali B.O",
      "Phejiwade B.O",
      "Rhe Works B.O",
      "Sarawade B.O",
      "Savarde Patankar B.O",
      "Shiroli (Sonyachi) B.O",
      "Solankur B.O"
    ]
  },
  "416213": {
    "pincode": "416213",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Bambavada S.O",
      "Awali B.O",
      "Bhadale B.O",
      "Borivade B.O",
      "Borpadale B.O",
      "Charan B.O",
      "Dewale B.O",
      "Gogave B.O",
      "Parkhandale B.O",
      "Pishavi B.O",
      "Salshi B.O",
      "Save B.O",
      "Shittur Turf Malkapur B.O",
      "Supatre B.O",
      "Thergaon B.O"
    ]
  },
  "416214": {
    "pincode": "416214",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Sarud S.O",
      "Akurle B.O",
      "Bhedasgaon B.O",
      "Kapshi B.O",
      "Kotoli (Warna) B.O",
      "Rethare B.O",
      "Shimpe B.O",
      "Shirale T/Ff Warun B.O",
      "Shittur T/F Warun B.O",
      "Sondoli B.O",
      "Ukhalu B.O",
      "Virale B.O"
    ]
  },
  "416215": {
    "pincode": "416215",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Shahuwadi S.O",
      "Ambarde B.O",
      "Arul B.O",
      "Molawade B.O",
      "Panundre B.O",
      "Shirala B.O",
      "Tekoli B.O",
      "Ukoli B.O"
    ]
  },
  "416216": {
    "pincode": "416216",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Kenavade B.O",
      "Lingnur Dumala B.O",
      "Mhakave B.O",
      "Mouje Sangaon B.O",
      "Pimpalgaon KH B.O",
      "Sulkud B.O",
      "Vandur B.O",
      "Vhannur B.O",
      "Kagal S.O",
      "Gorambe B.O",
      "Karnur B.O",
      "Kasaba Sangaon B.O"
    ]
  },
  "416218": {
    "pincode": "416218",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Baleghol B.O",
      "Belewadi Kalamma B.O",
      "Bolavi B.O",
      "Hasur Khurd B.O",
      "Kardyal B.O",
      "Kasari B.O",
      "Madyal B.O",
      "Mugali B.O",
      "Tamnakwada B.O",
      "Vadgaon B.O",
      "Kapshi S.O"
    ]
  },
  "416219": {
    "pincode": "416219",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Murgud S.O",
      "Karanjivane B.O",
      "Malage BK B.O",
      "Mouje Haladi B.O",
      "Nidhori B.O",
      "Savarde BK B.O",
      "Sonali B.O",
      "Yamage B.O"
    ]
  },
  "416220": {
    "pincode": "416220",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Uttur S.O",
      "Aralgundi B.O",
      "Ardal B.O",
      "Bahirewadi B.O",
      "Chimane B.O",
      "Dhamane B.O",
      "Halewadi B.O",
      "Honyali B.O",
      "Mahagond B.O",
      "Masewadi B.O",
      "Mumewadi B.O",
      "Shipur T/F Ajara B.O",
      "Zulpewadi B.O"
    ]
  },
  "416221": {
    "pincode": "416221",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Walwa Bk S.O",
      "Bachani B.O",
      "Belawale BK B.O",
      "Khebavade B.O",
      "Sake B.O",
      "Vhanali B.O",
      "Walwa KH B.O"
    ]
  },
  "416223": {
    "pincode": "416223",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Mhasve S.O",
      "Bhativade B.O",
      "Hedawade B.O",
      "Kolwan B.O",
      "Minche BK B.O",
      "Minche KH B.O",
      "Nilpan B.O"
    ]
  },
  "416229": {
    "pincode": "416229",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Ambewadi S.O (Kolhapur)",
      "Bhuye B.O",
      "Chikhali B.O",
      "Kerle B.O",
      "Kerli B.O",
      "Nigave Dumala B.O",
      "Pohale Turf Alate B.O",
      "Porle Turf Thane B.O",
      "Vadange B.O",
      "Varange Padali B.O",
      "Wadi Ratnagiri B.O"
    ]
  },
  "416230": {
    "pincode": "416230",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Kotoli S.O",
      "Bandiwade B.O",
      "Borgaon B.O",
      "Ghotavade B.O",
      "Kaneri (Panhala) B.O",
      "Karanjphen B.O",
      "Kololi B.O",
      "Nandgaon B.O",
      "Tirpan B.O",
      "Waghave B.O"
    ]
  },
  "416231": {
    "pincode": "416231",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Dudhganganagar S.O",
      "Panori B.O"
    ]
  },
  "416232": {
    "pincode": "416232",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Sidhnerli S.O",
      "Bamani B.O",
      "Ekondi B.O",
      "Shankarwadi B.O",
      "Shendur B.O"
    ]
  },
  "416234": {
    "pincode": "416234",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "MIDC Gokul Shirgaon S.O",
      "Kaneri B.O",
      "Nerli B.O"
    ]
  },
  "416235": {
    "pincode": "416235",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "Sadashivnagar S.O",
      "Anur B.O",
      "Arjuni B.O",
      "Banage B.O",
      "Bastavade B.O",
      "Chikhali B.O",
      "Khadakewada B.O",
      "Koulage B.O",
      "Pimpalgaon BK B.O",
      "Sonage B.O",
      "Surupali B.O",
      "Hamidwada B.O"
    ]
  },
  "416236": {
    "pincode": "416236",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "Kolhapur Division",
    "offices": [
      "5 Star MIDC Kagal S.O",
      "Talandage B.O",
      "Yalgud B.O"
    ]
  },
  "416301": {
    "pincode": "416301",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Ashte S.O",
      "Bavachi B.O",
      "Gotkhindi B.O",
      "Karandwadi B.O",
      "Mardwadi B.O",
      "Mirajwadi B.O",
      "Pokharni B.O",
      "Tung B.O"
    ]
  },
  "416302": {
    "pincode": "416302",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Bagani S.O",
      "Bhadkimbe B.O",
      "Dhavali B.O",
      "Koregaon B.O",
      "Nagaon B.O",
      "Shigaon B.O"
    ]
  },
  "416303": {
    "pincode": "416303",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Bhilavadi S.O",
      "Audumbar B.O",
      "Bhilwadi R.S. B.O",
      "Tavadarwadi B.O"
    ]
  },
  "416304": {
    "pincode": "416304",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Bisur B.O",
      "Kavaji Khotwadi B.O",
      "Budhgaon S.O"
    ]
  },
  "416305": {
    "pincode": "416305",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Digraj Kasaba Agri School B.O",
      "Digraj Kasaba S.O"
    ]
  },
  "416306": {
    "pincode": "416306",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Kanchanpur B.O",
      "Kumatha B.O",
      "Upalavi B.O",
      "Kavalapur S.O"
    ]
  },
  "416307": {
    "pincode": "416307",
    "circle": "Maharashtra Circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Kavathe Ekand S.O"
    ]
  },
  "416308": {
    "pincode": "416308",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Amnapur BO",
      "Burli B.O",
      "Dudhondi B.O",
      "Nagarale B.O",
      "Punadi T/F Walwa B.O",
      "Kirloskarwadi S.O",
      "Ramanandnagar B.O"
    ]
  },
  "416309": {
    "pincode": "416309",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Balavadi(Bhalavani) B.O",
      "Ghogaon B.O",
      "Kundal S.O (Sangli)"
    ]
  },
  "416310": {
    "pincode": "416310",
    "circle": "Maharashtra circle",
    "region": "Goa-Panaji Region",
    "division": "SANGLI DIVISION",
    "offices": [
      "Andhali B.O",
      "Bambavade B.O",
      "Morale B.O",
      "Rajapur B.O",
      "Palus S.O",
      "Sawantpur B.O"
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
