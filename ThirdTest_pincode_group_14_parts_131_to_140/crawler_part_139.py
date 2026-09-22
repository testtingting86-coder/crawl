"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 139 / 400
================================================================================
- Group: ThirdTest_pincode_group_14_parts_131_to_140
- Assigned PIN Codes: 48 (Range: 424106 to 425302)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_139.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_139.csv & .json
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

PART_ID = "part_139"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-139] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "424106",
  "424107",
  "424108",
  "424109",
  "424119",
  "424201",
  "424202",
  "424203",
  "424204",
  "424205",
  "424206",
  "424207",
  "424208",
  "424301",
  "424302",
  "424303",
  "424304",
  "424305",
  "424306",
  "424307",
  "424308",
  "424309",
  "424310",
  "424311",
  "424318",
  "425001",
  "425002",
  "425003",
  "425004",
  "425101",
  "425102",
  "425103",
  "425104",
  "425105",
  "425107",
  "425108",
  "425109",
  "425110",
  "425111",
  "425112",
  "425113",
  "425114",
  "425115",
  "425116",
  "425201",
  "425203",
  "425301",
  "425302"
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
  "424106": {
    "pincode": "424106",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Mehunbare S.O",
      "Bhaur B.O",
      "Dahivad B.O",
      "Dhamangaon B.O",
      "Jamda B.O",
      "Kalmadu B.O",
      "Khadki Sim B.O",
      "Kunzar B.O",
      "Londhe B.O",
      "Varkhede BK B.O"
    ]
  },
  "424107": {
    "pincode": "424107",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Khedgaon S.O (Jalgaon)",
      "Juwardi B.O",
      "Pohare B.O",
      "Daskebardi B.O",
      "Gudhe B.O",
      "Bahal B.O"
    ]
  },
  "424108": {
    "pincode": "424108",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Hirapur S.O",
      "Brahmanshevge B.O",
      "Ghodegaon B.O",
      "Hatgaon B.O",
      "Malshevge B.O",
      "Rohini B.O",
      "Talegaon B.O"
    ]
  },
  "424109": {
    "pincode": "424109",
    "circle": "Maharashtra Circle",
    "region": "Navi Mumbai Region",
    "division": "Malegaon Division",
    "offices": [
      "Naydongri S.O",
      "Chandora B.O",
      "Jalgaon KH B.O",
      "Pardhadi B.O",
      "Pimparkhed B.O"
    ]
  },
  "424119": {
    "pincode": "424119",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Shindi B.O",
      "Pimparkhed B.O",
      "C.T. Mill Road S.O",
      "Bilakhed B.O",
      "Lonje B.O",
      "Umbarkhed B.O",
      "Deoli B.O",
      "Pilkhod B.O",
      "Waghadu B.O",
      "Bhoras B.O",
      "Tarwade B.O",
      "Hatale B.O",
      "Kharjai B.O",
      "Patne B.O",
      "Talonde Digar B.O",
      "Kherde B.O",
      "Shirasgaon B.O",
      "Vadgaon Lambe B.O",
      "Vade B.O",
      "Tambole B.O",
      "Beldarwadi B.O",
      "Kargaon B.O",
      "Ozar B.O",
      "Ranjangaon B.O",
      "Sangvi B.O",
      "Saygaon B.O",
      "Takali PD B.O",
      "Upkhed B.O",
      "Khadki bk B.O",
      "Ganeshpur B.O",
      "Borkheda B.O"
    ]
  },
  "424201": {
    "pincode": "424201",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Pachora S.O",
      "Anturli Bk B.O",
      "Anturli Kh B.O",
      "Anturli B.O",
      "Bambrud Bk B.O",
      "Galan B.O",
      "Girad B.O",
      "Goradkheda B.O",
      "Kasampura B.O",
      "Khadakdeola B.O",
      "Khedgaon ( N ) B.O",
      "Lohara B.O",
      "Lohatar B.O",
      "Nimbhori B.O",
      "Pardhade B.O",
      "Sarole Bk B.O",
      "Satgaon B.O",
      "Tarkheda B.O",
      "Veruli Kh B.O",
      "Wadi B.O",
      "Vivekanand nagar Pachora S.O"
    ]
  },
  "424202": {
    "pincode": "424202",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Varkhedi S.O",
      "Kurhad B.O",
      "Lohari Bk B.O",
      "Shindad Bk B.O",
      "Vadgaon Ambe B.O"
    ]
  },
  "424203": {
    "pincode": "424203",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Pimpalgaon S.O",
      "Chinchapura B.O"
    ]
  },
  "424204": {
    "pincode": "424204",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Shendurni S.O",
      "Dondwade B.O",
      "Gondedaon B.O",
      "Kalamsare B.O",
      "Lihedigar B.O",
      "Malkheda B.O",
      "Mengaon B.O"
    ]
  },
  "424205": {
    "pincode": "424205",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Pahur S.O",
      "Paldhi Pahur B.O",
      "Wakod B.O"
    ]
  },
  "424206": {
    "pincode": "424206",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Shelgaon B.O",
      "Khadgaon B.O",
      "Palaskheda B.O",
      "Jamner S.O",
      "Garkheda B.O",
      "Gondkhed B.O",
      "Kasali B.O",
      "Khadki B.O",
      "Lahasar B.O",
      "Londhari B.O",
      "Maldabhadi B.O",
      "Moygaon B.O",
      "Moykheda B.O",
      "Nachankheda B.O",
      "Samrod B.O",
      "Shahpur B.O",
      "Takali B.O",
      "Waghari B.O"
    ]
  },
  "424207": {
    "pincode": "424207",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Loni B.O",
      "Wakadi S.O",
      "Mandve K H B.O",
      "Shengole B.O",
      "Tondapur B.O"
    ]
  },
  "424208": {
    "pincode": "424208",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Fattepur S.O",
      "Chincholi Pimpri B.O",
      "Deulgaon B.O",
      "Godri B.O",
      "Kasba Pimpri B.O",
      "Tornala B.O"
    ]
  },
  "424301": {
    "pincode": "424301",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Fagana S.O",
      "Ajang B.O",
      "Chinchkheda B.O",
      "Kalkheda B.O",
      "Mukti B.O",
      "Varkheda B.O"
    ]
  },
  "424302": {
    "pincode": "424302",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Kusumba S.O",
      "Chaugaon B.O",
      "Deur Bk B.O",
      "Kheda B.O",
      "Morane Pr Ner B.O",
      "Nandre B.O",
      "Udane B.O"
    ]
  },
  "424303": {
    "pincode": "424303",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Ner S.O",
      "Bhadane B.O",
      "Khandalai B.O",
      "Lonkhedi B.O",
      "Shirdhane B.O"
    ]
  },
  "424304": {
    "pincode": "424304",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Sakri (Dhule) S.O",
      "Ashtane B.O",
      "Balsane B.O",
      "Bhadne B.O",
      "Bodgaon B.O",
      "Chaupale B.O",
      "Chinchkhede B.O",
      "Dahivel B.O",
      "Datarti B.O",
      "Devjipada B.O",
      "Dhamnar B.O",
      "Dusane B.O",
      "Ghodade B.O",
      "Kakani B.O",
      "Kalambhir B.O",
      "Kavthe B.O",
      "Kharadbari B.O",
      "Mauje chadvel B.O",
      "Mhasdi B.O",
      "Navapada B.O",
      "Perejpur B.O",
      "Pinjarzadi B.O",
      "Rainpada B.O",
      "Rohod B.O",
      "S.F.BHADNE B.O",
      "Shewali B.O",
      "Shirsole B.O",
      "Tamasvadi B.O",
      "Vardhane B.O",
      "Vasmar B.O"
    ]
  },
  "424305": {
    "pincode": "424305",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Nizampur S.O (Dhule)",
      "Akhade B.O",
      "Amkhel B.O",
      "Bhamer B.O",
      "Brahamanvel B.O",
      "Chadvel B.O",
      "Khori B.O",
      "Khudane B.O",
      "Mhasale B.O",
      "Runmaili B.O",
      "Titane B.O",
      "Vaskhedi B.O",
      "Vehergaon B.O"
    ]
  },
  "424306": {
    "pincode": "424306",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Pimpalner S.O",
      "Balhane B.O",
      "Chikase B.O",
      "Dangshirvade B.O",
      "Degaon B.O",
      "Deshshirwade B.O",
      "Dhadne B.O",
      "Jebapur B.O",
      "Kudashi B.O",
      "Pankheda B.O",
      "Pimpalgaon B.O",
      "Samode B.O",
      "Shendwad B.O",
      "Shenpur B.O",
      "Shevge B.O",
      "Sukapur B.O",
      "Tembhe B.O",
      "Umarpatta B.O",
      "Umbharti B.O",
      "Warsa B.O"
    ]
  },
  "424307": {
    "pincode": "424307",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Kapadna S.O",
      "Boris B.O",
      "Burzad B.O",
      "Deobhane B.O",
      "Dhanur B.O",
      "Lamkani B.O",
      "Lonkheda B.O",
      "Nandane B.O",
      "Rami B.O"
    ]
  },
  "424308": {
    "pincode": "424308",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Shirud S.O",
      "Borkund B.O",
      "Dhamangaon B.O",
      "Hendrun B.O",
      "Moghan B.O",
      "Mordad B.O",
      "Nandale B.O"
    ]
  },
  "424309": {
    "pincode": "424309",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Songir S.O",
      "Gorane B.O",
      "Pimparkheda B.O",
      "Waghadi Bk B.O",
      "Walkheda B.O",
      "Waypur B.O"
    ]
  },
  "424310": {
    "pincode": "424310",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Kasara S.O (Dhule)",
      "Behed B.O",
      "Chhail B.O",
      "Dighave B.O",
      "Ganeshpur B.O",
      "Malpur B.O",
      "Nadse B.O",
      "Pratappur B.O",
      "Vitai B.O"
    ]
  },
  "424311": {
    "pincode": "424311",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Mohadi Laling S.O",
      "Borvihir B.O",
      "Junawane B.O",
      "Narvhal B.O",
      "Nimgul B.O",
      "Talvade B.O",
      "Tikhi B.O",
      "Velhane B.O",
      "Vinchur B.O"
    ]
  },
  "424318": {
    "pincode": "424318",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Dhule Division",
    "offices": [
      "Naval Nagar S.O",
      "Ambode B.O",
      "Mohadi dangri B.O",
      "Navara navari B.O",
      "Satarne B.O",
      "Shirdhane B.O",
      "Vani B.O",
      "Vishvanath B.O"
    ]
  },
  "425001": {
    "pincode": "425001",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Jalgaon H.O",
      "Jalgaon Bazar S.O",
      "Jalgaon Collectorate S.O",
      "Jalgaon Kawarnagar S.O",
      "Jalgaon Peth S.O",
      "Jalgaon Premnagar S.O",
      "Jalgaon Navi Peth S.O"
    ]
  },
  "425002": {
    "pincode": "425002",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Nandra B.O",
      "Phupnagari B.O",
      "Savkheda Bk B.O",
      "Shirsoli B.O",
      "M.J.College Jalgaon S.O",
      "Pimprala SO",
      "Amoda B.O",
      "Avhane B.O",
      "Bambhori B.O",
      "Bhokar B.O",
      "Dhamangaon B.O",
      "Idgaon B.O",
      "Kanalda B.O",
      "Kathora B.O",
      "Kinod B.O",
      "Mamurabad B.O",
      "Mohadi B.O"
    ]
  },
  "425003": {
    "pincode": "425003",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Audyogik Vasahat Jalgaon S.O",
      "Chincholi B.O",
      "Dhanwad B.O",
      "Kandari B.O",
      "Kusumbe Kh B.O",
      "Kadgaon B.O",
      "Tarsod B.O",
      "Umale B.O"
    ]
  },
  "425004": {
    "pincode": "425004",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "N M University Jalgaon S.O"
    ]
  },
  "425101": {
    "pincode": "425101",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Asoda S.O"
    ]
  },
  "425102": {
    "pincode": "425102",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Bhadli BK S.O",
      "Bholane B.O"
    ]
  },
  "425103": {
    "pincode": "425103",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Anjanvihire B.O",
      "Chandsar B.O",
      "Chorgaon B.O",
      "Dongaon B.O",
      "Kadholi B.O",
      "Kharchi B.O",
      "Pimpalkotha B.O",
      "Ravanje Bk B.O",
      "Ringangaon B.O",
      "Varad Bk B.O",
      "Zurkheda B.O",
      "Shindi B.O",
      "Paldhi S.O"
    ]
  },
  "425104": {
    "pincode": "425104",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Borkheda B.O",
      "Hingona BK B.O",
      "Satkheda B.O",
      "Sonwad B.O",
      "Pimpri Khurd S.O"
    ]
  },
  "425105": {
    "pincode": "425105",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Dharangaon S.O",
      "Bhone B.O",
      "Dhanora B.O",
      "Dheku B.O",
      "Jambhore B.O",
      "Kharde B.O",
      "Kurhe Kh B.O",
      "Mathgavan B.O",
      "Nanded B.O",
      "Pastane Bk B.O",
      "Rajwad B.O",
      "Rotwad B.O",
      "Sakre B.O",
      "Salve B.O",
      "Sarbete B.O",
      "Savkheda B.O",
      "Shelave B.O",
      "Takarkheda B.O"
    ]
  },
  "425107": {
    "pincode": "425107",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Adgaon B.O",
      "Mangrul B.O",
      "Chopda S.O",
      "Bhokari B.O",
      "Chahardi B.O",
      "Chunchale B.O",
      "Ghadvel B.O",
      "Gorgavale B.O",
      "Hated B.O",
      "Khachane B.O",
      "Kolambe B.O",
      "Kurvel B.O",
      "Lasur B.O",
      "Mamalde B.O",
      "Melane B.O",
      "Nagalwadi B.O",
      "Nimgavhan B.O",
      "Sanpule B.O",
      "Satrasen B.O",
      "Tawase B.O",
      "Umarti B.O",
      "Vadti B.O",
      "Vaijpur Settelement B.O",
      "Vardi B.O",
      "Vele B.O",
      "Virwade B.O",
      "Vishnapur B.O"
    ]
  },
  "425108": {
    "pincode": "425108",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Akulkheda S.O",
      "Anwarde K H B.O",
      "Ganpur B.O",
      "Ghodgaon B.O",
      "Kazipura B.O",
      "Mauje Hingona B.O",
      "Vadhode B.O",
      "Velode B.O",
      "Walaki B.O"
    ]
  },
  "425109": {
    "pincode": "425109",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Erandol S.O",
      "Bhatkheda B.O",
      "Bhavarkheda B.O",
      "Borgaon BK B.O",
      "Hanamantkheda B.O",
      "Khadke Bk B.O",
      "Khadke Kh B.O",
      "Savkheda Hol B.O",
      "Toli Kh B.O",
      "Utran B.O",
      "Vikharan B.O",
      "Padmalay B.O"
    ]
  },
  "425110": {
    "pincode": "425110",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Kasoda S.O",
      "Adgaon B.O",
      "Farkande B.O",
      "Nipane B.O",
      "Talai B.O",
      "Vankothe B.O"
    ]
  },
  "425111": {
    "pincode": "425111",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Parola SO",
      "Adgaon B.O",
      "Bahute B.O",
      "Bole B.O",
      "Chorwad B.O",
      "Dholi B.O",
      "Dhul Pimpri B.O",
      "Hol Pimpri B.O",
      "Karmad B.O",
      "Mangrul B.O",
      "Mhasave B.O",
      "Mondhale B.O",
      "Mundane B.O",
      "Palaskheda Sim B.O",
      "Sadavan B.O",
      "Sarve B.O",
      "Shirasmani B.O",
      "Shivare Digar B.O",
      "Tamaswadi B.O",
      "Tehu B.O",
      "Titvi B.O",
      "Undirkhede B.O"
    ]
  },
  "425112": {
    "pincode": "425112",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Deogaon S.O (Jalgaon)",
      "Toli B.O"
    ]
  },
  "425113": {
    "pincode": "425113",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Bhilali B.O",
      "Bholane B.O",
      "Bahadarpur S.O (Jalgaon)",
      "Jirali B.O",
      "Shevage B.O"
    ]
  },
  "425114": {
    "pincode": "425114",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Neri BK S.O",
      "Devpimpri B.O",
      "Hiwarkheda B.O",
      "Kekat Nimbhora B.O",
      "Mohadi B.O",
      "Palaskheda Mirache B.O",
      "Rotwad B.O",
      "Sunasgaon B.O"
    ]
  },
  "425115": {
    "pincode": "425115",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Asankheda B.O",
      "Bambrud  ( R ) B.O",
      "Kurangi B.O",
      "Maheji B.O",
      "Pahan B.O",
      "Pathari B.O",
      "Samner B.O",
      "Vadli B.O",
      "Varad Kh B.O",
      "Nandra Pachora S.O"
    ]
  },
  "425116": {
    "pincode": "425116",
    "circle": "Maharashtra circle",
    "region": "Aurangabad Region",
    "division": "Jalgaon Division",
    "offices": [
      "Bornar B.O",
      "Dahigaon B.O",
      "Jalke B.O",
      "Vitner B.O",
      "Vavadade B.O",
      "Mhasawad S.O (Jalgaon)"
    ]
  },
  "425201": {
    "pincode": "425201",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Bhusawal H.O",
      "Sakegaon B.O",
      "Bhusawal Athawade Bazar S.O",
      "Bhusawal Kutchery S.O",
      "Bhusawal Shivaji Nagar S.O"
    ]
  },
  "425203": {
    "pincode": "425203",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Bhusawal Ordnance Factory S.O",
      "Duskheda B.O",
      "Kandari B.O",
      "Raipur B.O"
    ]
  },
  "425301": {
    "pincode": "425301",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Yawal S.O",
      "Atraval B.O",
      "Dahigaon B.O",
      "Kathora B.O",
      "Korpavali B.O",
      "Moharala B.O",
      "Satod Kolwad B.O",
      "Vadri B.O",
      "Viravali K H B.O"
    ]
  },
  "425302": {
    "pincode": "425302",
    "circle": "Maharashtra Circle",
    "region": "Aurangabad Region",
    "division": "Bhusaval Division",
    "offices": [
      "Sakali S.O",
      "Naigaon B.O",
      "Adgaon B.O",
      "Dambhurni B.O",
      "Dongaon B.O",
      "Kingaon B.O",
      "Manvel B.O",
      "Naygaon B.O",
      "Waghzira B.O",
      "Chunchale B.O"
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
